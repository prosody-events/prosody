//! The Cassandra peer directory.

use super::{DirectAddress, Endpoint, NetworkId, PeerDirectory, PeerRegistration, RegistrationTtl};
use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraStore, TABLE_PEER_DIRECTORY};
use crate::cassandra_queries;
use crate::peer::router::{Host, PeerId};
use fixedstr::Flexstr;
use scylla::statement::Consistency;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{instrument, warn};
use uuid::Uuid;

cassandra_queries! {
    /// Prepared statements of the peer directory. Each one is an unconditional
    /// write or a single-partition point read: no lightweight transaction, no
    /// filtering, and no client-supplied write timestamp.
    pub(crate) struct DirectoryQueries {
        /// Writes every column of one peer's row under one lease.
        register: (
            "INSERT INTO $keyspace.{} (peer_id, direct_socket_address, advertised_connect, network, \
             hostname) VALUES (?, ?, ?, ?, ?) USING TTL ?",
            TABLE_PEER_DIRECTORY
        ),

        /// Point-reads one peer's row.
        read: (
            "SELECT direct_socket_address, advertised_connect, network, hostname \
             FROM $keyspace.{} WHERE peer_id = ?",
            TABLE_PEER_DIRECTORY
        ),

        /// Removes one peer's row on a clean shutdown.
        remove: (
            "DELETE FROM $keyspace.{} WHERE peer_id = ?",
            TABLE_PEER_DIRECTORY
        ),
    }
}

/// The directory row as the driver hands it over: every column nullable.
type DirectoryColumns = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// The read-write handle on the Cassandra peer directory.
///
/// Every statement runs at `LOCAL_ONE`. A registration is idempotent and
/// rewritten every interval, so a write that reaches one replica and is then
/// lost heals on the next refresh, well inside the lease. A quorum would make
/// every cache miss on the response path wait for a second replica and would
/// buy nothing on top of a row that is rewritten every interval.
#[derive(Clone, Debug)]
pub(crate) struct CassandraPeerDirectory {
    store: CassandraStore,
    queries: Arc<DirectoryQueries>,
    ttl: RegistrationTtl,
}

impl CassandraPeerDirectory {
    /// Prepares the directory's statements against `store` and fixes the lease
    /// every write publishes.
    ///
    /// # Errors
    ///
    /// Returns the driver's error when a statement cannot be prepared.
    pub(crate) async fn new(
        store: CassandraStore,
        ttl: RegistrationTtl,
    ) -> Result<Self, CassandraStoreError> {
        let mut queries = DirectoryQueries::new(store.session(), store.keyspace()).await?;
        for statement in [
            &mut queries.register,
            &mut queries.read,
            &mut queries.remove,
        ] {
            statement.set_consistency(Consistency::LocalOne);
        }
        Ok(Self {
            store,
            queries: Arc::new(queries),
            ttl,
        })
    }

    /// The consistency each prepared statement carries.
    #[cfg(test)]
    pub(crate) fn statement_consistencies(&self) -> [Option<Consistency>; 3] {
        [
            self.queries.register.get_consistency(),
            self.queries.read.get_consistency(),
            self.queries.remove.get_consistency(),
        ]
    }
}

impl PeerDirectory for CassandraPeerDirectory {
    type Error = CassandraStoreError;

    fn ttl(&self) -> RegistrationTtl {
        self.ttl
    }

    /// Publishes `registration` under a fresh lease.
    ///
    /// A refresh calls this same method, which is what makes "a refresh
    /// rewrites every cell that shares the lease" true by construction: one
    /// statement lists every column, so a partial update is unwritable.
    ///
    /// # Errors
    ///
    /// Returns the driver's error when the write fails.
    #[instrument(level = "debug", skip_all, fields(peer = %registration.peer), err)]
    async fn register(&self, registration: &PeerRegistration) -> Result<(), CassandraStoreError> {
        let direct_socket_address = registration.direct.socket().to_string();
        let advertised_connect = registration
            .advertised
            .as_ref()
            .map(|endpoint| endpoint.uri().to_string());
        // An absent column binds CQL NULL, never `MaybeUnset::Unset`. An unset
        // column would keep its previous cell, and that cell would then expire
        // on its own older lease while the rest of the row lives — the row that
        // has half vanished.
        self.store
            .execute_unpaged_discard(
                &self.queries.register,
                (
                    Uuid::from(registration.peer),
                    direct_socket_address,
                    advertised_connect,
                    registration.network.as_ref().map(Flexstr::as_str),
                    registration.hostname.as_str(),
                    self.ttl.seconds(),
                ),
            )
            .await
    }

    /// Reads one peer's registration.
    ///
    /// A partial row or a row with an invalid label reads as absent. Such a row
    /// has half expired or comes from another writer. The
    /// caller then reports the peer unreachable instead of dialing a partial
    /// address.
    ///
    /// # Errors
    ///
    /// Returns the driver's error when the read fails. An unusable row is a
    /// data outcome, not an error.
    #[instrument(level = "debug", skip_all, fields(%peer), err)]
    async fn read(&self, peer: PeerId) -> Result<Option<PeerRegistration>, CassandraStoreError> {
        let row = self
            .store
            .session()
            .execute_unpaged(&self.queries.read, (Uuid::from(peer),))
            .await?
            .into_rows_result()?
            .maybe_first_row::<DirectoryColumns>()?;
        // Every column decodes as `Option`: a row can carry NULLs, and a
        // deserialization error would be Terminal where "absent" is the answer.
        let Some((direct_socket_address, advertised_connect, network, hostname)) = row else {
            return Ok(None);
        };
        // An empty label cannot identify a network or host.
        let named = [&network, &hostname]
            .into_iter()
            .flatten()
            .all(|label| !label.is_empty());
        let advertised = match advertised_connect {
            Some(connect) => match Endpoint::from_shared(connect) {
                Ok(endpoint) => Some(endpoint),
                Err(error) => {
                    warn!(%error, %peer, "directory row has an invalid advertised endpoint");
                    return Ok(None);
                }
            },
            None => None,
        };
        let Some(Ok(socket)) = direct_socket_address.map(|address| address.parse::<SocketAddr>())
        else {
            warn!(%peer, "directory row has an invalid direct socket address");
            return Ok(None);
        };
        let direct = match DirectAddress::new(socket) {
            Ok(direct) => direct,
            Err(error) => {
                warn!(%error, %peer, "directory row has an invalid direct socket address");
                return Ok(None);
            }
        };
        let (true, Some(hostname)) = (named, hostname) else {
            warn!(%peer, "directory row is not resolvable");
            return Ok(None);
        };
        Ok(Some(PeerRegistration {
            peer,
            direct,
            advertised,
            network: network.map(|network| NetworkId::make(&network)),
            hostname: Host::make(&hostname),
        }))
    }

    /// Removes `registration`'s row.
    ///
    /// Idempotent: a CQL delete of an absent row is a no-op, so a repeated
    /// shutdown costs one write and changes nothing.
    ///
    /// # Errors
    ///
    /// Returns the driver's error when the delete fails.
    #[instrument(level = "debug", skip_all, fields(peer = %registration.peer), err)]
    async fn deregister(&self, registration: &PeerRegistration) -> Result<(), CassandraStoreError> {
        self.store
            .execute_unpaged_discard(&self.queries.remove, (Uuid::from(registration.peer),))
            .await
    }
}
