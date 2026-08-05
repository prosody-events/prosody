//! The Cassandra node directory.

use super::{
    Endpoint, GroupMembership, NetworkId, NodeDirectory, NodeRegistration, RegistrationTtl,
};
use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraStore, TABLE_NODE_DIRECTORY};
use crate::cassandra_queries;
use crate::router::{Host, MAX_LABEL_BYTES, NodeId};
use fixedstr::Flexstr;
use scylla::statement::Consistency;
use std::sync::Arc;
use tracing::{instrument, warn};
use uuid::Uuid;

cassandra_queries! {
    /// Prepared statements of the node directory. Each one is an unconditional
    /// write or a single-partition point read: no lightweight transaction, no
    /// filtering, and no client-supplied write timestamp.
    pub(crate) struct DirectoryQueries {
        /// Writes every column of one node's row under one lease.
        register: (
            "INSERT INTO $keyspace.{} (node_id, direct_host, direct_port, advertised_host, \
             advertised_port, network, kafka_cluster_id, group_id, hostname) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            TABLE_NODE_DIRECTORY
        ),

        /// Point-reads one node's row.
        read: (
            "SELECT direct_host, direct_port, advertised_host, advertised_port, network, \
             kafka_cluster_id, group_id, hostname FROM $keyspace.{} WHERE node_id = ?",
            TABLE_NODE_DIRECTORY
        ),

        /// Removes one node's row on a clean shutdown.
        remove: (
            "DELETE FROM $keyspace.{} WHERE node_id = ?",
            TABLE_NODE_DIRECTORY
        ),
    }
}

/// The directory row as the driver hands it over: every column nullable.
type DirectoryColumns = (
    Option<String>,
    Option<i32>,
    Option<String>,
    Option<i32>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// The read-write handle on the Cassandra node directory.
///
/// Every statement runs at `LOCAL_ONE`. A registration is idempotent and
/// rewritten every interval, so a write that reaches one replica and is then
/// lost heals on the next refresh, well inside the lease. A quorum would make
/// every cache miss on the response path wait for a second replica and would
/// buy nothing on top of a row that is rewritten every interval.
#[derive(Clone, Debug)]
pub(crate) struct CassandraNodeDirectory {
    store: CassandraStore,
    queries: Arc<DirectoryQueries>,
    ttl: RegistrationTtl,
}

impl CassandraNodeDirectory {
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

impl NodeDirectory for CassandraNodeDirectory {
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
    #[instrument(level = "debug", skip_all, fields(node = %registration.node), err)]
    async fn register(&self, registration: &NodeRegistration) -> Result<(), CassandraStoreError> {
        let (advertised_host, advertised_port) = match &registration.advertised {
            Some(endpoint) => (Some(endpoint.host.as_str()), Some(i32::from(endpoint.port))),
            None => (None, None),
        };
        let (cluster, group) = match &registration.group {
            Some(membership) => (
                Some(membership.cluster.as_str()),
                Some(membership.group.as_str()),
            ),
            None => (None, None),
        };
        // An absent column binds CQL NULL, never `MaybeUnset::Unset`. An unset
        // column would keep its previous cell, and that cell would then expire
        // on its own older lease while the rest of the row lives — the row that
        // has half vanished.
        self.store
            .execute_unpaged_discard(
                &self.queries.register,
                (
                    Uuid::from(registration.node),
                    registration.direct.host.as_str(),
                    i32::from(registration.direct.port),
                    advertised_host,
                    advertised_port,
                    registration.network.as_ref().map(Flexstr::as_str),
                    cluster,
                    group,
                    registration.hostname.as_str(),
                    self.ttl.seconds(),
                ),
            )
            .await
    }

    /// Reads one node's registration.
    ///
    /// A row that has lost its direct endpoint or its hostname, and a row
    /// carrying a label over [`MAX_LABEL_BYTES`], read as absent. Both are rows
    /// that half expired or that something other than this code wrote. The
    /// caller then reports the node unreachable instead of dialing a partial
    /// address.
    ///
    /// # Errors
    ///
    /// Returns the driver's error when the read fails. An unusable row is a
    /// data outcome, not an error.
    #[instrument(level = "debug", skip_all, fields(%node), err)]
    async fn read(&self, node: NodeId) -> Result<Option<NodeRegistration>, CassandraStoreError> {
        let row = self
            .store
            .session()
            .execute_unpaged(&self.queries.read, (Uuid::from(node),))
            .await?
            .into_rows_result()?
            .maybe_first_row::<DirectoryColumns>()?;
        // Every column decodes as `Option`: a row can carry NULLs, and a
        // deserialization error would be Terminal where "absent" is the answer.
        let Some((
            direct_host,
            direct_port,
            advertised_host,
            advertised_port,
            network,
            cluster,
            group,
            hostname,
        )) = row
        else {
            return Ok(None);
        };
        // A label longer than a registration may publish makes the whole row
        // unresolvable rather than a shorter label: truncating would dial a
        // different host, and keeping it would put an unbounded string in the
        // address cache, which counts entries and not bytes.
        let bounded = [
            &direct_host,
            &advertised_host,
            &network,
            &cluster,
            &group,
            &hostname,
        ]
        .into_iter()
        .flatten()
        .all(|label| label.len() <= MAX_LABEL_BYTES);
        let (true, Some(direct), Some(hostname)) =
            (bounded, endpoint(direct_host, direct_port), hostname)
        else {
            warn!(%node, "directory row is not resolvable");
            return Ok(None);
        };
        Ok(Some(NodeRegistration {
            node,
            direct,
            advertised: endpoint(advertised_host, advertised_port),
            network: network.map(|network| NetworkId::make(&network)),
            group: membership(cluster, group),
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
    #[instrument(level = "debug", skip_all, fields(node = %registration.node), err)]
    async fn deregister(&self, registration: &NodeRegistration) -> Result<(), CassandraStoreError> {
        self.store
            .execute_unpaged_discard(&self.queries.remove, (Uuid::from(registration.node),))
            .await
    }
}

/// An endpoint from its two columns, or nothing when either is missing or the
/// port is outside the range a port can hold.
fn endpoint(host: Option<String>, port: Option<i32>) -> Option<Endpoint> {
    let (Some(host), Some(port)) = (host, port) else {
        return None;
    };
    let Ok(port) = u16::try_from(port) else {
        return None;
    };
    Some(Endpoint {
        host: Host::make(&host),
        port,
    })
}

/// A group membership from its two columns. Both are written together, so one
/// without the other names no group at all.
fn membership(cluster: Option<String>, group: Option<String>) -> Option<GroupMembership> {
    let (Some(cluster), Some(group)) = (cluster, group) else {
        return None;
    };
    Some(GroupMembership {
        cluster: Flexstr::make(&cluster),
        group: Flexstr::make(&group),
    })
}
