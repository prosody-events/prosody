//! The node directory: where every live prosody process publishes how peers
//! can reach it, and how any process resolves another by id.
//!
//! A registration is soft state. A process writes its own row under a lease,
//! rewrites the row inside that lease, and deletes the row on a clean
//! shutdown. A process that dies expires with the lease. Node ids are minted
//! fresh at startup and never reused, so a row has exactly one writer for its
//! whole life — which is why every statement here is an unconditional upsert or
//! delete, with no lightweight transaction and nothing to fence.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the destination fleet and the process runtime are this module's production \
                  callers; every item here is exercised by this module's tests"
    )
)]

use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraStore, TABLE_NODE_DIRECTORY, TABLE_NODES_BY_GROUP};
use crate::cassandra_queries;
use crate::router::{Host, NodeId};
use fixedstr::Flexstr;
use scylla::statement::Consistency;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{instrument, warn};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

pub(crate) mod cache;

#[cfg(test)]
pub(crate) mod tests;

/// Number of partitions one group's membership index is spread over.
///
/// A group has no ceiling, so a single partition would bound neither the write
/// rate nor the size of the index, and every listing would read the one
/// partition that every renewal, expiry and shutdown tombstone lands in.
const GROUP_SHARDS: u64 = 16;

/// Where a process can be reached: a host and the port its listener bound.
///
/// A tagged pair rather than a `host:port` string, because an entry point may
/// later need a TLS server name or a scheme. Those are ordinary columns, so
/// the set can grow without a key change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Endpoint {
    pub(crate) host: Host,
    pub(crate) port: u16,
}

/// The operator's name for a set of processes that can reach each other on
/// their direct endpoints.
///
/// Not a CIDR and not a Kubernetes object: a label that two nodes either share
/// or do not. Absent means "unknown", which never counts as a match.
pub(crate) type NetworkId = Flexstr<64>;

/// The consumer group a process belongs to, and the Kafka cluster that scopes
/// it.
///
/// Both parts are required because both sit in the membership index's partition
/// key: Kafka scopes a group id to its cluster, so two unrelated clusters can
/// each run a group of the same name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupMembership {
    pub(crate) cluster: Flexstr<64>,
    pub(crate) group: Flexstr<64>,
}

/// One live process, as the directory publishes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NodeRegistration {
    pub(crate) node: NodeId,
    /// Where this process is reachable on its own network. Always present, so
    /// "a node with no reachable address" is unrepresentable.
    pub(crate) direct: Endpoint,
    /// An entry point that reaches this process from another network. Present
    /// only where an operator arranged one; absent means intra-network only.
    pub(crate) advertised: Option<Endpoint>,
    pub(crate) network: Option<NetworkId>,
    /// Carried for operators and for the peer features that route on it, never
    /// for addressing.
    pub(crate) group: Option<GroupMembership>,
    /// The name a person would recognise this machine by.
    pub(crate) hostname: Host,
}

/// How long a registration survives without a refresh.
///
/// The bound is checked once, here, so no write site tests it again: the value
/// is a positive number of seconds far below Cassandra's maximum TTL, and every
/// statement binds [`RegistrationTtl::seconds`] directly. This is a fixed
/// lease rather than a retention window anchored on a natural end time, which
/// is why it does not go through
/// [`calculate_ttl`](crate::cassandra::CassandraStore::calculate_ttl).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegistrationTtl(i32);

/// The read-write handle on the node directory.
///
/// Every statement runs at `LOCAL_ONE`. A registration is idempotent and
/// rewritten every interval, so a write that reaches one replica and is then
/// lost heals on the next refresh, well inside the lease. A quorum would make
/// every cache miss on the response path pay an inter-datacentre round trip and
/// would buy nothing on top of single-replica writes.
#[derive(Clone, Debug)]
pub(crate) struct NodeDirectory {
    store: CassandraStore,
    queries: Arc<DirectoryQueries>,
    ttl: RegistrationTtl,
}

impl RegistrationTtl {
    /// Longest lease a caller can ask for. A dead process stays resolvable for
    /// at most this long, and each stale resolution costs one dropped response.
    pub(crate) const MAX: Duration = Duration::from_hours(1);
    /// Shortest lease a caller can ask for. Below this, a refresh and its
    /// jitter leave no room for a lost write to heal.
    pub(crate) const MIN: Duration = Duration::from_secs(5);

    /// The lease in seconds, ready to bind to a `USING TTL` placeholder.
    pub(crate) const fn seconds(self) -> i32 {
        self.0
    }

    /// The lease as a duration, for callers that pace themselves against it.
    pub(crate) fn duration(self) -> Duration {
        Duration::from_secs(u64::from(self.0.unsigned_abs()))
    }
}

impl TryFrom<Duration> for RegistrationTtl {
    type Error = RegistrationTtlError;

    fn try_from(lease: Duration) -> Result<Self, Self::Error> {
        if lease < Self::MIN || lease > Self::MAX {
            return Err(RegistrationTtlError {
                min: Self::MIN,
                max: Self::MAX,
                actual: lease,
            });
        }
        // The check above caps the value at 3600, so the cast cannot truncate.
        Ok(Self(lease.as_secs() as i32))
    }
}

impl NodeDirectory {
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
            &mut queries.register_node,
            &mut queries.register_member,
            &mut queries.read_node,
            &mut queries.remove_node,
            &mut queries.remove_member,
        ] {
            statement.set_consistency(Consistency::LocalOne);
        }
        Ok(Self {
            store,
            queries: Arc::new(queries),
            ttl,
        })
    }

    /// The lease every write this directory issues publishes.
    pub(crate) const fn ttl(&self) -> RegistrationTtl {
        self.ttl
    }

    /// The consistency each prepared statement carries.
    #[cfg(test)]
    pub(crate) fn statement_consistencies(&self) -> [Option<Consistency>; 5] {
        [
            self.queries.register_node.get_consistency(),
            self.queries.register_member.get_consistency(),
            self.queries.read_node.get_consistency(),
            self.queries.remove_node.get_consistency(),
            self.queries.remove_member.get_consistency(),
        ]
    }

    /// Publishes `registration` under a fresh lease.
    ///
    /// A refresh calls this same method, which is what makes "a refresh
    /// rewrites every cell that shares the lease" true by construction: one
    /// statement lists every column, so a partial update is unwritable.
    ///
    /// The node row is written first and the membership index second. The two
    /// are deliberately not batched — they are different partitions. A reader
    /// depends on the order: it may find an index entry whose node row is
    /// missing, and treats that as a stale entry, but never the reverse.
    ///
    /// # Errors
    ///
    /// Returns the driver's error when either write fails.
    #[instrument(level = "debug", skip_all, fields(node = %registration.node), err)]
    pub(crate) async fn register(
        &self,
        registration: &NodeRegistration,
    ) -> Result<(), CassandraStoreError> {
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
                &self.queries.register_node,
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
            .await?;
        if let Some(membership) = &registration.group {
            self.store
                .execute_unpaged_discard(
                    &self.queries.register_member,
                    (
                        membership.cluster.as_str(),
                        membership.group.as_str(),
                        shard_for(registration.node),
                        Uuid::from(registration.node),
                        self.ttl.seconds(),
                    ),
                )
                .await?;
        }
        Ok(())
    }

    /// Reads one node's registration.
    ///
    /// A row that has lost its direct endpoint or its hostname — a row that
    /// half expired, or one written by something other than this code — reads
    /// as absent. The caller then reports the node unreachable instead of
    /// dialing a partial address.
    ///
    /// # Errors
    ///
    /// Returns the driver's error when the read fails. An unusable row is a
    /// data outcome, not an error.
    #[instrument(level = "debug", skip_all, fields(%node), err)]
    pub(crate) async fn read(
        &self,
        node: NodeId,
    ) -> Result<Option<NodeRegistration>, CassandraStoreError> {
        let row = self
            .store
            .session()
            .execute_unpaged(&self.queries.read_node, (Uuid::from(node),))
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
        let (Some(direct), Some(hostname)) = (endpoint(direct_host, direct_port), hostname) else {
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

    /// Removes `registration`'s rows.
    ///
    /// Idempotent: a CQL delete of an absent row is a no-op, so a repeated
    /// shutdown costs two writes and changes nothing.
    ///
    /// # Errors
    ///
    /// Returns the driver's error when either delete fails.
    #[instrument(level = "debug", skip_all, fields(node = %registration.node), err)]
    pub(crate) async fn deregister(
        &self,
        registration: &NodeRegistration,
    ) -> Result<(), CassandraStoreError> {
        self.store
            .execute_unpaged_discard(&self.queries.remove_node, (Uuid::from(registration.node),))
            .await?;
        if let Some(membership) = &registration.group {
            self.store
                .execute_unpaged_discard(
                    &self.queries.remove_member,
                    (
                        membership.cluster.as_str(),
                        membership.group.as_str(),
                        shard_for(registration.node),
                        Uuid::from(registration.node),
                    ),
                )
                .await?;
        }
        Ok(())
    }
}

cassandra_queries! {
    /// Prepared statements of the node directory. Each one is an unconditional
    /// write or a single-partition point read: no lightweight transaction, no
    /// filtering, and no client-supplied write timestamp.
    pub(crate) struct DirectoryQueries {
        /// Writes every column of one node's row under one lease.
        register_node: (
            "INSERT INTO $keyspace.{} (node_id, direct_host, direct_port, advertised_host, \
             advertised_port, network, kafka_cluster_id, group_id, hostname) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            TABLE_NODE_DIRECTORY
        ),

        /// Adds one node to its group's membership index, under the same lease.
        register_member: (
            "INSERT INTO $keyspace.{} (kafka_cluster_id, group_id, shard, node_id) \
             VALUES (?, ?, ?, ?) USING TTL ?",
            TABLE_NODES_BY_GROUP
        ),

        /// Point-reads one node's row.
        read_node: (
            "SELECT direct_host, direct_port, advertised_host, advertised_port, network, \
             kafka_cluster_id, group_id, hostname FROM $keyspace.{} WHERE node_id = ?",
            TABLE_NODE_DIRECTORY
        ),

        /// Removes one node's row on a clean shutdown.
        remove_node: (
            "DELETE FROM $keyspace.{} WHERE node_id = ?",
            TABLE_NODE_DIRECTORY
        ),

        /// Removes one node from its group's membership index.
        remove_member: (
            "DELETE FROM $keyspace.{} WHERE kafka_cluster_id = ? AND group_id = ? \
             AND shard = ? AND node_id = ?",
            TABLE_NODES_BY_GROUP
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

/// The index partition a node's membership row lands in.
///
/// Derived from the node id and nothing else, so registration and
/// deregistration compute the same partition without reading anything. The hash
/// makes the spread independent of which UUID version minted the id.
fn shard_for(node: NodeId) -> i32 {
    // The remainder is below `GROUP_SHARDS`, so the cast cannot truncate.
    (xxh3_64(&node.into_bytes()) % GROUP_SHARDS) as i32
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

/// A lease outside the range [`RegistrationTtl`] accepts.
#[derive(Debug, Error)]
#[error("a registration lease must be between {min:?} and {max:?}, not {actual:?}")]
pub(crate) struct RegistrationTtlError {
    min: Duration,
    max: Duration,
    actual: Duration,
}
