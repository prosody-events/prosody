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

use crate::router::{Host, LABEL_CAPACITY, NodeId};
use fixedstr::Flexstr;
use std::error::Error;
use std::future::Future;
use std::time::Duration;
use thiserror::Error;

pub(crate) mod cache;
pub(crate) mod cassandra;

#[cfg(test)]
pub(crate) mod memory;

#[cfg(test)]
pub(crate) mod tests;

/// Where a process can be reached: a host and the port peers dial there.
///
/// A tagged pair rather than a `host:port` string, because an entry point may
/// later need a TLS server name or a scheme. Those are ordinary columns, so
/// the set can grow without a key change.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Endpoint {
    pub(crate) host: Host,
    pub(crate) port: u16,
}

/// The operator's name for a set of processes that can reach each other on
/// their direct endpoints.
///
/// Not a CIDR and not a Kubernetes object: a label that two nodes either share
/// or do not. Absent means "unknown", which never counts as a match.
pub(crate) type NetworkId = Flexstr<LABEL_CAPACITY>;

/// The consumer group a process belongs to, and the Kafka cluster that scopes
/// it.
///
/// Both parts are required because Kafka scopes a group id to its cluster: two
/// unrelated clusters can each run a group of the same name, so the group alone
/// names no set of processes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupMembership {
    pub(crate) cluster: Flexstr<LABEL_CAPACITY>,
    pub(crate) group: Flexstr<LABEL_CAPACITY>,
}

/// One live process, as the directory publishes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NodeRegistration {
    pub(crate) node: NodeId,
    /// Where this process is reachable on its own network: the address it
    /// discovered for itself, on the port its listener bound. Always present,
    /// so "a node with no reachable address" is unrepresentable.
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

/// What a process publishes about itself, and how it resolves another node.
///
/// Two implementations exist:
/// [`CassandraNodeDirectory`](cassandra::CassandraNodeDirectory), which every
/// deployment uses, and `MemoryNodeDirectory`, which serves same-process tests.
///
/// Construction is each implementation's own: one is opened over a Cassandra
/// store and prepares statements, the other holds a bounded map. A process
/// picks one at its construction boundary, and the choice travels from there as
/// a type.
pub(crate) trait NodeDirectory: Clone + Send + Sync + 'static {
    /// What can stop a directory operation.
    ///
    /// No error-classification bound: no caller reads a directory error's
    /// category. Both readers fold every value into one outcome, so a
    /// classification constraint here would be unused.
    type Error: Error + Send + Sync + 'static;

    /// The lease every write this directory issues publishes. It is the single
    /// source of the lease: the address cache ages on it and the refresher
    /// paces itself inside it.
    fn ttl(&self) -> RegistrationTtl;

    /// Publishes `registration` under a fresh lease.
    fn register(
        &self,
        registration: &NodeRegistration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Reads one node's registration. A registration that is not resolvable
    /// reads as absent rather than as an error.
    fn read(
        &self,
        node: NodeId,
    ) -> impl Future<Output = Result<Option<NodeRegistration>, Self::Error>> + Send;

    /// Removes `registration`'s entry. Idempotent.
    fn deregister(
        &self,
        registration: &NodeRegistration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// How long a registration survives without a refresh.
///
/// The bound is checked once, here, so no write site tests it again: the value
/// is a positive number of seconds far below Cassandra's maximum TTL, and every
/// statement binds [`RegistrationTtl::seconds`] directly. This is a fixed
/// lease rather than a retention window anchored on a natural end time, so a
/// write site needs no lease arithmetic and no overflow check of its own.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegistrationTtl(i32);

impl RegistrationTtl {
    /// The lease a process publishes when an operator asks for none. Long
    /// enough that a refresher paces itself well inside it, short enough that a
    /// dead process's row expires within half a minute.
    pub(crate) const DEFAULT: Self = Self(30);
    /// Longest lease a caller can ask for. A dead process stays resolvable for
    /// at most this long, and each stale resolution costs one dropped response.
    pub(crate) const MAX: Duration = Duration::from_hours(1);
    /// Shortest lease a caller can ask for. Below this, a refresh falls due
    /// less than a second after the one before it, and each write's own round
    /// trip then takes a large part of the margin the jitter leaves.
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

/// A lease outside the range [`RegistrationTtl`] accepts.
#[derive(Debug, Error)]
#[error("a registration lease must be between {min:?} and {max:?}, not {actual:?}")]
pub(crate) struct RegistrationTtlError {
    min: Duration,
    max: Duration,
    actual: Duration,
}
