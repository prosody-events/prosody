//! The node directory: where every live prosody process publishes how peers
//! can reach it, and how any process resolves another by id.
//!
//! A registration is soft state. A process writes its own entry under a lease,
//! rewrites the entry inside that lease, and deletes the entry on a clean
//! shutdown. A process that dies expires with the lease. Node ids are minted
//! fresh at startup and never reused, so an entry has exactly one writer for
//! its whole life. That is why a backend needs no lightweight transaction and
//! nothing to fence: every write is an unconditional upsert or delete.

use crate::router::{Host, LABEL_CAPACITY, NodeId};
use fixedstr::Flexstr;
use std::error::Error;
use std::future::Future;

pub(crate) mod cache;
pub(crate) mod cassandra;
mod lease;

#[cfg(test)]
pub(crate) mod tests;

/// The lease lives in its own module so that no write site is a descendant of
/// it. Its range is then the constructor's alone, rather than a convention the
/// write sites keep.
pub(crate) use self::lease::{RegistrationTtl, RegistrationTtlError};

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
    /// The name a person would recognise this machine by.
    pub(crate) hostname: Host,
}

/// What a process publishes about itself, and how it resolves another node.
///
/// [`CassandraNodeDirectory`](cassandra::CassandraNodeDirectory) is the
/// production implementation. Tests use small in-process implementations.
pub(crate) trait NodeDirectory: Clone + Send + Sync + 'static {
    /// What can stop a directory operation.
    ///
    /// No error-classification bound: no caller reads a directory error's
    /// category. Both readers fold every value into one outcome, so a
    /// classification constraint here would be unused.
    type Error: Error + Send + Sync + 'static;

    /// The lease every write this directory issues publishes.
    ///
    /// It is the single source of the lease.
    /// [`AddressResolver::new`](cache::AddressResolver::new) builds its cache
    /// from this value, and the process runtime paces its refresher inside it.
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
