//! The peer directory: where every live prosody process publishes how peers
//! can reach it, and how any process resolves another by id.
//!
//! A registration is soft state. A process writes its own entry under a lease,
//! rewrites the entry inside that lease, and deletes the entry on a clean
//! shutdown. A process that dies expires with the lease. Peer ids are minted
//! fresh at startup and never reused, so an entry has exactly one writer for
//! its whole life. That is why a backend needs no lightweight transaction and
//! nothing to fence: every write is an unconditional upsert or delete.

use crate::router::{Host, LABEL_CAPACITY, PeerId};
use fixedstr::Flexstr;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use tonic::transport::Error as TransportError;

pub(crate) mod cache;
pub(crate) mod cassandra;
mod lease;

#[cfg(test)]
pub(crate) mod tests;

/// The lease lives in its own module so that no write site is a descendant of
/// it. Its range is then the constructor's alone, rather than a convention the
/// write sites keep.
pub(crate) use self::lease::{RegistrationTtl, RegistrationTtlError};
pub(crate) use tonic::transport::Endpoint;

/// The operator's name for a set of processes that can reach each other on
/// their direct endpoints.
///
/// Not a CIDR and not a Kubernetes object: a label that two peers either share
/// or do not. Absent means "unknown", which never counts as a match.
pub(crate) type NetworkId = Flexstr<LABEL_CAPACITY>;

/// A peer's direct socket address.
///
/// Construction also builds the endpoint once. Response routing then adds no
/// allocation for address conversion.
#[derive(Clone, Debug)]
pub(crate) struct DirectAddress {
    socket: SocketAddr,
    endpoint: Endpoint,
}

impl DirectAddress {
    /// Builds a direct address and its reusable endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket address cannot form an endpoint.
    pub(crate) fn new(socket: SocketAddr) -> Result<Self, TransportError> {
        Ok(Self {
            socket,
            endpoint: Endpoint::from_shared(format!("http://{socket}"))?,
        })
    }

    /// Returns the address stored in the peer directory.
    pub(crate) const fn socket(&self) -> SocketAddr {
        self.socket
    }

    /// Returns the endpoint that response routing uses.
    pub(crate) const fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

/// One live process, as the directory publishes it.
#[derive(Clone, Debug)]
pub(crate) struct PeerRegistration {
    pub(crate) peer: PeerId,
    /// The socket address that this process bound and published.
    pub(crate) direct: DirectAddress,
    /// An entry point that reaches this process from another network. Present
    /// only where an operator arranged one; absent means intra-network only.
    pub(crate) advertised: Option<Endpoint>,
    pub(crate) network: Option<NetworkId>,
    /// The name a person would recognise this machine by.
    pub(crate) hostname: Host,
}

/// What a process publishes about itself, and how it resolves another peer.
///
/// [`CassandraPeerDirectory`](cassandra::CassandraPeerDirectory) is the
/// production implementation. Tests use small in-process implementations.
pub(crate) trait PeerDirectory: Clone + Send + Sync + 'static {
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
        registration: &PeerRegistration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Reads one peer's registration. A registration that is not resolvable
    /// reads as absent rather than as an error.
    fn read(
        &self,
        peer: PeerId,
    ) -> impl Future<Output = Result<Option<PeerRegistration>, Self::Error>> + Send;

    /// Removes `registration`'s entry. Idempotent.
    fn deregister(
        &self,
        registration: &PeerRegistration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
