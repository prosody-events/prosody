//! Reaching any prosody process by id.
//!
//! Every peer feature routes through here, and nothing in this module knows
//! what a response is — [`NodeId`] and a frame's bytes are the only vocabulary
//! it shares with them.

use crate::cassandra::errors::CassandraStoreError;
use crate::router::directory::Endpoint;
use crate::router::directory::cache::AddressResolver;
use crate::router::fleet::DestinationFleet;
use bytes::BufMut;
use fixedstr::Flexstr;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tonic::Code;
use uuid::Uuid;

pub(crate) mod directory;
pub(crate) mod fleet;
pub(crate) mod grpc;
#[cfg(test)]
pub(crate) mod loopback;
pub(crate) mod runtime;

/// The host a node publishes for its peers to dial. Any ordinary hostname or
/// address stays inline; a longer one spills to the heap.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the node directory and the process runtime are this alias's production users; \
                  both are exercised by this module's tests"
    )
)]
pub(crate) type Host = Flexstr<64>;

/// Identifies one live prosody process.
///
/// Minted fresh at startup and **never reused across restarts**. That is
/// load-bearing rather than tidy: directory writes are unconditional, so a
/// reused id would let a late refresh or a shutdown delete from the previous
/// incarnation overwrite the new one's entry. A fresh id makes that race
/// unrepresentable without any conditional write.
///
/// On the wire it is 16 opaque bytes, so a peer that mints ids some other way
/// is still addressable.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct NodeId(Uuid);

/// One frame, as bytes on the wire.
///
/// The router delivers frames without reading them, which is what keeps
/// response vocabulary out of this module.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the peer transport is this trait's production caller; it is exercised by this \
                  module's tests"
    )
)]
pub(crate) trait Framed {
    /// The exact number of bytes [`Framed::write`] produces.
    fn bytes(&self) -> usize;

    /// Writes the frame into `dst`.
    ///
    /// `dst` must be able to take [`Framed::bytes`] more bytes, because
    /// [`BufMut`]'s writes are infallible and panic instead of failing.
    fn write<B: BufMut>(&self, dst: &mut B);
}

/// The one outbound network path a responder has.
///
/// `Ok` means the destination accepted the frame. Everything else is a
/// [`SendFailure`], and only an ambiguous one may be tried again.
///
/// The frame is borrowed, so a sender writes straight from the one scratch
/// buffer its worker owns rather than building a frame per response. A
/// transport whose own encoder needs an owned item still pays for one: it
/// copies the scratch into a buffer of its own, and that buffer is what the
/// borrow keeps bounded by the frame ceiling. An owned seam here would put that
/// allocation on every response whatever the transport needs.
pub(crate) trait ResponseSender: Send + Sync + 'static {
    /// Delivers one frame to one resolved address.
    fn deliver<F: Framed + Sync>(
        &self,
        address: &Endpoint,
        frame: &F,
    ) -> impl Future<Output = Result<(), SendFailure>> + Send;
}

/// Everything the response path needs to reach a peer: a node's address, the
/// transport that dials it, and the shared destination fleet.
///
/// One trait rather than three type parameters, so every signature on the
/// response path names one `R`. Address resolution belongs here rather than at
/// the apply hook that queues a response: reading the directory is an await,
/// and an apply hook must not await.
pub(crate) trait Router: Clone + Send + Sync + 'static {
    /// The transport frames leave through.
    type Sender: ResponseSender;

    /// What can stop a node id from becoming an address.
    type Error: Error + Send + Sync + 'static;

    /// The address `node` published, or `None` when it published none.
    ///
    /// # Errors
    ///
    /// Returns [`Router::Error`] when the lookup itself failed, which is
    /// distinct from a node that is simply not published.
    fn address(
        &self,
        node: NodeId,
    ) -> impl Future<Output = Result<Option<Endpoint>, Self::Error>> + Send;

    /// The transport.
    fn sender(&self) -> &Self::Sender;

    /// The process-wide fleet a response reserves a send slot from.
    ///
    /// Shared rather than borrowed, so a sender sized from this fleet can hold
    /// it and can never reserve from another one.
    fn fleet(&self) -> &Arc<DestinationFleet>;
}

/// The production [`Router`]: addresses from the directory's bounded cache,
/// frames through one transport, slots from the one fleet the process owns.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the respond layer is this type's production caller; it is exercised by this \
                  module's tests"
    )
)]
pub(crate) struct RouterHandle<S> {
    addresses: AddressResolver,
    fleet: Arc<DestinationFleet>,
    transport: Arc<S>,
}

impl NodeId {
    /// Mints an id for one incarnation of one process.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the process runtime is production-dead until consumer wiring owns it; this \
                      constructor is exercised by this module's tests"
        )
    )]
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Reads an id from its 16-byte wire form.
    pub(crate) const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// The 16-byte wire form.
    pub(crate) const fn into_bytes(self) -> [u8; 16] {
        self.0.into_bytes()
    }
}

/// The directory stores a node id in a Cassandra `uuid` column, so the driver's
/// own `Uuid` serde carries it. This conversion is the one place the newtype is
/// unwrapped for that purpose.
impl From<NodeId> for Uuid {
    fn from(node: NodeId) -> Self {
        node.0
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self.0, f)
    }
}

/// Cloning shares the cache, the fleet and the transport rather than copying
/// them: one process has exactly one of each.
impl<S> Clone for RouterHandle<S> {
    fn clone(&self) -> Self {
        Self {
            addresses: self.addresses.clone(),
            fleet: Arc::clone(&self.fleet),
            transport: Arc::clone(&self.transport),
        }
    }
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the process runtime is production-dead until consumer wiring owns it; this \
                  constructor is exercised by this module's tests"
    )
)]
impl<S> RouterHandle<S> {
    /// Binds one process's resolver, fleet and transport together.
    pub(in crate::router) fn new(
        addresses: AddressResolver,
        fleet: Arc<DestinationFleet>,
        transport: Arc<S>,
    ) -> Self {
        Self {
            addresses,
            fleet,
            transport,
        }
    }
}

impl<S: ResponseSender> Router for RouterHandle<S> {
    type Error = CassandraStoreError;
    type Sender = S;

    /// The direct endpoint the node published. [`Host`] holds an ordinary host
    /// inline, so this clone copies one; only a longer host allocates.
    async fn address(&self, node: NodeId) -> Result<Option<Endpoint>, CassandraStoreError> {
        let registration = self.addresses.resolve(node).await?;
        Ok(registration.map(|registration| registration.direct.clone()))
    }

    fn sender(&self) -> &S {
        &self.transport
    }

    fn fleet(&self) -> &Arc<DestinationFleet> {
        &self.fleet
    }
}

impl SendFailure {
    /// Whether another attempt could still get an answer.
    ///
    /// A destination that never answered may or may not have received the
    /// frame, so a retry is the only way to find out. A retry is safe because a
    /// requester accepts at most one response per request and subsystem: a
    /// duplicate is dropped, never counted twice. Every other status is the
    /// destination's own answer, and repeating the send would only repeat it.
    ///
    /// An address the transport cannot dial is not ambiguous either: the
    /// address is resolved once per response, so the next attempt would be
    /// given the same one and fail the same way, having spent the
    /// destination's pacing on nothing.
    pub(crate) const fn is_ambiguous(self) -> bool {
        match self {
            Self::Unreachable | Self::Status(Code::Unavailable | Code::DeadlineExceeded) => true,
            Self::Undialable | Self::Status(_) => false,
        }
    }
}

/// Why one delivery attempt did not reach its destination.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the peer transport is this enum's production producer; the retry rule is \
                  exercised by this module's tests"
    )
)]
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SendFailure {
    /// The destination answered with a status other than `OK`.
    #[error("destination answered {0:?}")]
    Status(Code),

    /// The destination could not be reached at all.
    #[error("destination could not be reached")]
    Unreachable,

    /// The address the destination published is not one this transport can
    /// dial, so nothing left this process.
    #[error("destination published an address that cannot be dialed")]
    Undialable,
}

#[cfg(test)]
mod tests;
