//! Reaching any prosody process by id.
//!
//! Every peer feature routes through here, and nothing in this module knows
//! what a response is — [`NodeId`] and a frame's bytes are the only vocabulary
//! it shares with them.

use crate::cassandra::errors::CassandraStoreError;
use crate::router::directory::cache::AddressResolver;
use crate::router::directory::{Endpoint, NetworkId, NodeRegistration};
use crate::router::fleet::DestinationFleet;
use bytes::BufMut;
use fixedstr::Flexstr;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tokio::time::Instant;
use tonic::Code;
use uuid::Uuid;

pub(crate) mod directory;
pub(crate) mod fleet;
pub(crate) mod grpc;
#[cfg(test)]
pub(crate) mod loopback;
pub(crate) mod relay;
pub(crate) mod runtime;

/// Inline capacity of an operator-configured label. One byte holds the length,
/// so a label of `LABEL_CAPACITY - 1` bytes never reaches the heap — which is
/// what keeps a resolved address off the response path's allocator.
pub(crate) const LABEL_CAPACITY: usize = 64;

/// The host a node publishes for its peers to dial. Any ordinary hostname or
/// address stays inline; a longer one spills to the heap.
pub(crate) type Host = Flexstr<LABEL_CAPACITY>;

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

/// Which of a node's two endpoints answered last.
///
/// A destination remembers one of these and never an [`Endpoint`]. A remembered
/// address would outlive the registration that published it, and would be
/// dialed after the node moved. The route is resolved for every response; the
/// preference only orders the candidates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the response sender is this enum's production reader; the order it decides is \
                  exercised by this module's tests"
    )
)]
pub(crate) enum Preference {
    /// The address the node discovered for itself on its own network.
    Direct,
    /// The entry point that reaches the node from another network.
    Advertised,
}

/// The endpoints one node may be dialed on, in the order the rules put them.
///
/// Never more than two, and the second exists only where a failed first attempt
/// has somewhere else to go. [`choose_route`] is the only way to build one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Route {
    first: (Preference, Endpoint),
    second: Option<(Preference, Endpoint)>,
}

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
    /// Delivers one frame to one resolved address, and gives up at `deadline`.
    ///
    /// The deadline is an instant rather than a duration because the sender
    /// still has a channel lookup and a readiness wait in front of it, and
    /// every attempt is given the same argument. A duration the caller computed
    /// would be stale by the time the header is written.
    fn deliver<F: Framed + Sync>(
        &self,
        address: &Endpoint,
        frame: &F,
        deadline: Instant,
    ) -> impl Future<Output = Result<(), SendFailure>> + Send;
}

/// What one forward reads: the endpoint a node published for its neighbours,
/// the transport that dials it, and the shared destination fleet.
///
/// A process that forwards stands beside its target already, so it reads no
/// declared label. This trait offers none, and that is why a relay is bound by
/// it rather than by [`Router`]: [`Router::route`] is the one function that
/// applies the operator's rules, so a forward that consulted them does not
/// compile.
pub(crate) trait RelayHop: Clone + Send + Sync + 'static {
    /// The transport frames leave through.
    type Sender: ResponseSender;

    /// What can stop a node id from becoming an address.
    type Error: Error + Send + Sync + 'static;

    /// The direct endpoint alone. This is the lookup a process uses when it
    /// sends a frame on to the process that frame names.
    ///
    /// # Errors
    ///
    /// Returns [`RelayHop::Error`] when the lookup itself failed.
    fn direct(
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

/// Everything the response path needs to reach a peer: every endpoint a node
/// may be dialed on, the transport that dials them, and the shared destination
/// fleet.
///
/// One trait rather than three type parameters, so every signature on the
/// response path names one `R`. Address resolution belongs here rather than at
/// the apply hook that queues a response: reading the directory is an await,
/// and an apply hook must not await.
pub(crate) trait Router: RelayHop {
    /// The endpoints `node` may be dialed on from this process, in order. This
    /// is the responder's lookup, and [`choose_route`] decides what it answers.
    ///
    /// `None` means "do not dial", which covers both a node the directory does
    /// not hold and one the rules refuse to reach from here.
    ///
    /// # Errors
    ///
    /// Returns [`RelayHop::Error`] when the lookup itself failed, which is
    /// distinct from a node that is simply not published.
    fn route(
        &self,
        node: NodeId,
    ) -> impl Future<Output = Result<Option<Route>, Self::Error>> + Send;
}

/// The production [`Router`]: addresses from the directory's bounded cache,
/// frames through one transport, slots from the one fleet the process owns.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "no production caller yet: the respond layer will own this; every item here is \
                  exercised by this module's tests"
    )
)]
pub(crate) struct RouterHandle<S> {
    addresses: AddressResolver,
    fleet: Arc<DestinationFleet>,
    transport: Arc<S>,
    here: Option<NetworkId>,
}

impl NodeId {
    /// Mints an id for one incarnation of one process.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "no production caller yet: consumer wiring will own the process runtime; \
                      every item here is exercised by this module's tests"
        )
    )]
    pub(in crate::router) fn new() -> Self {
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
            here: self.here.clone(),
        }
    }
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "no production caller yet: the respond layer will own this; every item here is \
                  exercised by this module's tests"
    )
)]
impl<S> RouterHandle<S> {
    /// Binds one process's resolver, fleet and transport together.
    pub(in crate::router) fn new(
        addresses: AddressResolver,
        fleet: Arc<DestinationFleet>,
        transport: Arc<S>,
        here: Option<NetworkId>,
    ) -> Self {
        Self {
            addresses,
            fleet,
            transport,
            here,
        }
    }
}

impl<S: ResponseSender> RelayHop for RouterHandle<S> {
    type Error = CassandraStoreError;
    type Sender = S;

    async fn direct(&self, node: NodeId) -> Result<Option<Endpoint>, CassandraStoreError> {
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

impl<S: ResponseSender> Router for RouterHandle<S> {
    async fn route(&self, node: NodeId) -> Result<Option<Route>, CassandraStoreError> {
        let registration = self.addresses.resolve(node).await?;
        Ok(registration
            .as_deref()
            .and_then(|registration| choose_route(self.here.as_ref(), registration)))
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
            Self::Undialable | Self::Expired | Self::Status(_) => false,
        }
    }

    /// Whether nothing served the frame here, so the node's other endpoint is
    /// worth trying inside the same response.
    ///
    /// Every failure that answers `false` is a status some process gave the
    /// frame after reading it, which is what lets the send path remember the
    /// endpoint that gave it. The rest are these:
    ///
    /// - **Nothing was dialed, or nothing answered.** The address could not be
    ///   dialed, or the endpoint said nothing at all. `UNAVAILABLE` and
    ///   `UNIMPLEMENTED` are the same fact from something that answered but
    ///   does not serve this method — which is what a misapplied label reaches,
    ///   an address that belongs to something unrelated on this network.
    /// - **This process gave up first.** `CANCELLED` is what the transport's
    ///   own timer reads as, and [`SendFailure::Expired`] is the same fact
    ///   before anything left. Neither is the destination's word.
    ///
    /// `DEADLINE_EXCEEDED` is deliberately not here. It is the answer of a peer
    /// that read the frame and ran out of the budget this response stated, so
    /// it is that peer speaking about the whole path rather than about the
    /// address. The other endpoint has no more time to spend than this one had.
    pub(crate) const fn is_wrong_endpoint(self) -> bool {
        match self {
            Self::Unreachable
            | Self::Undialable
            | Self::Expired
            | Self::Status(Code::Unavailable | Code::Unimplemented | Code::Cancelled) => true,
            Self::Status(_) => false,
        }
    }
}

impl Route {
    /// The candidates to try, the remembered one first when this route offers
    /// it.
    ///
    /// A fixed-size array, so walking a route allocates nothing.
    pub(crate) fn candidates(
        &self,
        remembered: Option<Preference>,
    ) -> [Option<(Preference, &Endpoint)>; 2] {
        let first = Some((self.first.0, &self.first.1));
        let second = self
            .second
            .as_ref()
            .map(|(preference, endpoint)| (*preference, endpoint));
        if second.is_some_and(|(preference, _)| Some(preference) == remembered) {
            [second, first]
        } else {
            [first, second]
        }
    }
}

/// The endpoints `registration` is dialed on from a process labelled `here`.
///
/// A label names the set of processes that reach each other on their direct
/// endpoints. An operator declares it; nothing infers it. Three rules follow
/// from that, and this is the one function in the crate that reads a label:
///
/// - **Both present and equal.** Dial `direct`, and fall back to `advertised`
///   when the node published one. Neighbours skip the entry point, which
///   matters less for latency than for load.
/// - **Both present and unequal.** Dial `advertised` alone, and `None` when the
///   node published none. The node is known to be elsewhere, so its direct
///   address is a foreign one that most likely belongs to something unrelated
///   here. Refusing to dial is only expressible because the labels were
///   declared.
/// - **Either absent.** That means "cannot tell", never "different". Dial
///   `advertised` if the node published one, else `direct`. With nothing
///   configured anywhere, every node resolves to `direct`, which is the
///   single-network case working with no configuration at all.
///
/// `None` means "do not dial".
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the response path is this rule's production caller, through `Router::route`; \
                  each label shape is exercised by this module's tests"
    )
)]
pub(crate) fn choose_route(
    here: Option<&NetworkId>,
    registration: &NodeRegistration,
) -> Option<Route> {
    match (here, registration.network.as_ref()) {
        (Some(here), Some(there)) if here == there => Some(Route {
            first: (Preference::Direct, registration.direct.clone()),
            second: registration
                .advertised
                .clone()
                .map(|endpoint| (Preference::Advertised, endpoint)),
        }),
        (Some(_), Some(_)) => registration.advertised.clone().map(|endpoint| Route {
            first: (Preference::Advertised, endpoint),
            second: None,
        }),
        (None, _) | (_, None) => Some(match registration.advertised.clone() {
            Some(endpoint) => Route {
                first: (Preference::Advertised, endpoint),
                second: None,
            },
            None => Route {
                first: (Preference::Direct, registration.direct.clone()),
                second: None,
            },
        }),
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

    /// The budget ran out before the frame left this process, so nothing
    /// reached the destination and the destination said nothing.
    #[error("the send budget ran out before the frame left this process")]
    Expired,
}

#[cfg(test)]
mod tests;
