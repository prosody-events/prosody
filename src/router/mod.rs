//! Reaching any prosody process by id.
//!
//! Every peer feature routes through here. Remote paths know only a [`NodeId`]
//! and frame bytes. The local target owns this process's request registry.

use crate::requester::registry::PendingRegistry;
use crate::response::ResponseDisposition;
use crate::response::frame::ResponseFrame;
use crate::router::directory::cache::AddressResolver;
use crate::router::directory::{Endpoint, NetworkId, NodeDirectory, NodeRegistration};
use crate::router::fleet::{Destination, DestinationFleet};
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

pub(crate) mod config;
pub(crate) mod directory;
pub(crate) mod fleet;
pub(crate) mod grpc;
#[cfg(test)]
pub(crate) mod loopback;
pub(crate) mod relay;
pub(crate) mod runtime;

/// Inline capacity of a label. One byte holds the length, so a label of
/// [`MAX_LABEL_BYTES`] never reaches the heap.
const LABEL_CAPACITY: usize = 64;

/// Longest label this crate publishes or resolves.
///
/// It is the largest label that stays inline in [`Host`] and
/// [`NetworkId`](directory::NetworkId), and both ends of the directory hold to
/// it: a process refuses to publish a longer one, and an entry carrying a
/// longer one reads as unresolvable. That is what makes the address cache
/// bounded in bytes as well as in entries — the cache charges one unit per
/// entry however many bytes it holds, so an unbounded label would make a
/// bounded entry count buy nothing.
pub(crate) const MAX_LABEL_BYTES: usize = LABEL_CAPACITY - 1;

/// Reports whether a nonempty label fits the directory label bound.
pub(crate) fn label_fits(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_LABEL_BYTES
}

/// The host a node publishes for its peers to dial. Every host that reaches the
/// directory is bounded by [`MAX_LABEL_BYTES`], so a resolved address stays off
/// the response path's allocator.
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
#[cfg_attr(test, derive(strum::VariantArray))]
pub enum Preference {
    /// The address the node discovered for itself on its own network.
    Direct,
    /// The entry point that reaches the node from another network.
    Advertised,
}

/// The endpoints one node may be dialed on, in the order the rules put them.
///
/// Never more than two, and the second exists only where a failed first attempt
/// has somewhere else to go. [`choose_route`] is the only way to build one.
#[derive(Clone, Debug)]
pub(crate) struct Route {
    first: (Preference, Endpoint),
    second: Option<(Preference, Endpoint)>,
}

/// This process's node id and the request registry that serves it.
///
/// Both values exist together. Thus, a production router always has a local
/// delivery path, and no caller can pair its node with another registry.
#[derive(Clone)]
pub(crate) struct LocalTarget {
    node: NodeId,
    registry: Arc<PendingRegistry>,
}

impl LocalTarget {
    /// Binds one process identity to its request registry.
    pub(in crate::router) fn new(node: NodeId, registry: Arc<PendingRegistry>) -> Self {
        Self { node, registry }
    }

    /// Whether this target owns `node`.
    pub(crate) fn owns(&self, node: NodeId) -> bool {
        self.node == node
    }

    /// This process's node id.
    pub(crate) const fn node(&self) -> NodeId {
        self.node
    }

    /// The request registry bound to this node id.
    pub(crate) const fn pending(&self) -> &Arc<PendingRegistry> {
        &self.registry
    }

    /// Deposits one same-node response into this process's registry.
    pub(crate) fn accept(&self, frame: ResponseFrame) -> ResponseDisposition {
        self.registry.accept(frame)
    }
}

/// One frame, as bytes on the wire.
///
/// The router delivers frames without reading them, which is what keeps
/// response vocabulary out of this module.
pub(crate) trait Framed: Clone + Send + 'static {
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
/// The frame is borrowed so retries can share it. A transport can clone its
/// immutable handles when its encoder requires an owned item.
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
/// it rather than by [`NetworkRouter`]: [`NetworkRouter::route`] is the one
/// function that applies the operator's rules, so a forward that consulted them
/// does not compile.
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
}

/// Everything the response path needs to reach a peer: every endpoint a node
/// may be dialed on, the transport that dials them, and the shared destination
/// fleet.
///
/// One trait rather than three type parameters, so every signature on the
/// response path names one `R`. Address resolution belongs here, with the
/// route call that can await it.
pub(crate) trait NetworkRouter: RelayHop {
    /// Returns the bounded delivery state for `node`.
    fn destination(&self, node: NodeId) -> Arc<Destination>;

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

/// The production [`NetworkRouter`]: cached peer addresses, one remote
/// transport, and the process's destination fleet.
pub(crate) struct NetworkRoute<S, D> {
    addresses: AddressResolver<D>,
    fleet: Arc<DestinationFleet>,
    transport: Arc<S>,
    here: Option<NetworkId>,
}

impl NodeId {
    /// Mints an id for one incarnation of one process.
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
impl<S, D: Clone> Clone for NetworkRoute<S, D> {
    fn clone(&self) -> Self {
        Self {
            addresses: self.addresses.clone(),
            fleet: Arc::clone(&self.fleet),
            transport: Arc::clone(&self.transport),
            here: self.here.clone(),
        }
    }
}

impl<S, D> NetworkRoute<S, D> {
    /// Binds one process's resolver, fleet and transport together.
    pub(in crate::router) fn new(
        addresses: AddressResolver<D>,
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

impl<S: ResponseSender, D: NodeDirectory> RelayHop for NetworkRoute<S, D> {
    type Error = D::Error;
    type Sender = S;

    async fn direct(&self, node: NodeId) -> Result<Option<Endpoint>, D::Error> {
        let registration = self.addresses.resolve(node).await?;
        Ok(registration.map(|registration| registration.direct.clone()))
    }

    fn sender(&self) -> &S {
        &self.transport
    }
}

impl<S: ResponseSender, D: NodeDirectory> NetworkRouter for NetworkRoute<S, D> {
    fn destination(&self, node: NodeId) -> Arc<Destination> {
        self.fleet.destination(node)
    }

    async fn route(&self, node: NodeId) -> Result<Option<Route>, D::Error> {
        let registration = self.addresses.resolve(node).await?;
        Ok(registration
            .as_deref()
            .and_then(|registration| choose_route(self.here.as_ref(), registration)))
    }
}

impl SendFailure {
    /// Whether this attempt got no proof that this address serves the node, so
    /// the other endpoint is worth trying inside the same response.
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
    /// that read the frame after this response's deadline, so
    /// it is that peer speaking about the whole path rather than about the
    /// address. The other endpoint has no more time to spend than this one had.
    pub(crate) const fn is_wrong_endpoint(self) -> bool {
        match self {
            Self::Unreachable
            | Self::Expired
            | Self::Status(Code::Unavailable | Code::Unimplemented | Code::Cancelled) => true,
            Self::Status(_) => false,
        }
    }
}

impl Preference {
    /// The fixed metric attribute this preference is counted and traced under.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Advertised => "advertised",
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

/// Why one delivery attempt did not succeed.
///
/// [`Status`](Self::Status) carries the gRPC status the attempt came to, rather
/// than a code of this crate's own, for the reason [`crate::router::grpc`]
/// states.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SendFailure {
    /// The attempt came to a gRPC status other than `OK`. It may be what
    /// something at the address answered, or what this transport produced on
    /// its own, and which process at that address answered is not knowable
    /// here. [`Self::is_wrong_endpoint`] says which of these codes make the
    /// other endpoint worth trying.
    #[error("the attempt came to {0:?}")]
    Status(Code),

    /// Nothing answered before the send gave up. The frame may never have left
    /// this process, or it may be in flight.
    #[error("nothing answered before the send gave up")]
    Unreachable,

    /// The deadline elapsed before the frame left this process, so nothing
    /// reached the destination and the destination said nothing.
    #[error("the send deadline elapsed before the frame left this process")]
    Expired,
}

#[cfg(test)]
mod tests;
