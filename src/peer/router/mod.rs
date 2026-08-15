//! Reaching any prosody process by id.
//!
//! Every peer feature routes through here. Remote paths know only a [`PeerId`]
//! and frame bytes. The local target owns this process's request registry.

use crate::peer::metrics::PeerMetrics;
use crate::peer::requester::registry::PendingRegistry;
use crate::peer::response::ResponseDisposition;
use crate::peer::response::frame::ResponseFrame;
use crate::peer::router::directory::cache::AddressResolver;
use crate::peer::router::directory::{Endpoint, NetworkId, PeerDirectory, PeerRegistration};
use bytes::BufMut;
use fixedstr::Flexstr;
use opentelemetry::Context;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tokio::time::Instant;
use tonic::Code;
use uuid::{Bytes as UuidBytes, Uuid};

pub(crate) mod api;
pub(crate) mod cache_config;
pub(crate) mod config;
pub(crate) mod directory;
pub(crate) mod grpc;
#[cfg(test)]
pub(crate) mod loopback;
pub(crate) mod relay;
pub(crate) mod runtime;

/// Inline capacity for common host and network labels.
const LABEL_CAPACITY: usize = 25;

/// The host label a peer publishes for diagnostics.
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
pub(crate) struct PeerId(Uuid);

/// Which peer endpoint a route selected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(strum::VariantArray))]
pub enum EndpointKind {
    /// The address the peer discovered for itself on its own network.
    Direct,
    /// The entry point that reaches the peer from another network.
    Advertised,
}

/// The one endpoint selected for a peer.
#[derive(Clone, Debug)]
pub(crate) struct Route {
    kind: EndpointKind,
    endpoint: Endpoint,
}

/// This process's peer id and the request registry that serves it.
///
/// Both values exist together. Thus, a production router always has a local
/// delivery path, and no caller can pair its peer with another registry.
#[derive(Clone)]
pub(crate) struct LocalTarget {
    peer: PeerId,
    registry: Arc<PendingRegistry>,
}

impl LocalTarget {
    /// Binds one process identity to its request registry.
    pub(in crate::peer::router) fn new(peer: PeerId, registry: Arc<PendingRegistry>) -> Self {
        Self { peer, registry }
    }

    /// Whether this target owns `peer`.
    pub(crate) fn owns(&self, peer: PeerId) -> bool {
        self.peer == peer
    }

    /// This process's peer id.
    pub(crate) const fn peer(&self) -> PeerId {
        self.peer
    }

    /// The request registry bound to this peer id.
    pub(crate) const fn pending(&self) -> &Arc<PendingRegistry> {
        &self.registry
    }

    /// Deposits one same-peer response into this process's registry.
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
        context: &Context,
    ) -> impl Future<Output = Result<(), SendFailure>> + Send;
}

/// What one forward reads: the endpoint a peer published for its neighbours,
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

    /// What can stop a peer id from becoming an address.
    type Error: Error + Send + Sync + 'static;

    /// The direct endpoint alone. This is the lookup a process uses when it
    /// sends a frame on to the process that frame names.
    ///
    /// # Errors
    ///
    /// Returns [`RelayHop::Error`] when the lookup itself failed.
    fn direct(
        &self,
        peer: PeerId,
    ) -> impl Future<Output = Result<Option<Endpoint>, Self::Error>> + Send;

    /// The transport.
    fn sender(&self) -> &Self::Sender;
}

/// Everything the response path needs to reach a peer: every endpoint a peer
/// may be dialed on, the transport that dials them, and the shared destination
/// fleet.
///
/// One trait rather than three type parameters, so every signature on the
/// response path names one `R`. Address resolution belongs here, with the
/// route call that can await it.
pub(crate) trait NetworkRouter: RelayHop {
    /// The instruments owned by this router's peer runtime.
    fn peer_metrics(&self) -> &PeerMetrics;

    /// The endpoint `peer` may be dialed on from this process. This
    /// is the responder's lookup, and [`choose_route`] decides what it answers.
    ///
    /// `None` means "do not dial", which covers both a peer the directory does
    /// not hold and one the rules refuse to reach from here.
    ///
    /// # Errors
    ///
    /// Returns [`RelayHop::Error`] when the lookup itself failed, which is
    /// distinct from a peer that is simply not published.
    fn route(
        &self,
        peer: PeerId,
    ) -> impl Future<Output = Result<Option<Route>, Self::Error>> + Send;
}

/// The production [`NetworkRouter`]: cached peer addresses, one remote
/// transport.
pub(crate) struct NetworkRoute<S, D> {
    addresses: AddressResolver<D>,
    transport: Arc<S>,
    here: Option<NetworkId>,
    metrics: PeerMetrics,
}

impl PeerId {
    /// Mints an id for one incarnation of one process.
    pub(in crate::peer::router) fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Reads an id from its 16-byte wire form.
    pub(crate) const fn from_bytes(bytes: UuidBytes) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// The 16-byte wire form.
    pub(crate) const fn into_bytes(self) -> UuidBytes {
        self.0.into_bytes()
    }
}

/// The directory stores a peer id in a Cassandra `uuid` column, so the driver's
/// own `Uuid` serde carries it. This conversion is the one place the newtype is
/// unwrapped for that purpose.
impl From<PeerId> for Uuid {
    fn from(peer: PeerId) -> Self {
        peer.0
    }
}

impl Display for PeerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self.0, f)
    }
}

/// Cloning shares the cache and transport rather than copying them.
impl<S, D: Clone> Clone for NetworkRoute<S, D> {
    fn clone(&self) -> Self {
        Self {
            addresses: self.addresses.clone(),
            transport: Arc::clone(&self.transport),
            here: self.here.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<S, D> NetworkRoute<S, D> {
    /// Binds one process's resolver and transport together.
    pub(in crate::peer::router) fn new(
        addresses: AddressResolver<D>,
        transport: Arc<S>,
        here: Option<NetworkId>,
        metrics: PeerMetrics,
    ) -> Self {
        Self {
            addresses,
            transport,
            here,
            metrics,
        }
    }
}

impl<S: ResponseSender, D: PeerDirectory> RelayHop for NetworkRoute<S, D> {
    type Error = D::Error;
    type Sender = S;

    async fn direct(&self, peer: PeerId) -> Result<Option<Endpoint>, D::Error> {
        let registration = self.addresses.resolve(peer).await?;
        Ok(registration.map(|registration| registration.direct.endpoint().clone()))
    }

    fn sender(&self) -> &S {
        &self.transport
    }
}

impl<S: ResponseSender, D: PeerDirectory> NetworkRouter for NetworkRoute<S, D> {
    fn peer_metrics(&self) -> &PeerMetrics {
        &self.metrics
    }

    async fn route(&self, peer: PeerId) -> Result<Option<Route>, D::Error> {
        let registration = self.addresses.resolve(peer).await?;
        Ok(registration
            .as_deref()
            .and_then(|registration| choose_route(self.here.as_ref(), registration)))
    }
}

impl EndpointKind {
    /// The fixed metric and trace label for this endpoint kind.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Advertised => "advertised",
        }
    }
}

impl Route {
    /// Returns the selected endpoint and its route type.
    pub(crate) fn endpoint(&self) -> (EndpointKind, &Endpoint) {
        (self.kind, &self.endpoint)
    }
}

/// The endpoints `registration` is dialed on from a process labelled `here`.
///
/// A label names the set of processes that reach each other on their direct
/// endpoints. An operator declares it; nothing infers it. Three rules follow
/// from that, and this is the one function in the crate that reads a label:
///
/// - **Both present and equal.** Dial `direct`. Neighbours skip the entry
///   point, which matters less for latency than for load.
/// - **Both present and unequal.** Dial `advertised` alone, and `None` when the
///   peer published none. The peer is known to be elsewhere, so its direct
///   address is a foreign one that most likely belongs to something unrelated
///   here. Refusing to dial is only expressible because the labels were
///   declared.
/// - **Either absent.** Dial `direct`. With nothing configured anywhere, every
///   peer uses its direct address. This is the single-network default.
///
/// `None` means "do not dial".
pub(crate) fn choose_route(
    here: Option<&NetworkId>,
    registration: &PeerRegistration,
) -> Option<Route> {
    match (here, registration.network.as_ref()) {
        (Some(here), Some(there)) if here != there => {
            registration.advertised.as_ref().map(|endpoint| Route {
                kind: EndpointKind::Advertised,
                endpoint: endpoint.clone(),
            })
        }
        _ => Some(Route {
            kind: EndpointKind::Direct,
            endpoint: registration.direct.endpoint().clone(),
        }),
    }
}

/// Why one delivery attempt did not succeed.
///
/// [`Status`](Self::Status) carries the gRPC status that the attempt reached.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SendFailure {
    /// The attempt came to a gRPC status other than `OK`.
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
