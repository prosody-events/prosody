//! What a process does with a frame it is not the target of.
//!
//! Two shapes of subject live here. The in-process one drives the real peer
//! service over the loopback transport, so a routing decision is observed
//! without a socket. The wire one binds two real listeners, because a loop, a
//! deadline crossing a hop, and a trace crossing a hop are all facts about two
//! processes and cannot be seen inside one.

mod capacity;
mod decision;
mod forward;
mod wire;

use super::Relay;
use crate::codec::Codec;
use crate::requester::config::RequesterConfiguration;
use crate::requester::registry::PendingRegistry;
use crate::response::frame::tests::CountingCodec;
use crate::response::frame::{FrameCap, FrameHeader, ResponseFrame};
use crate::response::{FormatToken, RequestId, ResponseStatus};
use crate::router::directory::{Endpoint, NetworkId, NodeRegistration};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::client::GrpcSender;
use crate::router::grpc::generated::peer_server::Peer;
use crate::router::grpc::service::PeerService;
use crate::router::grpc::{BoundListener, TransportConfiguration, serve};
use crate::router::loopback::{Delivery, TestHealth, TestRouter, node};
use crate::router::{Host, NodeId, RelayHop, Route, Router, choose_route};
use crate::subsystem::SubsystemName;
use bytes::BytesMut;
use color_eyre::Result;
use std::convert::Infallible;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::slice::from_ref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::{Sender, channel};
use tokio::task::JoinHandle;
use tonic::{Code, Request};

/// The node the in-process suites run as. The test router publishes it, so a
/// case that accepts is not one that simply resolved nothing.
pub(super) const THIS: u8 = 0;

/// The frame ceiling every suite here encodes and forwards under.
pub(super) const CAP_BYTES: usize = 4096;

/// The ceiling one process puts on one forward. Far longer than any suite here
/// takes, except where a case states a shorter budget of its own.
pub(super) const BUDGET: Duration = Duration::from_secs(30);

/// The subsystem every request in these suites awaits.
pub(super) const ALPHA: &str = "alpha";

/// The body every frame in these suites carries.
pub(super) const PAYLOAD: &[u8] = b"relayed";

/// One process under test, driven without a socket.
///
/// The service is the real one, so a case reads the routing decision from what
/// the registry holds, what the transport recorded, and what the call answered
/// — never from the decision function itself.
pub(super) struct Process {
    pub(super) node: NodeId,
    pub(super) registry: Arc<PendingRegistry>,
    pub(super) router: TestRouter,
    pub(super) deliveries: UnboundedReceiver<Delivery>,
    service: PeerService<TestRouter>,
}

/// Two live listeners: one that forwards, and the one it forwards to.
pub(super) struct Pair {
    pub(super) relay: Live,
    pub(super) target: Live,
}

/// One live listener, and what a caller needs to reach it.
pub(super) struct Live {
    pub(super) node: NodeId,
    pub(super) registry: Arc<PendingRegistry>,
    pub(super) address: Endpoint,
    stop: Option<Sender<()>>,
    served: Option<JoinHandle<()>>,
}

/// Where the target process's own router points.
pub(super) enum TargetRoute {
    /// Back at the relay, so a frame neither process owns would pass between
    /// them for as long as anything let it.
    Relay,
    /// Nowhere. The target then forwards nothing and reaches nothing.
    Nowhere,
}

/// A router that resolves every node to one registration, or to none at all.
///
/// One registration for every node is not a contrivance: it is what a stale
/// directory entry looks like, which is the case forwarding exists for. `here`
/// is the label the process holding this router was configured with, so
/// [`Router::route`] applies the declared rules exactly as the production
/// router does.
#[derive(Clone)]
pub(super) struct FixedRouter {
    fleet: Arc<DestinationFleet>,
    transport: Arc<GrpcSender>,
    registration: Option<NodeRegistration>,
    here: Option<NetworkId>,
}

impl Process {
    /// Builds one process over a fleet of `config`, with `budget` as its own
    /// ceiling on a forward.
    pub(super) fn new(config: FleetConfiguration, budget: Duration) -> Result<Self> {
        let node = node(THIS);
        let (router, deliveries) = TestRouter::new(config)?;
        let registry = PendingRegistry::new(&RequesterConfiguration::default())?;
        let service = PeerService::new(
            node,
            Arc::clone(&registry),
            Relay::new(router.clone()),
            FrameCap::new(CAP_BYTES)?,
            budget,
        );
        Ok(Self {
            node,
            registry,
            router,
            deliveries,
            service,
        })
    }

    /// Registers one request this process waits for.
    pub(super) fn expects(&self) -> Result<RequestId> {
        Ok(self.registry.register_unguarded(
            from_ref(&SubsystemName::try_new(ALPHA)?),
            CountingCodec::FORMAT_ID,
            BUDGET,
        )?)
    }

    /// Whether this process stored the response `request` waited for.
    pub(super) fn stored(&self, request: RequestId) -> Result<bool> {
        Ok(self
            .registry
            .stored_payload(request, &SubsystemName::try_new(ALPHA)?)
            .is_some())
    }

    /// Hands `frame` to the service and reports the status it answered.
    ///
    /// `granted` is the budget the caller states, exactly as a caller over a
    /// socket states it.
    pub(super) async fn deliver(
        &self,
        frame: ResponseFrame,
        granted: Option<Duration>,
    ) -> Result<Code> {
        let mut request = Request::new(frame);
        if let Some(granted) = granted {
            request.set_timeout(granted);
        }
        Ok(match self.service.deliver_response(request).await {
            Ok(_) => Code::Ok,
            Err(status) => status.code(),
        })
    }

    /// The next attempt the transport recorded, if it recorded one.
    ///
    /// Every attempt is recorded before it is answered, and the call under test
    /// has already returned, so an empty channel means no attempt was made
    /// rather than that one is still coming.
    pub(super) fn recorded(&mut self) -> Option<Delivery> {
        self.deliveries.try_recv().ok()
    }
}

impl Pair {
    /// Binds and serves two listeners: a relay that resolves every node to the
    /// target, and a target whose own router points where `route` says.
    ///
    /// Each process is given its own ceiling on one forward, because what one
    /// hop hands the next is only readable where the two differ.
    ///
    /// Both are bound before either is served, because the two routers name
    /// each other.
    pub(super) async fn start(
        relaying: Duration,
        targeted: Duration,
        route: TargetRoute,
    ) -> Result<Self> {
        let relay_bound = bind().await?;
        let target_bound = bind().await?;
        let relay_address = endpoint(&relay_bound);
        let target_address = endpoint(&target_bound);
        let relay = Live::serve(relay_bound, Some(target_address), relaying)?;
        let seen = match route {
            TargetRoute::Relay => Some(relay_address),
            TargetRoute::Nowhere => None,
        };
        match Live::serve(target_bound, seen, targeted) {
            Ok(target) => Ok(Self { relay, target }),
            Err(error) => {
                relay.stop().await?;
                Err(error)
            }
        }
    }

    /// Stops both listeners, whatever either one answered.
    pub(super) async fn stop(self) -> Result<()> {
        let relayed = self.relay.stop().await;
        self.target.stop().await?;
        relayed
    }
}

impl Live {
    /// Serves `bound`, sending every frame it does not own on to `seen`.
    fn serve(bound: BoundListener, seen: Option<Endpoint>, budget: Duration) -> Result<Self> {
        let node = NodeId::new();
        let address = endpoint(&bound);
        let cap = bound.frame_cap();
        let registry = PendingRegistry::new(&RequesterConfiguration::default())?;
        let router = FixedRouter::new(cap, seen.map(registration), None)?;
        let (stop, stopped) = channel();
        let served = serve(
            bound,
            PeerService::new(node, Arc::clone(&registry), Relay::new(router), cap, budget),
            TestHealth::new(true, true),
            async move { stopped.await.unwrap_or(()) },
        )?;
        Ok(Self {
            node,
            registry,
            address,
            stop: Some(stop),
            served: Some(served),
        })
    }

    /// Stops this listener and waits for it to finish.
    async fn stop(mut self) -> Result<()> {
        drop(self.stop.take());
        if let Some(served) = self.served.take() {
            served.await?;
        }
        Ok(())
    }
}

impl FixedRouter {
    /// A router over its own fleet and transport, resolving every node to
    /// `registration` from a process labelled `here`.
    pub(super) fn new(
        cap: FrameCap,
        registration: Option<NodeRegistration>,
        here: Option<NetworkId>,
    ) -> Result<Self> {
        let fleet = Arc::new(DestinationFleet::new(FleetConfiguration::default())?);
        Ok(Self {
            transport: Arc::new(GrpcSender::new(cap, &fleet)),
            fleet,
            registration,
            here,
        })
    }
}

impl Router for FixedRouter {
    fn route(
        &self,
        _node: NodeId,
    ) -> impl Future<Output = Result<Option<Route>, Infallible>> + Send {
        let route = self
            .registration
            .as_ref()
            .and_then(|registration| choose_route(self.here.as_ref(), registration));
        async move { Ok(route) }
    }
}

impl RelayHop for FixedRouter {
    type Error = Infallible;
    type Sender = GrpcSender;

    fn direct(
        &self,
        _node: NodeId,
    ) -> impl Future<Output = Result<Option<Endpoint>, Infallible>> + Send {
        let direct = self
            .registration
            .as_ref()
            .map(|registration| registration.direct.clone());
        async move { Ok(direct) }
    }

    fn sender(&self) -> &GrpcSender {
        &self.transport
    }

    fn fleet(&self) -> &Arc<DestinationFleet> {
        &self.fleet
    }
}

/// One frame for `target`, answering `request`, naming `relay`.
pub(super) fn frame(
    target: NodeId,
    request: RequestId,
    relay: Option<NodeId>,
) -> Result<ResponseFrame> {
    Ok(ResponseFrame {
        header: FrameHeader {
            target,
            request,
            subsystem: SubsystemName::try_new(ALPHA)?,
            status: ResponseStatus::Success,
            relay,
        },
        format: FormatToken::make(CountingCodec::FORMAT_ID),
        payload: BytesMut::from(PAYLOAD),
    })
}

/// A listener bound on a port the operating system chooses.
async fn bind() -> Result<BoundListener> {
    Ok(BoundListener::bind(&TransportConfiguration {
        bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        frame_cap: FrameCap::new(CAP_BYTES)?,
        ..TransportConfiguration::default()
    })
    .await?)
}

/// Where a bound listener is, as a peer dials it.
fn endpoint(bound: &BoundListener) -> Endpoint {
    Endpoint {
        host: Host::make("127.0.0.1"),
        port: bound.address().port(),
    }
}

/// A registration publishing `direct` and nothing else.
fn registration(direct: Endpoint) -> NodeRegistration {
    NodeRegistration {
        node: NodeId::new(),
        direct,
        advertised: None,
        network: None,
        group: None,
        hostname: Host::make("relay-suite"),
    }
}
