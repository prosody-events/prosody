//! What a process does with a frame it is not the target of.
//!
//! Two shapes of subject live here. The in-process one drives the real peer
//! service over the loopback transport, so a routing decision is observed
//! without a socket. The wire one binds two real listeners, because a loop, a
//! deadline crossing a hop, and a trace crossing a hop are all facts about two
//! processes and cannot be seen inside one.

mod decision;
mod forward;
mod wire;

use super::Relay;
use crate::codec::Codec;
use crate::requester::registry::PendingRegistry;
use crate::requester::registry::tests::TestRegistration;
use crate::response::frame::tests::CountingCodec;
use crate::response::frame::{FrameHeader, ResponseFrame};
use crate::response::{FormatToken, RequestId, ResponseStatus};
use crate::router::directory::Endpoint;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::BoundListener;
use crate::router::grpc::generated::peer_service_server::PeerService as PeerServiceApi;
use crate::router::grpc::service::PeerService;
use crate::router::loopback::listener::{FixedRouter, Served, bind, endpoint};
use crate::router::loopback::{Delivery, TestRouter, node, registration};
use crate::router::{LocalTarget, NodeId};
use crate::subsystem::SubsystemName;
use bytes::Bytes;
use color_eyre::Result;
use std::slice::from_ref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tonic::{Code, Request};

/// The node the in-process suites run as. The test router publishes it, so a
/// case that accepts is not one that simply resolved nothing.
pub(super) const THIS: u8 = 0;

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
    served: Served,
}

/// Where the target process's own router points.
pub(super) enum TargetRoute {
    /// Back at the relay, so a frame neither process owns would pass between
    /// them for as long as anything let it.
    Relay,
    /// Nowhere. The target then forwards nothing and reaches nothing.
    Nowhere,
}

impl Process {
    /// Builds one process over a fleet of `config`.
    pub(super) fn new(config: FleetConfiguration) -> Result<Self> {
        let node = node(THIS);
        let (router, deliveries) = TestRouter::new(config)?;
        let registry = PendingRegistry::new();
        let service = PeerService::new(
            LocalTarget::new(node, Arc::clone(&registry)),
            Relay::new(router.clone()),
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
    pub(super) fn expects(&self) -> Result<TestRegistration> {
        TestRegistration::new(
            &self.registry,
            from_ref(&SubsystemName::try_new(ALPHA)?),
            BUDGET,
        )
    }

    /// Hands `frame` to the service and reports the status it answered.
    ///
    /// `granted` is the budget the caller states, exactly as a caller over a
    /// socket states it.
    pub(super) async fn deliver(&self, frame: ResponseFrame, granted: Duration) -> Result<Code> {
        let mut request = Request::new(frame);
        request.set_timeout(granted);
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
    /// Both are bound before either is served, because the two routers name
    /// each other.
    pub(super) async fn start(route: TargetRoute) -> Result<Self> {
        let relay_bound = bind().await?;
        let target_bound = bind().await?;
        let relay_address = endpoint(&relay_bound)?;
        let target_address = endpoint(&target_bound)?;
        let relay = Live::serve(relay_bound, Some(target_address))?;
        let seen = match route {
            TargetRoute::Relay => Some(relay_address),
            TargetRoute::Nowhere => None,
        };
        match Live::serve(target_bound, seen) {
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
    fn serve(bound: BoundListener, seen: Option<Endpoint>) -> Result<Self> {
        let node = NodeId::new();
        let address = endpoint(&bound)?;
        let registry = PendingRegistry::new();
        let router = FixedRouter::new(FleetConfiguration::default(), seen.map(registration), None)?;
        let served = Served::start(
            bound,
            PeerService::new(
                LocalTarget::new(node, Arc::clone(&registry)),
                Relay::new(router),
            ),
        )?;
        Ok(Self {
            node,
            registry,
            address,
            served,
        })
    }

    /// Stops this listener and waits for it to finish.
    async fn stop(self) -> Result<()> {
        Ok(self.served.stop().await?)
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
        payload: Bytes::from_static(PAYLOAD),
    })
}
