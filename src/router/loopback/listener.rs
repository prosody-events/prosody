//! A real peer listener, for the suites that need a socket: how one is bound,
//! how it is served and stopped, and the router a served process reaches its
//! neighbours through.

use super::TestHealth;
use crate::response::frame::FrameCap;
use crate::router::directory::{Endpoint, NetworkId, NodeRegistration};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::client::GrpcSender;
use crate::router::grpc::service::PeerService;
use crate::router::grpc::{BoundListener, TransportConfiguration, serve};
use crate::router::{Host, NodeId, RelayHop, Route, Router, choose_route};
use color_eyre::Result;
use std::convert::Infallible;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::oneshot::{Sender, channel};
use tokio::task::{JoinError, JoinHandle};

/// A served peer listener and the handles that stop it.
///
/// Every suite that binds a real listener serves and stops it the same way;
/// only the service differs. Stopping takes `self`, so a second stop is
/// unwritable.
pub(crate) struct Served {
    stop: Sender<()>,
    task: JoinHandle<()>,
}

/// A router that resolves every node to one registration, or to none at all.
///
/// One registration for every node is not a contrivance: it is what a stale
/// directory entry looks like, which is the case forwarding exists for. It is
/// also the shape a suite needs to drive a real sender all the way to a
/// listener. `here` is the label the process holding this router was configured
/// with, so [`Router::route`] applies the declared rules exactly as the
/// production router does.
#[derive(Clone)]
pub(crate) struct FixedRouter {
    fleet: Arc<DestinationFleet>,
    transport: Arc<GrpcSender>,
    registration: Option<NodeRegistration>,
    here: Option<NetworkId>,
}

impl Served {
    /// Serves `service` on `bound`, reporting ready and live.
    pub(crate) fn start<R: RelayHop>(
        bound: BoundListener,
        service: PeerService<R>,
    ) -> Result<Self> {
        let (stop, stopped) = channel();
        let task = serve(bound, service, TestHealth::new(true, true), async move {
            stopped.await.unwrap_or(());
        })?;
        Ok(Self { stop, task })
    }

    /// Stops the listener and waits for it to finish.
    ///
    /// The listener task logs its own serve error, so a join failure here is a
    /// panic inside that task and is reported rather than dropped.
    pub(crate) async fn stop(self) -> Result<(), JoinError> {
        drop(self.stop);
        self.task.await
    }
}

impl FixedRouter {
    /// A router over its own fleet and transport, resolving every node to
    /// `registration` from a process labelled `here`.
    pub(crate) fn new(
        cap: FrameCap,
        config: FleetConfiguration,
        registration: Option<NodeRegistration>,
        here: Option<NetworkId>,
    ) -> Result<Self> {
        let fleet = Arc::new(DestinationFleet::new(config)?);
        Ok(Self {
            transport: Arc::new(GrpcSender::new(cap, &fleet)),
            fleet,
            registration,
            here,
        })
    }
}

impl Router for FixedRouter {
    #[cfg(test)]
    fn fleet(&self) -> &Arc<DestinationFleet> {
        &self.fleet
    }

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
}

/// A listener configuration on the loopback interface, on a port the operating
/// system chooses, with `cap` as its frame ceiling.
pub(crate) fn transport(cap: usize) -> Result<TransportConfiguration> {
    Ok(TransportConfiguration {
        bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        frame_cap: FrameCap::new(cap)?,
        ..TransportConfiguration::default()
    })
}

/// A listener bound on the loopback interface under [`transport`].
pub(crate) async fn bind(cap: usize) -> Result<BoundListener> {
    Ok(BoundListener::bind(&transport(cap)?).await?)
}

/// Where a bound listener is, as a peer on this machine dials it.
pub(crate) fn endpoint(bound: &BoundListener) -> Endpoint {
    Endpoint {
        host: Host::make("127.0.0.1"),
        port: bound.address().port(),
    }
}
