//! A real peer listener, for the suites that need a socket: how one is bound,
//! how it is served and stopped, and the router a served process reaches its
//! neighbours through.

use crate::router::directory::{Endpoint, NetworkId, PeerRegistration};
use crate::router::fleet::config::FleetConfiguration;
use crate::router::fleet::{Destination, DestinationFleet};
use crate::router::grpc::client::GrpcSender;
use crate::router::grpc::service::PeerService;
use crate::router::grpc::{BoundListener, serve};
use crate::router::{NetworkRouter, PeerId, RelayHop, Route, choose_route};
use color_eyre::Result;
use std::convert::Infallible;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::oneshot::{Sender, channel};
use tokio::task::{JoinError, JoinHandle};
use tonic::transport::Error as TransportError;

/// A served peer listener and the handles that stop it.
///
/// Every suite that binds a real listener serves and stops it the same way;
/// only the service differs. Stopping takes `self`, so a second stop is
/// unwritable.
pub(crate) struct Served {
    stop: Sender<()>,
    task: JoinHandle<()>,
}

/// A router that resolves every peer to one registration, or to none at all.
///
/// One registration for every peer is not a contrivance: it is what a stale
/// directory entry looks like, which is the case forwarding exists for. It is
/// also the shape a suite needs to drive a real sender all the way to a
/// listener. `here` is the label the process holding this router was configured
/// with, so [`NetworkRouter::route`] applies the declared rules exactly as the
/// production router does.
#[derive(Clone)]
pub(crate) struct FixedRouter {
    fleet: Arc<DestinationFleet>,
    transport: Arc<GrpcSender>,
    registration: Option<Arc<PeerRegistration>>,
    here: Option<NetworkId>,
}

impl Served {
    /// Serves `service` on `bound`, reporting ready and live.
    pub(crate) fn start<R: RelayHop>(
        bound: BoundListener,
        service: PeerService<R>,
    ) -> Result<Self> {
        let (stop, stopped) = channel();
        let task = serve(bound, service, async move {
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
    /// A router over its own fleet and transport, resolving every peer to
    /// `registration` from a process labelled `here`.
    pub(crate) fn new(
        config: FleetConfiguration,
        registration: Option<PeerRegistration>,
        here: Option<NetworkId>,
    ) -> Result<Self> {
        let fleet = Arc::new(DestinationFleet::new(config)?);
        Ok(Self {
            transport: Arc::new(GrpcSender::new(&fleet)),
            fleet,
            registration: registration.map(Arc::new),
            here,
        })
    }
}

impl NetworkRouter for FixedRouter {
    fn destination(&self, peer: PeerId) -> Arc<Destination> {
        self.fleet.destination(peer)
    }

    fn route(
        &self,
        _peer: PeerId,
    ) -> impl Future<Output = Result<Option<Route>, Infallible>> + Send {
        let route = self
            .registration
            .as_ref()
            .and_then(|registration| choose_route(self.here.as_ref(), Arc::clone(registration)));
        async move { Ok(route) }
    }
}

impl RelayHop for FixedRouter {
    type Error = Infallible;
    type Sender = GrpcSender;

    fn direct(
        &self,
        _peer: PeerId,
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

/// The loopback address with an operating-system-selected port.
pub(crate) fn bind_address() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

/// A listener bound on [`bind_address`].
pub(crate) async fn bind() -> Result<BoundListener> {
    Ok(BoundListener::bind(bind_address()).await?)
}

/// Where a bound listener is, as a peer on this machine dials it.
pub(crate) fn endpoint(bound: &BoundListener) -> Result<Endpoint, TransportError> {
    Endpoint::from_shared(format!("http://{}", bound.address()))
}
