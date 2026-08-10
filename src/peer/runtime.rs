//! Router construction and request admission.

use crate::Codec;
#[cfg(test)]
use crate::PeerConfiguration;
use crate::consumer::{ConsumerError, PeerInitError, ShutdownError};
#[cfg(test)]
use crate::peer::PeerBackend;
use crate::peer::heartbeat_registry;
use crate::producer::ProsodyProducer;
use crate::requester::ProsodyRequester;
use crate::response::sender::{ResponseRoute, Then};
#[cfg(test)]
use crate::router::NodeId;
use crate::router::config::PeerParts;
use crate::router::directory::NodeDirectory;
use crate::router::grpc::BoundListener;
use crate::router::grpc::client::GrpcSender;
use crate::router::runtime::{
    LocalPeerRuntime, PeerInputs, PeerRuntime, PreparedLocalPeerRuntime, PreparedPeerRuntime,
};
use crate::router::{LocalTarget, NetworkRoute};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::error;

mod local_route {
    use crate::response::sender::{ResponseRoute, Then};
    use crate::router::LocalTarget;

    pub trait Sealed {}

    impl Sealed for LocalTarget {}
    impl<R: ResponseRoute> Sealed for Then<LocalTarget, R> {}
}

/// A response route that always tries the local node first.
pub(crate) trait LocalRoute: local_route::Sealed + ResponseRoute {
    /// The local target that owns this route's request identity.
    fn local(&self) -> &LocalTarget;
}

impl LocalRoute for LocalTarget {
    fn local(&self) -> &LocalTarget {
        self
    }
}
impl<R: ResponseRoute> LocalRoute for Then<LocalTarget, R> {
    fn local(&self) -> &LocalTarget {
        &self.0
    }
}

/// Request identity and pending requests from one router.
#[derive(Clone)]
pub struct ProducerHandle {
    local: LocalTarget,
}

/// One running router with its request, response, and lifecycle capabilities.
pub(crate) struct PeerRouter<R: LocalRoute> {
    producer: ProducerHandle,
    route: R,
    /// Sending or dropping asks the coordinator to stop.
    stop: oneshot::Sender<()>,
    /// The explicit teardown result.
    coordinator: JoinHandle<Result<(), ShutdownError>>,
}

/// A prepared runtime selected by the backend type.
pub(crate) trait PreparedRuntime: Sized + Send {
    type Running: RunningRuntime;
    type Route: LocalRoute;

    fn prepared_route(&self) -> Self::Route;
    fn launch(self) -> impl Future<Output = Result<Self::Running, (Self, ConsumerError)>> + Send;
    fn release(self) -> impl Future<Output = ()> + Send;
}

/// A running runtime with the common shutdown contract.
pub(crate) trait RunningRuntime: Sized + Send + 'static {
    fn stop<F, Fut>(self, drain: F) -> impl Future<Output = Result<(), ShutdownError>> + Send
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = ()> + Send;
}

/// Prepared runtime selected by `B`.
#[cfg(test)]
pub(crate) type RuntimeFor<B> = <B as PeerBackend>::Runtime;

/// Response route selected by `B`.
#[cfg(test)]
pub(crate) type RouteFor<B> = <RuntimeFor<B> as PreparedRuntime>::Route;

impl<R: LocalRoute> PeerRouter<R> {
    #[cfg(test)]
    pub(crate) const fn node(&self) -> NodeId {
        self.producer.local.node()
    }

    pub(crate) fn producer_handle(&self) -> ProducerHandle {
        self.producer.clone()
    }

    pub(crate) fn route(&self) -> R
    where
        R: Clone,
    {
        self.route.clone()
    }

    pub(crate) async fn shutdown_runtime(self) -> Result<(), ShutdownError> {
        self.producer.local.pending().close_admission();
        let _ = self.stop.send(());
        self.coordinator.await.map_err(|error| {
            error!(%error, "peer coordinator did not stop cleanly");
            ShutdownError::Teardown
        })?
    }
}

impl ProducerHandle {
    /// Builds request access for a producer.
    #[must_use]
    pub fn requester<C: Codec, RC: Codec>(
        &self,
        producer: ProsodyProducer<C>,
    ) -> ProsodyRequester<C, RC> {
        ProsodyRequester::new(
            producer,
            self.local.node(),
            Arc::clone(self.local.pending()),
        )
    }
}

impl<D: NodeDirectory> PreparedRuntime for PreparedPeerRuntime<D> {
    type Route = Then<LocalTarget, NetworkRoute<GrpcSender, D>>;
    type Running = PeerRuntime<D>;

    fn prepared_route(&self) -> Self::Route {
        self.response_route()
    }

    async fn launch(self) -> Result<Self::Running, (Self, ConsumerError)> {
        self.activate().await.map_err(|(prepared, error)| {
            (
                prepared,
                PeerInitError::Directory {
                    message: format!("{error:#}"),
                }
                .into(),
            )
        })
    }

    async fn release(self) {
        self.abandon().await;
    }
}

impl PreparedRuntime for PreparedLocalPeerRuntime {
    type Route = LocalTarget;
    type Running = LocalPeerRuntime;

    fn prepared_route(&self) -> Self::Route {
        self.response_route()
    }

    async fn launch(self) -> Result<Self::Running, (Self, ConsumerError)> {
        Ok(self.activate())
    }

    async fn release(self) {
        self.abandon();
    }
}

impl<D: NodeDirectory> RunningRuntime for PeerRuntime<D> {
    async fn stop<F, Fut>(self, drain: F) -> Result<(), ShutdownError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = ()> + Send,
    {
        self.shutdown(drain)
            .await
            .map_err(|error| ShutdownError::Directory {
                message: format!("{error:#}"),
            })
    }
}

impl RunningRuntime for LocalPeerRuntime {
    async fn stop<F, Fut>(self, drain: F) -> Result<(), ShutdownError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = ()> + Send,
    {
        self.shutdown(drain).await;
        Ok(())
    }
}

pub(crate) async fn prepare_network<D: NodeDirectory>(
    parts: PeerParts,
    directory: D,
) -> Result<PreparedPeerRuntime<D>, ConsumerError> {
    let listener =
        BoundListener::bind(parts.bind)
            .await
            .map_err(|error| PeerInitError::Listener {
                message: format!("{error:#}"),
            })?;
    PreparedPeerRuntime::start(PeerInputs {
        directory,
        listener,
        heartbeats: heartbeat_registry(),
        router: &parts.router,
        fleet: parts.fleet,
    })
    .await
    .map_err(PeerInitError::from)
    .map_err(Into::into)
}

pub(crate) fn prepare_local() -> PreparedLocalPeerRuntime {
    PreparedLocalPeerRuntime::start()
}

/// Builds and starts the router selected by `B`.
///
/// The listener binds first, so a misconfigured address fails in microseconds
/// and no other resource is live to release. Every later failure releases what
/// the earlier steps took.
///
/// Call this as the **last** fallible step of a mode. Every `?` that ran after
/// it would drop a served listener with no arm to release it.
///
/// # Errors
///
/// Returns [`ConsumerError::Peer`] when the configuration, the listener, the
/// directory, or the runtime refuses to start.
#[cfg(test)]
pub(crate) async fn prepare_router<B>(
    peer: &PeerConfiguration,
    backend: &B,
) -> Result<PeerRouter<RouteFor<B>>, ConsumerError>
where
    B: PeerBackend,
{
    let prepared = backend.prepare(peer).await?;
    start_router(prepared).await
}

pub(crate) async fn start_local_router() -> Result<PeerRouter<LocalTarget>, ConsumerError> {
    start_router(prepare_local()).await
}

pub(super) async fn start_router<P: PreparedRuntime>(
    prepared: P,
) -> Result<PeerRouter<P::Route>, ConsumerError> {
    let route = prepared.prepared_route();
    let runtime = match prepared.launch().await {
        Ok(runtime) => runtime,
        Err((prepared, error)) => {
            prepared.release().await;
            return Err(error);
        }
    };
    Ok(finish_router(route, runtime))
}

fn finish_router<R: LocalRoute, RT>(route: R, runtime: RT) -> PeerRouter<R>
where
    RT: RunningRuntime,
{
    let producer = ProducerHandle {
        local: route.local().clone(),
    };
    let (stop, stopped) = oneshot::channel();
    let coordinator = tokio::spawn(run_coordinator(runtime, stopped));
    PeerRouter {
        producer,
        route,
        stop,
        coordinator,
    }
}

/// Owns the peer runtime until its owner asks for the teardown.
///
/// The runtime moves in here, which is what keeps the directory type out of
/// [`ProsodyConsumer`](crate::consumer::ProsodyConsumer). A dropped stop sender
/// is also a request to stop, so a consumer dropped without a shutdown still
/// tears the peer down. Every teardown failure is logged before the task
/// returns it, so canceling an explicit shutdown cannot discard the failure.
async fn run_coordinator<R: RunningRuntime>(
    runtime: R,
    stopped: oneshot::Receiver<()>,
) -> Result<(), ShutdownError> {
    drop(stopped.await);
    let report = runtime.stop(|| async {}).await;
    if let Err(error) = &report {
        error!(%error, "peer teardown failed");
    }
    report
}
