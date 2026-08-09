//! Router construction and typed consumer response resources.

use crate::Codec;
use crate::PeerConfiguration;
use crate::consumer::decode::{NoRequests, RequestAdmission, SubsystemRequests};
use crate::consumer::middleware::providers::{FallibleCloneProvider, LeafHandler};
use crate::consumer::middleware::respond::{RespondHandler, Responder, responding_provider};
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use crate::consumer::{ConsumerError, PeerInitError, ShutdownError};
use crate::peer::{PeerBackend, heartbeat_registry};
use crate::producer::ProsodyProducer;
use crate::requester::ProsodyRequester;
use crate::requester::registry::PendingRegistry;
use crate::response::sender::{ResponseRoute, Then};
use crate::router::config::PeerParts;
use crate::router::directory::NodeDirectory;
use crate::router::grpc::BoundListener;
use crate::router::grpc::client::GrpcSender;
use crate::router::runtime::{
    LocalPeerRuntime, PeerInputs, PeerRuntime, PreparedLocalPeerRuntime, PreparedPeerRuntime,
};
use crate::router::{LocalTarget, NodeId, RouterHandle};
use crate::subsystem::SubsystemName;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::error;

pub(super) type RespondingLeaf<H, C, R> =
    FallibleCloneProvider<RespondHandler<LeafHandler<H>, C, R>>;

/// Response resources selected by the consumer type.
pub(crate) trait ConsumerResources: Sized + Send + 'static {
    /// Request policy selected by this resource type.
    type Admission: RequestAdmission;

    /// Builds the request policy before the resources move to shutdown.
    fn admission(&self) -> Self::Admission;
}

/// A consumer that sends no peer responses.
pub(crate) struct NoPeer;

/// Response resources owned by one responding consumer.
#[doc(hidden)]
pub struct RespondingPeer {
    subsystem: SubsystemName,
}

/// Request identity and pending requests from one router.
#[derive(Clone)]
pub struct ProducerHandle {
    node: NodeId,
    pending: Arc<PendingRegistry>,
}

/// Response route from one router.
#[doc(hidden)]
#[derive(Clone)]
pub struct ConsumerHandle<R> {
    route: R,
}

/// One running router with its request, response, and lifecycle capabilities.
pub(crate) struct PeerRouter<R> {
    producer: ProducerHandle,
    consumer: ConsumerHandle<R>,
    /// Sending or dropping asks the coordinator to stop.
    stop: oneshot::Sender<()>,
    /// The explicit teardown result.
    coordinator: JoinHandle<Result<(), ShutdownError>>,
}

/// A prepared runtime selected by the backend type.
pub(crate) trait PreparedRuntime: Sized + Send {
    type Running: RunningRuntime;
    type Route: ResponseRoute;

    fn prepared_node(&self) -> NodeId;
    fn prepared_route(&self) -> Self::Route;
    fn launch(self) -> impl Future<Output = Result<Self::Running, (Self, ConsumerError)>> + Send;
    fn release(self) -> impl Future<Output = ()> + Send;
}

/// A running runtime with the common shutdown contract.
pub(crate) trait RunningRuntime: Sized + Send + 'static {
    fn registry(&self) -> &Arc<PendingRegistry>;
    fn stop<F, Fut>(self, drain: F) -> impl Future<Output = Result<(), ShutdownError>> + Send
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = ()> + Send;
}

/// Prepared runtime selected by `B`.
pub(crate) type RuntimeFor<B> = <B as PeerBackend>::Runtime;

/// Response route selected by `B`.
pub(crate) type RouteFor<B> = <RuntimeFor<B> as PreparedRuntime>::Route;

impl<R> PeerRouter<R> {
    #[cfg(test)]
    pub(crate) const fn node(&self) -> NodeId {
        self.producer.node
    }

    pub(crate) fn producer(&self) -> ProducerHandle {
        self.producer.clone()
    }

    pub(crate) fn consumer(&self) -> ConsumerHandle<R>
    where
        R: Clone,
    {
        self.consumer.clone()
    }

    pub(crate) async fn shutdown(self) -> Result<(), ShutdownError> {
        self.producer.pending.close_admission();
        let _closed = self.stop.send(());
        self.coordinator.await.map_err(|error| {
            error!(%error, "peer coordinator did not stop cleanly");
            ShutdownError::Teardown
        })?
    }
}

impl<R> ConsumerHandle<R> {
    pub(super) fn map_route<T>(self, map: impl FnOnce(R) -> T) -> ConsumerHandle<T> {
        ConsumerHandle {
            route: map(self.route),
        }
    }
}

impl ProducerHandle {
    /// Builds request access for a producer.
    #[must_use]
    pub fn requester<C: Codec, RC: Codec>(
        &self,
        producer: ProsodyProducer<C>,
    ) -> ProsodyRequester<C, RC> {
        ProsodyRequester::new(producer, self.node, Arc::clone(&self.pending))
    }
}

impl<R: ResponseRoute> ConsumerHandle<R> {
    pub(crate) fn responding_provider<C, M, H>(
        &self,
        subsystem: SubsystemName,
        middleware: &M,
        handler: H,
    ) -> (M::Provider<RespondingLeaf<H, C, R>>, RespondingPeer)
    where
        C: Codec<Payload = Result<H::Output, H::Error>>,
        M: HandlerMiddleware<H::Payload>,
        H: FallibleHandler + Clone + Send + Sync + 'static,
        H::Output: Sync + 'static,
        H::Error: Sync + 'static,
    {
        let responder = Arc::new(Responder::new_route(self.route.clone(), subsystem.clone()));
        (
            responding_provider(middleware, handler, responder),
            RespondingPeer { subsystem },
        )
    }
}

impl ConsumerResources for NoPeer {
    type Admission = NoRequests;

    fn admission(&self) -> Self::Admission {
        NoRequests
    }
}

impl ConsumerResources for RespondingPeer {
    type Admission = SubsystemRequests;

    fn admission(&self) -> Self::Admission {
        SubsystemRequests(self.subsystem.clone())
    }
}

impl<D: NodeDirectory> PreparedRuntime for PreparedPeerRuntime<D> {
    type Route = Then<LocalTarget, RouterHandle<GrpcSender, D>>;
    type Running = PeerRuntime<D>;

    fn prepared_node(&self) -> NodeId {
        self.node()
    }

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

    fn prepared_node(&self) -> NodeId {
        self.node()
    }

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
    fn registry(&self) -> &Arc<PendingRegistry> {
        self.pending()
    }

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
    fn registry(&self) -> &Arc<PendingRegistry> {
        self.pending()
    }

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
    let node = prepared.prepared_node();
    let route = prepared.prepared_route();
    let runtime = match prepared.launch().await {
        Ok(runtime) => runtime,
        Err((prepared, error)) => {
            prepared.release().await;
            return Err(error);
        }
    };
    Ok(finish_router(node, route, runtime))
}

fn finish_router<R, RT>(node: NodeId, route: R, runtime: RT) -> PeerRouter<R>
where
    RT: RunningRuntime,
{
    let pending = Arc::clone(runtime.registry());
    let (stop, stopped) = oneshot::channel();
    let coordinator = tokio::spawn(run_coordinator(runtime, stopped));
    PeerRouter {
        producer: ProducerHandle { node, pending },
        consumer: ConsumerHandle { route },
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
