//! Router construction and typed consumer response resources.

use crate::Codec;
use crate::PeerConfiguration;
use crate::consumer::decode::{NoRequests, RequestAdmission, SubsystemRequests};
use crate::consumer::middleware::providers::{FallibleCloneProvider, LeafHandler};
use crate::consumer::middleware::respond::{RespondHandler, responding_provider};
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use crate::consumer::{ConsumerError, PeerInitError, ShutdownError};
use crate::peer::{PeerBackend, PeerResponder, heartbeat_registry};
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
pub struct ProducerHandle {
    node: NodeId,
    pending: Arc<PendingRegistry>,
}

/// Response route and fleet from one router.
#[doc(hidden)]
pub struct ConsumerHandle<R> {
    responses: PeerResponder<R>,
}

/// Exclusive lifecycle ownership of one router.
pub struct RouterOwner {
    pending: Arc<PendingRegistry>,
    peer: PeerOwner,
}

/// One running router before its capabilities are separated.
pub(crate) struct PeerRouter<R> {
    producer: ProducerHandle,
    consumer: ConsumerHandle<R>,
    owner: PeerOwner,
}

/// The coordinator owned by one [`PeerRouter`].
struct PeerOwner {
    /// Sending a reply channel asks for the teardown report. Dropping this
    /// sender asks the coordinator to stop and to log its report instead.
    stop: oneshot::Sender<oneshot::Sender<Result<(), ShutdownError>>>,
    /// Held so a caller that asked for the report can also observe a
    /// coordinator that ended without one.
    coordinator: JoinHandle<()>,
}

/// A prepared runtime selected by the backend type.
pub(crate) trait PreparedRuntime: Sized + Send {
    type Running: RunningRuntime;
    type Route: ResponseRoute;

    fn prepared_node(&self) -> NodeId;
    fn responses(&self) -> PeerResponder<Self::Route>;
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

    pub(crate) fn into_parts(self) -> (ProducerHandle, ConsumerHandle<R>, RouterOwner) {
        let Self {
            producer,
            consumer,
            owner,
        } = self;
        let pending = Arc::clone(&producer.pending);
        (
            producer,
            consumer,
            RouterOwner {
                pending,
                peer: owner,
            },
        )
    }
}

impl<R> ConsumerHandle<R> {
    pub(super) fn map_route<T>(self, map: impl FnOnce(R) -> T) -> ConsumerHandle<T> {
        ConsumerHandle {
            responses: self.responses.map_route(map),
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
        let responder = Arc::new(self.responses.responder(subsystem.clone()));
        (
            responding_provider(middleware, handler, responder),
            RespondingPeer { subsystem },
        )
    }
}

impl RouterOwner {
    /// Stops this router and consumes its lifecycle owner.
    ///
    /// # Errors
    ///
    /// Returns an error when router teardown fails.
    pub async fn shutdown(self) -> Result<(), ShutdownError> {
        self.pending.close_admission();
        self.peer.stop().await
    }
}

impl PeerOwner {
    /// Asks the coordinator to tear the peer runtime down, and reports what it
    /// found.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownError::Directory`] when the directory did not confirm
    /// the removal of this node. Returns [`ShutdownError::Teardown`] when the
    /// coordinator ended without a report. Both leave the outcome unknown: a
    /// delete that fails after the coordinator applied it removes the row all
    /// the same, and the steps that follow the delete can fail after it.
    async fn stop(self) -> Result<(), ShutdownError> {
        let (reply, report) = oneshot::channel();
        // A closed receiver means the coordinator already ended, which the
        // report read below reports as `Teardown`.
        drop(self.stop.send(reply));
        let report = report.await;
        if let Err(error) = self.coordinator.await {
            error!(%error, "peer coordinator did not stop cleanly");
        }
        match report {
            Ok(report) => report,
            Err(_) => Err(ShutdownError::Teardown),
        }
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

    fn responses(&self) -> PeerResponder<Self::Route> {
        PeerResponder::new(
            self.response_route(),
            Arc::clone(self.fleet()),
            self.frame_cap(),
        )
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

    fn responses(&self) -> PeerResponder<Self::Route> {
        PeerResponder::new(
            self.response_route(),
            Arc::clone(self.fleet()),
            self.frame_cap(),
        )
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
    let listener = BoundListener::bind(&parts.transport)
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

pub(crate) fn prepare_local(
    peer: &PeerConfiguration,
) -> Result<PreparedLocalPeerRuntime, ConsumerError> {
    let parts = peer.parts().map_err(PeerInitError::from)?;
    PreparedLocalPeerRuntime::start(parts.transport.frame_cap, parts.fleet)
        .map_err(PeerInitError::from)
        .map_err(Into::into)
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

pub(crate) async fn start_local_router(
    peer: &PeerConfiguration,
) -> Result<PeerRouter<LocalTarget>, ConsumerError> {
    start_router(prepare_local(peer)?).await
}

pub(super) async fn start_router<P: PreparedRuntime>(
    prepared: P,
) -> Result<PeerRouter<P::Route>, ConsumerError> {
    let node = prepared.prepared_node();
    let responses = prepared.responses();
    let runtime = match prepared.launch().await {
        Ok(runtime) => runtime,
        Err((prepared, error)) => {
            prepared.release().await;
            return Err(error);
        }
    };
    let pending = Arc::clone(runtime.registry());
    let (stop, stopped) = oneshot::channel();
    let coordinator = tokio::spawn(run_coordinator(runtime, stopped));
    Ok(PeerRouter {
        producer: ProducerHandle { node, pending },
        consumer: ConsumerHandle { responses },
        owner: PeerOwner { stop, coordinator },
    })
}

/// Owns the peer runtime until its owner asks for the teardown.
///
/// The runtime moves in here, which is what keeps the directory type out of
/// [`ProsodyConsumer`](crate::consumer::ProsodyConsumer). A dropped stop sender
/// is also a request to stop, so a consumer dropped without a shutdown still
/// tears the peer down. Nothing waits for that teardown, so its report goes to
/// the log.
async fn run_coordinator<R: RunningRuntime>(
    runtime: R,
    stopped: oneshot::Receiver<oneshot::Sender<Result<(), ShutdownError>>>,
) {
    let reply = stopped.await;
    let report = runtime.stop(|| async {}).await;
    match reply {
        Ok(reply) => {
            if let Err(report) = reply.send(report) {
                error!(?report, "peer teardown report receiver closed");
            }
        }
        Err(_) => {
            if let Err(error) = report {
                error!(%error, "peer teardown failed after its owner dropped");
            }
        }
    }
}
