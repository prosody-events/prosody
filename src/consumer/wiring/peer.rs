//! How consumer startup carries a peer runtime, or carries none.
//!
//! A mode prepares one attachment before it builds the Kafka client, and
//! startup activates it after. The two implementors are the whole choice:
//! [`NoPeer`] names no directory type at all, and [`PreparedPeer`] carries the
//! one its backend selected. So the directory backend stops at the coordinator
//! task and never reaches the consumer's own type parameters.

use crate::Codec;
use crate::PeerConfiguration;
use crate::consumer::Managers;
use crate::consumer::error::{ConsumerError, PeerInitError, ShutdownError};
use crate::consumer::middleware::providers::{FallibleCloneProvider, LeafHandler};
use crate::consumer::middleware::respond::{RespondHandler, Responder, responding_provider};
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use crate::consumer::observer::KafkaObserver;
use crate::heartbeat::HeartbeatRegistry;
use crate::producer::ProsodyProducer;
use crate::requester::ProsodyRequester;
use crate::requester::registry::PendingRegistry;
use crate::response::sender::ResponseWorkers;
use crate::router::NodeId;
use crate::router::directory::{GroupMembership, NodeDirectory};
use crate::router::fleet::config::FleetConfigurationError;
use crate::router::grpc::BoundListener;
use crate::router::grpc::health::ConsumerHealth;
use crate::router::runtime::{
    LocalPeerRuntime, PeerInputs, PeerRuntime, PreparedLocalPeerRuntime, PreparedPeerRuntime,
};
use crate::state_reader::{LocalPeerMode, NetworkPeerBackend, NetworkPeerMode, PeerBackend};
use crate::subsystem::SubsystemName;
use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{error, warn};

type RespondingLeaf<H, R> = FallibleCloneProvider<RespondHandler<LeafHandler<H>, R>>;

/// What one consumer does about the peer fleet.
///
/// A behaviour selector, never a flag: the read of the Kafka cluster id blocks
/// for as long as the observer's startup timeout, so a consumer that joins no
/// fleet must not pay it. [`NoPeer`] answers `None` and calls nothing.
pub(in crate::consumer) trait PeerAttachment: Sized + Send {
    /// Reads the cluster id on the blocking thread that owns the client.
    ///
    /// That client is the only authority on the cluster it joined, and the read
    /// blocks, so it runs inside the task that already subscribes.
    fn cluster_id<Ctx: ConsumerContext>(
        consumer: &BaseConsumer<Ctx>,
        observer: &KafkaObserver,
    ) -> Option<String>;

    /// The subsystem the decode path admits a request tag for, or `None` when
    /// this consumer answers none.
    ///
    /// The attachment is the one source of it, and only a prepared responder
    /// fills it. So a consumer admits a request exactly when a responder exists
    /// to answer it, and it admits the name that responder frames its answers
    /// with.
    fn responder(&self) -> Option<SubsystemName>;

    /// Publishes this node and starts the coordinator.
    ///
    /// A failure hands the attachment back unspent, so the caller releases what
    /// it holds in the order only the caller knows.
    fn activate(
        self,
        group: Option<GroupMembership>,
    ) -> impl Future<Output = Result<Option<PeerHandles>, (Self, ConsumerError)>> + Send;

    /// Releases everything preparation took, without publishing this node.
    fn abandon(self) -> impl Future<Output = ()> + Send;
}

/// A consumer that joins no peer fleet.
pub(in crate::consumer) struct NoPeer;

/// A served peer runtime waiting for the cluster id only a live client knows.
pub(in crate::consumer) struct PreparedPeer<P: PreparedRuntime> {
    prepared: P,
    /// `None` for a consumer that answers no request. See
    /// [`PeerAttachment::responder`].
    answering: Option<Answering>,
}

/// What a consumer needs to answer a request: the delivery workers its teardown
/// joins, and the name its answers claim.
///
/// One value carries both. So an attachment that admits a name always holds the
/// workers that answer under it.
struct Answering {
    workers: ResponseWorkers,
    subsystem: SubsystemName,
}

/// A prepared peer that also holds the responder its consumer answers with.
///
/// The fields are private and [`terminate`](Self::terminate) takes them by
/// value. So the responder can reach one chain and nothing else, and the mode
/// keeps no clone of it. That is what lets the peer teardown join the delivery
/// workers: a surviving clone would hold a send handle open forever.
///
/// This value has no abandon method. Every construction site calls `terminate`
/// next, so an abandon would have no caller.
pub(in crate::consumer) struct PreparedResponder<P: PreparedRuntime, R: Codec> {
    peer: PreparedPeer<P>,
    responder: Arc<Responder<R>>,
}

/// The running peer coordinator, and the one way to ask it for its report.
pub(in crate::consumer) struct PeerHandles {
    node: NodeId,
    /// Read at the first shutdown step: no new request enters while the
    /// partition handlers finish.
    pending: Arc<PendingRegistry>,
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

    const NEEDS_CLUSTER_ID: bool;

    fn prepared_node(&self) -> NodeId;
    fn build_responder<C: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<(Responder<C>, ResponseWorkers), FleetConfigurationError>;
    fn launch(
        self,
        group: Option<GroupMembership>,
    ) -> impl Future<Output = Result<Self::Running, (Self, ConsumerError)>> + Send;
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

/// Prepares the runtime selected by one backend family.
pub(crate) trait PreparePeer<B: PeerBackend> {
    type Runtime: PreparedRuntime;

    fn prepare<P: Send + Sync + 'static>(
        peer: &PeerConfiguration,
        backend: &B,
        mock: bool,
        managers: Arc<Managers<P>>,
        heartbeats: &HeartbeatRegistry,
    ) -> impl Future<Output = Result<Self::Runtime, ConsumerError>> + Send;
}

impl Answering {
    /// Pairs `workers` with the name `responder` frames its answers with.
    ///
    /// The name comes off the responder, never off a caller. So the decode path
    /// admits the name this consumer's own answers claim.
    fn new<R: Codec>(responder: &Responder<R>, workers: ResponseWorkers) -> Self {
        Self {
            workers,
            subsystem: responder.subsystem().clone(),
        }
    }
}

impl PeerHandles {
    /// Builds a requester over this runtime's identity and registry.
    pub(in crate::consumer) fn requester<C: Codec, R: Codec>(
        &self,
        producer: ProsodyProducer<C>,
    ) -> ProsodyRequester<C, R> {
        ProsodyRequester::new(producer, self.node, Arc::clone(&self.pending))
    }

    /// Refuses new requests. Requests already in flight stay open.
    pub(in crate::consumer) fn close_admission(&self) {
        self.pending.close_admission();
    }

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
    pub(in crate::consumer) async fn stop(self) -> Result<(), ShutdownError> {
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

impl<P: PreparedRuntime, R: Codec> PreparedResponder<P, R> {
    /// Terminates `chain` with a handler that answers peer requests, and hands
    /// back the attachment startup activates.
    ///
    /// This step cannot fail, so a mode calls it after its last `?`.
    pub(in crate::consumer) fn terminate<M, H>(
        self,
        chain: &M,
        handler: H,
    ) -> (M::Provider<RespondingLeaf<H, R>>, PreparedPeer<P>)
    where
        M: HandlerMiddleware<H::Payload>,
        H: FallibleHandler + Clone + Send + Sync + 'static,
        H::Output: Sync + 'static,
        H::Error: Sync + 'static,
        R: Codec<Payload = Result<H::Output, H::Error>>,
    {
        (
            responding_provider(chain, handler, self.responder),
            self.peer,
        )
    }

    /// Pairs a responder built elsewhere with a prepared peer.
    ///
    /// This is the one way to pair a responder with workers that
    /// [`prepare_responding`] did not build. A suite uses it to answer over an
    /// in-process transport rather than over gRPC. It is test-only for exactly
    /// that reason: in production the two are built together.
    #[cfg(test)]
    pub(in crate::consumer) fn from_parts(
        mut peer: PreparedPeer<P>,
        responder: Responder<R>,
        workers: ResponseWorkers,
    ) -> Self {
        peer.answering = Some(Answering::new(&responder, workers));
        Self {
            peer,
            responder: Arc::new(responder),
        }
    }
}

impl PeerAttachment for NoPeer {
    fn cluster_id<Ctx: ConsumerContext>(
        _consumer: &BaseConsumer<Ctx>,
        _observer: &KafkaObserver,
    ) -> Option<String> {
        None
    }

    fn responder(&self) -> Option<SubsystemName> {
        None
    }

    async fn activate(
        self,
        _group: Option<GroupMembership>,
    ) -> Result<Option<PeerHandles>, (Self, ConsumerError)> {
        Ok(None)
    }

    async fn abandon(self) {}
}

impl<P: PreparedRuntime> PeerAttachment for PreparedPeer<P> {
    fn cluster_id<Ctx: ConsumerContext>(
        consumer: &BaseConsumer<Ctx>,
        observer: &KafkaObserver,
    ) -> Option<String> {
        if !P::NEEDS_CLUSTER_ID {
            return None;
        }
        let cluster = observer.cluster_id(consumer);
        if cluster.is_none() {
            warn!("Kafka cluster id is missing; peer registration omits group membership");
        }
        cluster
    }

    fn responder(&self) -> Option<SubsystemName> {
        self.answering
            .as_ref()
            .map(|answering| answering.subsystem.clone())
    }

    async fn activate(
        self,
        group: Option<GroupMembership>,
    ) -> Result<Option<PeerHandles>, (Self, ConsumerError)> {
        let Self {
            prepared,
            answering,
        } = self;
        let node = prepared.prepared_node();
        let runtime = match prepared.launch(group).await {
            Ok(runtime) => runtime,
            Err((prepared, error)) => {
                return Err((
                    Self {
                        prepared,
                        answering,
                    },
                    error,
                ));
            }
        };
        let workers = answering.map(|answering| answering.workers);
        let pending = Arc::clone(runtime.registry());
        let (stop, stopped) = oneshot::channel();
        let coordinator = tokio::spawn(run_coordinator(runtime, workers, stopped));
        Ok(Some(PeerHandles {
            node,
            pending,
            stop,
            coordinator,
        }))
    }

    async fn abandon(self) {
        // Stop the listener and registry first. Join workers after the caller
        // releases the provider that holds the last responder clone.
        self.prepared.release().await;
        if let Some(answering) = self.answering {
            answering.workers.join().await;
        }
    }
}

impl<D: NodeDirectory> PreparedRuntime for PreparedPeerRuntime<D> {
    type Running = PeerRuntime<D>;

    const NEEDS_CLUSTER_ID: bool = true;

    fn prepared_node(&self) -> NodeId {
        self.node()
    }

    fn build_responder<C: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<(Responder<C>, ResponseWorkers), FleetConfigurationError> {
        self.responder(subsystem)
    }

    async fn launch(
        self,
        group: Option<GroupMembership>,
    ) -> Result<Self::Running, (Self, ConsumerError)> {
        self.activate(group).await.map_err(|(prepared, error)| {
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
    type Running = LocalPeerRuntime;

    const NEEDS_CLUSTER_ID: bool = false;

    fn prepared_node(&self) -> NodeId {
        self.node()
    }

    fn build_responder<C: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<(Responder<C>, ResponseWorkers), FleetConfigurationError> {
        self.responder(subsystem)
    }

    async fn launch(
        self,
        _group: Option<GroupMembership>,
    ) -> Result<Self::Running, (Self, ConsumerError)> {
        Ok(self.activate())
    }

    async fn release(self) {
        self.abandon().await;
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

impl<B: NetworkPeerBackend> PreparePeer<B> for NetworkPeerMode {
    type Runtime = PreparedPeerRuntime<B::Directory>;

    async fn prepare<P: Send + Sync + 'static>(
        peer: &PeerConfiguration,
        backend: &B,
        _mock: bool,
        managers: Arc<Managers<P>>,
        heartbeats: &HeartbeatRegistry,
    ) -> Result<Self::Runtime, ConsumerError> {
        let parts = peer.parts().map_err(PeerInitError::from)?;
        let listener = BoundListener::bind(&parts.transport)
            .await
            .map_err(|error| PeerInitError::Listener {
                message: format!("{error:#}"),
            })?;
        let directory = backend.node_directory(parts.lease).await?;
        PreparedPeerRuntime::start(PeerInputs {
            directory,
            listener,
            health: ConsumerHealth::new(managers, heartbeats.clone()),
            probe: parts.probe,
            router: &parts.router,
            fleet: parts.fleet,
            requester: &parts.requester,
        })
        .await
        .map_err(PeerInitError::from)
        .map_err(ConsumerError::from)
    }
}

impl<B: PeerBackend> PreparePeer<B> for LocalPeerMode {
    type Runtime = PreparedLocalPeerRuntime;

    async fn prepare<P: Send + Sync + 'static>(
        peer: &PeerConfiguration,
        _backend: &B,
        mock: bool,
        _managers: Arc<Managers<P>>,
        _heartbeats: &HeartbeatRegistry,
    ) -> Result<Self::Runtime, ConsumerError> {
        if !mock {
            return Err(PeerInitError::MemoryDirectory.into());
        }
        let parts = peer.parts().map_err(PeerInitError::from)?;
        PreparedLocalPeerRuntime::start(parts.transport.frame_cap, parts.fleet, &parts.requester)
            .map_err(PeerInitError::from)
            .map_err(ConsumerError::from)
    }
}

/// Binds the peer listener, opens the directory this backend selects, and
/// serves the runtime.
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
pub(in crate::consumer) async fn prepare_requester<B, P>(
    peer: &PeerConfiguration,
    backend: &B,
    mock: bool,
    managers: Arc<Managers<P>>,
    heartbeats: &HeartbeatRegistry,
) -> Result<PreparedPeer<<B::PeerMode as PreparePeer<B>>::Runtime>, ConsumerError>
where
    B: PeerBackend,
    B::PeerMode: PreparePeer<B>,
    P: Send + Sync + 'static,
{
    let prepared = B::PeerMode::prepare(peer, backend, mock, managers, heartbeats).await?;
    Ok(PreparedPeer {
        prepared,
        answering: None,
    })
}

/// Prepares a peer that also answers requests for `subsystem`.
///
/// The prepared runtime builds the responder, so this function selects neither
/// the router nor the frame ceiling. A responder failure releases the prepared
/// peer.
///
/// # Errors
///
/// Returns [`ConsumerError::Peer`] when preparation or the responder fails.
pub(in crate::consumer) async fn prepare_responding<R, B, P>(
    peer: &PeerConfiguration,
    backend: &B,
    mock: bool,
    subsystem: SubsystemName,
    managers: Arc<Managers<P>>,
    heartbeats: &HeartbeatRegistry,
) -> Result<PreparedResponder<<B::PeerMode as PreparePeer<B>>::Runtime, R>, ConsumerError>
where
    R: Codec,
    B: PeerBackend,
    B::PeerMode: PreparePeer<B>,
    P: Send + Sync + 'static,
{
    let mut peer = prepare_requester(peer, backend, mock, managers, heartbeats).await?;
    match peer.prepared.build_responder(subsystem) {
        Ok((responder, workers)) => {
            peer.answering = Some(Answering::new(&responder, workers));
            Ok(PreparedResponder {
                peer,
                responder: Arc::new(responder),
            })
        }
        Err(error) => {
            peer.abandon().await;
            Err(PeerInitError::Fleet {
                message: format!("{error:#}"),
            }
            .into())
        }
    }
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
    workers: Option<ResponseWorkers>,
    stopped: oneshot::Receiver<oneshot::Sender<Result<(), ShutdownError>>>,
) {
    // The join closure moves the workers, so one teardown call is the only
    // shape both arms can share. Who asked decides where the report goes.
    let reply = stopped.await;
    let report = runtime
        .stop(|| async move {
            if let Some(workers) = workers {
                workers.join().await;
            }
        })
        .await;
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
