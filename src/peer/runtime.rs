//! How consumer startup carries a peer runtime, or carries none.
//!
//! A mode prepares one attachment before it builds the Kafka client, and
//! startup activates it after. The two implementors are the whole choice:
//! [`NoPeer`] names no directory type at all, and [`PreparedPeer`] carries the
//! one its backend selected. So the directory backend stops at the coordinator
//! task and never reaches the consumer's own type parameters.

use crate::Codec;
use crate::PeerConfiguration;
use crate::consumer::middleware::providers::{FallibleCloneProvider, LeafHandler};
use crate::consumer::middleware::respond::{RespondHandler, Responder, responding_provider};
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use crate::consumer::{ConsumerError, PeerInitError, ShutdownError};
use crate::peer::{
    LocalPeerMode, NetworkPeerBackend, NetworkPeerMode, PeerBackend, heartbeat_registry,
};
use crate::producer::ProsodyProducer;
use crate::requester::ProsodyRequester;
use crate::requester::registry::PendingRegistry;
use crate::response::sender::ResponseWorkers;
use crate::router::NodeId;
use crate::router::directory::NodeDirectory;
use crate::router::fleet::config::FleetConfigurationError;
use crate::router::grpc::BoundListener;
use crate::router::runtime::{
    LocalPeerRuntime, PeerInputs, PeerRuntime, PreparedLocalPeerRuntime, PreparedPeerRuntime,
};
use crate::subsystem::SubsystemName;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::error;

type RespondingLeaf<H, R> = FallibleCloneProvider<RespondHandler<LeafHandler<H>, R>>;

/// What one consumer does about the peer fleet.
///
/// A behaviour selector, never a flag: the read of the Kafka cluster id blocks
/// for as long as the observer's startup timeout, so a consumer that joins no
/// fleet must not pay it. [`NoPeer`] answers `None` and calls nothing.
pub(crate) trait PeerAttachment: Sized + Send {
    /// The subsystem the decode path admits a request tag for, or `None` when
    /// this consumer answers none.
    ///
    /// The attachment is the one source of it, and only a prepared responder
    /// fills it. So a consumer admits a request exactly when a responder exists
    /// to answer it, and it admits the name that responder frames its answers
    /// with.
    fn responder(&self) -> Option<SubsystemName>;

    /// Hands the running peer to the consumer.
    fn attach(self) -> Option<PeerHandles>;

    /// Releases everything preparation took, without publishing this node.
    fn abandon(self) -> impl Future<Output = ()> + Send;
}

/// A consumer that joins no peer fleet.
pub(crate) struct NoPeer;

/// A running peer waiting for a consumer to attach.
pub(crate) struct PreparedPeer {
    handles: PeerHandles,
    subsystem: Option<SubsystemName>,
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
pub(crate) struct PreparedResponder<R: Codec> {
    peer: PreparedPeer,
    responder: Arc<Responder<R>>,
}

/// The running peer coordinator, and the one way to ask it for its report.
pub(crate) struct PeerHandles {
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

    fn prepared_node(&self) -> NodeId;
    fn build_responder<C: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<(Responder<C>, ResponseWorkers), FleetConfigurationError>;
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

/// Prepares the runtime selected by one backend family.
pub(crate) trait PreparePeer<B: PeerBackend> {
    type Runtime: PreparedRuntime;

    fn prepare(
        peer: &PeerConfiguration,
        backend: &B,
        mock: bool,
    ) -> impl Future<Output = Result<Self::Runtime, ConsumerError>> + Send;
}

impl PeerHandles {
    /// Builds a requester over this runtime's identity and registry.
    pub(crate) fn requester<C: Codec, R: Codec>(
        &self,
        producer: ProsodyProducer<C>,
    ) -> ProsodyRequester<C, R> {
        ProsodyRequester::new(producer, self.node, Arc::clone(&self.pending))
    }

    /// Refuses new requests. Requests already in flight stay open.
    pub(crate) fn close_admission(&self) {
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
    pub(crate) async fn stop(self) -> Result<(), ShutdownError> {
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

impl<R: Codec> PreparedResponder<R> {
    /// Terminates `chain` with a handler that answers peer requests, and hands
    /// back the attachment startup activates.
    ///
    /// This step cannot fail, so a mode calls it after its last `?`.
    pub(crate) fn terminate<M, H>(
        self,
        chain: &M,
        handler: H,
    ) -> (M::Provider<RespondingLeaf<H, R>>, PreparedPeer)
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
}

impl PeerAttachment for NoPeer {
    fn responder(&self) -> Option<SubsystemName> {
        None
    }

    fn attach(self) -> Option<PeerHandles> {
        None
    }

    async fn abandon(self) {}
}

impl PeerAttachment for PreparedPeer {
    fn responder(&self) -> Option<SubsystemName> {
        self.subsystem.clone()
    }

    fn attach(self) -> Option<PeerHandles> {
        Some(self.handles)
    }

    async fn abandon(self) {
        if let Err(error) = self.handles.stop().await {
            error!(%error, "peer teardown failed during startup rollback");
        }
    }
}

impl<D: NodeDirectory> PreparedRuntime for PreparedPeerRuntime<D> {
    type Running = PeerRuntime<D>;

    fn prepared_node(&self) -> NodeId {
        self.node()
    }

    fn build_responder<C: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<(Responder<C>, ResponseWorkers), FleetConfigurationError> {
        self.responder(subsystem)
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
    type Running = LocalPeerRuntime;

    fn prepared_node(&self) -> NodeId {
        self.node()
    }

    fn build_responder<C: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<(Responder<C>, ResponseWorkers), FleetConfigurationError> {
        self.responder(subsystem)
    }

    async fn launch(self) -> Result<Self::Running, (Self, ConsumerError)> {
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

    async fn prepare(
        peer: &PeerConfiguration,
        backend: &B,
        _mock: bool,
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
            heartbeats: heartbeat_registry(),
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

    async fn prepare(
        peer: &PeerConfiguration,
        _backend: &B,
        mock: bool,
    ) -> Result<Self::Runtime, ConsumerError> {
        if !mock {
            return Err(PeerInitError::MemoryDirectory.into());
        }
        let parts = peer.parts().map_err(PeerInitError::from)?;
        PreparedLocalPeerRuntime::start(
            parts.transport.frame_cap,
            parts.fleet,
            &parts.requester,
            &heartbeat_registry(),
        )
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
pub(crate) async fn prepare_requester<B>(
    peer: &PeerConfiguration,
    backend: &B,
    mock: bool,
) -> Result<PreparedPeer, ConsumerError>
where
    B: PeerBackend,
    B::PeerMode: PreparePeer<B>,
{
    let prepared = B::PeerMode::prepare(peer, backend, mock).await?;
    start_peer(prepared, None, None).await
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
pub(crate) async fn prepare_responding<R, B>(
    peer: &PeerConfiguration,
    backend: &B,
    mock: bool,
    subsystem: SubsystemName,
) -> Result<PreparedResponder<R>, ConsumerError>
where
    R: Codec,
    B: PeerBackend,
    B::PeerMode: PreparePeer<B>,
{
    let prepared = B::PeerMode::prepare(peer, backend, mock).await?;
    match prepared.build_responder(subsystem.clone()) {
        Ok((responder, workers)) => Ok(PreparedResponder {
            peer: start_peer(prepared, Some(workers), Some(subsystem)).await?,
            responder: Arc::new(responder),
        }),
        Err(error) => {
            prepared.release().await;
            Err(PeerInitError::Fleet {
                message: format!("{error:#}"),
            }
            .into())
        }
    }
}

async fn start_peer<P: PreparedRuntime>(
    prepared: P,
    workers: Option<ResponseWorkers>,
    subsystem: Option<SubsystemName>,
) -> Result<PreparedPeer, ConsumerError> {
    let node = prepared.prepared_node();
    let runtime = match prepared.launch().await {
        Ok(runtime) => runtime,
        Err((prepared, error)) => {
            prepared.release().await;
            return Err(error);
        }
    };
    let pending = Arc::clone(runtime.registry());
    let (stop, stopped) = oneshot::channel();
    let coordinator = tokio::spawn(run_coordinator(runtime, workers, stopped));
    Ok(PreparedPeer {
        handles: PeerHandles {
            node,
            pending,
            stop,
            coordinator,
        },
        subsystem,
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
