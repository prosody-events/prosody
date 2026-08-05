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
use crate::requester::registry::PendingRegistry;
use crate::response::sender::ResponseWorkers;
use crate::router::directory::{GroupMembership, NodeDirectory};
use crate::router::grpc::BoundListener;
use crate::router::grpc::health::ConsumerHealth;
use crate::router::runtime::{PeerInputs, PeerRuntime, PreparedPeerRuntime};
use crate::state_reader::PeerDirectoryBackend;
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
pub(in crate::consumer) struct PreparedPeer<D: NodeDirectory> {
    prepared: PreparedPeerRuntime<D>,
    workers: Option<ResponseWorkers>,
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
pub(in crate::consumer) struct PreparedResponder<D: NodeDirectory, R: Codec> {
    peer: PreparedPeer<D>,
    responder: Arc<Responder<R>>,
}

/// The running peer coordinator, and the one way to ask it for its report.
pub(in crate::consumer) struct PeerHandles {
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

impl PeerHandles {
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

impl<D: NodeDirectory, R: Codec> PreparedResponder<D, R> {
    /// Terminates `chain` with a handler that answers peer requests, and hands
    /// back the attachment startup activates.
    ///
    /// This step cannot fail, so a mode calls it after its last `?`.
    pub(in crate::consumer) fn terminate<M, H>(
        self,
        chain: &M,
        handler: H,
    ) -> (M::Provider<RespondingLeaf<H, R>>, PreparedPeer<D>)
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
        mut peer: PreparedPeer<D>,
        responder: Responder<R>,
        workers: ResponseWorkers,
    ) -> Self {
        peer.workers = Some(workers);
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

    async fn activate(
        self,
        _group: Option<GroupMembership>,
    ) -> Result<Option<PeerHandles>, (Self, ConsumerError)> {
        Ok(None)
    }

    async fn abandon(self) {}
}

impl<D: NodeDirectory> PeerAttachment for PreparedPeer<D> {
    fn cluster_id<Ctx: ConsumerContext>(
        consumer: &BaseConsumer<Ctx>,
        observer: &KafkaObserver,
    ) -> Option<String> {
        let cluster = observer.cluster_id(consumer);
        if cluster.is_none() {
            warn!("Kafka cluster id is missing; peer registration omits group membership");
        }
        cluster
    }

    async fn activate(
        self,
        group: Option<GroupMembership>,
    ) -> Result<Option<PeerHandles>, (Self, ConsumerError)> {
        let Self { prepared, workers } = self;
        let runtime = match prepared.activate(group).await {
            Ok(runtime) => runtime,
            Err((prepared, error)) => {
                return Err((
                    Self { prepared, workers },
                    PeerInitError::Directory {
                        message: format!("{error:#}"),
                    }
                    .into(),
                ));
            }
        };
        let pending = Arc::clone(runtime.pending());
        let (stop, stopped) = oneshot::channel();
        let coordinator = tokio::spawn(run_coordinator(runtime, workers, stopped));
        Ok(Some(PeerHandles {
            pending,
            stop,
            coordinator,
        }))
    }

    async fn abandon(self) {
        // Stop the listener and registry first. Join workers after the caller
        // releases the provider that holds the last responder clone.
        self.prepared.abandon().await;
        if let Some(workers) = self.workers {
            workers.join().await;
        }
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
    managers: Arc<Managers<P>>,
    heartbeats: &HeartbeatRegistry,
) -> Result<PreparedPeer<B::Directory>, ConsumerError>
where
    B: PeerDirectoryBackend,
    P: Send + Sync + 'static,
{
    let parts = peer.parts().map_err(PeerInitError::from)?;
    let listener = BoundListener::bind(&parts.transport)
        .await
        .map_err(|error| PeerInitError::Listener {
            message: format!("{error:#}"),
        })?;
    let directory = backend.node_directory(parts.lease).await?;
    let prepared = PreparedPeerRuntime::start(PeerInputs {
        directory,
        listener,
        health: ConsumerHealth::new(managers, heartbeats.clone()),
        probe: parts.probe,
        router: &parts.router,
        fleet: parts.fleet,
        requester: &parts.requester,
    })
    .await
    .map_err(PeerInitError::from)?;
    Ok(PreparedPeer {
        prepared,
        workers: None,
    })
}

/// Prepares a peer that also answers requests for `subsystem`.
///
/// It reads the frame cap from the prepared runtime, so the listener and sender
/// use one ceiling. A responder failure releases the prepared peer.
///
/// # Errors
///
/// Returns [`ConsumerError::Peer`] when preparation or the responder fails.
pub(in crate::consumer) async fn prepare_responding<R, B, P>(
    peer: &PeerConfiguration,
    backend: &B,
    subsystem: SubsystemName,
    managers: Arc<Managers<P>>,
    heartbeats: &HeartbeatRegistry,
) -> Result<PreparedResponder<B::Directory, R>, ConsumerError>
where
    R: Codec,
    B: PeerDirectoryBackend,
    P: Send + Sync + 'static,
{
    let mut peer = prepare_requester(peer, backend, managers, heartbeats).await?;
    let responder = Responder::new(peer.prepared.router(), peer.prepared.frame_cap(), subsystem);
    match responder {
        Ok((responder, workers)) => {
            peer.workers = Some(workers);
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
async fn run_coordinator<D: NodeDirectory>(
    runtime: PeerRuntime<D>,
    workers: Option<ResponseWorkers>,
    stopped: oneshot::Receiver<oneshot::Sender<Result<(), ShutdownError>>>,
) {
    match stopped.await {
        Ok(reply) => {
            let report = runtime
                .shutdown(|| async move {
                    if let Some(workers) = workers {
                        workers.join().await;
                    }
                })
                .await
                .map_err(|error| ShutdownError::Directory {
                    message: format!("{error:#}"),
                });
            if let Err(report) = reply.send(report) {
                error!(?report, "peer teardown report receiver closed");
            }
        }
        Err(_) => {
            if let Err(error) = runtime
                .shutdown(|| async move {
                    if let Some(workers) = workers {
                        workers.join().await;
                    }
                })
                .await
            {
                error!(%error, "peer teardown failed after its owner dropped");
            }
        }
    }
}
