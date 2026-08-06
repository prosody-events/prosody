//! One process's peer machinery: what it publishes about itself, what it hands
//! consumers and requesters, and the order it stops in.

use crate::codec::Codec;
use crate::consumer::middleware::respond::Responder;
use crate::requester::config::RequesterConfiguration;
use crate::requester::registry::PendingRegistry;
use crate::response::frame::FrameCap;
use crate::response::sender::ResponseWorkers;
use crate::router::directory::cache::AddressResolver;
use crate::router::directory::{GroupMembership, NodeDirectory, NodeRegistration, RegistrationTtl};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::{FleetConfiguration, FleetConfigurationError};
use crate::router::grpc::client::GrpcSender;
use crate::router::grpc::health::ProcessHealth;
use crate::router::grpc::service::PeerService;
use crate::router::grpc::{BoundListener, serve};
use crate::router::relay::Relay;
use crate::router::{LocalTarget, NodeId, RouterHandle};
use crate::subsystem::SubsystemName;
use rand::RngExt;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, warn};
use validator::Validate;

mod config;
mod discovery;
mod error;

pub(crate) use self::error::PeerRuntimeError;

pub(crate) use self::config::RouterConfiguration;
use self::discovery::{discover, registration};

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(in crate::router) use tests::start_runtime;

/// Everything one process shares for peer traffic, under one owner.
///
/// The runtime holds this node's published registration, the resolver it reads
/// peers through, the bound listener, the destination fleet, the pending
/// request registry, and the router responses leave by. Consumers and
/// requesters take handles from it and construct none of these themselves. It
/// mints one [`NodeId`] and the listener answers for that same id, so one
/// runtime has one identity. One process runs one runtime.
///
/// The router names no response vocabulary except at the wire seam it owns.
/// This type is inside that seam: the listener's service is the pending
/// registry's server side, so the runtime must name both.
///
/// A runtime exists only after a directory write.
/// [`PreparedPeerRuntime`] holds the same parts before that write, and
/// [`PreparedPeerRuntime::activate`] is the one way to reach this type.
///
/// [`shutdown`](Self::shutdown) is the ordered teardown, and it must be driven
/// to completion. A dropped shutdown future stops between two steps and leaves
/// the remaining tasks detached.
///
/// A runtime that is dropped instead still ends: the refresher stops when the
/// watch channel closes, the listener stops when the one-shot sender drops, the
/// registry's sweep stops when its last [`Arc`] drops, and this node's entry
/// expires on its lease. What a plain drop cannot do is wake a parked waiter or
/// wait for a reservation, which is why shutdown exists.
///
/// The directory backend travels with this type and stops here. Its handles
/// hand out values that name `D`, and those values are `Clone`, so no borrow
/// confines them. What keeps `D` out of the code above is inference: a consumer
/// passes a handle to a constructor that returns a type naming no `D`, or calls
/// a method on it, and writes the parameter nowhere. An owner infers `D` where
/// it prepares the runtime, and moves the whole runtime into a task of its own.
pub(crate) struct PeerRuntime<D> {
    addresses: AddressResolver<D>,
    router: RouterHandle<GrpcSender, D>,
    /// The write side of the directory. The resolver beside it only reads, so
    /// the two directions stay separate types rather than one that does both.
    directory: D,
    registration: NodeRegistration,
    fleet: Arc<DestinationFleet>,
    pending: Arc<PendingRegistry>,
    stop_refresh: watch::Sender<bool>,
    refresh: JoinHandle<()>,
    /// Dropping it resolves the listener's shutdown future.
    stop_listener: oneshot::Sender<()>,
    listener: JoinHandle<()>,
}

/// One process's peer machinery, served but not yet published.
///
/// [`start`](Self::start) builds and serves every local part and writes
/// nothing. The directory learns this node only at
/// [`activate`](Self::activate). So the first write is the last step that can
/// fail, and an owner that fails after preparation publishes nothing to undo.
///
/// This value owns a listening socket. Spend it exactly once, through
/// `activate` or [`abandon`](Self::abandon). A plain drop detaches the listener
/// task, and a retry on the same port then fails to bind.
pub(crate) struct PreparedPeerRuntime<D> {
    addresses: AddressResolver<D>,
    router: RouterHandle<GrpcSender, D>,
    frame_cap: FrameCap,
    directory: D,
    registration: NodeRegistration,
    fleet: Arc<DestinationFleet>,
    pending: Arc<PendingRegistry>,
    stop_listener: oneshot::Sender<()>,
    listener: JoinHandle<()>,
}

/// Local peer machinery that has no listener, directory, or remote transport.
pub(crate) struct PreparedLocalPeerRuntime {
    node: NodeId,
    frame_cap: FrameCap,
    fleet: Arc<DestinationFleet>,
    pending: Arc<PendingRegistry>,
}

/// A running local-only peer runtime.
pub(crate) struct LocalPeerRuntime {
    pending: Arc<PendingRegistry>,
    fleet: Arc<DestinationFleet>,
}

/// Everything one peer runtime is built from.
///
/// The bound listener inside is a unique resource, so one value serves one
/// process. Two runtimes need a second, grep-visible call to
/// [`PreparedPeerRuntime::start`].
pub(crate) struct PeerInputs<'a, H, D> {
    /// Where this process publishes itself, and how it resolves a peer. Its
    /// lease is the single source: the runtime ages its address cache on it and
    /// paces its refresher inside it.
    pub(crate) directory: D,
    /// The already-bound peer listener. The runtime serves this listener and
    /// publishes the port that the operating system assigned.
    pub(crate) listener: BoundListener,
    pub(crate) health: H,
    /// The address that the routed-address probe aims at.
    pub(crate) probe: Option<SocketAddr>,
    pub(crate) router: &'a RouterConfiguration,
    pub(crate) fleet: FleetConfiguration,
    pub(crate) requester: &'a RequesterConfiguration,
}

impl<D: NodeDirectory> PreparedPeerRuntime<D> {
    /// This process's node id.
    pub(crate) fn node(&self) -> NodeId {
        self.registration.node
    }

    /// Builds every shared piece and serves the listener.
    ///
    /// The listener is served here rather than at activation, so no peer can
    /// learn a port before that port accepts connections. This function writes
    /// nothing to the directory.
    ///
    /// Each failure arm releases what the earlier steps took, so a failure
    /// leaves no task behind and returns nothing to release.
    ///
    /// # Errors
    ///
    /// Returns [`PeerRuntimeError`] when the configuration, discovery, the
    /// registry, the fleet, or the listener refuses to start.
    pub(crate) async fn start<H: ProcessHealth>(
        inputs: PeerInputs<'_, H, D>,
    ) -> Result<Self, PeerRuntimeError> {
        inputs.router.validate()?;
        // One ceiling for both seams: the frames this process sends and the
        // frames its listener admits.
        let frame_cap = inputs.listener.frame_cap();
        // The requester's ceiling and the listener's are validated apart and
        // meet here for the first time. A requester that admits a payload no
        // frame this listener accepts could carry would never receive one, and
        // the caller would learn that only from its own timeout.
        if inputs.requester.max_response_bytes > frame_cap.bytes() {
            return Err(PeerRuntimeError::ResponseCeiling {
                bytes: inputs.requester.max_response_bytes,
                cap: frame_cap.bytes(),
            });
        }
        let fleet = Arc::new(DestinationFleet::new(inputs.fleet)?);
        let pending = PendingRegistry::new(inputs.requester)?;
        let transport = Arc::new(GrpcSender::new(frame_cap, &fleet));
        let directory = inputs.directory;
        // The blocking pool owns this wait; a runtime thread must not. The
        // machine-name lookup and the route probe are private to `discovery`,
        // so this file reaches them only through `discover`.
        let discovered = match discover(inputs.probe).await {
            Ok(discovered) => discovered,
            Err(error) => {
                pending.terminate().await;
                return Err(PeerRuntimeError::Discovery(error));
            }
        };
        let registration = registration(NodeId::new(), &inputs.listener, discovered, inputs.router);
        let addresses =
            AddressResolver::new(inputs.router.address_cache_capacity, directory.clone());
        let router = RouterHandle::new(
            registration.node,
            Arc::clone(&pending),
            addresses.clone(),
            Arc::clone(&fleet),
            transport,
            registration.network.clone(),
        );
        let (stop_listener, stopped) = oneshot::channel();
        let listener = match serve(
            inputs.listener,
            PeerService::new(
                registration.node,
                Arc::clone(&pending),
                Relay::new(router.clone()),
                frame_cap,
                inputs.fleet.send_deadline,
            ),
            inputs.health,
            async move { drop(stopped.await) },
        ) {
            Ok(listener) => listener,
            Err(error) => {
                pending.terminate().await;
                return Err(PeerRuntimeError::Listener(error));
            }
        };
        Ok(Self {
            addresses,
            router,
            frame_cap,
            directory,
            registration,
            fleet,
            pending,
            stop_listener,
            listener,
        })
    }

    /// Publishes this node and starts its registration refresh task.
    ///
    /// The group label is set here because only the running Kafka client knows
    /// the cluster that scopes it. It is the one registration field that
    /// preparation leaves unset.
    ///
    /// A failed write issues one delete before it gives up. Node ids are minted
    /// fresh and never reused. So this process is the only writer of its own
    /// row, and the delete can remove no other. The delete is best effort, not
    /// a guarantee: a delete that also fails leaves a row that expires on its
    /// lease.
    ///
    /// # Errors
    ///
    /// Returns this value and the directory's error when the first write fails.
    /// The caller then owns the release order, because only the caller knows
    /// what else it holds.
    pub(crate) async fn activate(
        mut self,
        group: Option<GroupMembership>,
    ) -> Result<PeerRuntime<D>, (Self, D::Error)> {
        self.registration.group = group;
        if let Err(error) = self.directory.register(&self.registration).await {
            if let Err(delete_error) = self.directory.deregister(&self.registration).await {
                warn!(%delete_error, node = %self.registration.node, "failed peer registration rollback");
            }
            return Err((self, error));
        }
        let ttl = self.directory.ttl();
        let (stop_refresh, mut stopped) = watch::channel(false);
        let refresh = tokio::spawn({
            let directory = self.directory.clone();
            let registration = self.registration.clone();
            async move {
                loop {
                    select! {
                        () = sleep(refresh_delay(ttl)) => {}
                        outcome = stopped.changed() => {
                            if outcome.is_err() {
                                break;
                            }
                        }
                    }
                    // Checked before every write, so a refresh cannot land
                    // after the shutdown delete and resurrect this node.
                    if *stopped.borrow() {
                        break;
                    }
                    // A store failure must never end this task: a dead
                    // refresher makes the node vanish one lease later.
                    if let Err(error) = directory.register(&registration).await {
                        warn!(%error, node = %registration.node, "node registration refresh failed");
                    }
                }
            }
        });
        Ok(PeerRuntime {
            addresses: self.addresses,
            router: self.router,
            directory: self.directory,
            registration: self.registration,
            fleet: self.fleet,
            pending: self.pending,
            stop_refresh,
            refresh,
            stop_listener: self.stop_listener,
            listener: self.listener,
        })
    }

    /// Builds the responder this process answers peer requests with.
    ///
    /// The router and the frame ceiling come from this runtime, and the caller
    /// selects neither. So a responder's encoder cannot disagree with the
    /// ceiling this process's listener admits. No code outside this module
    /// builds a router, so no caller can pair a responder with another
    /// runtime's.
    ///
    /// Call this before activation: the responder must exist before the Kafka
    /// client starts.
    ///
    /// # Errors
    ///
    /// Returns [`FleetConfigurationError`] when one encode buffer per
    /// destination at this ceiling exceeds what one sender may commit to.
    pub(crate) fn responder<C: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<(Responder<C>, ResponseWorkers), FleetConfigurationError> {
        Responder::new(&self.router, self.frame_cap, subsystem)
    }

    /// Stops every local resource without publishing the node.
    pub(crate) async fn abandon(self) {
        self.pending.terminate().await;
        abandon(self.stop_listener, self.listener).await;
    }
}

impl PreparedLocalPeerRuntime {
    /// Builds local peer machinery without network resources.
    pub(crate) fn start(
        frame_cap: FrameCap,
        fleet: FleetConfiguration,
        requester: &RequesterConfiguration,
    ) -> Result<Self, PeerRuntimeError> {
        if requester.max_response_bytes > frame_cap.bytes() {
            return Err(PeerRuntimeError::ResponseCeiling {
                bytes: requester.max_response_bytes,
                cap: frame_cap.bytes(),
            });
        }
        Ok(Self {
            node: NodeId::new(),
            frame_cap,
            fleet: Arc::new(DestinationFleet::new(fleet)?),
            pending: PendingRegistry::new(requester)?,
        })
    }

    /// This process's node id.
    pub(crate) const fn node(&self) -> NodeId {
        self.node
    }

    /// Builds a responder over the local route only.
    pub(crate) fn responder<C: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<(Responder<C>, ResponseWorkers), FleetConfigurationError> {
        Responder::new_local(
            LocalTarget::new(self.node, Arc::clone(&self.pending)),
            &self.fleet,
            self.frame_cap,
            subsystem,
        )
    }

    /// Starts the local runtime. No external activation is necessary.
    pub(crate) fn activate(self) -> LocalPeerRuntime {
        LocalPeerRuntime {
            pending: self.pending,
            fleet: self.fleet,
        }
    }

    /// Stops local resources before activation.
    pub(crate) async fn abandon(self) {
        self.pending.terminate().await;
    }
}

impl LocalPeerRuntime {
    /// The process-wide request registry.
    pub(crate) const fn pending(&self) -> &Arc<PendingRegistry> {
        &self.pending
    }

    /// Shuts the local peer machinery down in delivery order.
    pub(crate) async fn shutdown<F, Fut>(self, drain: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        self.pending.terminate().await;
        self.fleet.close().await;
        drain().await;
    }
}

impl<D: NodeDirectory> PeerRuntime<D> {
    /// This process's node id.
    #[cfg(test)]
    pub(crate) fn node(&self) -> NodeId {
        self.registration.node
    }

    /// How this process resolves another node, through the bounded address
    /// cache.
    #[cfg(test)]
    pub(crate) const fn addresses(&self) -> &AddressResolver<D> {
        &self.addresses
    }

    /// The process-wide destination fleet.
    #[cfg(test)]
    pub(crate) const fn fleet(&self) -> &Arc<DestinationFleet> {
        &self.fleet
    }

    /// The process-wide pending request registry.
    pub(crate) const fn pending(&self) -> &Arc<PendingRegistry> {
        &self.pending
    }

    /// The router every responder in this process sends through.
    ///
    /// Production reads the prepared runtime. This accessor serves the suites.
    #[cfg(test)]
    pub(crate) fn router(&self) -> RouterHandle<GrpcSender, D> {
        self.router.clone()
    }

    /// Shuts this process's peer machinery down, in the one order that cannot
    /// leave a reservation behind.
    ///
    /// Takes `self`, so a second shutdown is unwritable and no handle has to be
    /// taken out from behind a lock. `drain` is a closure rather than a future,
    /// so this call alone decides when the drain starts. It runs it once the
    /// gate has closed and emptied. A drain that joins response workers
    /// terminates only after the last send handle drops; that precondition
    /// belongs to [`ResponseWorkers`].
    ///
    /// The body reads as the order. Three of its steps are where they are for a
    /// reason the code cannot show:
    ///
    /// - **Join the refresher before the delete.** A refresh that landed after
    ///   the delete would republish this node under a fresh lease and outlive
    ///   the process. The delete's outcome is returned, but every later step
    ///   runs whatever it was — a failed delete heals on the lease, and an
    ///   abandoned teardown does not.
    /// - **Wake every parked request before the listener is joined.** One
    ///   long-lived inbound call could hold tonic's graceful shutdown open, and
    ///   every parked caller would then wait behind it. That call's own frame
    ///   can still reach the registry afterwards, and the registry refuses it.
    /// - **Close the gate before the drain.** Reversed, a hook could reserve a
    ///   slot on a fleet whose workers had already stopped, and its response
    ///   would be dropped instead of delivered. [`DestinationFleet`]'s counted
    ///   admission is what makes that an order rather than a race.
    ///
    /// Drive this to completion. A dropped shutdown future stops between two
    /// steps, and the tasks the remaining steps would have joined detach.
    ///
    /// # Errors
    ///
    /// Returns the directory's error when a delete fails.
    pub(crate) async fn shutdown<F, Fut>(self, drain: F) -> Result<(), D::Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        let Self {
            directory,
            registration,
            fleet,
            pending,
            stop_refresh,
            refresh,
            stop_listener,
            listener,
            addresses,
            router,
        } = self;

        // Neither needs a step: the resolver only reads, and the transport the
        // router carries is already cloned into every response delivery worker.
        drop((addresses, router));
        // `send_replace` rather than `send`: a refresh task that already exited
        // leaves no receiver, and that is not a failure.
        stop_refresh.send_replace(true);
        if let Err(error) = refresh.await {
            error!(%error, "node registration refresh task did not exit cleanly");
        }
        let deregistered = directory.deregister(&registration).await;

        drop(stop_listener);
        pending.terminate().await;
        fleet.close().await;
        // A `Drained` token minted by the close, and demanded by every drain,
        // would make the reversed order uncompilable. It was examined and
        // rejected: a consumer that abandons a prepared peer joins its response
        // workers and closes no fleet, so the token would reach a caller that
        // holds none.
        drain().await;
        if let Err(error) = listener.await {
            error!(%error, "the peer listener task did not exit cleanly");
        }
        deregistered
    }
}

/// Stops the listener that preparation served.
///
/// A dropped handle leaves its task live. A retry could then fail to bind the
/// same port.
async fn abandon(stop: oneshot::Sender<()>, listener: JoinHandle<()>) {
    drop(stop);
    if let Err(join_error) = listener.await {
        error!(%join_error, "the abandoned peer listener task did not exit cleanly");
    }
}

/// A refresh delay inside the lease, jittered so a fleet does not renew into
/// the same partitions at the same instant.
///
/// Always between a fifth and a quarter of the lease. Three delays therefore
/// leave a quarter of the lease unspent, so two lost refreshes still heal while
/// the writes between them return inside that spare. Nothing bounds a write's
/// own round trip, which is why the margin is a quarter of the lease and not
/// the last instant of it. The lower bound caps what the margin costs at five
/// refreshes per lease.
fn refresh_delay(ttl: RegistrationTtl) -> Duration {
    let millis = ttl.duration().as_secs() * 1000;
    let base = millis / 5;
    let span = millis / 20;
    Duration::from_millis(base + rand::rng().random_range(0..=span))
}
