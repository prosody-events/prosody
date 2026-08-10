//! One process's peer transport, registration, routing, and shutdown order.

use crate::heartbeat::{Heartbeat, HeartbeatRegistry};
use crate::requester::registry::PendingRegistry;
use crate::response::sender::Then;
use crate::router::directory::cache::AddressResolver;
use crate::router::directory::{NodeDirectory, NodeRegistration, RegistrationTtl};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::client::GrpcSender;
use crate::router::grpc::health::RuntimeHealth;
use crate::router::grpc::service::PeerService;
use crate::router::grpc::{BoundListener, serve};
use crate::router::relay::Relay;
use crate::router::{LocalTarget, NetworkRoute, NodeId};
use rand::RngExt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};
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
/// request registry, and the local and network response routes. Consumers and
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
/// registry drops when its last [`Arc`] drops, and this node's entry expires on
/// its lease. What a plain drop cannot do is wake a parked waiter or wait for a
/// reservation. This is why shutdown exists.
///
/// The directory backend travels with this type and stops here. Its handles
/// hand out values that name `D`, and those values are `Clone`, so no borrow
/// confines them. What keeps `D` out of the code above is inference: a consumer
/// passes a handle to a constructor that returns a type naming no `D`, or calls
/// a method on it, and writes the parameter nowhere. An owner infers `D` where
/// it prepares the runtime, and moves the whole runtime into a task of its own.
pub(crate) struct PeerRuntime<D> {
    local: LocalTarget,
    network: NetworkRoute<GrpcSender, D>,
    /// The write side of the directory. The resolver beside it only reads, so
    /// the two directions stay separate types rather than one that does both.
    directory: D,
    registration: NodeRegistration,
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
    local: LocalTarget,
    network: NetworkRoute<GrpcSender, D>,
    directory: D,
    registration: NodeRegistration,
    heartbeats: HeartbeatRegistry,
    stop_listener: oneshot::Sender<()>,
    listener: JoinHandle<()>,
}

/// Local peer machinery that has no listener, directory, or remote transport.
pub(crate) struct PreparedLocalPeerRuntime {
    local: LocalTarget,
}

/// A running local-only peer runtime.
pub(crate) struct LocalPeerRuntime {
    pending: Arc<PendingRegistry>,
}

/// Everything one peer runtime is built from.
///
/// The bound listener inside is a unique resource, so one value serves one
/// process. Two runtimes need a second, grep-visible call to
/// [`PreparedPeerRuntime::start`].
pub(crate) struct PeerInputs<'a, D> {
    /// Where this process publishes itself, and how it resolves a peer. Its
    /// lease is the single source: the runtime ages its address cache on it and
    /// paces its refresher inside it.
    pub(crate) directory: D,
    /// The already-bound peer listener. The runtime serves this listener and
    /// publishes the port that the operating system assigned.
    pub(crate) listener: BoundListener,
    /// Heartbeats owned by this peer runtime.
    pub(crate) heartbeats: HeartbeatRegistry,
    pub(crate) router: &'a RouterConfiguration,
    pub(crate) fleet: FleetConfiguration,
}

impl<D: NodeDirectory> PreparedPeerRuntime<D> {
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
    pub(crate) async fn start(inputs: PeerInputs<'_, D>) -> Result<Self, PeerRuntimeError> {
        inputs.router.validate()?;
        let fleet = Arc::new(DestinationFleet::new(inputs.fleet)?);
        let pending = PendingRegistry::new();
        let transport = Arc::new(GrpcSender::new(&fleet));
        let directory = inputs.directory;
        // The blocking pool owns this wait; a runtime thread must not. The
        // Machine-name lookup is private to `discovery`, so this file reaches
        // it only through `discover`.
        let discovered = match discover().await {
            Ok(discovered) => discovered,
            Err(error) => {
                pending.terminate();
                return Err(PeerRuntimeError::Discovery(error));
            }
        };
        let registration =
            registration(NodeId::new(), &inputs.listener, discovered, inputs.router)?;
        let addresses = AddressResolver::new(inputs.fleet.peer_capacity, directory.clone());
        let local = LocalTarget::new(registration.node, Arc::clone(&pending));
        let network = NetworkRoute::new(
            addresses.clone(),
            Arc::clone(&fleet),
            transport,
            registration.network.clone(),
        );
        let (stop_listener, stopped) = oneshot::channel();
        let listener = match serve(
            inputs.listener,
            PeerService::new(local.clone(), Relay::new(network.clone())),
            RuntimeHealth::new(inputs.heartbeats.clone()),
            async move { drop(stopped.await) },
        ) {
            Ok(listener) => listener,
            Err(error) => {
                pending.terminate();
                return Err(PeerRuntimeError::Listener(error));
            }
        };
        Ok(Self {
            local,
            network,
            directory,
            registration,
            heartbeats: inputs.heartbeats,
            stop_listener,
            listener,
        })
    }

    /// Publishes this node and starts its registration refresh task.
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
    pub(crate) async fn activate(self) -> Result<PeerRuntime<D>, (Self, D::Error)> {
        if let Err(error) = self.directory.register(&self.registration).await {
            if let Err(delete_error) = self.directory.deregister(&self.registration).await {
                warn!(%delete_error, node = %self.registration.node, "failed peer registration rollback");
            }
            return Err((self, error));
        }
        let ttl = self.directory.ttl();
        let (stop_refresh, stopped) = watch::channel(false);
        let refresh = tokio::spawn(refresh_registration(
            self.directory.clone(),
            self.registration.clone(),
            self.heartbeats.register("directory refresh"),
            ttl,
            stopped,
        ));
        Ok(PeerRuntime {
            local: self.local,
            network: self.network,
            directory: self.directory,
            registration: self.registration,
            stop_refresh,
            refresh,
            stop_listener: self.stop_listener,
            listener: self.listener,
        })
    }

    /// Returns the local-first response route for this runtime.
    pub(crate) fn response_route(&self) -> Then<LocalTarget, NetworkRoute<GrpcSender, D>> {
        Then(self.local.clone(), self.network.clone())
    }

    /// Stops every local resource without publishing the node.
    pub(crate) async fn abandon(self) {
        self.local.registry.terminate();
        abandon(self.stop_listener, self.listener).await;
    }
}

impl PreparedLocalPeerRuntime {
    /// Builds local peer machinery without network resources.
    pub(crate) fn start() -> Self {
        Self {
            local: LocalTarget::new(NodeId::new(), PendingRegistry::new()),
        }
    }

    /// Returns the local response route for this runtime.
    pub(crate) fn response_route(&self) -> LocalTarget {
        self.local.clone()
    }

    /// Starts the local runtime. No external activation is necessary.
    pub(crate) fn activate(self) -> LocalPeerRuntime {
        LocalPeerRuntime {
            pending: self.local.registry,
        }
    }

    /// Stops local resources before activation.
    pub(crate) fn abandon(self) {
        self.local.registry.terminate();
    }
}

impl LocalPeerRuntime {
    /// Shuts the local peer machinery down in delivery order.
    pub(crate) async fn shutdown<F, Fut>(self, drain: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        self.pending.terminate();
        drain().await;
    }
}

impl<D: NodeDirectory> PeerRuntime<D> {
    /// This process's node id.
    #[cfg(test)]
    pub(crate) fn node(&self) -> NodeId {
        self.registration.node
    }

    /// Shuts this process's peer machinery down.
    ///
    /// Takes `self`, so a second shutdown is unwritable. `drain` is a closure,
    /// so this call alone decides when the drain starts.
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
            stop_refresh,
            refresh,
            stop_listener,
            listener,
            local,
            network,
        } = self;

        let pending = Arc::clone(&local.registry);
        drop((local, network));
        // `send_replace` rather than `send`: a refresh task that already exited
        // leaves no receiver, and that is not a failure.
        stop_refresh.send_replace(true);
        if let Err(error) = refresh.await {
            error!(%error, "node registration refresh task did not exit cleanly");
        }
        let deregistered = directory.deregister(&registration).await;

        drop(stop_listener);
        pending.terminate();
        drain().await;
        if let Err(error) = listener.await {
            error!(%error, "the peer listener task did not exit cleanly");
        }
        deregistered
    }
}

/// Refreshes one registration until shutdown starts.
async fn refresh_registration<D: NodeDirectory>(
    directory: D,
    registration: NodeRegistration,
    heartbeat: Heartbeat,
    ttl: RegistrationTtl,
    mut stopped: watch::Receiver<bool>,
) {
    heartbeat.beat();
    let mut refresh_at = Instant::now() + refresh_delay(ttl);
    loop {
        select! {
            () = sleep_until(refresh_at) => {}
            () = heartbeat.next() => {
                heartbeat.beat();
                continue;
            }
            outcome = stopped.changed() => {
                if outcome.is_err() {
                    break;
                }
            }
        }
        // Checked before every write, so a refresh cannot land after the
        // shutdown delete and resurrect this node.
        if *stopped.borrow() {
            break;
        }
        // A store failure must never end this task: a dead refresher makes the
        // node vanish one lease later.
        if let Err(error) = directory.register(&registration).await {
            warn!(%error, node = %registration.node, "node registration refresh failed");
        }
        heartbeat.beat();
        refresh_at = Instant::now() + refresh_delay(ttl);
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
