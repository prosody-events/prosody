//! One process's peer machinery: what it publishes about itself, what it hands
//! consumers and requesters, and the order it stops in.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "no production caller yet: consumer wiring will own this; every item here is \
                  exercised by this module's tests"
    )
)]

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::requester::config::RequesterConfiguration;
use crate::requester::registry::{PendingRegistry, RegistryError};
use crate::router::directory::cache::{AddressCache, AddressResolver};
use crate::router::directory::{
    Endpoint, GroupMembership, NetworkId, NodeDirectory, NodeRegistration, RegistrationTtl,
};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::{FleetConfiguration, FleetConfigurationError};
use crate::router::grpc::client::GrpcSender;
use crate::router::grpc::health::ProcessHealth;
use crate::router::grpc::service::PeerService;
use crate::router::grpc::{BoundListener, TransportError, serve};
use crate::router::{Host, NodeId, RouterHandle};
use rand::RngExt;
use std::future::Future;
use std::net::{ToSocketAddrs, UdpSocket};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::select;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, warn};
use validator::{Validate, ValidationErrors};
use whoami::hostname;

mod config;

pub(crate) use self::config::RouterConfiguration;

#[cfg(test)]
mod tests;

/// Everything one process shares for peer traffic, under one owner.
///
/// The runtime holds this node's registration, the resolver it reads peers
/// through, the bound listener, the destination fleet, the pending request
/// registry, and the transport responses leave by. Consumers and requesters
/// take handles from it and construct none of these themselves. It mints one
/// [`NodeId`] and the listener answers for that same id, so one runtime has one
/// identity. One process runs one runtime.
///
/// The router names no response vocabulary except at the wire seam it owns.
/// This type is inside that seam: the listener's service is the pending
/// registry's server side, so the runtime must name both.
///
/// [`shutdown`](Self::shutdown) is the ordered teardown, and it must be driven
/// to completion. A dropped shutdown future stops between two steps and leaves
/// the remaining tasks detached.
///
/// A runtime that is dropped instead still ends: the refresher stops when the
/// watch channel closes, the listener stops when the one-shot sender drops, the
/// registry's sweep stops when its last [`Arc`] drops, and this node's row
/// expires on its lease. What a plain drop cannot do is wake a parked waiter or
/// wait for a reservation, which is why shutdown exists.
pub(crate) struct PeerRuntime {
    addresses: AddressResolver,
    /// The write side of the directory. The resolver beside it only reads, so
    /// the two directions stay separate types rather than one that does both.
    directory: NodeDirectory,
    registration: NodeRegistration,
    fleet: Arc<DestinationFleet>,
    pending: Arc<PendingRegistry>,
    transport: Arc<GrpcSender>,
    stop_refresh: watch::Sender<bool>,
    refresh: JoinHandle<()>,
    /// Dropping it resolves the listener's shutdown future.
    stop_listener: oneshot::Sender<()>,
    listener: JoinHandle<()>,
}

/// Everything one peer runtime is built from.
///
/// The bound listener inside is a unique resource, so one value serves one
/// process. Two runtimes need a second, grep-visible call to
/// [`PeerRuntime::start`].
pub(crate) struct PeerInputs<'a, H> {
    pub(crate) store: CassandraStore,
    /// The already-bound peer listener. The runtime serves this listener and
    /// publishes the port that the operating system assigned.
    pub(crate) listener: BoundListener,
    pub(crate) health: H,
    /// A Cassandra contact point that the routed-address probe aims at.
    pub(crate) contact: &'a str,
    pub(crate) group: Option<GroupMembership>,
    pub(crate) router: &'a RouterConfiguration,
    pub(crate) fleet: FleetConfiguration,
    pub(crate) requester: &'a RequesterConfiguration,
}

impl PeerRuntime {
    /// Builds every shared piece, serves the listener, and publishes this
    /// process.
    ///
    /// The listener is served before the registration is written, so no peer
    /// can learn a port before that port accepts connections. The first write
    /// is awaited here rather than left to the refresh task, so a returned
    /// runtime is one whose node is already published.
    ///
    /// # Errors
    ///
    /// Returns [`PeerRuntimeError`] when the configuration, discovery, the
    /// directory, the registry, the fleet, or the listener refuses to start.
    /// A failed first write stops the listener again before it returns. It
    /// issues no delete: a row the failed write still applied expires on its
    /// lease.
    pub(crate) async fn start<H: ProcessHealth>(
        inputs: PeerInputs<'_, H>,
    ) -> Result<Self, PeerRuntimeError> {
        inputs.router.validate()?;
        let fleet = Arc::new(DestinationFleet::new(inputs.fleet)?);
        let pending = PendingRegistry::new(inputs.requester)?;
        let transport = Arc::new(GrpcSender::new(inputs.listener.frame_cap(), &fleet));
        let ttl = inputs.router.registration_ttl;
        let directory = NodeDirectory::new(inputs.store, ttl).await?;
        let registration = discover_registration(
            NodeId::new(),
            &inputs.listener,
            inputs.contact,
            inputs.router,
            inputs.group,
        )?;
        let (stop_listener, stopped) = oneshot::channel();
        let listener = serve(
            inputs.listener,
            PeerService::new(registration.node, Arc::clone(&pending)),
            inputs.health,
            async move { drop(stopped.await) },
        )?;
        if let Err(error) = directory.register(&registration).await {
            abandon(stop_listener, listener).await;
            return Err(error.into());
        }
        let (stop_refresh, mut stopped) = watch::channel(false);
        let refresh = tokio::spawn({
            let directory = directory.clone();
            let registration = registration.clone();
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
        Ok(Self {
            addresses: AddressResolver::new(
                AddressCache::new(inputs.router.address_cache_capacity, ttl),
                directory.clone(),
            ),
            directory,
            registration,
            fleet,
            pending,
            transport,
            stop_refresh,
            refresh,
            stop_listener,
            listener,
        })
    }

    /// This process's node id.
    pub(crate) fn node(&self) -> NodeId {
        self.registration.node
    }

    /// How this process resolves another node, through the bounded address
    /// cache.
    pub(crate) const fn addresses(&self) -> &AddressResolver {
        &self.addresses
    }

    /// The process-wide destination fleet.
    pub(crate) const fn fleet(&self) -> &Arc<DestinationFleet> {
        &self.fleet
    }

    /// The process-wide pending request registry.
    pub(crate) const fn pending(&self) -> &Arc<PendingRegistry> {
        &self.pending
    }

    /// The router every responder in this process sends through.
    pub(crate) fn router(&self) -> RouterHandle<GrpcSender> {
        RouterHandle::new(
            self.addresses.clone(),
            Arc::clone(&self.fleet),
            Arc::clone(&self.transport),
        )
    }

    /// Shuts this process's peer machinery down, in the one order that cannot
    /// leave a reservation behind.
    ///
    /// Takes `self`, so a second shutdown is unwritable and no handle has to be
    /// taken out from behind a lock. `drain` is a closure rather than a future,
    /// so the response drain cannot have begun before the gate closed: this
    /// call runs it once the gate has closed and emptied, and nothing else
    /// calls it.
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
    pub(crate) async fn shutdown<F, D>(self, drain: F) -> Result<(), CassandraStoreError>
    where
        F: FnOnce() -> D,
        D: Future<Output = ()>,
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
            transport,
        } = self;

        // Neither needs a step: the resolver only reads, and every sender the
        // drain flushes holds its own clone of the transport.
        drop((addresses, transport));
        // `send_replace` rather than `send`: a refresh task that already exited
        // leaves no receiver, and that is not a failure.
        stop_refresh.send_replace(true);
        if let Err(error) = refresh.await {
            error!(%error, "node registration refresh task did not exit cleanly");
        }
        let deregistered = directory.deregister(&registration).await;

        drop(stop_listener);
        pending.shutdown().await;
        fleet.close().await;
        // An unforgeable `Drained` token that a drain required would make this
        // order uncompilable. It was examined and rejected: it would also
        // require every caller that flushes one sender to close the process's
        // fleet, and one fleet serves several senders.
        drain().await;
        if let Err(error) = listener.await {
            error!(%error, "the peer listener task did not exit cleanly");
        }
        deregistered
    }
}

/// Stops the listener that this start served.
///
/// A dropped handle leaves its task live. A retry could then fail to bind the
/// same port.
async fn abandon(stop: oneshot::Sender<()>, listener: JoinHandle<()>) {
    drop(stop);
    if let Err(join_error) = listener.await {
        error!(%join_error, "the abandoned peer listener task did not exit cleanly");
    }
}

/// The local address the operating system would use to reach `contact`.
///
/// Connecting a UDP socket sends nothing: it only asks the routing table which
/// interface would carry that traffic. The answer is the address that reaches
/// the contact point, and nothing more. A loopback contact point answers with a
/// loopback address, and a host that reaches Cassandra over a management
/// interface answers with the management address. A peer elsewhere reaches
/// neither, which is what [`RouterConfiguration::advertised_host`] is for. Any
/// failure — an unresolvable contact point, no route — yields `None`, and the
/// next source in the discovery order answers.
fn routed_host(contact: &str) -> Option<Host> {
    let Ok(mut targets) = contact.to_socket_addrs() else {
        return None;
    };
    let target = targets.next()?;
    // An IPv4-bound socket cannot discover an IPv6 route, so the probe binds
    // the family of the address it aims at.
    let unspecified = if target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let Ok(probe) = UdpSocket::bind(unspecified) else {
        return None;
    };
    let Ok(()) = probe.connect(target) else {
        return None;
    };
    let Ok(local) = probe.local_addr() else {
        return None;
    };
    Some(Host::make(&local.ip().to_string()))
}

/// Builds this process's registration.
///
/// Each configured field reaches only the endpoint it describes. `direct` is
/// discovered and never configured: the local address the operating system
/// would route to the contact point, else this machine's name, on the port the
/// listener actually bound. That is what makes an equal network label an
/// optimization — a neighbour dials an address it reaches without the entry
/// point. The configured host and port reach `advertised` alone.
fn discover_registration(
    node: NodeId,
    listener: &BoundListener,
    contact: &str,
    config: &RouterConfiguration,
    group: Option<GroupMembership>,
) -> Result<NodeRegistration, whoami::Error> {
    let listener_port = listener.address().port();
    // The machine name is published in its own right, so the lookup is paid
    // once and reused where the routed probe finds no address.
    let hostname = Host::make(&hostname()?);
    Ok(NodeRegistration {
        node,
        direct: Endpoint {
            host: routed_host(contact).unwrap_or_else(|| hostname.clone()),
            port: listener_port,
        },
        advertised: config.advertised_host.as_deref().map(|host| Endpoint {
            host: Host::make(host),
            port: config.advertised_port.unwrap_or(listener_port),
        }),
        network: config.network.as_deref().map(NetworkId::make),
        group,
        hostname,
    })
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

/// What can stop a process from taking its place in the directory.
#[derive(Debug, Error)]
pub(crate) enum PeerRuntimeError {
    /// The configuration this process was started with is invalid.
    #[error("router configuration is invalid: {0:#}")]
    Configuration(#[from] ValidationErrors),

    /// The machine's own name could not be read. Every registration publishes
    /// it, so the lookup is not optional.
    #[error("host discovery failed: {0:#}")]
    Discovery(#[from] whoami::Error),

    /// The directory could not prepare its statements, or it rejected this
    /// process's first registration.
    #[error("node registration failed: {0:#}")]
    Directory(#[from] CassandraStoreError),

    /// The destination limits were invalid.
    #[error("the destination fleet could not be built: {0:#}")]
    Fleet(#[from] FleetConfigurationError),

    /// The pending request limits were invalid.
    #[error("the pending registry could not be built: {0:#}")]
    Registry(#[from] RegistryError),

    /// The bound peer listener could not start its service.
    #[error("the peer listener could not be served: {0:#}")]
    Listener(#[from] TransportError),
}
