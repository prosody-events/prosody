//! This process's identity in the node directory: what it publishes, how it
//! keeps the publication alive, and how it resolves other nodes.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the consumer wiring is this module's production caller; every item here is \
                  exercised by this module's tests"
    )
)]

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::router::directory::cache::{AddressCache, AddressResolver};
use crate::router::directory::{
    Endpoint, GroupMembership, NetworkId, NodeDirectory, NodeRegistration, RegistrationTtl,
};
use crate::router::grpc::BoundListener;
use crate::router::{Host, NodeId};
use derive_builder::Builder;
use rand::RngExt;
use std::net::{ToSocketAddrs, UdpSocket};
use std::time::Duration;
use thiserror::Error;
use tokio::select;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, warn};
use validator::{Validate, ValidationError, ValidationErrors};
use whoami::hostname;

#[cfg(test)]
mod tests;

/// How many peer registrations stay cached at once, by default.
const DEFAULT_ADDRESS_CACHE_CAPACITY: usize = 1024;

/// Most peer registrations a process may hold at once. A registration is a few
/// short strings, so a million of them is already more memory than the cache is
/// worth; the bound stops a typo from asking for a heap the process lacks.
const MAX_ADDRESS_CACHE_CAPACITY: usize = 1_048_576;

/// Longest label an operator can configure for a host or a network. The largest
/// label that stays inline in [`Host`], so a configured label never reaches the
/// heap.
const MAX_LABEL_BYTES: usize = 63;

/// What an operator sets for peer routing.
///
/// Every field has a working default, so a deployment on one network needs no
/// configuration at all.
#[derive(Builder, Clone, Debug, Validate)]
#[builder(setter(into, strip_option), default)]
#[validate(schema(function = "validate_entry_point"))]
pub(crate) struct RouterConfiguration {
    /// The host that peers on another network use to reach this process — a
    /// gateway, an ingress, a translated address. Unset means intra-network
    /// only.
    ///
    /// Folding this and `advertised_port` into one optional pair would make a
    /// port with no host beside it unrepresentable. The two stay separate
    /// because every cross-field rule in this crate is a schema validation, and
    /// one flat builder setter per field is the shape an operator expects.
    #[validate(custom(function = "validate_label"))]
    pub(crate) advertised_host: Option<String>,

    /// The port to publish beside `advertised_host`. Unset publishes the
    /// listener's own port, which is what an entry point that forwards a port
    /// unchanged wants. Set with no host beside it, the configuration is
    /// refused. Zero is refused too: an advertised port is a port peers dial,
    /// never a request for one the operating system chooses.
    #[validate(range(min = 1_u16))]
    pub(crate) advertised_port: Option<u16>,

    /// The operator's name for the set of processes that reach each other
    /// directly. Two processes that share it skip the entry point.
    #[validate(custom(function = "validate_label"))]
    pub(crate) network: Option<String>,

    /// How long a registration survives without a refresh. The type carries the
    /// range, so a lease outside it never reaches a configuration at all.
    pub(crate) registration_ttl: RegistrationTtl,

    /// How many peer registrations stay cached at once.
    #[validate(range(min = 1_usize, max = MAX_ADDRESS_CACHE_CAPACITY))]
    pub(crate) address_cache_capacity: usize,
}

/// This process's identity and its place in the directory.
///
/// The runtime mints the node id, publishes a registration before it returns,
/// rewrites the registration on a jittered interval, and deletes it on
/// shutdown. Nothing here is conditional on a peer feature: every process
/// registers, always, which is what makes any node reachable from any other.
/// The consumer wiring owns construction and shutdown order.
///
/// Dropping the runtime without a shutdown ends the refresh task through the
/// stop channel and leaves this process's row to expire on its lease.
pub(crate) struct PeerRuntime {
    addresses: AddressResolver,
    /// The write side of the directory. The resolver beside it only reads, so
    /// the two directions stay separate types rather than one that does both.
    directory: NodeDirectory,
    registration: NodeRegistration,
    stop: watch::Sender<bool>,
    refresh: Mutex<Option<JoinHandle<()>>>,
}

impl Default for RouterConfiguration {
    fn default() -> Self {
        Self {
            advertised_host: None,
            advertised_port: None,
            network: None,
            registration_ttl: RegistrationTtl::DEFAULT,
            address_cache_capacity: DEFAULT_ADDRESS_CACHE_CAPACITY,
        }
    }
}

impl RouterConfiguration {
    /// Creates a configuration builder.
    #[must_use]
    pub(crate) fn builder() -> RouterConfigurationBuilder {
        RouterConfigurationBuilder::default()
    }
}

impl PeerRuntime {
    /// Mints this process's node id, publishes what it discovers about itself,
    /// and starts refreshing that publication.
    ///
    /// `listener` is this process's already-bound peer listener, and `contact`
    /// is a Cassandra contact point the routed-address probe aims at. The
    /// listener is taken rather than a port number so the published port is
    /// always the one the operating system assigned: there is no other port to
    /// pass.
    ///
    /// The lease comes from `config` and reaches the directory, the refresh
    /// pace and the address cache from there, so one configured value governs
    /// all three. The first write is awaited here rather than left to the
    /// refresh task, so a returned runtime is one whose node is already
    /// published.
    ///
    /// # Errors
    ///
    /// Returns [`PeerRuntimeError`] when the configuration is invalid, or when
    /// discovery, statement preparation, or the first write fails.
    pub(crate) async fn start(
        store: CassandraStore,
        listener: &BoundListener,
        contact: &str,
        config: &RouterConfiguration,
        group: Option<GroupMembership>,
    ) -> Result<Self, PeerRuntimeError> {
        config.validate()?;
        let ttl = config.registration_ttl;
        let directory = NodeDirectory::new(store, ttl).await?;
        let registration = discover_registration(NodeId::new(), listener, contact, config, group)?;
        directory.register(&registration).await?;
        let (stop, mut stopped) = watch::channel(false);
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
                AddressCache::new(config.address_cache_capacity, ttl),
                directory.clone(),
            ),
            directory,
            registration,
            stop,
            refresh: Mutex::new(Some(refresh)),
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

    /// Stops refreshing and removes this process's rows.
    ///
    /// The refresh task is signalled and **joined before** the deletes are
    /// issued, so a refresh cannot land after a delete and resurrect this node
    /// for a whole lease. The join and the deletes run under one lock, and the
    /// handle is cleared only after the join returns. A second caller therefore
    /// waits for the first, and a call dropped at the join leaves the handle
    /// for the next call to join — neither can delete while the refresher is
    /// still live. Repeating a completed shutdown re-issues deletes that change
    /// nothing.
    ///
    /// # Errors
    ///
    /// Returns the directory's error when a delete fails.
    pub(crate) async fn shutdown(&self) -> Result<(), CassandraStoreError> {
        // `send_replace` rather than `send`: a refresh task that already exited
        // leaves no receiver, and that is not a failure.
        self.stop.send_replace(true);
        let mut refresh = self.refresh.lock().await;
        if let Some(handle) = refresh.as_mut() {
            if let Err(error) = handle.await {
                error!(%error, "node registration refresh task did not exit cleanly");
            }
            // A `JoinHandle` cannot be polled again once it has completed.
            *refresh = None;
        }
        self.directory.deregister(&self.registration).await
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

/// Refuses a blank label and one longer than a host or network name may be.
/// An absent label never reaches this function.
///
/// A `length` rule cannot replace this one: `validator` counts characters,
/// while [`MAX_LABEL_BYTES`] is the byte capacity that keeps a label inline in
/// [`Host`].
fn validate_label(label: &str) -> Result<(), ValidationError> {
    if label.is_empty() {
        return Err(ValidationError::new("label_empty"));
    }
    if label.len() > MAX_LABEL_BYTES {
        return Err(ValidationError::new("label_too_long"));
    }
    Ok(())
}

/// Refuses a published port with no host beside it. An entry point is a host
/// and a port together, and a port alone reaches nothing.
fn validate_entry_point(config: &RouterConfiguration) -> Result<(), ValidationError> {
    if config.advertised_port.is_some() && config.advertised_host.is_none() {
        return Err(ValidationError::new("advertised_port_without_host"));
    }
    Ok(())
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

    /// The directory rejected this process's first registration.
    #[error("node registration failed: {0:#}")]
    Directory(#[from] CassandraStoreError),
}
