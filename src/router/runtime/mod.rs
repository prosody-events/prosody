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

use crate::cassandra::errors::CassandraStoreError;
use crate::router::directory::cache::AddressCache;
use crate::router::directory::{
    Endpoint, GroupMembership, NetworkId, NodeDirectory, NodeRegistration, RegistrationTtl,
};
use crate::router::{Host, NodeId, select_host};
use derive_builder::Builder;
use parking_lot::Mutex;
use rand::RngExt;
use std::net::{ToSocketAddrs, UdpSocket};
use std::time::Duration;
use thiserror::Error;
use tokio::select;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, warn};
use validator::{Validate, ValidationError, ValidationErrors};
use whoami::hostname;

#[cfg(test)]
mod tests;

/// How long a registration survives without a refresh, by default.
const DEFAULT_REGISTRATION_TTL: Duration = Duration::from_secs(30);

/// How many peer registrations stay cached at once, by default.
const DEFAULT_ADDRESS_CACHE_CAPACITY: usize = 1024;

/// Longest label an operator can configure for a host or a network.
const MAX_LABEL_BYTES: usize = 64;

/// What an operator sets for peer routing.
///
/// Every field has a working default, so a deployment on one network needs no
/// configuration at all.
#[derive(Builder, Clone, Debug, Validate)]
#[builder(setter(into, strip_option), default)]
pub(crate) struct RouterConfiguration {
    /// The host that peers on another network use to reach this process — a
    /// gateway, an ingress, a translated address. Unset means intra-network
    /// only.
    #[validate(custom(function = "validate_label"))]
    pub(crate) advertised_host: Option<String>,

    /// The port to publish beside `advertised_host`. Unset publishes the
    /// listener's own port, which is what an entry point that forwards a port
    /// unchanged wants.
    #[validate(custom(function = "validate_port"))]
    pub(crate) advertised_port: Option<u16>,

    /// The operator's name for the set of processes that reach each other
    /// directly. Two processes that share it skip the entry point.
    #[validate(custom(function = "validate_label"))]
    pub(crate) network: Option<String>,

    /// How long a registration survives without a refresh.
    #[validate(custom(function = "validate_registration_ttl"))]
    pub(crate) registration_ttl: Duration,

    /// How many peer registrations stay cached at once.
    #[validate(range(min = 1_usize))]
    pub(crate) address_cache_capacity: usize,
}

/// This process's identity and its place in the directory.
///
/// The runtime mints the node id, publishes a registration before it returns,
/// rewrites the registration on a jittered interval, and deletes it on
/// shutdown. Nothing here is conditional on a peer feature: every process
/// registers, always, which is what makes any node reachable from any other.
pub(crate) struct PeerRuntime {
    directory: NodeDirectory,
    addresses: AddressCache,
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
            registration_ttl: DEFAULT_REGISTRATION_TTL,
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
    /// `listener_port` is the port this process's peer listener bound, and
    /// `contact` is a Cassandra contact point the routed-address probe aims at.
    /// The first write is awaited here rather than left to the refresh task, so
    /// a returned runtime is one whose node is already resolvable.
    ///
    /// # Errors
    ///
    /// Returns [`PeerRuntimeError`] when the configuration is invalid, or when
    /// discovery or the first write fails.
    pub(crate) async fn start(
        directory: NodeDirectory,
        listener_port: u16,
        contact: &str,
        config: &RouterConfiguration,
        group: Option<GroupMembership>,
    ) -> Result<Self, PeerRuntimeError> {
        config.validate()?;
        let ttl = directory.ttl();
        let registration =
            discover_registration(NodeId::new(), listener_port, contact, config, group)?;
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
            addresses: AddressCache::new(config.address_cache_capacity, ttl),
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

    /// Resolves another node through the bounded address cache.
    ///
    /// # Errors
    ///
    /// Returns the directory's error when a cache miss cannot be filled.
    pub(crate) async fn resolve(
        &self,
        node: NodeId,
    ) -> Result<Option<NodeRegistration>, CassandraStoreError> {
        self.addresses
            .resolve(node, || self.directory.read(node))
            .await
    }

    /// Stops refreshing and removes this process's rows.
    ///
    /// The refresh task is signalled and **joined before** the deletes are
    /// issued, so a refresh cannot land after a delete and resurrect this node
    /// for a whole lease. A second call finds the task already gone and
    /// re-issues deletes that change nothing.
    ///
    /// # Errors
    ///
    /// Returns the directory's error when a delete fails.
    pub(crate) async fn shutdown(&self) -> Result<(), CassandraStoreError> {
        // `send_replace` rather than `send`: a refresh task that already exited
        // leaves no receiver, and that is not a failure.
        self.stop.send_replace(true);
        let refresh = self.refresh.lock().take();
        if let Some(refresh) = refresh
            && let Err(error) = refresh.await
        {
            error!(%error, "node registration refresh task did not exit cleanly");
        }
        self.directory.deregister(&self.registration).await
    }
}

/// The local address the operating system would use to reach `contact`.
///
/// Connecting a UDP socket sends nothing: it only asks the routing table which
/// interface would carry that traffic, so the answer is by construction an
/// address on a network the rest of the deployment already shares. Any failure
/// — an unresolvable contact point, no route — yields `None`, and the next
/// source in the discovery order answers.
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
/// The host follows the order a deployment can supply it: the operator's
/// configured host, else the routed local address, else this machine's name.
/// The port is the listener's own, unless the operator published a different
/// one beside `advertised_host`.
fn discover_registration(
    node: NodeId,
    listener_port: u16,
    contact: &str,
    config: &RouterConfiguration,
    group: Option<GroupMembership>,
) -> Result<NodeRegistration, whoami::Error> {
    // The machine name is published in its own right, so the lookup is paid
    // once and reused as the last source of the discovery order.
    let hostname = Host::make(&hostname()?);
    let port = config.advertised_port.unwrap_or(listener_port);
    let configured = config.advertised_host.as_deref().map(Host::make);
    Ok(NodeRegistration {
        node,
        direct: Endpoint {
            host: select_host(configured, || routed_host(contact), || Ok(hostname.clone()))?,
            port,
        },
        advertised: config.advertised_host.as_deref().map(|host| Endpoint {
            host: Host::make(host),
            port,
        }),
        network: config.network.as_deref().map(NetworkId::make),
        group,
        hostname,
    })
}

/// A refresh delay inside the lease, jittered so a fleet does not renew into
/// the same partitions at the same instant.
///
/// Always between a third and a half of the lease, so two consecutive refreshes
/// can be lost before a row expires.
fn refresh_delay(ttl: RegistrationTtl) -> Duration {
    let millis = ttl.duration().as_secs() * 1000;
    let base = millis / 3;
    let span = millis / 6;
    Duration::from_millis(base + rand::rng().random_range(0..=span))
}

/// Refuses a blank label and one longer than a host or network name may be.
/// An absent label never reaches this function.
fn validate_label(label: &str) -> Result<(), ValidationError> {
    if label.is_empty() {
        return Err(ValidationError::new("label_empty"));
    }
    if label.len() > MAX_LABEL_BYTES {
        return Err(ValidationError::new("label_too_long"));
    }
    Ok(())
}

/// Refuses port zero: an advertised port is a port peers dial, never a request
/// for one the operating system chooses.
fn validate_port(port: u16) -> Result<(), ValidationError> {
    if port == 0 {
        return Err(ValidationError::new("advertised_port_zero"));
    }
    Ok(())
}

/// Delegates to [`RegistrationTtl`], so the lease bound lives in one place.
fn validate_registration_ttl(ttl: &Duration) -> Result<(), ValidationError> {
    RegistrationTtl::try_from(*ttl).map(drop).map_err(|error| {
        let mut failure = ValidationError::new("registration_ttl_out_of_range");
        failure.message = Some(error.to_string().into());
        failure
    })
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
