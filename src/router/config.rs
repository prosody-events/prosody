//! What an operator sets to join the peer fleet, and how it becomes the four
//! sections the runtime is built from.

use crate::response::frame::{FrameCap, FrameCapError};
use crate::router::directory::{RegistrationTtl, RegistrationTtlError};
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::TransportConfiguration;
use crate::router::runtime::RouterConfiguration;
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback, from_option_env};
use derive_builder::Builder;
use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;
use validator::{Validate, ValidationError, ValidationErrors};

/// How this process joins the peer fleet: what its listener binds, what it
/// publishes about itself, how it answers, and what it may ask for.
///
/// The presence of this section is the switch. A process that sets none starts
/// no listener, publishes no node, and joins no fleet.
///
/// It restates the fields of four crate-private sections rather than nesting
/// them, because a caller outside this crate cannot name a crate-private type.
/// One crate-internal conversion turns it back into those sections.
///
/// Validation delegates to the four sections that consume these values. This
/// keeps each rule in one place while exposing the standard [`Validate`] API.
#[derive(Builder, Clone, Debug, Validate)]
#[builder(setter(into, strip_option), default)]
#[validate(schema(function = "validate_peer"))]
pub struct PeerConfiguration {
    /// The address for the peer listener.
    #[builder(default = "from_env_with_fallback(\"PROSODY_PEER_BIND_ADDRESS\", \
                         PeerConfiguration::default().bind_address)?")]
    pub bind_address: SocketAddr,
    /// The maximum encoded frame size.
    #[builder(default = "from_env_with_fallback(\"PROSODY_PEER_MAX_FRAME_BYTES\", \
                         PeerConfiguration::default().max_frame_bytes)?")]
    pub max_frame_bytes: usize,
    /// Enables schema reflection on the peer listener.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_PEER_ENABLE_REFLECTION\", \
                   PeerConfiguration::default().enable_reflection)?"
    )]
    pub enable_reflection: bool,
    /// The host that peers on another network use.
    #[builder(default = "from_option_env(\"PROSODY_PEER_ADVERTISED_HOST\")?")]
    pub advertised_host: Option<String>,
    /// The advertised port, or the listener port when absent.
    #[builder(default = "from_option_env(\"PROSODY_PEER_ADVERTISED_PORT\")?")]
    pub advertised_port: Option<u16>,
    /// The network label for direct routes.
    #[builder(default = "from_option_env(\"PROSODY_PEER_NETWORK_NAME\")?")]
    pub network_name: Option<String>,
    /// The maximum number of peers held in each node-keyed cache.
    #[builder(default = "from_env_with_fallback(\"PROSODY_PEER_CACHE_CAPACITY\", \
                         PeerConfiguration::default().peer_cache_capacity)?")]
    pub peer_cache_capacity: usize,
    /// The duration of each directory registration lease.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_PEER_REGISTRATION_TTL\", \
                   PeerConfiguration::default().registration_ttl)?"
    )]
    pub registration_ttl: Duration,
    /// The address used to find the routed host.
    #[builder(default = "from_option_env(\"PROSODY_PEER_ROUTE_PROBE_ADDRESS\")?")]
    pub route_probe_address: Option<SocketAddr>,
    /// The deadline for one response delivery.
    #[builder(default = "from_duration_env_with_fallback(\"\
                         PROSODY_PEER_RESPONSE_DELIVERY_TIMEOUT\", \
                         PeerConfiguration::default().response_delivery_timeout)?")]
    pub response_delivery_timeout: Duration,
}

/// The four internal sections one peer configuration becomes, and the two
/// values that belong to no section: the directory lease and the probe address.
pub(crate) struct PeerParts {
    pub(crate) transport: TransportConfiguration,
    pub(crate) router: RouterConfiguration,
    pub(crate) fleet: FleetConfiguration,
    pub(crate) lease: RegistrationTtl,
    pub(crate) probe: Option<SocketAddr>,
}

impl Default for PeerConfiguration {
    fn default() -> Self {
        let transport = TransportConfiguration::default();
        let router = RouterConfiguration::default();
        let fleet = FleetConfiguration::default();
        Self {
            bind_address: transport.bind,
            max_frame_bytes: transport.frame_cap.bytes(),
            enable_reflection: transport.reflection,
            advertised_host: router.advertised_host,
            advertised_port: router.advertised_port,
            network_name: router.network,
            peer_cache_capacity: fleet.peer_capacity,
            registration_ttl: RegistrationTtl::DEFAULT.duration(),
            route_probe_address: None,
            response_delivery_timeout: fleet.response_timeout,
        }
    }
}

impl PeerConfiguration {
    /// Creates a peer configuration builder.
    #[must_use]
    pub fn builder() -> PeerConfigurationBuilder {
        PeerConfigurationBuilder::default()
    }

    /// Splits this configuration into the sections each component takes.
    ///
    /// # Errors
    ///
    /// Returns [`PeerConfigurationError`] when any peer value is invalid.
    pub(crate) fn parts(&self) -> Result<PeerParts, PeerConfigurationError> {
        self.validate()?;
        self.unvalidated_parts()
    }

    fn unvalidated_parts(&self) -> Result<PeerParts, PeerConfigurationError> {
        let frame_cap = FrameCap::new(self.max_frame_bytes)?;
        let lease = RegistrationTtl::try_from(self.registration_ttl)?;
        Ok(PeerParts {
            transport: TransportConfiguration {
                bind: self.bind_address,
                frame_cap,
                reflection: self.enable_reflection,
            },
            router: RouterConfiguration {
                advertised_host: self.advertised_host.clone(),
                advertised_port: self.advertised_port,
                network: self.network_name.clone(),
            },
            fleet: FleetConfiguration {
                peer_capacity: self.peer_cache_capacity,
                response_timeout: self.response_delivery_timeout,
            },
            lease,
            probe: self.route_probe_address,
        })
    }
}

fn validate_peer(config: &PeerConfiguration) -> Result<(), ValidationError> {
    let parts = config
        .unvalidated_parts()
        .map_err(|_| ValidationError::new("peer_parts"))?;
    parts
        .transport
        .validate()
        .map_err(|_| ValidationError::new("transport"))?;
    parts
        .router
        .validate()
        .map_err(|_| ValidationError::new("router"))?;
    parts
        .fleet
        .validate()
        .map_err(|_| ValidationError::new("fleet"))?;
    Ok(())
}

/// Why a peer configuration cannot form its internal sections.
#[derive(Debug, Error)]
pub(crate) enum PeerConfigurationError {
    /// One peer value or a combination of values is invalid.
    #[error("peer configuration is invalid: {0:#}")]
    Invalid(#[from] ValidationErrors),
    /// The frame size is outside its supported range.
    #[error("invalid frame size: {0:#}")]
    Frame(#[from] FrameCapError),
    /// The registration lease is outside its supported range.
    #[error("invalid registration lease: {0:#}")]
    Lease(#[from] RegistrationTtlError),
}
