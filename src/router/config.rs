//! What an operator sets to join the peer fleet and how the runtime uses it.

use crate::router::directory::{RegistrationTtl, RegistrationTtlError};
use crate::router::fleet::config::FleetConfiguration;
use crate::router::runtime::RouterConfiguration;
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback, from_option_env};
use derive_builder::Builder;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use thiserror::Error;
use tonic::transport::Endpoint;
use validator::{Validate, ValidationError, ValidationErrors};

/// How this process joins the peer fleet: what its listener binds, what it
/// publishes about itself, how it answers, and what it may ask for.
/// It keeps the public configuration flat. One crate-internal conversion
/// groups the related values for their owners.
///
/// Validation delegates to the components that consume these values. This
/// keeps each rule in one place and exposes the standard [`Validate`] API.
#[derive(Builder, Clone, Debug, Validate)]
#[builder(setter(into, strip_option), default)]
#[validate(schema(function = "validate_peer"))]
pub struct PeerConfiguration {
    /// The address for the peer listener.
    #[builder(default = "from_env_with_fallback(\"PROSODY_PEER_BIND_ADDRESS\", \
                         PeerConfiguration::default().bind_address)?")]
    pub bind_address: SocketAddr,
    /// The gRPC connect URI that peers on another network use.
    #[builder(default = "from_option_env(\"PROSODY_PEER_ADVERTISED_CONNECT\")?")]
    pub advertised_connect: Option<Endpoint>,
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
}

/// The internal values one peer configuration becomes.
pub(crate) struct PeerParts {
    pub(crate) bind: SocketAddr,
    pub(crate) router: RouterConfiguration,
    pub(crate) fleet: FleetConfiguration,
    pub(crate) lease: RegistrationTtl,
}

impl Default for PeerConfiguration {
    fn default() -> Self {
        let fleet = FleetConfiguration::default();
        Self {
            bind_address: SocketAddr::from((Ipv4Addr::UNSPECIFIED, 9099)),
            advertised_connect: None,
            network_name: None,
            peer_cache_capacity: fleet.peer_capacity,
            registration_ttl: RegistrationTtl::DEFAULT.duration(),
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
        let lease = RegistrationTtl::try_from(self.registration_ttl)?;
        Ok(PeerParts {
            bind: self.bind_address,
            router: RouterConfiguration {
                advertised: self.advertised_connect.clone(),
                network: self.network_name.clone(),
            },
            fleet: FleetConfiguration {
                peer_capacity: self.peer_cache_capacity,
            },
            lease,
        })
    }
}

fn validate_peer(config: &PeerConfiguration) -> Result<(), ValidationError> {
    let parts = config
        .unvalidated_parts()
        .map_err(|_| ValidationError::new("peer_parts"))?;
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
    /// The registration lease is outside its supported range.
    #[error("invalid registration lease: {0:#}")]
    Lease(#[from] RegistrationTtlError),
}
