//! What an operator sets to join the peer fleet and how the runtime uses it.

use crate::peer::PeerEndpoint;
use crate::peer::router::cache_config::PeerCacheConfiguration;
use crate::peer::router::directory::{RegistrationTtl, RegistrationTtlError};
use crate::peer::router::interface::{InterfaceError, default_address};
use crate::peer::router::runtime::RouterConfiguration;
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback, from_option_env};
use derive_builder::{Builder, UninitializedFieldError};
use std::env::{VarError, var};
use std::net::AddrParseError;
use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;
use validator::{Validate, ValidationError, ValidationErrors};

/// The peer listener port when the operator sets no address.
const DEFAULT_PORT: u16 = 9099;

/// How this process joins the peer fleet: what its listener binds, what it
/// publishes about itself, how it answers, and what it may ask for.
/// It keeps the public configuration flat. One crate-internal conversion
/// groups the related values for their owners.
///
/// Validation delegates to the components that consume these values. This
/// keeps each rule in one place and exposes the standard [`Validate`] API.
#[derive(Builder, Clone, Debug, Validate)]
#[builder(
    setter(into, strip_option),
    build_fn(error = "PeerConfigurationBuilderError")
)]
#[validate(schema(function = "validate_peer"))]
pub struct PeerConfiguration {
    /// The address for the peer listener.
    ///
    /// Peers connect to the address that the listener binds. A listener on an
    /// unspecified IP address, such as `0.0.0.0` or `::`, accepts on every
    /// interface and publishes the default interface address of that family.
    /// If this value is absent, a network router binds the default interface
    /// address on port 9099. Local-only routers do not select an address.
    ///
    /// Without an advertised entry point, a held port does not stop startup.
    /// The operating system then selects a port, and peers find it through the
    /// peer directory.
    #[builder(default = "configured_bind_address()?")]
    pub bind_address: Option<SocketAddr>,
    /// The gRPC connect URI that peers on another network use.
    #[builder(default = "from_option_env(\"PROSODY_PEER_ADVERTISED_CONNECT\")?")]
    pub advertised_connect: Option<PeerEndpoint>,
    /// The network label for direct routes.
    #[builder(default = "from_option_env(\"PROSODY_PEER_NETWORK_NAME\")?")]
    pub network_name: Option<String>,
    /// The maximum number of peers held in each peer-keyed cache.
    #[builder(default = "from_env_with_fallback(\"PROSODY_PEER_CACHE_CAPACITY\", \
                         PeerCacheConfiguration::default().peer_capacity)?")]
    pub peer_cache_capacity: usize,
    /// The duration of each directory registration lease.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_PEER_REGISTRATION_TTL\", \
                   RegistrationTtl::DEFAULT.duration())?"
    )]
    pub registration_ttl: Duration,
}

/// The internal values one peer configuration becomes.
pub(crate) struct PeerParts {
    pub(crate) bind: SocketAddr,
    pub(crate) router: RouterConfiguration,
    pub(crate) cache: PeerCacheConfiguration,
    pub(crate) lease: RegistrationTtl,
}

fn configured_bind_address() -> Result<Option<SocketAddr>, PeerConfigurationBuilderError> {
    match var("PROSODY_PEER_BIND_ADDRESS") {
        Ok(value) => Ok(Some(value.parse()?)),
        Err(VarError::NotPresent) => Ok(None),
        Err(error) => Err(PeerConfigurationBuilderError::Environment(error)),
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
            bind: self
                .bind_address
                .map_or_else(|| default_address(None, DEFAULT_PORT), Ok)?,
            router: RouterConfiguration {
                advertised: self
                    .advertised_connect
                    .clone()
                    .map(PeerEndpoint::into_inner),
                network: self.network_name.clone(),
            },
            cache: PeerCacheConfiguration {
                peer_capacity: self.peer_cache_capacity,
            },
            lease,
        })
    }
}

fn validate_peer(config: &PeerConfiguration) -> Result<(), ValidationError> {
    RouterConfiguration {
        advertised: config
            .advertised_connect
            .clone()
            .map(PeerEndpoint::into_inner),
        network: config.network_name.clone(),
    }
    .validate()
    .map_err(|_| ValidationError::new("router"))?;
    PeerCacheConfiguration {
        peer_capacity: config.peer_cache_capacity,
    }
    .validate()
    .map_err(|_| ValidationError::new("fleet"))?;
    RegistrationTtl::try_from(config.registration_ttl)
        .map_err(|_| ValidationError::new("registration_ttl"))?;
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
    /// The default network interface has no address to bind.
    #[error(transparent)]
    Interface(#[from] InterfaceError),
}

/// Why the peer configuration builder cannot build a configuration.
#[derive(Debug, Error)]
pub enum PeerConfigurationBuilderError {
    /// A required field has no value.
    #[error(transparent)]
    Uninitialized(#[from] UninitializedFieldError),
    /// The configured bind address is not a socket address.
    #[error("PROSODY_PEER_BIND_ADDRESS is not a socket address: {0}")]
    InvalidBindAddress(#[from] AddrParseError),
    /// The bind address environment variable is not valid Unicode.
    #[error("PROSODY_PEER_BIND_ADDRESS cannot be read: {0}")]
    Environment(VarError),
    /// An existing environment parser rejected a configured value.
    #[error("{0}")]
    ConfiguredValue(String),
    /// A configured value fails validation.
    #[error(transparent)]
    Validation(#[from] ValidationErrors),
}

impl From<String> for PeerConfigurationBuilderError {
    fn from(error: String) -> Self {
        Self::ConfiguredValue(error)
    }
}
