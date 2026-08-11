//! What an operator sets to join the peer fleet and how the runtime uses it.

use crate::router::cache_config::PeerCacheConfiguration;
use crate::router::directory::{RegistrationTtl, RegistrationTtlError};
use crate::router::runtime::RouterConfiguration;
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback, from_option_env};
use derive_builder::{Builder, UninitializedFieldError};
use std::env::{VarError, var};
use std::net::AddrParseError;
use std::net::{IpAddr, SocketAddr, SocketAddrV6};
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
#[builder(
    setter(into, strip_option),
    build_fn(error = "PeerConfigurationBuilderError")
)]
#[validate(schema(function = "validate_peer"))]
pub struct PeerConfiguration {
    /// The address for the peer listener.
    #[builder(default = "default_bind_address()?")]
    pub bind_address: SocketAddr,
    /// The gRPC connect URI that peers on another network use.
    #[builder(default = "from_option_env(\"PROSODY_PEER_ADVERTISED_CONNECT\")?")]
    pub advertised_connect: Option<Endpoint>,
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

fn default_bind_address() -> Result<SocketAddr, PeerConfigurationBuilderError> {
    match var("PROSODY_PEER_BIND_ADDRESS") {
        Ok(value) => Ok(value.parse()?),
        Err(VarError::NotPresent) => default_socket_address(),
        Err(error) => Err(PeerConfigurationBuilderError::Environment(error)),
    }
}

fn default_socket_address() -> Result<SocketAddr, PeerConfigurationBuilderError> {
    let interface = netdev::get_interfaces()
        .into_iter()
        .find(|interface| interface.default)
        .ok_or(PeerConfigurationBuilderError::NoDefaultInterface)?;
    if let Some(network) = interface.ipv4.iter().find(|network| {
        let address = network.addr();
        !address.is_link_local() && !address.is_loopback() && !address.is_unspecified()
    }) {
        return Ok(SocketAddr::new(IpAddr::V4(network.addr()), 9099));
    }
    if let Some(network) = interface.ipv6.iter().find(|network| {
        let address = network.addr();
        !address.is_unicast_link_local() && !address.is_loopback() && !address.is_unspecified()
    }) {
        return Ok(SocketAddr::new(IpAddr::V6(network.addr()), 9099));
    }
    if let Some(network) = interface.ipv4.first() {
        return Ok(SocketAddr::new(IpAddr::V4(network.addr()), 9099));
    }
    interface
        .ipv6
        .first()
        .map(|network| SocketAddrV6::new(network.addr(), 9099, 0, interface.index).into())
        .ok_or(PeerConfigurationBuilderError::NoInterfaceAddress)
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
            cache: PeerCacheConfiguration {
                peer_capacity: self.peer_cache_capacity,
            },
            lease,
        })
    }
}

fn validate_peer(config: &PeerConfiguration) -> Result<(), ValidationError> {
    if config.bind_address.ip().is_unspecified() {
        return Err(ValidationError::new("unspecified_bind_address"));
    }
    let parts = config
        .unvalidated_parts()
        .map_err(|_| ValidationError::new("peer_parts"))?;
    parts
        .router
        .validate()
        .map_err(|_| ValidationError::new("router"))?;
    parts
        .cache
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

/// Why the peer configuration builder cannot build a configuration.
#[derive(Debug, Error)]
pub enum PeerConfigurationBuilderError {
    /// A required field has no value.
    #[error(transparent)]
    Uninitialized(#[from] UninitializedFieldError),
    /// The configured bind address is not a socket address.
    #[error("PROSODY_PEER_BIND_ADDRESS is not a socket address: {0}")]
    InvalidBindAddress(#[from] AddrParseError),
    /// The operating system did not identify a default interface.
    #[error("the operating system did not identify a default network interface")]
    NoDefaultInterface,
    /// The default interface has no address.
    #[error("the default network interface has no address")]
    NoInterfaceAddress,
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
