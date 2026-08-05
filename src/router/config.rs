//! What an operator sets to join the peer fleet, and how it becomes the four
//! sections the runtime is built from.

use crate::requester::config::RequesterConfiguration;
use crate::response::frame::{FrameCap, FrameCapError};
use crate::router::directory::{RegistrationTtl, RegistrationTtlError};
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::TransportConfiguration;
use crate::router::runtime::RouterConfiguration;
use derive_builder::Builder;
use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;

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
/// **This type derives no `Validate`, and that is deliberate.** Every field
/// that can express a degenerate value carries a rule at the section that
/// consumes it. The startup path builds every one of those sections. The three
/// fields that carry none — the two addresses and the reflection switch — have
/// no degenerate value to refuse. A derive here would restate those rules in a
/// second place or promise a check it does not make. The cost is that `build`
/// accepts a degenerate value and the operator learns of it when the consumer
/// starts.
#[derive(Builder, Clone, Debug)]
#[builder(setter(into, strip_option), default)]
pub struct PeerConfiguration {
    /// The address for the peer listener.
    pub bind: SocketAddr,
    /// The maximum encoded frame size.
    pub frame_bytes: usize,
    /// The maximum number of open peer connections.
    pub max_connections: usize,
    /// The maximum number of concurrent streams per connection.
    pub max_concurrent_streams: u32,
    /// Enables schema reflection on the peer listener.
    pub reflection: bool,
    /// The host that peers on another network use.
    pub advertised_host: Option<String>,
    /// The advertised port, or the listener port when absent.
    pub advertised_port: Option<u16>,
    /// The network label for direct routes.
    pub network: Option<String>,
    /// The maximum number of cached peer addresses.
    pub address_cache_capacity: usize,
    /// The duration of each directory registration lease.
    pub registration_lease: Duration,
    /// The address used to find the routed host.
    pub route_probe: Option<SocketAddr>,
    /// The maximum number of active destinations.
    pub max_destinations: usize,
    /// The number of send slots for each destination.
    pub slots_each: usize,
    /// The send rate for each destination.
    pub sends_per_second: u32,
    /// The deadline for one response delivery.
    pub send_deadline: Duration,
    /// The maximum number of send attempts.
    pub max_send_attempts: u32,
    /// The maximum number of active requests.
    pub max_in_flight: usize,
    /// The maximum number of awaited subsystems per request.
    pub max_awaited: usize,
    /// The maximum response payload size.
    pub max_response_bytes: usize,
    /// The maximum request timeout.
    pub max_timeout: Duration,
    /// The delay before an expired request is removed.
    pub sweep_grace: Duration,
}

/// The four internal sections one peer configuration becomes, and the two
/// values that belong to no section: the directory lease and the probe address.
pub(crate) struct PeerParts {
    pub(crate) transport: TransportConfiguration,
    pub(crate) router: RouterConfiguration,
    pub(crate) fleet: FleetConfiguration,
    pub(crate) requester: RequesterConfiguration,
    pub(crate) lease: RegistrationTtl,
    pub(crate) probe: Option<SocketAddr>,
}

impl Default for PeerConfiguration {
    fn default() -> Self {
        let transport = TransportConfiguration::default();
        let router = RouterConfiguration::default();
        let fleet = FleetConfiguration::default();
        let requester = RequesterConfiguration::default();
        Self {
            bind: transport.bind,
            frame_bytes: transport.frame_cap.bytes(),
            max_connections: transport.max_connections,
            max_concurrent_streams: transport.max_concurrent_streams,
            reflection: transport.reflection,
            advertised_host: router.advertised_host,
            advertised_port: router.advertised_port,
            network: router.network,
            address_cache_capacity: router.address_cache_capacity,
            registration_lease: RegistrationTtl::DEFAULT.duration(),
            route_probe: None,
            max_destinations: fleet.max_destinations,
            slots_each: fleet.slots_each,
            sends_per_second: fleet.sends_per_second,
            send_deadline: fleet.send_deadline,
            max_send_attempts: fleet.max_send_attempts,
            max_in_flight: requester.max_in_flight,
            max_awaited: requester.max_awaited,
            max_response_bytes: requester.max_response_bytes,
            max_timeout: requester.max_timeout,
            sweep_grace: requester.sweep_grace,
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
    /// It runs no rule of its own beyond the two newtype conversions. Every
    /// other rule belongs to the section that consumes the field, and each of
    /// those sections validates itself where the runtime builds it.
    ///
    /// # Errors
    ///
    /// Returns [`PeerConfigurationError`] when the frame size or the
    /// registration lease is outside its supported range.
    pub(crate) fn parts(&self) -> Result<PeerParts, PeerConfigurationError> {
        let frame_cap = FrameCap::new(self.frame_bytes)?;
        let lease = RegistrationTtl::try_from(self.registration_lease)?;
        Ok(PeerParts {
            transport: TransportConfiguration {
                bind: self.bind,
                frame_cap,
                max_connections: self.max_connections,
                max_concurrent_streams: self.max_concurrent_streams,
                reflection: self.reflection,
            },
            router: RouterConfiguration {
                advertised_host: self.advertised_host.clone(),
                advertised_port: self.advertised_port,
                network: self.network.clone(),
                address_cache_capacity: self.address_cache_capacity,
            },
            fleet: FleetConfiguration {
                max_destinations: self.max_destinations,
                slots_each: self.slots_each,
                sends_per_second: self.sends_per_second,
                send_deadline: self.send_deadline,
                max_send_attempts: self.max_send_attempts,
            },
            requester: RequesterConfiguration {
                max_in_flight: self.max_in_flight,
                max_awaited: self.max_awaited,
                max_response_bytes: self.max_response_bytes,
                max_timeout: self.max_timeout,
                sweep_grace: self.sweep_grace,
            },
            lease,
            probe: self.route_probe,
        })
    }
}

/// Why a peer configuration cannot form its internal sections.
#[derive(Debug, Error)]
pub(crate) enum PeerConfigurationError {
    /// The frame size is outside its supported range.
    #[error("invalid frame size: {0:#}")]
    Frame(#[from] FrameCapError),
    /// The registration lease is outside its supported range.
    #[error("invalid registration lease: {0:#}")]
    Lease(#[from] RegistrationTtlError),
}
