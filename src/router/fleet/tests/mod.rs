//! What the fleet's own suites share.

use crate::router::NodeId;
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::{FleetConfiguration, FleetConfigurationError};

mod bounds;
mod config;
mod gate;
mod rate;

/// A node id from one repeated byte, so a pool index reads directly.
pub(super) fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index; 16])
}

/// A fleet of `max_destinations` cells with `slots_each` slots in each.
pub(super) fn fleet(
    max_destinations: usize,
    slots_each: usize,
) -> Result<DestinationFleet, FleetConfigurationError> {
    DestinationFleet::new(FleetConfiguration {
        max_destinations,
        slots_each,
        ..FleetConfiguration::default()
    })
}
