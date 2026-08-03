//! What the fleet's own suites share.

use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfigurationError;
use crate::router::loopback::config;
pub(super) use crate::router::loopback::node;

mod bounds;
mod config;
mod gate;
mod rate;

/// A fleet of `max_destinations` cells with `slots_each` slots in each.
pub(super) fn fleet(
    max_destinations: usize,
    slots_each: usize,
) -> Result<DestinationFleet, FleetConfigurationError> {
    DestinationFleet::new(config(max_destinations, slots_each))
}
