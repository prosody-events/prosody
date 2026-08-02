//! What a configuration must satisfy before a process builds a fleet from it.

use crate::response::frame::FrameCap;
use crate::router::fleet::config::{
    FleetConfiguration, FleetConfigurationError, validate_scratch_budget,
};
use color_eyre::Result;
use std::time::Duration;
use validator::Validate;

/// The largest table and the largest slot count the ceiling admits together.
const CEILING_DESTINATIONS: usize = 1024;
const CEILING_SLOTS: usize = 8;

/// A frame ceiling one process can afford at the default table size.
const AFFORDABLE_CAP: usize = 1024 * 1024;

/// The slot total is checked at startup, so a table and a slot count that are
/// each plausible cannot commit the process to a heap it does not have.
#[test]
fn the_startup_ceiling_refuses_an_over_large_fleet() -> Result<()> {
    let over = FleetConfiguration::builder()
        .max_destinations(CEILING_DESTINATIONS)
        .slots_each(CEILING_SLOTS + 1)
        .build()?;
    assert!(
        over.validate().is_err(),
        "a fleet over the slot ceiling must be refused"
    );

    let largest = FleetConfiguration::builder()
        .max_destinations(CEILING_DESTINATIONS)
        .slots_each(CEILING_SLOTS)
        .build()?;
    assert!(
        largest.validate().is_ok(),
        "the largest fleet inside the ceiling must be accepted"
    );
    Ok(())
}

/// Every field that can express a degenerate value refuses it, so no fleet is
/// built with a table of no cells, a destination of no slots, no rate, no
/// deadline, or no attempts.
#[test]
fn a_degenerate_field_is_refused() -> Result<()> {
    let degenerate = [
        FleetConfiguration::builder()
            .max_destinations(0_usize)
            .build()?,
        FleetConfiguration::builder().slots_each(0_usize).build()?,
        FleetConfiguration::builder()
            .sends_per_second(0_u32)
            .build()?,
        FleetConfiguration::builder()
            .send_deadline(Duration::ZERO)
            .build()?,
        FleetConfiguration::builder()
            .max_send_attempts(0_u32)
            .build()?,
        FleetConfiguration::builder()
            .send_deadline(Duration::from_hours(1))
            .build()?,
    ];
    for config in degenerate {
        assert!(
            config.validate().is_err(),
            "{config:?} must be refused before a fleet is built from it"
        );
    }
    Ok(())
}

/// One encode buffer per destination is what the frame ceiling really costs, so
/// the two numbers are checked together rather than separately.
#[test]
fn the_scratch_budget_refuses_a_ceiling_the_process_cannot_afford() -> Result<()> {
    let destinations = FleetConfiguration::default().max_destinations;
    assert!(
        matches!(
            validate_scratch_budget(destinations, FrameCap::new(FrameCap::MAX_BYTES)?),
            Err(FleetConfigurationError::ScratchBudget { .. })
        ),
        "the largest frame ceiling must be refused at the default table size"
    );
    assert!(
        validate_scratch_budget(destinations, FrameCap::new(AFFORDABLE_CAP)?).is_ok(),
        "a ceiling that fits the budget must be accepted"
    );
    Ok(())
}
