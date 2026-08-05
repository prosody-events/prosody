//! What bounds the destination fleet, and the ceilings a configuration is held
//! to at startup.

use crate::response::frame::FrameCap;
use derive_builder::Builder;
use std::time::Duration;
use thiserror::Error;
use validator::{Validate, ValidationError, ValidationErrors};

/// Most destinations one process may hold live at once. The table is scanned
/// linearly, so this bounds the scan too.
pub(super) const MAX_DESTINATIONS: usize = 1024;

/// Most send slots one destination may hold.
const MAX_SLOTS_EACH: usize = 256;

/// Most send slots one process may commit to in total.
///
/// A slot holds one moved handler result and one frame header, so this bounds
/// how many results a process holds at once, not how many bytes they come to: a
/// result is the application's own type and nothing measures it before the
/// codec runs. The frame ceiling bounds what one response puts on the wire;
/// this bounds how many of them wait for their turn.
pub(super) const MAX_TOTAL_SLOTS: usize = 8_192;

/// Most bytes one sender may commit to per-destination encode scratch.
///
/// One sender builds one encode buffer per destination, and one task beside it,
/// and holds both for the sender's whole life. A buffer is at the ceiling
/// between responses; a codec may grow it beyond that while it serializes one.
///
/// **The budget is per sender, and nothing counts senders.** That exposure is
/// accepted rather than overlooked. At the defaults one sender is 4 MiB and 64
/// tasks; at this ceiling it is 64 MiB and 1024 tasks. A process builds one
/// sender per handler-result type, which is a small deployment constant that no
/// traffic moves, so the multiplier is knowable by whoever writes the handlers
/// and is not a configuration value any validator could read.
const MAX_SCRATCH_BYTES_PER_SENDER: u64 = 64 * 1024 * 1024;

/// Fastest one destination may be sent to.
const MAX_SENDS_PER_SECOND: u32 = 1_000_000;

/// Longest deadline an operator may set on one response.
const MAX_SEND_DEADLINE: Duration = Duration::from_mins(5);

/// Most attempts one response may take, the first included.
const MAX_SEND_ATTEMPTS: u32 = 8;

/// Destinations a process holds live when an operator asks for no number.
const DEFAULT_MAX_DESTINATIONS: usize = 64;

/// Slots one destination holds by default.
const DEFAULT_SLOTS_EACH: usize = 8;

/// Sends per second to one destination by default.
const DEFAULT_SENDS_PER_SECOND: u32 = 100;

/// How long one response may take by default.
const DEFAULT_SEND_DEADLINE: Duration = Duration::from_secs(5);

/// Attempts one response takes by default.
const DEFAULT_SEND_ATTEMPTS: u32 = 3;

/// What an operator sets for response delivery.
///
/// Every field has a working default, so a deployment that answers peers needs
/// no configuration at all. `max_destinations` multiplied by `slots_each` is
/// what the process commits to, so that product is checked against a ceiling of
/// its own rather than left to two independently plausible numbers.
#[derive(Builder, Clone, Copy, Debug, Validate)]
#[builder(setter(into), default)]
#[validate(schema(function = "validate_total_slots"))]
pub(crate) struct FleetConfiguration {
    /// How many destinations may be live at once. A new destination beyond this
    /// evicts an idle one, and is refused when every one of them is busy.
    #[validate(range(min = 1_usize, max = MAX_DESTINATIONS))]
    pub(crate) max_destinations: usize,

    /// How many responses one destination may have waiting and in flight
    /// together, over the whole process. Each sender drives one destination
    /// with one worker. So a process that runs one sender overlaps no sends,
    /// and this is purely queue depth.
    #[validate(range(min = 1_usize, max = MAX_SLOTS_EACH))]
    pub(crate) slots_each: usize,

    /// How fast one destination may be sent to. The limit is the destination's
    /// own, so it paces every send to it whatever queued them.
    #[validate(range(min = 1_u32, max = MAX_SENDS_PER_SECOND))]
    pub(crate) sends_per_second: u32,

    /// How long one response may spend on pacing, address resolution, encoding
    /// and every attempt together.
    #[validate(custom(function = "validate_send_deadline"))]
    pub(crate) send_deadline: Duration,

    /// How many attempts one response may take, the first included.
    #[validate(range(min = 1_u32, max = MAX_SEND_ATTEMPTS))]
    pub(crate) max_send_attempts: u32,
}

impl Default for FleetConfiguration {
    fn default() -> Self {
        Self {
            max_destinations: DEFAULT_MAX_DESTINATIONS,
            slots_each: DEFAULT_SLOTS_EACH,
            sends_per_second: DEFAULT_SENDS_PER_SECOND,
            send_deadline: DEFAULT_SEND_DEADLINE,
            max_send_attempts: DEFAULT_SEND_ATTEMPTS,
        }
    }
}

impl FleetConfiguration {
    /// Creates a configuration builder.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn builder() -> FleetConfigurationBuilder {
        FleetConfigurationBuilder::default()
    }
}

/// Refuses a frame ceiling one sender cannot afford.
///
/// A table size and a frame ceiling are each plausible alone; their product is
/// what a sender commits to, so they are checked together. See
/// [`MAX_SCRATCH_BYTES_PER_SENDER`] for what that product buys.
///
/// # Errors
///
/// Returns [`FleetConfigurationError::ScratchBudget`] when the product exceeds
/// what one sender may commit to.
pub(crate) fn validate_scratch_budget(
    max_destinations: usize,
    cap: FrameCap,
) -> Result<(), FleetConfigurationError> {
    let bytes = (max_destinations as u64).saturating_mul(cap.bytes() as u64);
    if bytes > MAX_SCRATCH_BYTES_PER_SENDER {
        return Err(FleetConfigurationError::ScratchBudget {
            bytes,
            limit: MAX_SCRATCH_BYTES_PER_SENDER,
        });
    }
    Ok(())
}

/// Refuses a table whose slots together exceed what one process may commit to.
/// A product that overflows is over the ceiling by definition.
fn validate_total_slots(config: &FleetConfiguration) -> Result<(), ValidationError> {
    if config
        .max_destinations
        .checked_mul(config.slots_each)
        .is_none_or(|total| total > MAX_TOTAL_SLOTS)
    {
        return Err(ValidationError::new("total_slots"));
    }
    Ok(())
}

/// Refuses a deadline of zero, which would expire every response before it was
/// encoded, and one so long that a slot is held for the rest of the process.
fn validate_send_deadline(deadline: &Duration) -> Result<(), ValidationError> {
    if deadline.is_zero() {
        return Err(ValidationError::new("send_deadline_zero"));
    }
    if *deadline > MAX_SEND_DEADLINE {
        return Err(ValidationError::new("send_deadline_too_long"));
    }
    Ok(())
}

/// Why a fleet cannot be built from what an operator asked for.
#[derive(Clone, Debug, Error)]
pub(crate) enum FleetConfigurationError {
    /// A field is outside its supported range, or the slot total is.
    #[error("fleet configuration is invalid: {0:#}")]
    Invalid(#[from] ValidationErrors),

    /// One encode buffer per destination would need more than one sender may
    /// commit to.
    #[error("encode buffers would need {bytes} bytes, over the {limit}-byte budget")]
    ScratchBudget {
        /// What the configuration asks for.
        bytes: u64,
        /// The most one sender may commit to.
        limit: u64,
    },
}
