//! Request admission and deadline limits.

use crate::response::frame::FrameCap;
use crate::response::headers::MAX_AWAITED;
use derive_builder::Builder;
use std::time::Duration;
use validator::{Validate, ValidationError};

/// Most requests one process may hold at once.
pub(crate) const MAX_IN_FLIGHT: usize = 65_536;

/// Shortest timeout one request may ask for.
pub(in crate::requester) const MIN_TIMEOUT: Duration = Duration::from_millis(1);

/// Longest request timeout or sweep grace.
const MAX_TIMEOUT_CEILING: Duration = Duration::from_mins(10);

/// Shortest grace between one sweep pass and the next.
///
/// The grace is also the scan period, so a shorter one turns the sweep into a
/// continuous scan of the whole map.
const MIN_SWEEP_GRACE: Duration = Duration::from_millis(100);

/// Most bytes one process may commit to responses held in the registry.
///
/// Admission, the awaited limit, and the response ceiling are each plausible
/// alone. Their product is what the registry commits to, so the three are
/// checked together rather than one at a time.
const MAX_RETAINED_BYTES: u64 = 1024 * 1024 * 1024;

/// Requests one process holds when an operator sets no limit.
const DEFAULT_MAX_IN_FLIGHT: usize = 1_024;

/// Subsystems one request awaits when an operator sets no limit.
const DEFAULT_MAX_AWAITED: usize = 8;

/// Bytes one response carries when an operator sets no limit.
const DEFAULT_MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// Longest timeout a caller may use by default.
const DEFAULT_MAX_TIMEOUT: Duration = Duration::from_secs(30);

/// Time an expired entry remains available before removal by default.
const DEFAULT_SWEEP_GRACE: Duration = Duration::from_secs(5);

/// Sets limits for the requesting side of synchrony recovery.
#[derive(Builder, Clone, Debug, Validate)]
#[builder(setter(into), default)]
#[validate(schema(function = "validate_retained_bytes"))]
pub(crate) struct RequesterConfiguration {
    /// Requests that may be in flight at once.
    ///
    /// This value also sets the initial pending map capacity.
    #[validate(range(min = 1_usize, max = MAX_IN_FLIGHT))]
    pub(crate) max_in_flight: usize,

    /// Most subsystems one request may await.
    ///
    /// The wire parser uses the same upper bound.
    #[validate(range(min = 1_usize, max = MAX_AWAITED))]
    pub(crate) max_awaited: usize,

    /// Most bytes one response payload may carry.
    ///
    /// The registry refuses a larger payload, so this is what one filled
    /// position holds.
    #[validate(custom(function = "validate_response_bytes"))]
    pub(crate) max_response_bytes: usize,

    /// Longest timeout a caller may request.
    #[validate(custom(function = "validate_max_timeout"))]
    pub(crate) max_timeout: Duration,

    /// Time an expired entry remains before the sweep removes it.
    #[validate(custom(function = "validate_grace"))]
    pub(crate) sweep_grace: Duration,
}

impl Default for RequesterConfiguration {
    fn default() -> Self {
        Self {
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            max_awaited: DEFAULT_MAX_AWAITED,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_timeout: DEFAULT_MAX_TIMEOUT,
            sweep_grace: DEFAULT_SWEEP_GRACE,
        }
    }
}

impl RequesterConfiguration {
    /// Creates a requester configuration builder.
    #[must_use]
    pub(crate) fn builder() -> RequesterConfigurationBuilder {
        RequesterConfigurationBuilder::default()
    }
}

/// Refuses a ceiling no request could fit under, and one above the process
/// ceiling.
///
/// A ceiling below [`MIN_TIMEOUT`] leaves the accepted range empty, so every
/// request would fail at runtime with nothing said at startup.
fn validate_max_timeout(timeout: &Duration) -> Result<(), ValidationError> {
    validate_duration(
        timeout,
        MIN_TIMEOUT,
        "max_timeout_too_short",
        "max_timeout_too_long",
    )
}

/// Refuses a grace that can race a live waiter or retain an entry too long.
fn validate_grace(grace: &Duration) -> Result<(), ValidationError> {
    validate_duration(
        grace,
        MIN_SWEEP_GRACE,
        "sweep_grace_too_short",
        "sweep_grace_too_long",
    )
}

/// Refuses a response ceiling outside the range one peer frame may carry.
///
/// A payload no frame could hold is unreachable, and one above the frame
/// ceiling would let a single response exhaust the receive budget.
fn validate_response_bytes(bytes: usize) -> Result<(), ValidationError> {
    FrameCap::new(bytes)
        .map(drop)
        .map_err(|_| ValidationError::new("max_response_bytes_out_of_range"))
}

/// Refuses limits whose product is more than the registry may hold.
fn validate_retained_bytes(config: &RequesterConfiguration) -> Result<(), ValidationError> {
    let bytes = (config.max_in_flight as u64)
        .saturating_mul(config.max_awaited as u64)
        .saturating_mul(config.max_response_bytes as u64);
    if bytes > MAX_RETAINED_BYTES {
        return Err(ValidationError::new("retained_bytes"));
    }
    Ok(())
}

/// Applies one duration's own floor and the common ceiling.
fn validate_duration(
    duration: &Duration,
    min: Duration,
    short_code: &'static str,
    long_code: &'static str,
) -> Result<(), ValidationError> {
    if *duration < min {
        return Err(ValidationError::new(short_code));
    }
    if *duration > MAX_TIMEOUT_CEILING {
        return Err(ValidationError::new(long_code));
    }
    Ok(())
}
