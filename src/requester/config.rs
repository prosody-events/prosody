//! Request admission and deadline limits.

use crate::response::headers::MAX_AWAITED;
use derive_builder::Builder;
use std::time::Duration;
use validator::{Validate, ValidationError};

/// Most requests one process may hold at once.
pub(in crate::requester) const MAX_IN_FLIGHT: usize = 65_536;

/// Longest request timeout or sweep grace.
const MAX_TIMEOUT_CEILING: Duration = Duration::from_mins(10);

/// Requests one process holds when an operator sets no limit.
const DEFAULT_MAX_IN_FLIGHT: usize = 1_024;

/// Subsystems one request awaits when an operator sets no limit.
const DEFAULT_MAX_AWAITED: usize = 8;

/// Longest timeout a caller may use by default.
const DEFAULT_MAX_TIMEOUT: Duration = Duration::from_secs(30);

/// Time an expired entry remains available before removal by default.
const DEFAULT_SWEEP_GRACE: Duration = Duration::from_secs(5);

/// Sets limits for the requesting side of synchrony recovery.
#[derive(Builder, Clone, Debug, Validate)]
#[builder(setter(into), default)]
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

/// Refuses a zero timeout and a timeout above the process ceiling.
fn validate_max_timeout(timeout: &Duration) -> Result<(), ValidationError> {
    validate_duration(timeout, "max_timeout_zero", "max_timeout_too_long")
}

/// Refuses a grace that can race a live waiter or retain an entry too long.
fn validate_grace(grace: &Duration) -> Result<(), ValidationError> {
    validate_duration(grace, "sweep_grace_zero", "sweep_grace_too_long")
}

/// Applies the common nonzero duration ceiling.
fn validate_duration(
    duration: &Duration,
    zero_code: &'static str,
    long_code: &'static str,
) -> Result<(), ValidationError> {
    if duration.is_zero() {
        return Err(ValidationError::new(zero_code));
    }
    if *duration > MAX_TIMEOUT_CEILING {
        return Err(ValidationError::new(long_code));
    }
    Ok(())
}
