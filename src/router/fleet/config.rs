//! Response delivery policy.

use std::time::Duration;
use thiserror::Error;
use validator::{Validate, ValidationError, ValidationErrors};

const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_PEER_CAPACITY: usize = 256;

/// Response delivery policy.
#[derive(Clone, Copy, Debug, Validate)]
pub(crate) struct FleetConfiguration {
    /// Maximum peer records held in each node-keyed cache.
    #[validate(range(min = 1_usize))]
    pub(crate) peer_capacity: usize,
    /// Maximum time for one response delivery.
    #[validate(custom(function = "validate_response_timeout"))]
    pub(crate) response_timeout: Duration,
}

impl Default for FleetConfiguration {
    fn default() -> Self {
        Self {
            peer_capacity: DEFAULT_PEER_CAPACITY,
            response_timeout: DEFAULT_RESPONSE_TIMEOUT,
        }
    }
}

fn validate_response_timeout(timeout: &Duration) -> Result<(), ValidationError> {
    if timeout.is_zero() {
        return Err(ValidationError::new("response_timeout_zero"));
    }
    Ok(())
}

/// Why response delivery policy is invalid.
#[derive(Clone, Debug, Error)]
pub(crate) enum FleetConfigurationError {
    /// One value is outside its supported range.
    #[error("response delivery configuration is invalid: {0:#}")]
    Invalid(#[from] ValidationErrors),
}
