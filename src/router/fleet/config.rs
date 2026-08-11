//! Response delivery policy.

use thiserror::Error;
use validator::{Validate, ValidationErrors};

const DEFAULT_PEER_CAPACITY: usize = 256;

/// Response delivery policy.
#[derive(Clone, Copy, Debug, Validate)]
pub(crate) struct FleetConfiguration {
    /// Maximum peer records held in each peer-keyed cache.
    #[validate(range(min = 1_usize))]
    pub(crate) peer_capacity: usize,
}

impl Default for FleetConfiguration {
    fn default() -> Self {
        Self {
            peer_capacity: DEFAULT_PEER_CAPACITY,
        }
    }
}

/// Why response delivery policy is invalid.
#[derive(Clone, Debug, Error)]
pub(crate) enum FleetConfigurationError {
    /// One value is outside its supported range.
    #[error("response delivery configuration is invalid: {0:#}")]
    Invalid(#[from] ValidationErrors),
}
