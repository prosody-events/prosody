//! Bounds for peer-keyed caches.

use thiserror::Error;
use validator::{Validate, ValidationErrors};

const DEFAULT_PEER_CAPACITY: usize = 256;

/// The shared bound for peer address and channel caches.
#[derive(Clone, Copy, Debug, Validate)]
pub(crate) struct PeerCacheConfiguration {
    /// Maximum peer records held in each peer-keyed cache.
    #[validate(range(min = 1_usize))]
    pub(crate) peer_capacity: usize,
}

impl Default for PeerCacheConfiguration {
    fn default() -> Self {
        Self {
            peer_capacity: DEFAULT_PEER_CAPACITY,
        }
    }
}

/// Why a peer cache bound is invalid.
#[derive(Clone, Debug, Error)]
pub(crate) enum PeerCacheConfigurationError {
    /// One value is outside its supported range.
    #[error("peer cache configuration is invalid: {0:#}")]
    Invalid(#[from] ValidationErrors),
}
