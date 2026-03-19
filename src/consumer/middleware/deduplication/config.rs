//! Deduplication middleware configuration.

use derive_builder::Builder;
use std::time::Duration;
use validator::{Validate, ValidationError};

use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};

/// Configuration for the deduplication middleware.
#[derive(Builder, Clone, Debug, Validate)]
pub struct DeduplicationConfiguration {
    /// Version string for cache-busting deduplication hashes.
    ///
    /// Changing this value invalidates all previously recorded dedup entries,
    /// causing messages to be reprocessed.
    ///
    /// Environment variable: `PROSODY_IDEMPOTENCE_VERSION`
    /// Default: `"1"`
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_IDEMPOTENCE_VERSION\", \"1\".to_owned())?"
    )]
    pub version: String,

    /// Global shared cache capacity across all partitions. Set to 0 to disable
    /// the deduplication middleware entirely.
    ///
    /// Environment variable: `PROSODY_IDEMPOTENCE_CACHE_SIZE`
    /// Default: 8192
    #[builder(default = "from_env_with_fallback(\"PROSODY_IDEMPOTENCE_CACHE_SIZE\", 8192_usize)?")]
    pub cache_capacity: usize,

    /// Cassandra TTL for deduplication records. Must be at least 1 minute
    /// and must not exceed Cassandra's maximum TTL of 630,720,000 seconds.
    ///
    /// Environment variable: `PROSODY_IDEMPOTENCE_TTL`
    /// Default: 7 days
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_IDEMPOTENCE_TTL\", \
                   Duration::from_secs(7 * 24 * 3600))?"
    )]
    #[validate(custom(function = "validate_dedup_ttl"))]
    pub ttl: Duration,
}

impl DeduplicationConfiguration {
    /// Creates a new builder.
    #[must_use]
    pub fn builder() -> DeduplicationConfigurationBuilder {
        DeduplicationConfigurationBuilder::default()
    }
}

/// Minimum deduplication TTL (1 minute). Shorter TTLs risk records expiring
/// before a consumer rebalance completes.
const MIN_DEDUP_TTL: Duration = Duration::from_secs(60);

fn validate_dedup_ttl(ttl: &Duration) -> Result<(), ValidationError> {
    if *ttl < MIN_DEDUP_TTL {
        return Err(ValidationError::new("ttl_below_minimum"));
    }
    if ttl.as_secs() > MAX_CASSANDRA_TTL_SECS as u64 {
        return Err(ValidationError::new("ttl_exceeds_cassandra_max"));
    }
    Ok(())
}
