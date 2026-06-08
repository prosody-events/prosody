//! Deduplication middleware configuration.

use derive_builder::Builder;
use std::time::Duration;
use validator::{Validate, ValidationError};

use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::util::{
    DEFAULT_IDEMPOTENCE_CACHE_SIZE, from_duration_env_with_fallback, from_env_with_fallback,
};

/// Environment variable controlling the deduplication hash version.
///
/// Shared by the deduplication config default and any other component that
/// must reproduce the same dedup version (e.g. the keyed-state middleware),
/// so the version can never silently diverge between the writer and a reader.
pub const IDEMPOTENCE_VERSION_ENV: &str = "PROSODY_IDEMPOTENCE_VERSION";

/// Default deduplication hash version when [`IDEMPOTENCE_VERSION_ENV`] is
/// unset.
pub const DEFAULT_IDEMPOTENCE_VERSION: &str = "1";

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
    #[builder(default = "from_env_with_fallback(IDEMPOTENCE_VERSION_ENV, \
                         DEFAULT_IDEMPOTENCE_VERSION.to_owned())?")]
    pub version: String,

    /// Global shared cache capacity across all partitions. Deduplication is
    /// mandatory (the keyed-state commit oracle), so this must be at least 1.
    ///
    /// Environment variable: `PROSODY_IDEMPOTENCE_CACHE_SIZE`
    /// Default: 8192
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_IDEMPOTENCE_CACHE_SIZE\", \
                   DEFAULT_IDEMPOTENCE_CACHE_SIZE)?"
    )]
    #[validate(range(min = 1_usize))]
    pub cache_capacity: usize,

    /// Cassandra TTL for deduplication records. Must be at least 1 minute
    /// and must not exceed Cassandra's maximum TTL of 630,720,000 seconds.
    ///
    /// Environment variable: `PROSODY_IDEMPOTENCE_TTL`
    /// Default: 7 days
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_IDEMPOTENCE_TTL\", \
                   Duration::from_hours(7 * 24))?"
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
const MIN_DEDUP_TTL: Duration = Duration::from_mins(1);

fn validate_dedup_ttl(ttl: &Duration) -> Result<(), ValidationError> {
    if *ttl < MIN_DEDUP_TTL {
        return Err(ValidationError::new("ttl_below_minimum"));
    }
    if ttl.as_secs() > MAX_CASSANDRA_TTL_SECS as u64 {
        return Err(ValidationError::new("ttl_exceeds_cassandra_max"));
    }
    Ok(())
}
