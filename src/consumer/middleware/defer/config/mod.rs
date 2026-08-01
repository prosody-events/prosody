//! Configuration for defer middleware.

use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};
use derive_builder::Builder;
use std::time::Duration;
use thiserror::Error;
use validator::Validate;

/// Configuration for defer middleware.
///
/// Uses separate environment variables from `RetryMiddleware` to allow
/// different backoff parameters for persistent vs transient failures.
#[derive(Builder, Clone, Debug, Validate)]
#[builder(build_fn(private, name = "build_internal"))]
#[validate(schema(function = "validate_delays"))]
pub struct DeferConfiguration {
    /// Whether deferral is enabled for new messages.
    ///
    /// When disabled, transient failures will not be deferred and will instead
    /// propagate to the retry middleware. Already-deferred messages will still
    /// complete their retry cycles to maintain ordering guarantees.
    ///
    /// Environment variable: `PROSODY_DEFER_ENABLED`
    /// Default: true
    #[builder(default = "from_env_with_fallback(\"PROSODY_DEFER_ENABLED\", true)?")]
    pub enabled: bool,

    /// Base exponential backoff delay for deferred retries.
    ///
    /// This is longer than `RetryMiddleware`'s base delay because
    /// deferred retries handle persistent failures that need time to
    /// recover (e.g., downstream service outages).
    ///
    /// Environment variable: `PROSODY_DEFER_BASE`
    /// Default: 1 second
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_DEFER_BASE\", \
                   Duration::from_secs(1))?",
        setter(into)
    )]
    #[validate(custom(function = "validate_base_delay"))]
    pub base: Duration,

    /// Maximum retry delay for deferred retries.
    ///
    /// Caps the exponential backoff to prevent excessively long delays.
    ///
    /// Environment variable: `PROSODY_DEFER_MAX_DELAY`
    /// Default: 24 hours
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_DEFER_MAX_DELAY\", \
                   Duration::from_hours(24))?",
        setter(into)
    )]
    pub max_delay: Duration,

    /// Failure rate threshold for disabling deferral (0.0 to 1.0).
    ///
    /// When failure rate exceeds this threshold within the failure window,
    /// deferral is disabled to prevent cascading failures.
    ///
    /// Environment variable: `PROSODY_DEFER_FAILURE_THRESHOLD`
    /// Default: 0.9 (90%)
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_DEFER_FAILURE_THRESHOLD\", 0.9)?",
        setter(into)
    )]
    #[validate(range(min = 0.0_f64, max = 1.0_f64))]
    pub failure_threshold: f64,

    /// Time window for failure rate tracking.
    ///
    /// Failures are counted within this sliding window to determine
    /// whether deferral remains enabled.
    ///
    /// Environment variable: `PROSODY_DEFER_FAILURE_WINDOW`
    /// Default: 5 minutes
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_DEFER_FAILURE_WINDOW\", \
                   Duration::from_mins(5))?",
        setter(into)
    )]
    pub failure_window: Duration,

    /// Cache size for the per-partition Cassandra defer store cache.
    ///
    /// Controls the `(key → Option<(next_offset, retry_count)>)` cache inside
    /// each `CassandraMessageDeferStore` / `CassandraTimerDeferStore`. Larger
    /// values reduce `read_next_static` round-trips for wide-fan-out workloads
    /// at the cost of memory (~128 B/entry × 2 stores/partition × partitions).
    ///
    /// Environment variable: `PROSODY_DEFER_STORE_CACHE_SIZE`
    /// Default: 8,192 entries
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_DEFER_STORE_CACHE_SIZE\", 8_192)?",
        setter(into)
    )]
    #[validate(range(min = 1_usize))]
    pub store_cache_size: usize,
}

impl DeferConfiguration {
    /// Creates a new configuration builder.
    #[must_use]
    pub fn builder() -> DeferConfigurationBuilder {
        DeferConfigurationBuilder::default()
    }
}

/// Error type for defer configuration building and validation.
#[derive(Debug, Error)]
pub enum DeferConfigError {
    /// Error during configuration building (missing fields, etc).
    #[error("configuration build error: {0:#}")]
    Build(#[from] DeferConfigurationBuilderError),

    /// Error during configuration validation.
    #[error("configuration validation error: {0:#}")]
    Validation(#[from] validator::ValidationErrors),
}

impl DeferConfigurationBuilder {
    /// Builds the configuration and validates it.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any required field is missing
    /// - Any validation constraint is violated
    pub fn build(&mut self) -> Result<DeferConfiguration, DeferConfigError> {
        let config = self.build_internal()?;
        config.validate()?;
        Ok(config)
    }
}

fn validate_base_delay(base: &Duration) -> Result<(), validator::ValidationError> {
    // Minimum 1 second (1000 ms)
    if base.as_millis() < 1_000 {
        return Err(validator::ValidationError::new(
            "base delay must be at least 1 second",
        ));
    }
    Ok(())
}

fn validate_delays(config: &DeferConfiguration) -> Result<(), validator::ValidationError> {
    // Validate max_delay >= base
    if config.max_delay < config.base {
        let mut error = validator::ValidationError::new("max_delay_less_than_base");
        error.message = Some("max_delay must be greater than or equal to base".into());
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
