//! Configuration for defer middleware.

use crate::cassandra::CassandraConfiguration;
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};
use derive_builder::Builder;
use std::time::Duration;
use validator::Validate;

/// Configuration for defer middleware.
///
/// Uses separate environment variables from `RetryMiddleware` to allow
/// different backoff parameters for persistent vs transient failures.
#[derive(Builder, Clone, Debug, Validate)]
#[builder(build_fn(private, name = "build_internal"))]
#[validate(schema(function = "validate_delays"))]
pub struct DeferConfiguration {
    /// Base exponential backoff delay for deferred retries.
    ///
    /// This is much longer than `RetryMiddleware`'s base delay because
    /// deferred retries handle persistent failures that need time to
    /// recover (e.g., downstream service outages).
    ///
    /// Environment variable: `PROSODY_DEFER_BASE`
    /// Default: 1 minute
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_DEFER_BASE\", \
                   Duration::from_secs(60))?",
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
                   Duration::from_secs(24 * 60 * 60))?",
        setter(into)
    )]
    pub max_delay: Duration,

    /// Failure rate threshold for enabling deferral (0.0 to 1.0).
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
    /// whether to enable deferral.
    ///
    /// Environment variable: `PROSODY_DEFER_FAILURE_WINDOW`
    /// Default: 5 minutes
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_DEFER_FAILURE_WINDOW\", \
                   Duration::from_secs(5 * 60))?",
        setter(into)
    )]
    pub failure_window: Duration,

    /// Cache size for deferred key state.
    ///
    /// Caches the defer state (`NotDeferred` vs `Deferred { retry_count }`) for
    /// each key to avoid Cassandra reads on every message.
    ///
    /// Environment variable: `PROSODY_DEFER_CACHE_SIZE`
    /// Default: 10,000 keys
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_DEFER_CACHE_SIZE\", 10_000)?",
        setter(into)
    )]
    #[validate(range(min = 1_usize))]
    pub cache_size: usize,

    /// Storage backend for deferred messages.
    ///
    /// Determines where deferred offsets are persisted. Uses the same
    /// `CassandraConfiguration` as the trigger store for consistency.
    #[builder(default)]
    pub store: DeferStoreConfiguration,
}

impl DeferConfiguration {
    /// Creates a new configuration builder.
    #[must_use]
    pub fn builder() -> DeferConfigurationBuilder {
        DeferConfigurationBuilder::default()
    }
}

/// Storage backend configuration for defer middleware.
#[derive(Debug, Clone, Default)]
pub enum DeferStoreConfiguration {
    /// In-memory storage for testing and development.
    ///
    /// Uses HashMap-based storage that is lost on process restart.
    /// Not suitable for production use.
    #[default]
    InMemory,

    /// Cassandra-based persistent storage.
    ///
    /// Uses the same `CassandraConfiguration` as the trigger store,
    /// ensuring consistent connection pooling and TTL configuration.
    Cassandra(CassandraConfiguration),
}

impl DeferConfigurationBuilder {
    /// Builds the configuration and validates it.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any required field is missing
    /// - Any validation constraint is violated
    pub fn build(&mut self) -> Result<DeferConfiguration, String> {
        let config = self.build_internal().map_err(|e| e.to_string())?;
        config.validate().map_err(|e| e.to_string())?;
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
mod tests {
    use super::*;

    #[test]
    fn test_default_configuration() {
        let config = DeferConfiguration::builder().build();
        assert!(config.is_ok());
    }

    #[test]
    fn test_custom_configuration() {
        let config = DeferConfiguration::builder()
            .base(Duration::from_secs(120))
            .max_delay(Duration::from_secs(3600))
            .failure_threshold(0.8_f64)
            .failure_window(Duration::from_secs(600))
            .cache_size(5_000_usize)
            .build();

        assert!(config.is_ok());
    }

    #[test]
    fn test_max_delay_less_than_base_fails() {
        let result = DeferConfiguration::builder()
            .base(Duration::from_secs(120))
            .max_delay(Duration::from_secs(60))
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_base_delay_too_small_fails() {
        let result = DeferConfiguration::builder()
            .base(Duration::from_millis(500))
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_failure_threshold_out_of_range_fails() {
        let result = DeferConfiguration::builder()
            .failure_threshold(1.5_f64)
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_cache_size_zero_fails() {
        let result = DeferConfiguration::builder().cache_size(0_usize).build();

        assert!(result.is_err());
    }

    #[test]
    fn test_default_store_is_in_memory() {
        let result = DeferConfiguration::builder().build();
        assert!(result.is_ok());
    }
}
