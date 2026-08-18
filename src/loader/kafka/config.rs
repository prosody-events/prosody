use std::time::Duration;

use derive_builder::Builder;
use thiserror::Error;
use validator::Validate;

use super::loader_capacity;
use crate::consumer::ConsumerConfiguration;
use crate::otel::SpanRelation;
use crate::subsystem::SubsystemName;
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};

/// Internal Kafka loader settings derived from consumer configuration.
#[derive(Clone, Debug)]
pub struct LoaderConfiguration {
    /// Kafka broker addresses.
    ///
    /// List of host:port pairs for initial connection to the Kafka cluster.
    pub bootstrap_servers: Vec<String>,

    /// Consumer group ID base name. The loader appends `.loader`.
    pub group_id: String,

    /// Maximum number of loaded messages retained by callers.
    ///
    /// Controls the semaphore and request-channel capacity.
    pub max_permits: usize,

    /// Maximum number of messages to cache.
    ///
    /// The cache uses S3-FIFO eviction policy which quickly evicts "one-hit
    /// wonders" while keeping frequently accessed items. Cache capacity is
    /// managed purely by `quick_cache`'s internal mechanisms.
    pub cache_size: usize,

    /// Interval between poll operations when no messages are available.
    pub poll_interval: Duration,

    /// Timeout for seek operations.
    ///
    /// How long to wait for a seek operation to complete before failing.
    pub seek_timeout: Duration,

    /// Number of messages to read sequentially before performing a seek.
    ///
    /// If the next requested offset is within this threshold, we continue
    /// reading and discard intermediate messages rather than performing an
    /// expensive seek operation. This provides significant performance
    /// benefits:
    /// - Kafka seeks: ~10-100ms (network round trips, index lookups)
    /// - Reading 100 messages: ~1-10ms (sequential, already buffered)
    /// - Bandwidth cost: ~10-100KB per 100 messages
    pub discard_threshold: i64,

    /// Span relation for loaded message spans.
    pub message_spans: SpanRelation,

    /// The subsystem this consumer answers for, or `None` when it answers none.
    /// Responding consumer construction gives live and reloaded messages the
    /// same value.
    pub responder: Option<SubsystemName>,
}

impl LoaderConfiguration {
    /// Assembles the internal loader configuration from the consumer-wide
    /// settings.
    ///
    /// The connection and concurrency fields come from
    /// [`ConsumerConfiguration`]; the loader-specific tuning
    /// (`cache_size`, `seek_timeout`, `discard_threshold`) comes from its
    /// [`KafkaLoaderConfiguration`].
    #[must_use]
    pub(crate) fn for_consumer(
        consumer_config: &ConsumerConfiguration,
        responder: Option<&SubsystemName>,
    ) -> Self {
        Self {
            bootstrap_servers: consumer_config.bootstrap_servers.clone(),
            group_id: consumer_config.group_id.clone(),
            max_permits: loader_capacity(consumer_config.max_uncommitted),
            cache_size: consumer_config.loader.cache_size,
            poll_interval: consumer_config.poll_interval,
            seek_timeout: consumer_config.loader.seek_timeout,
            discard_threshold: consumer_config.loader.discard_threshold,
            message_spans: consumer_config.message_spans,
            responder: responder.cloned(),
        }
    }
}

/// User-facing tuning for the Kafka message loader, carried on
/// [`ConsumerConfiguration`].
///
/// Holds only the loader-specific knobs; connection and concurrency settings
/// are shared with the primary consumer and read from
/// [`ConsumerConfiguration`] directly when the loader is assembled. Built from
/// the environment like every other configuration — no hand-written literal on
/// any production path.
#[derive(Builder, Clone, Debug, Validate)]
#[builder(build_fn(private, name = "build_internal"))]
pub struct KafkaLoaderConfiguration {
    /// Maximum number of decoded messages to cache.
    ///
    /// The cache uses S3-FIFO eviction, which quickly evicts "one-hit wonders"
    /// while keeping frequently accessed items.
    ///
    /// Environment variable: `PROSODY_LOADER_CACHE_SIZE`
    /// Default: 1,024 entries
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_LOADER_CACHE_SIZE\", 1_024)?",
        setter(into)
    )]
    #[validate(range(min = 1_usize))]
    pub cache_size: usize,

    /// Timeout for Kafka seek operations.
    ///
    /// Environment variable: `PROSODY_LOADER_SEEK_TIMEOUT`
    /// Default: 30 seconds
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_LOADER_SEEK_TIMEOUT\", \
                   Duration::from_secs(30))?",
        setter(into)
    )]
    pub seek_timeout: Duration,

    /// Number of messages to read sequentially before seeking.
    ///
    /// If the next requested offset is within this threshold, the loader
    /// continues reading and discards intermediate messages rather than
    /// performing an expensive seek.
    ///
    /// Environment variable: `PROSODY_LOADER_DISCARD_THRESHOLD`
    /// Default: 100
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_LOADER_DISCARD_THRESHOLD\", 100)?",
        setter(into)
    )]
    #[validate(range(min = 0_i64))]
    pub discard_threshold: i64,
}

impl KafkaLoaderConfiguration {
    /// Creates a new configuration builder.
    #[must_use]
    pub fn builder() -> super::KafkaLoaderConfigurationBuilder {
        KafkaLoaderConfigurationBuilder::default()
    }
}

impl KafkaLoaderConfigurationBuilder {
    /// Builds the configuration and validates it.
    ///
    /// # Errors
    ///
    /// Returns an error if a field is missing or a validation constraint is
    /// violated.
    pub fn build(&self) -> Result<KafkaLoaderConfiguration, KafkaLoaderConfigError> {
        let config = self.build_internal()?;
        config.validate()?;
        Ok(config)
    }
}

/// Error building or validating a [`KafkaLoaderConfiguration`].
#[derive(Debug, Error)]
pub enum KafkaLoaderConfigError {
    /// A required field was missing during building.
    #[error("loader configuration build error: {0:#}")]
    Build(#[from] KafkaLoaderConfigurationBuilderError),

    /// A validation constraint was violated.
    #[error("loader configuration validation error: {0:#}")]
    Validation(#[from] validator::ValidationErrors),
}
