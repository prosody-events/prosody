//! Consumer and mode configuration, including shared keyed-state and dedup
//! settings.

use crate::consumer::middleware::deduplication::DeduplicationConfiguration;
use crate::consumer::middleware::defer::DeferConfiguration;
use crate::consumer::middleware::monopolization::MonopolizationConfiguration;
use crate::consumer::middleware::retry::RetryConfiguration;
use crate::consumer::middleware::scheduler::SchedulerConfiguration;
use crate::consumer::middleware::timeout::TimeoutConfiguration;
use crate::consumer::middleware::topic::FailureTopicConfiguration;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::loader::KafkaLoaderConfiguration;
use crate::otel::SpanRelation;
use crate::state::config::KeyedStateConfiguration;
use crate::state_reader::StateReaderDependencies;
use crate::util::{
    from_duration_env_with_fallback, from_env, from_env_with_fallback,
    from_option_env_with_fallback, from_optional_vec_env, from_vec_env,
};
use crate::{ByteSize, Codec};
use derive_builder::Builder;
use std::env::var;
use std::time::Duration;
use thiserror::Error;
use validator::{Validate, ValidationError};

/// Environment variable name for the Kafka consumer group ID.
const PROSODY_GROUP_ID: &str = "PROSODY_GROUP_ID";

/// Default statistics reporting interval. See
/// [`ConsumerConfiguration::statistics_interval`].
const DEFAULT_STATISTICS_INTERVAL: Duration = Duration::from_secs(5);

/// Longest statistics interval librdkafka accepts.
const MAX_STATISTICS_INTERVAL: Duration = Duration::from_hours(24);

/// Default prefetch of each partition. See
/// [`ConsumerConfiguration::partition_prefetch`].
const DEFAULT_PARTITION_PREFETCH: ByteSize = ByteSize::mebibytes(1);

/// Smallest prefetch librdkafka accepts: its queue limit counts whole KiB.
const MIN_PARTITION_PREFETCH: u64 = 1 << 10_u32;

/// Largest prefetch librdkafka accepts as a partition fetch size.
const MAX_PARTITION_PREFETCH: u64 = 1_000_000_000;

/// Default size limit of one fetch response. See
/// [`ConsumerConfiguration::fetch_max_bytes`].
const DEFAULT_FETCH_MAX_BYTES: ByteSize = ByteSize::mebibytes(50);

/// Smallest fetch response limit librdkafka accepts: the largest record it
/// accepts (`message.max.bytes`, 1 MB by default).
const MIN_FETCH_MAX_BYTES: u64 = 1_000_000;

/// Largest fetch response limit librdkafka accepts: the response also needs
/// 512 bytes of framing inside a 32-bit size.
const MAX_FETCH_MAX_BYTES: u64 = (1 << 31_u32) - 1 - 512;

/// Configuration for the Kafka consumer.
///
/// This struct holds all the necessary configuration options for creating a
/// Kafka consumer. It uses the Builder pattern for flexible initialization and
/// supports loading values from environment variables.
#[derive(Builder, Clone, Debug, Validate)]
pub struct ConsumerConfiguration {
    /// List of Kafka bootstrap servers.
    ///
    /// Environment variable: `PROSODY_BOOTSTRAP_SERVERS`
    /// Default: None (must be specified)
    ///
    /// At least one server must be specified.
    #[builder(default = "from_vec_env(\"PROSODY_BOOTSTRAP_SERVERS\")?", setter(into))]
    #[validate(length(min = 1_u64))]
    pub bootstrap_servers: Vec<String>,

    /// Consumer group ID.
    ///
    /// Environment variable: `PROSODY_GROUP_ID`
    /// Default: None (must be specified)
    ///
    /// The group ID must be a non-empty string and should be unique for each
    /// logically separate consumer application. Consumers with the same group
    /// ID will form a consumer group and share the load of consuming topics.
    #[builder(default = "from_env(PROSODY_GROUP_ID)?", setter(into))]
    #[validate(length(min = 1_u64))]
    pub group_id: String,

    /// List of topics to subscribe to.
    ///
    /// Environment variable: `PROSODY_SUBSCRIBED_TOPICS`
    /// Default: no subscriptions
    #[builder(
        default = "from_optional_vec_env(\"PROSODY_SUBSCRIBED_TOPICS\")?.unwrap_or_default()",
        setter(into)
    )]
    pub subscribed_topics: Vec<String>,

    /// Allowed event type prefixes.
    ///
    /// Environment variable: `PROSODY_ALLOWED_EVENTS`
    /// Default: None
    ///
    /// If specified, only messages with event types matching these prefixes
    /// will be processed. If not specified, all events are allowed.
    #[builder(
        default = "from_optional_vec_env(\"PROSODY_ALLOWED_EVENTS\")?",
        setter(into)
    )]
    #[validate(length(min = 1_u64))]
    pub allowed_events: Option<Vec<String>>,

    /// Maximum number of uncommitted messages.
    ///
    /// Environment variable: `PROSODY_MAX_UNCOMMITTED`
    /// Default: 64
    ///
    /// Limits the uncommitted messages of each partition. A message holds its
    /// slot from decode until its offset commits. A partition at the limit
    /// stops reading, and its fetched records wait in librdkafka. See
    /// [`partition_prefetch`](Self::partition_prefetch).
    ///
    /// Also limits the handlers that run at once, across all partitions. An
    /// event that waits behind a busy key does not count.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MAX_UNCOMMITTED\", 64)?",
        setter(into)
    )]
    #[validate(range(min = 1_usize))]
    pub max_uncommitted: usize,

    /// The bytes that each assigned partition fetches ahead of its handlers.
    ///
    /// Environment variable: `PROSODY_PARTITION_PREFETCH`
    /// Default: 1 MiB
    ///
    /// librdkafka stops fetching a partition when its queue holds this many
    /// bytes. One fetch adds at most this many bytes for a partition, but it
    /// always returns at least one record batch. So the fetched records of a
    /// partition take at most two times this value, plus one batch. A larger
    /// value lets a fast partition run longer between fetches.
    ///
    /// Accepts a human-readable byte size such as `1 MiB`. A bare number is
    /// interpreted as bytes. The accepted range is 1 KiB to 1 GB.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_PARTITION_PREFETCH\", \
                   DEFAULT_PARTITION_PREFETCH)?",
        setter(into)
    )]
    #[validate(range(min = MIN_PARTITION_PREFETCH, max = MAX_PARTITION_PREFETCH))]
    pub partition_prefetch: ByteSize,

    /// The size limit of one fetch response from one broker, across all of
    /// its partitions.
    ///
    /// Environment variable: `PROSODY_FETCH_MAX_BYTES`
    /// Default: 50 MiB
    ///
    /// A fetch response holds the fetched records of many partitions until the
    /// partitions read them, so this limit also bounds the memory of each
    /// response. A response still returns one record batch that is larger.
    /// Each partition gets at most
    /// [`partition_prefetch`](Self::partition_prefetch) of a response.
    ///
    /// Accepts a human-readable byte size such as `16 MiB`. A bare number is
    /// interpreted as bytes. The accepted range is 1 MB to 2 GiB less 512
    /// bytes.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_FETCH_MAX_BYTES\", DEFAULT_FETCH_MAX_BYTES)?",
        setter(into)
    )]
    #[validate(range(min = MIN_FETCH_MAX_BYTES, max = MAX_FETCH_MAX_BYTES))]
    pub fetch_max_bytes: ByteSize,

    /// Duration of inactivity allowed before considering a partition stalled.
    ///
    /// Environment variable: `PROSODY_STALL_THRESHOLD`
    /// Default: 5 minutes
    ///
    /// Used by the liveness probe to determine if a partition's processing has
    /// stalled. If message processing takes longer than this duration, the
    /// partition is considered stalled, and the liveness probe will report an
    /// unhealthy status.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_STALL_THRESHOLD\", \
                   Duration::from_mins(5))?",
        setter(into)
    )]
    pub stall_threshold: Duration,

    /// Timeout for partition shutdown.
    ///
    /// Environment variable: `PROSODY_SHUTDOWN_TIMEOUT`
    /// Default: 30 seconds
    ///
    /// Determines how long to wait for in-flight tasks to complete during
    /// partition shutdown. After this threshold is reached, any remaining
    /// tasks will be aborted.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_SHUTDOWN_TIMEOUT\", \
                   Duration::from_secs(30))?",
        setter(into)
    )]
    pub shutdown_timeout: Duration,

    /// Interval between poll operations.
    ///
    /// Environment variable: `PROSODY_POLL_INTERVAL`
    /// Default: 100 milliseconds
    ///
    /// Controls how often the consumer serves Kafka events, such as
    /// rebalances, and stores the offsets of finished messages. Each partition
    /// reads its records as soon as they arrive.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_POLL_INTERVAL\", \
                   Duration::from_millis(100))?",
        setter(into)
    )]
    pub poll_interval: Duration,

    /// Interval between commit operations.
    ///
    /// Environment variable: `PROSODY_COMMIT_INTERVAL`
    /// Default: 1 second
    ///
    /// Controls how frequently offsets are auto-committed to Kafka.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_COMMIT_INTERVAL\", \
                   Duration::from_secs(1))?",
        setter(into)
    )]
    pub commit_interval: Duration,

    /// How often the consumer's Kafka client reports librdkafka statistics.
    ///
    /// Environment variable: `PROSODY_STATISTICS_INTERVAL`
    /// Default: 5 seconds
    ///
    /// Each report arrives as JSON that rdkafka parses on the poll thread, and
    /// it grows with the client's brokers, topics, and partitions. A shorter
    /// interval spends more poll-thread time on that parse.
    ///
    /// Reporting cannot be turned off. Reports drive the fetch-queue and
    /// metadata-age gauges. Raise the interval to make them cheaper. The
    /// accepted range is 1 millisecond to 24 hours.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_STATISTICS_INTERVAL\", \
                   DEFAULT_STATISTICS_INTERVAL)?",
        setter(into)
    )]
    #[validate(custom(function = "validate_statistics_interval"))]
    pub statistics_interval: Duration,

    /// Use a mock consumer for testing purposes.
    ///
    /// Environment variable: `PROSODY_MOCK`
    /// Default: false
    ///
    /// When true, uses a mock Kafka cluster for testing instead of the
    /// configured bootstrap servers.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MOCK\", false)?",
        setter(into)
    )]
    pub mock: bool,

    /// Port for the probe server.
    ///
    /// Environment variable: `PROSODY_PROBE_PORT`
    /// Default: Some(8000)
    ///
    /// If set, starts an HTTP server on this port for health and readiness
    /// probes. Set to None to disable the probe server.
    #[builder(
        default = "from_option_env_with_fallback(\"PROSODY_PROBE_PORT\", 8000)?",
        setter(into)
    )]
    pub probe_port: Option<u16>,

    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_SLAB_SIZE\", \
                   Duration::from_hours(1))?",
        setter(into)
    )]
    /// Duration for timer slab partitioning.
    ///
    /// This setting controls how timers are partitioned into time-based slabs
    /// for efficient storage and retrieval. Smaller slabs provide more precise
    /// time ranges but increase metadata overhead, while larger slabs reduce
    /// overhead but may be less efficient for sparse timer patterns.
    ///
    /// # Recommended Values
    ///
    /// - **High-frequency timers**: 5-15 minutes
    /// - **Medium-frequency timers**: 15-60 minutes
    /// - **Low-frequency timers**: 1-4 hours
    ///
    /// # Default
    ///
    /// Defaults to 1 hour if not specified or if parsing from environment
    /// fails.
    pub slab_size: Duration,

    /// Span relation for message execution spans.
    ///
    /// Controls how the `receive` span connects to the `OTel` context
    /// propagated from the Kafka message producer.
    ///
    /// Environment variable: `PROSODY_MESSAGE_SPANS`
    /// Default: `child` (child-of relationship)
    #[builder(default = "from_env_with_fallback(\"PROSODY_MESSAGE_SPANS\", SpanRelation::Child)?")]
    pub message_spans: SpanRelation,

    /// Span relation for timer execution spans.
    ///
    /// Controls how timer spans connect to the `OTel` context stored when the
    /// timer was scheduled.
    ///
    /// Environment variable: `PROSODY_TIMER_SPANS`
    /// Default: `follows_from`
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_TIMER_SPANS\", SpanRelation::FollowsFrom)?"
    )]
    pub timer_spans: SpanRelation,

    /// Tuning for the Kafka message loader (deferred-retry reload and
    /// keyed-state message resolution).
    ///
    /// The loader is consumer-wide and shares this struct's connection and
    /// concurrency settings; only its own knobs live here. Env-loaded like the
    /// rest of the configuration (`PROSODY_LOADER_*`).
    #[builder(default = "KafkaLoaderConfiguration::builder().build().map_err(|e| e.to_string())?")]
    #[validate(nested)]
    pub loader: KafkaLoaderConfiguration,
}

/// Configuration shared by every consumer mode (pipeline, low-latency, and
/// best-effort).
///
/// Carries the scheduler, timeout, and deduplication configurations, plus the
/// mode-independent keyed-state configuration. Named without a "middleware"
/// qualifier because keyed state is not middleware — its durability sequence
/// runs at the `settle` boundary, outside the middleware stack.
#[derive(Clone, Debug)]
pub struct CommonConfiguration {
    /// Scheduler configuration for fair work-conserving dispatch.
    pub scheduler: SchedulerConfiguration,
    /// Timeout configuration for handler execution limits.
    pub timeout: TimeoutConfiguration,
    /// Deduplication configuration.
    ///
    /// Every consumer mode filters duplicates through this store.
    pub dedup: DeduplicationConfiguration,
    /// Keyed-state configuration (mode-independent, always-on; inert when no
    /// collections are registered).
    pub keyed_state: KeyedStateConfiguration,
}

/// Configuration for middleware specific to pipeline consumers.
///
/// Bundles the retry, monopolization, and defer configurations that are only
/// used by the pipeline processing mode.
#[derive(Clone, Debug)]
pub struct PipelineMiddlewareConfiguration {
    /// Retry configuration for failed messages.
    pub retry: RetryConfiguration,
    /// Monopolization detection configuration.
    pub monopolization: MonopolizationConfiguration,
    /// Defer middleware configuration.
    pub defer: DeferConfiguration,
}

/// Configuration for middleware specific to low-latency consumers.
///
/// Bundles the retry and failure-topic configurations that are only
/// used by the low-latency processing mode.
#[derive(Clone, Debug)]
pub struct LowLatencyMiddlewareConfiguration {
    /// Retry configuration for failed messages.
    pub retry: RetryConfiguration,
    /// Failure topic configuration for routing unrecoverable messages.
    pub failure_topic: FailureTopicConfiguration,
}

/// What every processing mode needs before its mode-specific middleware.
///
/// These three sections are one parameter because constructors consume them as
/// a unit. The constructor selects a backend once, then enters typed wiring.
pub struct ConsumerSetup<'a> {
    /// Kafka consumer settings: group, topics, mock mode, stall threshold.
    pub consumer: &'a ConsumerConfiguration,
    /// Backend settings for the timer trigger store.
    pub trigger_store: &'a TriggerStoreConfiguration,
    /// Settings every mode shares, keyed state and deduplication included.
    pub common: &'a CommonConfiguration,
}

/// A consumer setup whose backend family has already been selected.
pub(crate) struct TypedConsumerSetup<'a, C: Codec, B> {
    pub(crate) consumer: &'a ConsumerConfiguration,
    pub(crate) common: &'a CommonConfiguration,
    pub(crate) deps: StateReaderDependencies<C, B>,
}

impl ConsumerConfiguration {
    /// Creates a new `ConsumerConfigurationBuilder`.
    ///
    /// This method provides a convenient way to start building a
    /// `ConsumerConfiguration` using the builder pattern.
    #[must_use]
    pub fn builder() -> ConsumerConfigurationBuilder {
        ConsumerConfigurationBuilder::default()
    }
}

impl ConsumerConfigurationBuilder {
    /// Resolves only the configured mock mode.
    ///
    /// This does not build or validate unrelated consumer fields. High-level
    /// clients use it to select a concrete backend while retaining consumer
    /// configuration failures until subscription.
    pub(crate) fn configured_mock(&self) -> Result<bool, MockConfigurationError> {
        match self.mock {
            Some(mock) => Ok(mock),
            None => from_env_with_fallback("PROSODY_MOCK", false).map_err(MockConfigurationError),
        }
    }

    /// Retrieves the currently configured consumer group.
    ///
    /// Checks both the explicitly configured group ID and the environment
    /// variable.
    #[must_use]
    pub(crate) fn configured_consumer_group(&self) -> Option<String> {
        self.group_id.clone().or_else(|| var(PROSODY_GROUP_ID).ok())
    }
}

/// Validates the statistics interval against what librdkafka accepts.
///
/// The interval reaches librdkafka in whole milliseconds, where zero means "do
/// not report". A sub-millisecond duration would truncate to zero and disable
/// reporting silently, so it is rejected instead.
fn validate_statistics_interval(interval: &Duration) -> Result<(), ValidationError> {
    if interval.as_millis() == 0 {
        return Err(ValidationError::new(
            "statistics_interval_below_a_millisecond",
        ));
    }
    if *interval > MAX_STATISTICS_INTERVAL {
        return Err(ValidationError::new("statistics_interval_above_24_hours"));
    }
    Ok(())
}

/// The `PROSODY_MOCK` environment value is not a boolean.
#[derive(Debug, Error)]
#[error("invalid mock-mode configuration: {0}")]
pub struct MockConfigurationError(String);

#[cfg(test)]
mod tests;
