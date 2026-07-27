//! Consumer and mode configuration, plus the recovery-TTL margin the
//! keyed-state commit oracle depends on.

use crate::Codec;
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
use crate::state_reader::SharedDeps;
use crate::timers::duration::CompactDuration;
use crate::util::{
    from_duration_env_with_fallback, from_env, from_env_with_fallback,
    from_option_env_with_fallback, from_optional_vec_env, from_vec_env,
};
use derive_builder::Builder;
use std::env::var;
use std::time::Duration;
use thiserror::Error;
use validator::Validate;

/// Environment variable name for the Kafka consumer group ID.
const PROSODY_GROUP_ID: &str = "PROSODY_GROUP_ID";

/// Multiplier on `recovery_delay` for the minimum deduplication TTL: the
/// dedup marker is the commit oracle, so it must outlive the recovery window
/// by a wide margin to survive rebalances and retries. See
/// [`validate_recovery_ttl_margin`].
const RECOVERY_TTL_DELAY_MULTIPLIER: u64 = 48;

/// Absolute floor for the deduplication TTL when state is registered, in
/// seconds (1 hour) — the larger of this and `48 × recovery_delay` applies.
const MIN_RECOVERY_EVIDENCE_TTL_SECONDS: u64 = 3_600;

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
    /// Default: None (must be specified)
    ///
    /// At least one topic must be specified.
    #[builder(default = "from_vec_env(\"PROSODY_SUBSCRIBED_TOPICS\")?", setter(into))]
    #[validate(length(min = 1_u64))]
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
    /// Controls the global limit of messages being processed concurrently
    /// across all partitions. This provides backpressure when the system is
    /// under high load by pausing message consumption when the limit is
    /// reached. Also determines the buffer size for message queues.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MAX_UNCOMMITTED\", 64)?",
        setter(into)
    )]
    #[validate(range(min = 1_usize))]
    pub max_uncommitted: usize,

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
    /// Controls how frequently the consumer polls Kafka for new messages.
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
    /// Deduplication runs in **every** consumer mode: it is the commit oracle
    /// the keyed-state recovery path reads (a message's dedup row existing
    /// means it committed), so it cannot be pipeline-specific.
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

/// What every processing mode needs before its mode-specific middleware: the
/// three configuration sections and the infrastructure to build on.
///
/// These four are one parameter because they are consumed as a unit. Every
/// constructor hands the whole thing to `build_shared_state`, which validates
/// the configuration and opens (or reuses) the storage behind it.
pub struct ConsumerSetup<'a, C: Codec> {
    /// Kafka consumer settings: group, topics, mock mode, stall threshold.
    pub consumer: &'a ConsumerConfiguration,
    /// Backend settings for the timer trigger store.
    pub trigger_store: &'a TriggerStoreConfiguration,
    /// Settings every mode shares, keyed state and deduplication included.
    pub common: &'a CommonConfiguration,
    /// Infrastructure to reuse instead of composing fresh handles.
    ///
    /// The high-level client passes the bundle it already built, so one
    /// Cassandra session and one Kafka loader back the whole process. `None`
    /// makes the consumer compose its own.
    pub deps: Option<SharedDeps<C>>,
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
    /// Retrieves the currently configured consumer group.
    ///
    /// Checks both the explicitly configured group ID and the environment
    /// variable.
    #[must_use]
    pub(crate) fn configured_consumer_group(&self) -> Option<String> {
        self.group_id.clone().or_else(|| var(PROSODY_GROUP_ID).ok())
    }
}

/// Validates that the deduplication TTL clears the keyed-state recovery
/// window (resolution-before-evidence-expiry): `dedup.ttl ≥
/// max(48 × recovery_delay, 1h)`. The dedup marker is the commit oracle a
/// provisional cell is resolved against, so if the marker expires first the
/// cell can no longer be resolved correctly and a committed write is lost.
/// Returns [`RecoveryTtlMarginError`] when the dedup TTL is below the margin.
pub(in crate::consumer) fn validate_recovery_ttl_margin(
    dedup_ttl: Duration,
    recovery_delay: CompactDuration,
) -> Result<(), RecoveryTtlMarginError> {
    let recovery_delay_seconds = u64::from(recovery_delay.seconds());
    let required_seconds = recovery_delay_seconds
        .saturating_mul(RECOVERY_TTL_DELAY_MULTIPLIER)
        .max(MIN_RECOVERY_EVIDENCE_TTL_SECONDS);
    let dedup_ttl_seconds = dedup_ttl.as_secs();
    if dedup_ttl_seconds < required_seconds {
        return Err(RecoveryTtlMarginError {
            dedup_ttl: dedup_ttl_seconds,
            recovery_delay: recovery_delay_seconds,
            required: required_seconds,
        });
    }
    Ok(())
}

/// The deduplication TTL is below the keyed-state recovery margin: a
/// provisional cell could outlive its commit-oracle marker and be lost under
/// crash recovery. Returned at consumer build when state collections are
/// registered. Raise `PROSODY_IDEMPOTENCE_TTL` or lower
/// `recovery_delay` so `dedup.ttl ≥ max(48 × recovery_delay, 1h)` holds.
///
/// All fields are in seconds.
#[derive(Debug, Error)]
#[error(
    "deduplication TTL {dedup_ttl} seconds is below the keyed-state recovery margin of {required} \
     seconds (the larger of 48 × recovery_delay {recovery_delay}s and 3600s); a provisional cell \
     could outlive its commit-oracle marker and be lost"
)]
pub struct RecoveryTtlMarginError {
    /// The configured deduplication TTL.
    dedup_ttl: u64,

    /// The configured keyed-state recovery delay.
    recovery_delay: u64,

    /// The minimum deduplication TTL the configuration must meet.
    required: u64,
}

#[cfg(test)]
mod tests;
