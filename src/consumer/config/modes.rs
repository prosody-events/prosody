//! The configuration bundles that each processing mode consumes, beside the
//! Kafka consumer settings.

use super::ConsumerConfiguration;
use crate::Codec;
use crate::consumer::middleware::deduplication::DeduplicationConfiguration;
use crate::consumer::middleware::defer::DeferConfiguration;
use crate::consumer::middleware::monopolization::MonopolizationConfiguration;
use crate::consumer::middleware::retry::RetryConfiguration;
use crate::consumer::middleware::scheduler::SchedulerConfiguration;
use crate::consumer::middleware::timeout::TimeoutConfiguration;
use crate::consumer::middleware::topic::FailureTopicConfiguration;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::state::config::KeyedStateConfiguration;
use crate::state_reader::StateReaderDependencies;

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
