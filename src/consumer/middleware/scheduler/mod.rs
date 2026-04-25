//! Fair work-conserving scheduler for handler concurrency control.
//!
//! Enforces global concurrency limits while prioritizing keys by accumulated
//! virtual time, preventing monopolization by high-throughput keys and ensuring
//! timely execution of waiting tasks through urgency boosting.

use derive_builder::Builder;
use std::time::Duration;
use thiserror::Error;
use validator::{Validate, ValidationErrors};

use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::scheduler::dispatch::{DispatchError, Dispatcher};
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{DemandType, Keyed};
use crate::telemetry::Telemetry;
use crate::timers::Trigger;
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};
use crate::{Partition, Topic, TopicPartitionKey};

mod decay;
mod dispatch;

/// Configuration for the scheduler middleware.
#[derive(Builder, Clone, Debug, Validate)]
pub struct SchedulerConfiguration {
    /// Maximum number of handler invocations executing concurrently.
    ///
    /// This is a hard limit enforced by a semaphore. When this many handlers
    /// are executing, new tasks queue until a permit becomes available. The
    /// scheduler then selects the next task based on virtual time fairness
    /// and wait urgency.
    ///
    /// Lower values reduce peak resource usage at the cost of increased
    /// queuing delay. Higher values increase parallelism and throughput
    /// but consume more system resources.
    ///
    /// Environment variable: `PROSODY_MAX_CONCURRENCY`
    /// Default: 32
    #[builder(default = "from_env_with_fallback(\"PROSODY_MAX_CONCURRENCY\", 32)?")]
    #[validate(range(min = 1_usize))]
    pub max_concurrency: usize,

    /// Target proportion of execution time for failure/retry task processing.
    ///
    /// This controls bandwidth allocation between Normal and Failure task
    /// classes. Value represents the target fraction of total EXECUTION TIME
    /// (not task count) that should be spent on Failure tasks. Normal tasks
    /// receive the remaining `1.0 - failure_weight` proportion.
    ///
    /// The scheduler uses class-level time accounting to achieve this target:
    /// it selects from the underserved class (lowest `time / weight` ratio),
    /// causing execution time proportions to converge toward the target over
    /// time.
    ///
    /// Example: With `failure_weight = 0.3`, if Failure tasks take 10x longer
    /// than Normal tasks, Failure will get ~10x fewer executions but still
    /// achieve 30% of total execution time.
    ///
    /// Environment variable: `PROSODY_SCHEDULER_FAILURE_WEIGHT`
    /// Default: 0.3 (30% of execution time for failures)
    #[builder(default = "from_env_with_fallback(\"PROSODY_SCHEDULER_FAILURE_WEIGHT\", 0.3)?")]
    #[validate(range(min = 0.0_f64, max = 1.0_f64))]
    pub failure_weight: f64,

    /// Wait duration at which urgency boost reaches maximum intensity.
    ///
    /// This parameter controls how quickly wait urgency ramps up. The urgency
    /// boost uses quadratic scaling: `urgency = wait_weight * (t / T)²` where
    /// `t` is actual wait time and `T` is this parameter.
    ///
    /// - At 50% of `max_wait`: task gets 25% of maximum urgency boost
    /// - At 100% of `max_wait`: task gets 100% of maximum urgency boost
    /// - Beyond `max_wait`: urgency is capped at maximum
    ///
    /// **Shorter values** make the scheduler more responsive to wait time,
    /// with urgency ramping up faster. This prioritizes starvation prevention
    /// but may reduce long-term fairness.
    ///
    /// **Longer values** make urgency ramp up slower, giving more weight to
    /// virtual time fairness. This improves long-term fairness but may allow
    /// longer wait times before intervention.
    ///
    /// Environment variable: `PROSODY_SCHEDULER_MAX_WAIT`
    /// Default: 2 minutes
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_SCHEDULER_MAX_WAIT\", \
                   Duration::from_mins(2))?",
        setter(into)
    )]
    pub max_wait: Duration,

    /// Maximum urgency boost (in seconds of virtual time) for waiting tasks.
    ///
    /// This is the "weight" in the priority formula that balances virtual time
    /// fairness against starvation prevention. When a task waits for
    /// `max_wait`, it receives an urgency boost equivalent to this many
    /// seconds of negative virtual time, making it more likely to be selected.
    ///
    /// The priority formula is: `priority = key_vt - urgency_boost` where
    /// `urgency_boost = wait_weight * (wait_time / max_wait)²`.
    /// Both terms are in seconds. Lower priority values are selected first.
    ///
    /// **Higher values** increase the importance of wait time relative to
    /// virtual time fairness. Tasks will be selected sooner based on how long
    /// they've waited, even if they have high virtual time. This aggressively
    /// prevents starvation but may allow temporary monopolization.
    ///
    /// **Lower values** increase the importance of virtual time fairness
    /// relative to wait time. Keys with high accumulated execution time remain
    /// deprioritized longer. This ensures stricter fairness but may allow
    /// longer wait times.
    ///
    /// Example: With default `wait_weight = 200.0` and `max_wait = 120s`,
    /// a task waiting 120s gets priority equivalent to a key with 200s less
    /// virtual time.
    ///
    /// Environment variable: `PROSODY_SCHEDULER_WAIT_WEIGHT`
    /// Default: 200.0 seconds
    #[builder(default = "from_env_with_fallback(\"PROSODY_SCHEDULER_WAIT_WEIGHT\", 200.0)?")]
    #[validate(range(min = 0.0_f64))]
    pub wait_weight: f64,

    /// LRU cache capacity for tracking per-key virtual time.
    ///
    /// Virtual time (VT) represents accumulated execution time for each key.
    /// The scheduler uses VT to ensure fairness: keys with lower VT get
    /// priority. This cache stores VT for recently active keys.
    ///
    /// When the cache is full and a new key arrives, the least recently used
    /// key's VT is evicted. If that key returns later, it starts fresh at
    /// VT = 0, giving it high priority. This is intentional: keys absent from
    /// the cache haven't been consuming resources recently.
    ///
    /// **Larger caches** can track more distinct keys simultaneously, reducing
    /// cache misses and providing more accurate long-term fairness across many
    /// keys. Memory usage: ~64 bytes per entry.
    ///
    /// **Smaller caches** use less memory but cause more keys to start fresh
    /// at VT = 0, which may give temporary priority advantages to keys that
    /// were previously monopolizers if they weren't accessed recently enough
    /// to stay in cache.
    ///
    /// Note: Virtual times use exponential decay (120s half-life) so old VT
    /// accumulations lose influence over ~5 minutes regardless of cache size.
    ///
    /// Environment variable: `PROSODY_SCHEDULER_CACHE_SIZE`
    /// Default: 8192 keys
    #[builder(default = "from_env_with_fallback(\"PROSODY_SCHEDULER_CACHE_SIZE\", 8_192)?")]
    #[validate(range(min = 1_usize))]
    pub cache_size: usize,
}

/// Middleware that applies fair scheduling to handler invocations.
///
/// Wraps handlers to enforce concurrency limits and priority-based dispatch.
#[derive(Clone, Debug)]
pub struct SchedulerMiddleware {
    dispatcher: Dispatcher,
}

/// Provider that creates scheduled handlers for each partition.
#[derive(Clone, Debug)]
pub struct SchedulerProvider<T> {
    provider: T,
    dispatcher: Dispatcher,
}

/// Handler wrapper that acquires scheduler permits before delegating to inner
/// handler.
///
/// Permits are released automatically when the handler completes, allowing
/// other tasks to proceed.
#[derive(Clone, Debug)]
pub struct SchedulerHandler<T> {
    handler: T,
    dispatcher: Dispatcher,
    topic: Topic,
    partition: Partition,
}

/// Errors that can occur during scheduled handler execution.
#[derive(Debug, Error)]
pub enum SchedulerError<E> {
    /// The inner handler returned an error.
    #[error(transparent)]
    Handler(E),

    /// Failed to acquire a permit from the scheduler.
    #[error("Failed to acquire scheduler permit: {0:#}")]
    PermitAcquisition(#[from] DispatchError),
}

/// Errors that can occur during scheduler initialization.
#[derive(Debug, Error)]
pub enum SchedulerInitError {
    /// Configuration validation failed.
    #[error("Invalid configuration: {0:#}")]
    Validation(#[from] ValidationErrors),
}

impl<E> ClassifyError for SchedulerError<E>
where
    E: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            SchedulerError::Handler(error) => error.classify_error(),
            SchedulerError::PermitAcquisition(error) => error.classify_error(),
        }
    }
}

impl SchedulerConfiguration {
    /// Creates a builder for constructing [`SchedulerConfiguration`].
    #[must_use]
    pub fn builder() -> SchedulerConfigurationBuilder {
        SchedulerConfigurationBuilder::default()
    }
}

impl SchedulerMiddleware {
    /// Creates a new scheduler middleware with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn new(
        config: &SchedulerConfiguration,
        telemetry: &Telemetry,
    ) -> Result<Self, SchedulerInitError> {
        config.validate()?;
        let dispatcher = Dispatcher::new(config, telemetry);
        Ok(Self { dispatcher })
    }
}

impl HandlerMiddleware for SchedulerMiddleware {
    type Provider<T: FallibleHandlerProvider> = SchedulerProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        SchedulerProvider {
            provider,
            dispatcher: self.dispatcher.clone(),
        }
    }
}

impl<T> FallibleHandlerProvider for SchedulerProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = SchedulerHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        SchedulerHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            dispatcher: self.dispatcher.clone(),
            topic,
            partition,
        }
    }
}

impl<T> FallibleHandler for SchedulerHandler<T>
where
    T: FallibleHandler,
{
    type Error = SchedulerError<T::Error>;
    type Outcome = T::Outcome;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<Self::Outcome, Self::Error>
    where
        C: EventContext,
    {
        let tp_key = TopicPartitionKey::new(self.topic, self.partition, message.key().clone());
        let _permit = self.dispatcher.get_permit(tp_key, demand_type).await?;

        self.handler
            .on_message(context, message, demand_type)
            .await
            .map_err(SchedulerError::Handler)
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Outcome, Self::Error>
    where
        C: EventContext,
    {
        let tp_key = TopicPartitionKey::new(self.topic, self.partition, trigger.key.clone());
        let _permit = self.dispatcher.get_permit(tp_key, demand_type).await?;

        self.handler
            .on_timer(context, trigger, demand_type)
            .await
            .map_err(SchedulerError::Handler)
    }

    async fn after_commit<C>(
        &self,
        context: C,
        result: Result<Self::Outcome, Self::Error>,
    ) where
        C: EventContext,
    {
        match result {
            Ok(outcome) => self.handler.after_commit(context, Ok(outcome)).await,
            Err(SchedulerError::Handler(inner)) => {
                self.handler.after_commit(context, Err(inner)).await;
            }
            // Permit-acquisition error happened at this layer; the inner
            // handler did not run.
            Err(SchedulerError::PermitAcquisition(_)) => {}
        }
    }

    async fn after_abort<C>(
        &self,
        context: C,
        result: Result<Self::Outcome, Self::Error>,
    ) where
        C: EventContext,
    {
        match result {
            Ok(outcome) => self.handler.after_abort(context, Ok(outcome)).await,
            Err(SchedulerError::Handler(inner)) => {
                self.handler.after_abort(context, Err(inner)).await;
            }
            Err(SchedulerError::PermitAcquisition(_)) => {}
        }
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}
