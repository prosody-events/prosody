//! Monopolization detection middleware for preventing key-level execution
//! monopolies.
//!
//! Detects when a single key monopolizes handler execution time (>90% over 5
//! minutes) and returns an error for monopolizing keys.
//!
//! # Execution
//!
//! **Request Path:**
//! 1. Check if current key is monopolizing execution time
//! 2. Return error for monopolizing keys, otherwise proceed to inner handler
//!
//! **Background Processing:**
//! - Tracks execution intervals per key using `IntervalSet<u64>`
//! - Maintains rolling 5-minute window of execution intervals
//!
//! # Apply-hook contract
//!
//! This middleware is a "reject-at-this-layer" gate. The `FallibleHandler`
//! invariant requires that for every `on_message`/`on_timer` call on the
//! inner handler that runs and returns, the framework must call exactly one
//! of `after_commit`/`after_abort` on that same inner handler — and if the
//! inner handler did NOT run, neither apply hook fires for it.
//!
//! There are exactly two work outcomes here:
//!
//! - **Inner ran** — `Ok(_)` or `Err(MonopolizationError::Handler(_))`. The
//!   apply hook is forwarded to the inner handler with the inner-typed result.
//! - **Inner did NOT run** — `Err(MonopolizationError::Monopolization { .. })`
//!   was produced at this layer before delegation. The inner handler's apply
//!   hook is suppressed.
//!
//! The inner is invoked at most once per call; per-invocation invariant
//! trivially upheld.
//!
//! # Configuration
//!
//! - `monopolization_threshold`: Execution time ratio threshold (default: 0.9
//!   for 90%)
//! - `window_duration`: Rolling window duration (default: 5 minutes)

use ahash::RandomState;
use derive_builder::Builder;
use humantime::format_duration;
use interval::IntervalSet;
use interval::interval_set::ToIntervalSet;
use interval::prelude::{Bounded, Intersection, Union};
use quanta::Instant;
use quick_cache::UnitWeighter;
use quick_cache::sync::Cache;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::broadcast;
use tracing::{debug, trace, warn};
use validator::{Validate, ValidationErrors};

use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{DemandType, Keyed};
use crate::telemetry::Telemetry;
use crate::telemetry::event::{Data, KeyEvent, KeyState, TelemetryEvent};
use crate::timers::Trigger;
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};
use crate::{Key, Partition, Topic, TopicPartitionKey};

/// Configuration for monopolization detection.
#[derive(Builder, Clone, Debug, Validate)]
pub struct MonopolizationConfiguration {
    /// Whether monopolization detection is enabled.
    ///
    /// When disabled, the monopolization middleware is entirely bypassed and
    /// all messages/timers pass through to the inner handler without checks.
    ///
    /// Environment variable: `PROSODY_MONOPOLIZATION_ENABLED`
    /// Default: true
    #[builder(default = "from_env_with_fallback(\"PROSODY_MONOPOLIZATION_ENABLED\", true)?")]
    pub enabled: bool,

    /// Threshold for monopolization detection.
    ///
    /// If a key's execution time exceeds this fraction of the window duration,
    /// it is considered to be monopolizing execution.
    ///
    /// Environment variable: `PROSODY_MONOPOLIZATION_THRESHOLD`
    /// Default: 0.9 (90%)
    #[builder(default = "from_env_with_fallback(\"PROSODY_MONOPOLIZATION_THRESHOLD\", 0.9)?")]
    #[validate(range(min = 0.0_f64, max = 1.0_f64))]
    pub monopolization_threshold: f64,

    /// Rolling window duration for monopolization detection.
    ///
    /// Environment variable: `PROSODY_MONOPOLIZATION_WINDOW`
    /// Default: 5 minutes
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_MONOPOLIZATION_WINDOW\", \
                   Duration::from_mins(5))?",
        setter(into)
    )]
    pub window_duration: Duration,

    /// LRU cache size for tracking key execution intervals.
    ///
    /// Each entry stores the execution interval set for a key. When the cache
    /// is full, the least recently used key is evicted.
    ///
    /// Environment variable: `PROSODY_MONOPOLIZATION_CACHE_SIZE`
    /// Default: 8192
    #[builder(default = "from_env_with_fallback(\"PROSODY_MONOPOLIZATION_CACHE_SIZE\", 8_192)?")]
    #[validate(range(min = 1_usize))]
    pub cache_size: usize,
}

/// Middleware that detects and prevents key-level execution monopolies.
#[derive(Clone)]
pub struct MonopolizationMiddleware {
    monopolization_threshold: f64,
    window_duration: Duration,
    reference_instant: Instant,
    key_intervals: Arc<Cache<TopicPartitionKey, IntervalSet<u64>, UnitWeighter, RandomState>>,
}

/// Provider that creates monopolization handlers for each partition.
#[derive(Clone)]
pub struct MonopolizationProvider<T> {
    provider: T,
    monopolization_threshold: f64,
    window_duration: Duration,
    reference_instant: Instant,
    key_intervals: Arc<Cache<TopicPartitionKey, IntervalSet<u64>, UnitWeighter, RandomState>>,
}

/// Handler wrapper that checks for monopolization before delegating to inner
/// handler.
#[derive(Clone)]
pub struct MonopolizationHandler<T> {
    handler: T,
    topic: Topic,
    partition: Partition,
    reference_instant: Instant,
    key_intervals: Arc<Cache<TopicPartitionKey, IntervalSet<u64>, UnitWeighter, RandomState>>,
    monopolization_threshold: f64,
    window_duration: Duration,
}

impl MonopolizationConfiguration {
    /// Creates a builder for constructing [`MonopolizationConfiguration`].
    #[must_use]
    pub fn builder() -> MonopolizationConfigurationBuilder {
        MonopolizationConfigurationBuilder::default()
    }
}

impl MonopolizationMiddleware {
    /// Creates a new monopolization middleware with the given configuration.
    ///
    /// Returns `None` if monopolization detection is disabled in the config.
    /// The returned `Option<Self>` implements `HandlerMiddleware` directly,
    /// passing through to the inner handler when `None`.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for monopolization detection
    /// * `telemetry` - Telemetry instance for event subscription
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration validation fails.
    pub fn new(
        config: &MonopolizationConfiguration,
        telemetry: &Telemetry,
    ) -> Result<Option<Self>, MonopolizationInitError> {
        config.validate()?;

        if !config.enabled {
            debug!("Monopolization detection disabled by configuration");
            return Ok(None);
        }

        let reference_instant = Instant::now();
        let key_intervals = Arc::new(Cache::new(config.cache_size));

        let telemetry_rx = telemetry.subscribe();
        let key_intervals_clone = Arc::clone(&key_intervals);
        let window_duration = config.window_duration;

        debug!(
            threshold_pct = %format_args!("{:.1}%", config.monopolization_threshold * 100.0_f64),
            window = %format_duration(window_duration),
            cache_size = config.cache_size,
            "Monopolization detection enabled"
        );

        spawn(run_event_loop(
            reference_instant,
            key_intervals_clone,
            window_duration,
            telemetry_rx,
        ));

        Ok(Some(Self {
            monopolization_threshold: config.monopolization_threshold,
            window_duration,
            reference_instant,
            key_intervals,
        }))
    }
}

impl<P: Send + Sync + 'static> HandlerMiddleware<P> for MonopolizationMiddleware {
    type Provider<T>
        = MonopolizationProvider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>,
    {
        MonopolizationProvider {
            provider,
            monopolization_threshold: self.monopolization_threshold,
            window_duration: self.window_duration,
            reference_instant: self.reference_instant,
            key_intervals: Arc::clone(&self.key_intervals),
        }
    }
}

impl<T> FallibleHandlerProvider for MonopolizationProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = MonopolizationHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        MonopolizationHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            topic,
            partition,
            reference_instant: self.reference_instant,
            key_intervals: Arc::clone(&self.key_intervals),
            monopolization_threshold: self.monopolization_threshold,
            window_duration: self.window_duration,
        }
    }
}

impl<T> FallibleHandler for MonopolizationHandler<T>
where
    T: FallibleHandler,
{
    type Error = MonopolizationError<T::Error>;
    type Output = T::Output;
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let tp_key = TopicPartitionKey::new(self.topic, self.partition, message.key().clone());
        if let Some(error) = self.check_monopolization(&tp_key, Instant::now()) {
            return Err(error);
        }

        self.handler
            .on_message(context, message, demand_type)
            .await
            .map_err(MonopolizationError::Handler)
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let tp_key = TopicPartitionKey::new(self.topic, self.partition, trigger.key.clone());
        if let Some(error) = self.check_monopolization(&tp_key, Instant::now()) {
            return Err(error);
        }

        self.handler
            .on_timer(context, trigger, demand_type)
            .await
            .map_err(MonopolizationError::Handler)
    }

    /// Forwards the FINAL apply hook to the inner handler iff the inner
    /// handler actually ran.
    ///
    /// Work-centric reasoning, per the `FallibleHandler` invariant:
    ///
    /// - `Ok(_)` — inner ran and succeeded. Forward `Ok` so the inner handler
    ///   observes its own success exactly once.
    /// - `Err(Handler(inner))` — inner ran and returned an error. Unwrap and
    ///   forward the inner-typed error so the inner handler sees its own
    ///   failure exactly once.
    /// - `Err(Monopolization { .. })` — rejection produced at THIS layer before
    ///   delegation; the inner handler did not run. Suppress its apply hook
    ///   entirely; firing it would violate the invariant by reporting an
    ///   outcome for work that never happened.
    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        match result {
            Ok(output) => self.handler.after_commit(context, Ok(output)).await,
            Err(MonopolizationError::Handler(inner)) => {
                self.handler.after_commit(context, Err(inner)).await;
            }
            // Monopolization rejection happened at this layer; the inner
            // handler did not run, so there is no inner-typed error to
            // forward and no apply hook is owed to it.
            Err(MonopolizationError::Monopolization { .. }) => {}
        }
    }

    /// Forwards the non-final apply hook to the inner handler iff the inner
    /// handler actually ran (a retry of the same logical event is coming).
    ///
    /// Work-centric reasoning, per the `FallibleHandler` invariant:
    ///
    /// - `Ok(_)` / `Err(Handler(_))` — inner ran. Forward the inner-typed
    ///   result so the inner handler can observe the abort and prepare for the
    ///   upcoming retry.
    /// - `Err(Monopolization { .. })` — rejection produced at THIS layer before
    ///   delegation; the inner handler did not run. Suppress its apply hook.
    ///   The retry will reach this layer again and either be admitted (and then
    ///   dispatched to the inner) or rejected once more here, and the inner
    ///   handler must not see a phantom abort for work it never performed.
    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        match result {
            Ok(output) => self.handler.after_abort(context, Ok(output)).await,
            Err(MonopolizationError::Handler(inner)) => {
                self.handler.after_abort(context, Err(inner)).await;
            }
            Err(MonopolizationError::Monopolization { .. }) => {}
        }
    }

    async fn shutdown(self) {
        debug!(
            tracked_keys = self.key_intervals.len(),
            "Monopolization handler shutting down"
        );
        self.handler.shutdown().await;
    }
}

impl<T> MonopolizationHandler<T>
where
    T: FallibleHandler,
{
    /// Checks if a key is monopolizing execution time.
    #[expect(
        clippy::cast_precision_loss,
        reason = "nanosecond values for practical windows (< 104 days) stay below 2^53"
    )]
    fn check_monopolization(
        &self,
        tp_key: &TopicPartitionKey,
        now: Instant,
    ) -> Option<MonopolizationError<T::Error>> {
        // No intervals tracked for this key yet - fast path
        let intervals = self.key_intervals.get(tp_key)?;

        let now_nanos = now
            .saturating_duration_since(self.reference_instant)
            .as_nanos() as u64;
        let window_nanos = self.window_duration.as_nanos() as u64;
        let window_start = now_nanos.saturating_sub(window_nanos);

        // Clamp each interval to the window and sum directly — equivalent to
        // intersecting with a single-interval set, without allocating one per
        // event (intervals outside the window saturate to zero length).
        let key_time_nanos: u64 = intervals
            .iter()
            .map(|iv| {
                iv.upper()
                    .min(now_nanos)
                    .saturating_sub(iv.lower().max(window_start))
            })
            .sum();

        let monopolization_ratio = key_time_nanos as f64 / window_nanos as f64;

        if monopolization_ratio > self.monopolization_threshold {
            let percentage = monopolization_ratio * 100.0_f64;
            let threshold_pct = self.monopolization_threshold * 100.0_f64;

            // Only log when we're actually rejecting - this is the important event
            warn!(
                topic = %tp_key.topic,
                partition = tp_key.partition,
                key = %tp_key.key,
                usage = %format_args!("{percentage:.1}%"),
                limit = %format_args!("{threshold_pct:.1}%"),
                window = %format_duration(self.window_duration),
                "Key exceeded monopolization threshold"
            );

            return Some(MonopolizationError::Monopolization {
                topic: tp_key.topic,
                partition: tp_key.partition,
                key: tp_key.key.clone(),
                percentage,
                threshold: threshold_pct,
                window: self.window_duration,
            });
        }

        None
    }
}

async fn run_event_loop(
    reference_instant: Instant,
    key_intervals: Arc<Cache<TopicPartitionKey, IntervalSet<u64>, UnitWeighter, RandomState>>,
    window_duration: Duration,
    mut telemetry_rx: broadcast::Receiver<TelemetryEvent>,
) {
    let window_nanos = window_duration.as_nanos() as u64;

    debug!("Event loop started, listening for key state transitions");

    loop {
        let event = match telemetry_rx.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(
                    skipped_events = skipped,
                    "Telemetry channel lagged - some key execution intervals may be inaccurate"
                );
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                debug!("Telemetry channel closed, event loop shutting down");
                break;
            }
        };

        let Data::Key(KeyEvent { key, state, .. }) = &*event.data else {
            continue;
        };

        let tp_key = TopicPartitionKey::new(event.topic, event.partition, key.clone());

        let elapsed_nanos = event
            .timestamp
            .saturating_duration_since(reference_instant)
            .as_nanos() as u64;

        match *state {
            KeyState::HandlerInvoked => {
                const MAX_NANOS: u64 = u64::MAX - 1;
                let open_interval_set = [(elapsed_nanos, MAX_NANOS)].to_interval_set();

                if let Some(intervals) = key_intervals.get(&tp_key) {
                    key_intervals.insert(tp_key.clone(), intervals.union(&open_interval_set));
                    trace!(
                        topic = %tp_key.topic,
                        partition = tp_key.partition,
                        key = %tp_key.key,
                        "Handler invoked - extended execution interval"
                    );
                } else {
                    key_intervals.insert(tp_key.clone(), open_interval_set);
                    trace!(
                        topic = %tp_key.topic,
                        partition = tp_key.partition,
                        key = %tp_key.key,
                        "Handler invoked - opened new execution interval"
                    );
                }
            }
            KeyState::HandlerSucceeded | KeyState::HandlerFailed => {
                let window_start = elapsed_nanos.saturating_sub(window_nanos);
                let window_interval_set = [(window_start, elapsed_nanos)].to_interval_set();

                if let Some(intervals) = key_intervals.get(&tp_key) {
                    let windowed = intervals.intersection(&window_interval_set);
                    key_intervals.insert(tp_key.clone(), windowed);

                    trace!(
                        topic = %tp_key.topic,
                        partition = tp_key.partition,
                        key = %tp_key.key,
                        ?state,
                        "Handler completed - closed execution interval"
                    );
                } else {
                    // Handler completed without a corresponding invocation event
                    // (possibly due to telemetry lag or cache eviction)
                    debug!(
                        topic = %tp_key.topic,
                        partition = tp_key.partition,
                        key = %tp_key.key,
                        ?state,
                        "Handler completed but no open interval found"
                    );
                }
            }
        }
    }

    debug!("Event loop terminated");
}

/// Errors that can occur during monopolization detection.
#[derive(Debug, Error)]
pub enum MonopolizationError<E> {
    /// The inner handler returned an error.
    #[error(transparent)]
    Handler(E),

    /// A key has monopolized execution time.
    #[error(
        "Key '{key}' in {topic}:{partition} monopolized {percentage:.1}% of execution time over \
         {} window (threshold: {threshold:.1}%), preventing other keys from being processed \
         efficiently.",
        format_duration(*.window)
    )]
    Monopolization {
        /// The topic containing the monopolizing key.
        topic: Topic,
        /// The partition containing the monopolizing key.
        partition: Partition,
        /// The key that monopolized execution.
        key: Key,
        /// The percentage of execution time monopolized.
        percentage: f64,
        /// The configured threshold percentage that was exceeded.
        threshold: f64,
        /// The window duration over which monopolization was detected.
        window: Duration,
    },
}

impl<E> ClassifyError for MonopolizationError<E>
where
    E: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Handler(e) => e.classify_error(),
            Self::Monopolization { .. } => ErrorCategory::Transient,
        }
    }
}

/// Errors that can occur during monopolization middleware initialization.
#[derive(Debug, Error)]
pub enum MonopolizationInitError {
    /// Configuration validation failed.
    #[error("Invalid configuration: {0:#}")]
    Validation(#[from] ValidationErrors),
}

#[cfg(test)]
mod tests;
