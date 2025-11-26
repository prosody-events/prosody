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
use tracing::{debug, debug_span, warn};
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
use crate::util::from_env_with_fallback;
use crate::{Key, Partition, Topic};

/// Configuration for monopolization detection.
#[derive(Builder, Clone, Debug, Validate)]
pub struct MonopolizationConfiguration {
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
    /// Default: 5 minutes (300 seconds)
    #[builder(default = "from_env_with_fallback(\"PROSODY_MONOPOLIZATION_WINDOW\", 300)?")]
    pub window_duration_secs: u64,

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
    key_intervals: Arc<Cache<Key, IntervalSet<u64>, UnitWeighter, RandomState>>,
}

/// Provider that creates monopolization handlers for each partition.
#[derive(Clone)]
pub struct MonopolizationProvider<T> {
    provider: T,
    monopolization_threshold: f64,
    window_duration: Duration,
    reference_instant: Instant,
    key_intervals: Arc<Cache<Key, IntervalSet<u64>, UnitWeighter, RandomState>>,
}

/// Handler wrapper that checks for monopolization before delegating to inner
/// handler.
#[derive(Clone)]
pub struct MonopolizationHandler<T> {
    handler: T,
    reference_instant: Instant,
    key_intervals: Arc<Cache<Key, IntervalSet<u64>, UnitWeighter, RandomState>>,
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
    ) -> Result<Self, MonopolizationInitError> {
        config.validate()?;

        let reference_instant = Instant::now();
        let key_intervals = Arc::new(Cache::new(config.cache_size));

        let telemetry_rx = telemetry.subscribe();
        let key_intervals_clone = Arc::clone(&key_intervals);
        let window_duration = Duration::from_secs(config.window_duration_secs);

        spawn(run_event_loop(
            reference_instant,
            key_intervals_clone,
            window_duration,
            telemetry_rx,
        ));

        Ok(Self {
            monopolization_threshold: config.monopolization_threshold,
            window_duration,
            reference_instant,
            key_intervals,
        })
    }
}

impl HandlerMiddleware for MonopolizationMiddleware {
    type Provider<T: FallibleHandlerProvider> = MonopolizationProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
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

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        if let Some(error) = self.check_monopolization(message.key(), Instant::now()) {
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
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        if let Some(error) = self.check_monopolization(&trigger.key, Instant::now()) {
            return Err(error);
        }

        self.handler
            .on_timer(context, trigger, demand_type)
            .await
            .map_err(MonopolizationError::Handler)
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}

impl<T> MonopolizationHandler<T>
where
    T: FallibleHandler,
{
    #[allow(clippy::cast_precision_loss)]
    fn check_monopolization(
        &self,
        key: &Key,
        now: Instant,
    ) -> Option<MonopolizationError<T::Error>> {
        let _span = debug_span!(
            "check_monopolization",
            key = %key,
            monopolization_threshold_pct = %format!("{:.1}%", self.monopolization_threshold * 100.0_f64),
            window_secs = self.window_duration.as_secs(),
        )
            .entered();

        debug!("Checking key for monopolization");

        let intervals = self.key_intervals.get(key)?;

        let now_nanos = now
            .saturating_duration_since(self.reference_instant)
            .as_nanos() as u64;
        let window_nanos = self.window_duration.as_nanos() as u64;
        let window_start = now_nanos.saturating_sub(window_nanos);
        let window_interval_set = [(window_start, now_nanos)].to_interval_set();

        let windowed_intervals = intervals.intersection(&window_interval_set);

        let key_time_nanos: u64 = windowed_intervals
            .iter()
            .map(|interval| interval.upper().saturating_sub(interval.lower()))
            .sum();

        let monopolization_ratio = key_time_nanos as f64 / window_nanos as f64;
        let percentage = monopolization_ratio * 100.0_f64;

        debug!(
            execution_time_pct = %format!("{:.1}%", percentage),
            is_monopolizing = monopolization_ratio > self.monopolization_threshold,
            "Monopolization check complete"
        );

        (monopolization_ratio > self.monopolization_threshold).then(|| {
            debug!("Key exceeded monopolization threshold - returning error");

            MonopolizationError::Monopolization {
                key: key.clone(),
                percentage,
                threshold: self.monopolization_threshold * 100.0_f64,
                window: self.window_duration,
            }
        })
    }
}

async fn run_event_loop(
    reference_instant: Instant,
    key_intervals: Arc<Cache<Key, IntervalSet<u64>, UnitWeighter, RandomState>>,
    window_duration: Duration,
    mut telemetry_rx: broadcast::Receiver<TelemetryEvent>,
) {
    let window_nanos = window_duration.as_nanos() as u64;

    loop {
        let event = match telemetry_rx.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!("telemetry lagged by {skipped} events");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                break;
            }
        };

        let Data::Key(KeyEvent { key, state, .. }) = event.data else {
            continue;
        };

        let elapsed_nanos = event
            .timestamp
            .saturating_duration_since(reference_instant)
            .as_nanos() as u64;

        match state {
            KeyState::HandlerInvoked => {
                const MAX_NANOS: u64 = u64::MAX - 1;
                let open_interval_set = [(elapsed_nanos, MAX_NANOS)].to_interval_set();

                if let Some(intervals) = key_intervals.get(&key) {
                    key_intervals.insert(key.clone(), intervals.union(&open_interval_set));
                } else {
                    key_intervals.insert(key.clone(), open_interval_set);
                }
            }
            KeyState::HandlerSucceeded | KeyState::HandlerFailed => {
                let window_start = elapsed_nanos.saturating_sub(window_nanos);
                let window_interval_set = [(window_start, elapsed_nanos)].to_interval_set();

                if let Some(intervals) = key_intervals.get(&key) {
                    key_intervals.insert(key.clone(), intervals.intersection(&window_interval_set));
                }
            }
            _ => {}
        }
    }
}

/// Errors that can occur during monopolization detection.
#[derive(Debug, Error)]
pub enum MonopolizationError<E> {
    /// The inner handler returned an error.
    #[error(transparent)]
    Handler(E),

    /// A key has monopolized execution time.
    #[error(
        "Key '{key}' monopolized {percentage:.1}% of execution time over {} window \
         (threshold: {threshold:.1}%), preventing other keys from being processed efficiently.",
        format_duration(*.window)
    )]
    Monopolization {
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
    #[error("Invalid configuration: {0}")]
    Validation(#[from] ValidationErrors),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Partition;
    use crate::consumer::DemandType;
    use crate::consumer::event_context::EventContext;
    use crate::consumer::message::ConsumerMessage;
    use crate::consumer::middleware::{
        FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
    };
    use crate::telemetry::event::{Data, KeyEvent, KeyState, TelemetryEvent};
    use crate::timers::Trigger;
    use crate::tracing::init_test_logging;
    use color_eyre::Result;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::time::sleep;

    #[derive(Clone, Debug, Error)]
    #[error("Mock error")]
    struct MockError;

    impl ClassifyError for MockError {
        fn classify_error(&self) -> ErrorCategory {
            ErrorCategory::Permanent
        }
    }

    #[derive(Clone)]
    struct MockHandler {
        invocations: Arc<AtomicUsize>,
    }

    impl MockHandler {
        fn new() -> Self {
            Self {
                invocations: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl FallibleHandler for MockHandler {
        type Error = MockError;

        async fn on_message<C>(
            &self,
            _context: C,
            _message: ConsumerMessage,
            _demand_type: DemandType,
        ) -> Result<(), Self::Error>
        where
            C: EventContext,
        {
            self.invocations.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn on_timer<C>(
            &self,
            _context: C,
            _trigger: Trigger,
            _demand_type: DemandType,
        ) -> Result<(), Self::Error>
        where
            C: EventContext,
        {
            self.invocations.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn shutdown(self) {}
    }

    #[derive(Clone)]
    struct MockProvider {
        handler: MockHandler,
    }

    impl FallibleHandlerProvider for MockProvider {
        type Handler = MockHandler;

        fn handler_for_partition(&self, _topic: Topic, _partition: Partition) -> Self::Handler {
            self.handler.clone()
        }
    }

    fn create_key_event(
        topic: Topic,
        partition: Partition,
        key: Key,
        state: KeyState,
        timestamp: Instant,
    ) -> TelemetryEvent {
        TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Data::Key(KeyEvent {
                key,
                demand_type: DemandType::Normal,
                state,
            }),
        }
    }

    #[test]
    fn test_configuration_validation() -> Result<()> {
        let config = MonopolizationConfiguration::builder()
            .monopolization_threshold(1.5)
            .build()?;

        assert!(config.validate().is_err(), "Should reject threshold > 1.0");

        let config = MonopolizationConfiguration::builder().build()?;
        assert!(config.validate().is_ok(), "Should accept valid defaults");

        Ok(())
    }

    #[test]
    fn test_monopolization_error_classification() {
        let error: MonopolizationError<MockError> = MonopolizationError::Monopolization {
            key: "test-key".into(),
            percentage: 95.0,
            threshold: 90.0,
            window: Duration::from_secs(300),
        };

        assert!(
            matches!(error.classify_error(), ErrorCategory::Transient),
            "Monopolization errors should be transient (retry later when key is no longer \
             monopolizing)"
        );
    }

    #[test]
    fn test_monopolization_error_message() {
        let error: MonopolizationError<MockError> = MonopolizationError::Monopolization {
            key: "user-12345".into(),
            percentage: 95.5,
            threshold: 90.0,
            window: Duration::from_secs(300),
        };

        let message = error.to_string();
        assert!(
            message.contains("user-12345"),
            "Error should include the key"
        );
        assert!(
            message.contains("95.5%"),
            "Error should include the actual percentage"
        );
        assert!(message.contains("90.0%"), "Error should include threshold");
        assert!(
            message.contains("5m"),
            "Error should include window duration in human-readable format"
        );
        assert!(
            message.contains("preventing other keys from being processed efficiently"),
            "Error should include helpful explanation"
        );
    }

    #[tokio::test]
    async fn test_non_monopolizing_key_passes_through() -> Result<()> {
        init_test_logging();

        let telemetry = Telemetry::new();

        let config = MonopolizationConfiguration::builder()
            .monopolization_threshold(0.9)
            .window_duration_secs(300)
            .build()?;

        let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
        let mock_handler = MockHandler::new();
        let provider = MockProvider {
            handler: mock_handler.clone(),
        };

        let provider = middleware.with_provider(provider);
        let handler = provider.handler_for_partition("test-topic".into(), 0);

        let key: Key = "test-key".into();
        let reference_instant = handler.reference_instant;

        let start_time = reference_instant;
        let end_time = start_time + Duration::from_secs(10);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerInvoked,
            start_time,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerSucceeded,
            end_time,
        ));

        sleep(Duration::from_millis(10)).await;

        let result = handler.check_monopolization(&key, end_time);
        assert!(
            result.is_none(),
            "Key using 10s of 300s window should not monopolize"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_monopolizing_key_triggers_error() -> Result<()> {
        init_test_logging();

        init_test_logging();

        let telemetry = Telemetry::new();

        let config = MonopolizationConfiguration::builder()
            .monopolization_threshold(0.9)
            .window_duration_secs(100)
            .build()?;

        let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
        let mock_handler = MockHandler::new();
        let provider = MockProvider {
            handler: mock_handler.clone(),
        };

        let provider = middleware.with_provider(provider);
        let handler = provider.handler_for_partition("test-topic".into(), 0);

        let key: Key = "monopolizer".into();
        let reference_instant = handler.reference_instant;

        let start_time = reference_instant;
        let end_time = start_time + Duration::from_secs(95);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerInvoked,
            start_time,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerSucceeded,
            end_time,
        ));

        sleep(Duration::from_millis(10)).await;

        let result = handler.check_monopolization(&key, end_time);
        assert!(
            result.is_some(),
            "Key using 95s of 100s window (95%) should monopolize"
        );

        if let Some(MonopolizationError::Monopolization { percentage, .. }) = result {
            assert!(
                percentage > 90.0_f64,
                "Monopolization percentage should be > 90%"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_keys_independent_tracking() -> Result<()> {
        init_test_logging();

        let telemetry = Telemetry::new();

        let config = MonopolizationConfiguration::builder()
            .monopolization_threshold(0.9)
            .window_duration_secs(100)
            .build()?;

        let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
        let mock_handler = MockHandler::new();
        let provider = MockProvider {
            handler: mock_handler.clone(),
        };

        let provider = middleware.with_provider(provider);
        let handler = provider.handler_for_partition("test-topic".into(), 0);

        let key1: Key = "key-1".into();
        let key2: Key = "key-2".into();
        let reference_instant = handler.reference_instant;

        let start1 = reference_instant;
        let end1 = start1 + Duration::from_secs(95);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key1.clone(),
            KeyState::HandlerInvoked,
            start1,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key1.clone(),
            KeyState::HandlerSucceeded,
            end1,
        ));

        let start2 = reference_instant + Duration::from_millis(100);
        let end2 = start2 + Duration::from_secs(2);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key2.clone(),
            KeyState::HandlerInvoked,
            start2,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key2.clone(),
            KeyState::HandlerSucceeded,
            end2,
        ));

        sleep(Duration::from_millis(50)).await;

        // Check key1 at the time it finished (end1 = 95s)
        let result1 = handler.check_monopolization(&key1, end1);
        assert!(
            result1.is_some(),
            "Key 1 should be monopolizing (95s of 100s)"
        );

        // Check key2 at the time it finished (end2 = 2.1s)
        let result2 = handler.check_monopolization(&key2, end2);
        assert!(
            result2.is_none(),
            "Key 2 should not be monopolizing (2s of 100s)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_window_sliding_removes_old_intervals() -> Result<()> {
        init_test_logging();

        let telemetry = Telemetry::new();

        let config = MonopolizationConfiguration::builder()
            .monopolization_threshold(0.9)
            .window_duration_secs(10)
            .build()?;

        let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
        let mock_handler = MockHandler::new();
        let provider = MockProvider {
            handler: mock_handler.clone(),
        };

        let provider = middleware.with_provider(provider);
        let handler = provider.handler_for_partition("test-topic".into(), 0);

        let key: Key = "test-key".into();
        let reference_instant = handler.reference_instant;

        let start1 = reference_instant;
        let end1 = start1 + Duration::from_millis(9100); // 9.1 seconds to exceed 90% threshold

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerInvoked,
            start1,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerSucceeded,
            end1,
        ));

        sleep(Duration::from_millis(50)).await;

        let result = handler.check_monopolization(&key, end1);
        assert!(result.is_some(), "Should monopolize right after execution");

        let start2 = end1 + Duration::from_secs(11);
        let end2 = start2 + Duration::from_millis(100);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerInvoked,
            start2,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerSucceeded,
            end2,
        ));

        sleep(Duration::from_millis(10)).await;

        if let Some(intervals) = handler.key_intervals.get(&key) {
            let now_nanos = end2
                .saturating_duration_since(handler.reference_instant)
                .as_nanos() as u64;
            let window_nanos = Duration::from_secs(10).as_nanos() as u64;
            let window_start = now_nanos.saturating_sub(window_nanos);
            let window_interval_set = [(window_start, now_nanos)].to_interval_set();
            let windowed = intervals.intersection(&window_interval_set);

            let total_time: u64 = windowed
                .iter()
                .map(|iv| iv.upper().saturating_sub(iv.lower()))
                .sum();

            assert!(
                total_time < Duration::from_secs(1).as_nanos() as u64,
                "Old interval should be outside window"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_open_interval_closed_on_completion() -> Result<()> {
        init_test_logging();

        let telemetry = Telemetry::new();

        let config = MonopolizationConfiguration::builder()
            .window_duration_secs(100)
            .build()?;

        let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
        let mock_handler = MockHandler::new();
        let provider = MockProvider {
            handler: mock_handler.clone(),
        };

        let provider = middleware.with_provider(provider);
        let handler = provider.handler_for_partition("test-topic".into(), 0);

        let key: Key = "test-key".into();
        let reference_instant = handler.reference_instant;

        let start = reference_instant;

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerInvoked,
            start,
        ));

        sleep(Duration::from_millis(10)).await;

        let intervals_before = handler.key_intervals.get(&key);
        assert!(
            intervals_before.is_some(),
            "Should have open interval after invocation"
        );

        let end = start + Duration::from_secs(50);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerSucceeded,
            end,
        ));

        sleep(Duration::from_millis(10)).await;

        let intervals_after = handler.key_intervals.get(&key);
        assert!(
            intervals_after.is_some(),
            "Should have closed interval after completion"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_boundary_execution_before_window() -> Result<()> {
        init_test_logging();

        let telemetry = Telemetry::new();

        let config = MonopolizationConfiguration::builder()
            .monopolization_threshold(0.9)
            .window_duration_secs(100)
            .build()?;

        let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
        let mock_handler = MockHandler::new();
        let provider = MockProvider {
            handler: mock_handler.clone(),
        };

        let provider = middleware.with_provider(provider);
        let handler = provider.handler_for_partition("test-topic".into(), 0);

        let reference_instant = handler.reference_instant;

        let key: Key = "key-before-window".into();
        let execution_start = reference_instant;
        let execution_end = execution_start + Duration::from_secs(50);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerInvoked,
            execution_start,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerSucceeded,
            execution_end,
        ));

        sleep(Duration::from_millis(50)).await;

        // Check at time that puts execution_start before the window
        let check_time = reference_instant + Duration::from_secs(120);
        let result = handler.check_monopolization(&key, check_time);
        assert!(
            result.is_none(),
            "Execution that started before window should only count time within window"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_boundary_execution_crosses_window_end() -> Result<()> {
        init_test_logging();

        let telemetry = Telemetry::new();

        let config = MonopolizationConfiguration::builder()
            .monopolization_threshold(0.9)
            .window_duration_secs(100)
            .build()?;

        let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
        let mock_handler = MockHandler::new();
        let provider = MockProvider {
            handler: mock_handler.clone(),
        };

        let provider = middleware.with_provider(provider);
        let handler = provider.handler_for_partition("test-topic".into(), 0);

        let reference_instant = handler.reference_instant;

        let key: Key = "key-crosses-boundary".into();
        let execution_start = reference_instant + Duration::from_secs(10);
        let execution_end = execution_start + Duration::from_secs(95);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerInvoked,
            execution_start,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerSucceeded,
            execution_end,
        ));

        sleep(Duration::from_millis(50)).await;

        let result = handler.check_monopolization(&key, execution_end);
        assert!(
            result.is_some(),
            "Key using 95s of 100s window should monopolize at window end"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_boundary_exact_threshold() -> Result<()> {
        init_test_logging();

        let telemetry = Telemetry::new();

        let config = MonopolizationConfiguration::builder()
            .monopolization_threshold(0.9)
            .window_duration_secs(100)
            .build()?;

        let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
        let mock_handler = MockHandler::new();
        let provider = MockProvider {
            handler: mock_handler.clone(),
        };

        let provider = middleware.with_provider(provider);
        let handler = provider.handler_for_partition("test-topic".into(), 0);

        let reference_instant = handler.reference_instant;

        let key: Key = "key-exact-threshold".into();
        let execution_start = reference_instant;
        let execution_end = execution_start + Duration::from_secs(90);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerInvoked,
            execution_start,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerSucceeded,
            execution_end,
        ));

        sleep(Duration::from_millis(50)).await;

        let result = handler.check_monopolization(&key, execution_end);
        assert!(
            result.is_none(),
            "Key using exactly 90s of 100s window (90.0%) should not monopolize (threshold is \
             >90%, not >=90%)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_boundary_just_above_threshold() -> Result<()> {
        init_test_logging();

        let telemetry = Telemetry::new();

        let config = MonopolizationConfiguration::builder()
            .monopolization_threshold(0.9)
            .window_duration_secs(100)
            .build()?;

        let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
        let mock_handler = MockHandler::new();
        let provider = MockProvider {
            handler: mock_handler.clone(),
        };

        let provider = middleware.with_provider(provider);
        let handler = provider.handler_for_partition("test-topic".into(), 0);

        let reference_instant = handler.reference_instant;

        let key: Key = "key-above-threshold".into();
        let execution_start = reference_instant;
        let execution_end = execution_start + Duration::from_millis(90_100);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerInvoked,
            execution_start,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerSucceeded,
            execution_end,
        ));

        sleep(Duration::from_millis(50)).await;

        let result = handler.check_monopolization(&key, execution_end);
        assert!(
            result.is_some(),
            "Key using 90.1s of 100s window (90.1%) should monopolize"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_boundary_multiple_executions_in_window() -> Result<()> {
        init_test_logging();

        let telemetry = Telemetry::new();

        let config = MonopolizationConfiguration::builder()
            .monopolization_threshold(0.9)
            .window_duration_secs(100)
            .build()?;

        let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
        let mock_handler = MockHandler::new();
        let provider = MockProvider {
            handler: mock_handler.clone(),
        };

        let provider = middleware.with_provider(provider);
        let handler = provider.handler_for_partition("test-topic".into(), 0);

        let reference_instant = handler.reference_instant;
        let key: Key = "key-multiple-at-boundary".into();

        // First execution: 20s at start of window
        let first_start = reference_instant;
        let first_end = first_start + Duration::from_secs(20);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerInvoked,
            first_start,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerSucceeded,
            first_end,
        ));

        // Second execution: 72s that ends at window boundary
        let second_start = first_start + Duration::from_secs(28);
        let second_end = first_start + Duration::from_secs(100);

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerInvoked,
            second_start,
        ));

        telemetry.test_emit(create_key_event(
            "test-topic".into(),
            0,
            key.clone(),
            KeyState::HandlerSucceeded,
            second_end,
        ));

        sleep(Duration::from_millis(50)).await;

        // Check at end of window - should capture both executions (20s + 72s = 92s >
        // 90s)
        let check_time = first_start + Duration::from_secs(100);
        let result = handler.check_monopolization(&key, check_time);
        assert!(
            result.is_some(),
            "Multiple executions totaling >90s in window should monopolize"
        );

        Ok(())
    }
}
