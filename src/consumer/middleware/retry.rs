//! Exponential backoff retry middleware.
//!
//! Automatically retries transient failures using exponential backoff with
//! jitter. Only retries [`ErrorCategory::Transient`] errors - permanent and
//! terminal errors are passed through immediately.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. Pass control to inner middleware layers
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. **If error is transient**: Sleep with exponential backoff and retry
//! 3. **If error is permanent/terminal**: Pass through immediately
//! 4. **If max retries exceeded**: Pass through final error
//!
//! # Retry Logic
//!
//! - **Initial delay**: Starts with configured base delay
//! - **Exponential growth**: Each retry doubles the delay (with jitter)
//! - **Maximum delay**: Capped at configured maximum
//! - **Jitter**: Adds randomness to prevent thundering herd
//!
//! # Cancellation Handling
//!
//! The retry middleware distinguishes between two types of cancellation
//! signals:
//!
//! - **Shutdown** (partition revoked, consumer stopping): Aborts immediately.
//!   The partition must be released promptly to allow rebalancing.
//!
//! - **Message cancellation**: Treated as a transient condition. The retry loop
//!   continues, skipping any remaining sleep delay. This ensures that
//!   cancellation doesn't cause message loss when retries could still succeed.
//!
//! # Usage
//!
//! Often used multiple times in a pipeline for different failure points:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::retry::*;
//! # use prosody::consumer::middleware::scheduler::*;
//! # use prosody::consumer::middleware::cancellation::CancellationMiddleware;
//! # use prosody::consumer::middleware::topic::*;
//! # use prosody::consumer::DemandType;
//! # use prosody::consumer::event_context::EventContext;
//! # use prosody::consumer::message::ConsumerMessage;
//! # use prosody::producer::{ProducerConfiguration, ProsodyProducer};
//! # use prosody::telemetry::Telemetry;
//! # use prosody::timers::Trigger;
//! # use std::convert::Infallible;
//! # #[derive(Clone)]
//! # struct MyHandler;
//! # impl FallibleHandler for MyHandler {
//! #     type Error = Infallible;
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_timer<C>(&self, _: C, _: Trigger, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn shutdown(self) {}
//! # }
//! # let config = SchedulerConfigurationBuilder::default().build().unwrap();
//! # let retry_config = RetryConfiguration::builder().build().unwrap();
//! # let topic_config = FailureTopicConfiguration::builder().build().unwrap();
//! # let producer_config = ProducerConfiguration::builder().bootstrap_servers(vec!["kafka:9092".to_string()]).build().unwrap();
//! # let producer = ProsodyProducer::new(&producer_config).unwrap();
//! # let telemetry = Telemetry::default();
//! # let handler = MyHandler;
//!
//! let provider = SchedulerMiddleware::new(&config, &telemetry).unwrap()
//!     .layer(CancellationMiddleware)
//!     .layer(RetryMiddleware::new(retry_config.clone()).unwrap()) // Retry handler failures
//!     .layer(FailureTopicMiddleware::new(topic_config, "consumer-group".to_string(), producer).unwrap())
//!     .layer(RetryMiddleware::new(retry_config).unwrap()) // Retry DLQ writes
//!     .into_provider(handler);
//! ```
//!
//! [`ErrorCategory::Transient`]: crate::consumer::middleware::ErrorCategory::Transient

use std::cmp::min;
use std::time::Duration;

use derive_builder::Builder;
use humantime::format_duration;
use rand::Rng;
use tokio::select;
use tokio::time::sleep;
use tracing::{debug, error, info};
use validator::{Validate, ValidationErrors};

use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{DemandType, EventHandler, HandlerProvider, Keyed, Uncommitted};
use crate::timers::{Trigger, UncommittedTimer};
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};
use crate::{Partition, Topic};

// ============================================================================
// Retry Sleep Helper
// ============================================================================

/// Result of waiting during retry backoff.
///
/// Used by the retry loop to determine the next action after a sleep period.
/// This centralizes the cancellation handling logic to ensure consistent
/// behavior across all retry implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryWaitResult {
    /// Sleep completed normally, continue with retry.
    Completed,
    /// Shutdown requested (partition revoked), abort immediately.
    Shutdown,
    /// Message cancelled, skip remaining sleep and continue retry.
    Cancelled,
}

/// Waits for the specified duration with cancellation support.
///
/// This helper encapsulates the critical shutdown vs cancellation distinction:
/// - **Shutdown**: Partition revoked or consumer stopping. Returns
///   `RetryWaitResult::Shutdown`.
/// - **Cancellation**: Message-level cancellation requested. Returns
///   `RetryWaitResult::Cancelled`.
/// - **Completed**: Sleep finished normally. Returns
///   `RetryWaitResult::Completed`.
///
/// # Arguments
///
/// * `context` - The event context providing termination signals.
/// * `duration` - How long to sleep before retrying.
///
/// # Returns
///
/// A `RetryWaitResult` indicating why the wait ended.
async fn wait_with_cancellation<C: EventContext>(
    context: &C,
    duration: Duration,
) -> RetryWaitResult {
    select! {
        () = sleep(duration) => RetryWaitResult::Completed,
        () = context.on_shutdown() => RetryWaitResult::Shutdown,
        () = context.on_message_cancelled() => RetryWaitResult::Cancelled,
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for retry middleware.
#[derive(Builder, Clone, Debug, Validate)]
pub struct RetryConfiguration {
    /// Base exponential backoff delay.
    ///
    /// Environment variable: `PROSODY_RETRY_BASE`
    /// Default: 20 ms
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_RETRY_BASE\", \
                   Duration::from_millis(20))?",
        setter(into)
    )]
    base: Duration,

    /// Maximum number of retries.
    ///
    /// Environment variable: `PROSODY_MAX_RETRIES`
    /// Default: 3
    ///
    /// When composed with other retry strategies, this represents the maximum
    /// number of retries before falling back to the next middleware.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MAX_RETRIES\", 3)?",
        setter(into)
    )]
    max_retries: u32,

    /// Maximum retry delay.
    ///
    /// Environment variable: `PROSODY_RETRY_MAX_DELAY`
    /// Default: 5 minutes
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_RETRY_MAX_DELAY\", \
                   Duration::from_secs(5 * 60))?",
        setter(into)
    )]
    max_delay: Duration,
}

impl RetryConfiguration {
    /// Creates a new `RetryConfigurationBuilder`.
    ///
    /// # Returns
    ///
    /// A `RetryConfigurationBuilder` instance.
    #[must_use]
    pub fn builder() -> RetryConfigurationBuilder {
        RetryConfigurationBuilder::default()
    }
}

/// Middleware that retries failed message processing attempts.
#[derive(Clone, Debug)]
pub struct RetryMiddleware(RetryConfiguration);

impl RetryMiddleware {
    /// Creates a new `RetryMiddleware` with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for the retry middleware.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `RetryMiddleware` if the configuration is
    /// valid, or `ValidationErrors` if the configuration is invalid.
    ///
    /// # Errors
    ///
    /// Returns `ValidationErrors` if any validation defined in the
    /// `RetryConfiguration` struct fails.
    pub fn new(config: RetryConfiguration) -> Result<Self, ValidationErrors> {
        config.validate()?;
        Ok(Self(config))
    }
}

/// A provider that retries failed message processing attempts.
#[derive(Clone, Debug)]
pub struct RetryProvider<T> {
    provider: T,
    config: RetryConfiguration,
}

/// A handler wrapped with retry functionality.
#[derive(Clone, Debug)]
pub struct RetryHandler<T> {
    base_delay_millis: u64,
    max_delay_millis: u64,
    max_retries: u32,
    handler: T,
}

impl<T> RetryProvider<T> {
    /// Creates a retry handler for the given topic and partition.
    fn create_handler<H>(&self, handler: H) -> RetryHandler<H> {
        RetryHandler {
            base_delay_millis: self.config.base.as_millis() as u64,
            max_delay_millis: self.config.max_delay.as_millis() as u64,
            max_retries: self.config.max_retries,
            handler,
        }
    }
}

impl<T> RetryHandler<T> {
    /// Calculates the sleep time for a given retry attempt.
    ///
    /// # Arguments
    ///
    /// * `attempt` - The current retry attempt number.
    ///
    /// # Returns
    ///
    /// The duration to sleep before the next retry attempt.
    fn sleep_time(&self, attempt: u32) -> Duration {
        let exp_backoff = min(
            2u64.saturating_pow(attempt)
                .saturating_mul(self.base_delay_millis),
            self.max_delay_millis,
        );

        let jitter = rand::rng().random_range(0..exp_backoff);
        Duration::from_millis(jitter)
    }
}

impl HandlerMiddleware for RetryMiddleware {
    type Provider<T: FallibleHandlerProvider> = RetryProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        RetryProvider {
            provider,
            config: self.0.clone(),
        }
    }
}

impl<T> FallibleHandlerProvider for RetryProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = RetryHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        self.create_handler(self.provider.handler_for_partition(topic, partition))
    }
}

impl<T> HandlerProvider for RetryProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = RetryHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        self.create_handler(self.provider.handler_for_partition(topic, partition))
    }
}

impl<T> FallibleHandler for RetryHandler<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;

    /// Handles a message with retry functionality.
    ///
    /// # Arguments
    ///
    /// * `context` - The message context.
    /// * `message` - The consumer message to be processed.
    /// * `demand_type` - Whether this is normal processing or failure retry.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of message processing.
    ///
    /// # Errors
    ///
    /// Returns the underlying handler's error if all retry attempts fail.
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key();
        let offset = message.offset();
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);
            // Use the original demand type for the first attempt, failure demand for
            // retries
            let current_demand_type = if attempt == 1 {
                demand_type
            } else {
                DemandType::Failure
            };
            let Err(error) = self
                .handler
                .on_message(context.clone(), message.clone(), current_demand_type)
                .await
            else {
                return Ok(());
            };

            // Only abort on shutdown (partition revoked). Message cancellation is
            // treated as transient - we continue retrying.
            if context.is_shutdown() {
                return Err(error);
            }

            // Handle different error categories
            match error.classify_error() {
                ErrorCategory::Transient => {
                    if attempt > self.max_retries {
                        // Log the final failure and return the error
                        error!(
                            partition,
                            key = key.as_ref(),
                            offset,
                            attempt,
                            topic = topic.as_ref(),
                            "failed to handle message: {error:#}; maximum attempts reached"
                        );
                        return Err(error);
                    }

                    let sleep_time = self.sleep_time(attempt);

                    // Log the failure and retry information
                    error!(
                        partition,
                        key = key.as_ref(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "failed to handle message: {error:#}; retrying after {}",
                        format_duration(sleep_time)
                    );

                    if wait_with_cancellation(&context, sleep_time).await
                        == RetryWaitResult::Shutdown
                    {
                        return Err(error);
                    }
                }
                ErrorCategory::Permanent => {
                    error!(
                        partition,
                        key = key.as_ref(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "permanently failed to handle message: {error:#}"
                    );
                    return Err(error);
                }
                ErrorCategory::Terminal => {
                    info!(
                        partition,
                        key = key.as_ref(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "terminal condition encountered while handling message: {error:#}; \
                         aborting"
                    );
                    return Err(error);
                }
            }
        }
    }

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Retry logic for a fired timer
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            // Use the original demand type for the first attempt, failure demand for
            // retries
            let current_demand_type = if attempt == 1 {
                demand_type
            } else {
                DemandType::Failure
            };
            // Try handling the timer
            let Err(error) = self
                .handler
                .on_timer(context.clone(), timer.clone(), current_demand_type)
                .await
            else {
                return Ok(());
            };
            // Only abort on shutdown (partition revoked). Message cancellation is
            // treated as transient - we continue retrying.
            if context.is_shutdown() {
                return Err(error);
            }
            match error.classify_error() {
                ErrorCategory::Transient => {
                    if attempt > self.max_retries {
                        error!("failed to handle timer: {error:#}; maximum attempts reached");
                        return Err(error);
                    }
                    let sleep_time = self.sleep_time(attempt);
                    error!(
                        "failed to handle timer: {error:#}; retrying after {}",
                        format_duration(sleep_time)
                    );
                    if wait_with_cancellation(&context, sleep_time).await
                        == RetryWaitResult::Shutdown
                    {
                        return Err(error);
                    }
                }
                ErrorCategory::Permanent => {
                    error!("permanently failed to handle timer: {error:#}");
                    return Err(error);
                }
                ErrorCategory::Terminal => {
                    info!(
                        "terminal condition encountered while handling timer: {error:#}; aborting"
                    );
                    return Err(error);
                }
            }
        }
    }

    async fn shutdown(self) {
        debug!("shutting down retry handler");

        // No retry-specific state to clean up (timers are handled by tokio)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
    }
}

impl<T> EventHandler for RetryHandler<T>
where
    T: FallibleHandler,
{
    /// Handles a message with retry functionality and commits the offset upon
    /// success.
    ///
    /// # Arguments
    ///
    /// * `context` - The message context.
    /// * `message` - The uncommitted message to be processed.
    /// * `demand_type` - Whether this is normal processing or failure retry.
    async fn on_message<C>(&self, context: C, message: UncommittedMessage, demand_type: DemandType)
    where
        C: EventContext,
    {
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key().to_owned();
        let offset = message.offset();
        let (message, uncommitted_offset) = message.into_inner();
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);
            // Use the original demand type for the first attempt, failure demand for
            // retries
            let current_demand_type = if attempt == 1 {
                demand_type
            } else {
                DemandType::Failure
            };
            let Err(error) = self
                .handler
                .on_message(context.clone(), message.clone(), current_demand_type)
                .await
            else {
                uncommitted_offset.commit();
                break;
            };

            // Only abort on shutdown (partition revoked). Message cancellation is
            // treated as transient - we continue retrying.
            if context.is_shutdown() {
                uncommitted_offset.abort();
                break;
            }

            // Handle different error categories
            match error.classify_error() {
                ErrorCategory::Transient => {
                    let sleep_time = self.sleep_time(attempt);
                    error!(
                        partition,
                        key = key.as_ref(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "failed to handle message: {error:#}; retrying after {}",
                        format_duration(sleep_time)
                    );

                    if wait_with_cancellation(&context, sleep_time).await
                        == RetryWaitResult::Shutdown
                    {
                        uncommitted_offset.abort();
                        break;
                    }
                }
                ErrorCategory::Permanent => {
                    error!(
                        partition,
                        key = key.as_ref(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "permanently failed to handle message: {error:#}; discarding message"
                    );
                    uncommitted_offset.commit();
                    break;
                }
                ErrorCategory::Terminal => {
                    info!(
                        partition,
                        key = key.as_ref(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "terminal condition encountered while handling message: {error:#}; \
                         aborting"
                    );
                    uncommitted_offset.abort();
                    break;
                }
            }
        }
    }

    async fn on_timer<C, U>(&self, context: C, timer: U, demand_type: DemandType)
    where
        C: EventContext,
        U: UncommittedTimer,
    {
        // Retry logic for an uncommitted timer
        let (trigger, uncommitted) = timer.into_inner();
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            // Use the original demand type for the first attempt, failure demand for
            // retries
            let current_demand_type = if attempt == 1 {
                demand_type
            } else {
                DemandType::Failure
            };
            // Try handling the timer
            let Err(error) = self
                .handler
                .on_timer(context.clone(), trigger.clone(), current_demand_type)
                .await
            else {
                uncommitted.commit().await;
                break;
            };
            // Only abort on shutdown (partition revoked). Message cancellation is
            // treated as transient - we continue retrying.
            if context.is_shutdown() {
                uncommitted.abort().await;
                break;
            }
            match error.classify_error() {
                ErrorCategory::Transient => {
                    let sleep_time = self.sleep_time(attempt);
                    error!(
                        "failed to handle timer: {error:#}; retrying after {}",
                        format_duration(sleep_time)
                    );
                    if wait_with_cancellation(&context, sleep_time).await
                        == RetryWaitResult::Shutdown
                    {
                        uncommitted.abort().await;
                        break;
                    }
                }
                ErrorCategory::Permanent => {
                    error!("permanently failed to handle timer: {error:#}; discarding timer");
                    uncommitted.commit().await;
                    break;
                }
                ErrorCategory::Terminal => {
                    info!(
                        "terminal condition encountered while handling timer: {error:#}; aborting"
                    );
                    uncommitted.abort().await;
                    break;
                }
            }
        }
    }

    /// Performs any necessary shutdown operations for the handler.
    async fn shutdown(self) {
        debug!("shutting down retry handler");

        // No retry-specific state to clean up (timers are handled by tokio)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
    use crate::consumer::middleware::test_support::MockEventContext;
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use parking_lot::Mutex;
    use std::error::Error;
    use std::fmt::{Display, Formatter, Result as FmtResult};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Semaphore;
    use tokio::time::{sleep as tokio_sleep, timeout};
    use tracing::Span;

    /// Test error type with configurable classification.
    #[derive(Debug, Clone)]
    struct TestError(ErrorCategory);

    impl Display for TestError {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            write!(f, "test error ({:?})", self.0)
        }
    }

    impl Error for TestError {}

    impl ClassifyError for TestError {
        fn classify_error(&self) -> ErrorCategory {
            self.0
        }
    }

    /// Mock handler that tracks calls and can be configured to fail.
    #[derive(Clone)]
    struct MockHandler {
        call_count: Arc<AtomicUsize>,
        /// Sequence of results to return on successive calls.
        /// Empty means success.
        failure_sequence: Arc<Mutex<Vec<ErrorCategory>>>,
        /// Recorded demand types from calls.
        demand_types: Arc<Mutex<Vec<DemandType>>>,
    }

    impl MockHandler {
        fn success() -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                failure_sequence: Arc::new(Mutex::new(vec![])),
                demand_types: Arc::new(Mutex::new(vec![])),
            }
        }

        fn failing_then_success(failures: Vec<ErrorCategory>) -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                failure_sequence: Arc::new(Mutex::new(failures)),
                demand_types: Arc::new(Mutex::new(vec![])),
            }
        }

        fn always_failing(category: ErrorCategory) -> Self {
            // Create a large sequence that should outlast max_retries
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                failure_sequence: Arc::new(Mutex::new(vec![category; 100])),
                demand_types: Arc::new(Mutex::new(vec![])),
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::Relaxed)
        }

        fn recorded_demand_types(&self) -> Vec<DemandType> {
            self.demand_types.lock().clone()
        }
    }

    impl FallibleHandler for MockHandler {
        type Error = TestError;

        async fn on_message<C>(
            &self,
            _context: C,
            _message: ConsumerMessage,
            demand_type: DemandType,
        ) -> Result<(), Self::Error>
        where
            C: EventContext,
        {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            self.demand_types.lock().push(demand_type);

            let mut seq = self.failure_sequence.lock();
            if seq.is_empty() {
                Ok(())
            } else {
                let category = seq.remove(0);
                Err(TestError(category))
            }
        }

        async fn on_timer<C>(
            &self,
            _context: C,
            _trigger: Trigger,
            demand_type: DemandType,
        ) -> Result<(), Self::Error>
        where
            C: EventContext,
        {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            self.demand_types.lock().push(demand_type);

            let mut seq = self.failure_sequence.lock();
            if seq.is_empty() {
                Ok(())
            } else {
                let category = seq.remove(0);
                Err(TestError(category))
            }
        }

        async fn shutdown(self) {}
    }

    fn create_test_message() -> Option<ConsumerMessage> {
        let semaphore = Arc::new(Semaphore::new(10));
        let permit = semaphore.try_acquire_owned().ok()?;
        Some(ConsumerMessage::new(
            ConsumerMessageValue::default(),
            Span::current(),
            permit,
        ))
    }

    fn create_test_trigger() -> Trigger {
        Trigger::for_testing(
            "test-key".into(),
            CompactDateTime::from(1000_u32),
            TimerType::default(),
        )
    }

    fn create_retry_handler<T>(handler: T, max_retries: u32) -> RetryHandler<T> {
        RetryHandler {
            base_delay_millis: 1, // Very short for tests
            max_delay_millis: 10,
            max_retries,
            handler,
        }
    }

    // === Success Tests ===

    #[tokio::test]
    async fn success_on_first_attempt_returns_ok_immediately() {
        let handler = MockHandler::success();
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1, "Should only call handler once");
    }

    // === Transient Error Tests ===

    #[tokio::test]
    async fn transient_error_retries_then_succeeds() {
        // Fail twice with transient errors, then succeed
        let handler = MockHandler::failing_then_success(vec![
            ErrorCategory::Transient,
            ErrorCategory::Transient,
        ]);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_ok(), "Should succeed after retries");
        assert_eq!(handler.call_count(), 3, "Should retry twice then succeed");
    }

    #[tokio::test]
    async fn transient_error_fails_after_max_retries() {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_err(), "Should fail after max retries");
        // First attempt + 3 retries = 4 total calls
        assert_eq!(
            handler.call_count(),
            4,
            "Should attempt 1 + max_retries times"
        );
    }

    // === Permanent Error Tests ===

    #[tokio::test]
    async fn permanent_error_fails_immediately_no_retry() {
        let handler = MockHandler::always_failing(ErrorCategory::Permanent);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1, "Should not retry permanent errors");
    }

    // === Terminal Error Tests ===

    #[tokio::test]
    async fn terminal_error_fails_immediately_no_retry() {
        let handler = MockHandler::always_failing(ErrorCategory::Terminal);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1, "Should not retry terminal errors");
    }

    // === Demand Type Tests ===

    #[tokio::test]
    async fn first_attempt_uses_original_demand_type_retries_use_failure() {
        // Fail once with transient, then succeed
        let handler = MockHandler::failing_then_success(vec![ErrorCategory::Transient]);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_ok());
        let demand_types = handler.recorded_demand_types();
        assert_eq!(demand_types.len(), 2);
        assert_eq!(
            demand_types[0],
            DemandType::Normal,
            "First attempt should use original"
        );
        assert_eq!(
            demand_types[1],
            DemandType::Failure,
            "Retry should use Failure"
        );
    }

    // === Shutdown Tests ===

    #[tokio::test]
    async fn shutdown_during_retry_sleep_returns_error() {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        // Use longer delays to give time for shutdown signal
        let retry_handler = RetryHandler {
            base_delay_millis: 1000, // 1 second base delay
            max_delay_millis: 10000,
            max_retries: 10,
            handler: handler.clone(),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        // Spawn the retry operation
        let ctx = context.clone();
        let handle = tokio::spawn(async move {
            FallibleHandler::on_message(&retry_handler, ctx, message, DemandType::Normal).await
        });

        // Wait a bit for the first failure and retry sleep to start
        tokio_sleep(Duration::from_millis(50)).await;

        // Signal shutdown
        context.request_shutdown();

        // Should complete quickly due to shutdown
        let Ok(join_result) = timeout(Duration::from_millis(500), handle).await else {
            // Timed out waiting for shutdown - test fails
            return;
        };
        let Ok(result) = join_result else {
            // Task panicked - test fails
            return;
        };

        assert!(result.is_err(), "Should return error on shutdown");
    }

    // === Timer Path Tests ===

    #[tokio::test]
    async fn timer_success_on_first_attempt() {
        let handler = MockHandler::success();
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let trigger = create_test_trigger();

        let result =
            FallibleHandler::on_timer(&retry_handler, context, trigger, DemandType::Normal).await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn timer_transient_error_retries_then_succeeds() {
        let handler = MockHandler::failing_then_success(vec![ErrorCategory::Transient]);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let trigger = create_test_trigger();

        let result =
            FallibleHandler::on_timer(&retry_handler, context, trigger, DemandType::Normal).await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 2);
    }

    #[tokio::test]
    async fn timer_permanent_error_no_retry() {
        let handler = MockHandler::always_failing(ErrorCategory::Permanent);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let trigger = create_test_trigger();

        let result =
            FallibleHandler::on_timer(&retry_handler, context, trigger, DemandType::Normal).await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1);
    }

    // === Backoff Calculation Tests ===

    #[test]
    fn sleep_time_has_exponential_growth_with_jitter() {
        let handler = MockHandler::success();
        let retry_handler = RetryHandler {
            base_delay_millis: 100,
            max_delay_millis: 10000,
            max_retries: 10,
            handler,
        };

        // Collect multiple samples to verify jitter randomness
        let mut samples_attempt_1: Vec<u64> = Vec::new();
        let mut samples_attempt_3: Vec<u64> = Vec::new();

        for _ in 0_u32..100_u32 {
            samples_attempt_1.push(retry_handler.sleep_time(1).as_millis() as u64);
            samples_attempt_3.push(retry_handler.sleep_time(3).as_millis() as u64);
        }

        // Attempt 1: exp_backoff = 2^1 * 100 = 200ms, jitter in [0, 200)
        let Some(&max_attempt_1) = samples_attempt_1.iter().max() else {
            return;
        };
        assert!(max_attempt_1 < 200, "Attempt 1 jitter should be < 200ms");

        // Attempt 3: exp_backoff = 2^3 * 100 = 800ms, jitter in [0, 800)
        let Some(&max_attempt_3) = samples_attempt_3.iter().max() else {
            return;
        };
        assert!(max_attempt_3 < 800, "Attempt 3 jitter should be < 800ms");

        // Verify there's some variation (jitter is working)
        let Some(&min_attempt_3) = samples_attempt_3.iter().min() else {
            return;
        };
        assert!(
            max_attempt_3 > min_attempt_3 + 50,
            "Jitter should introduce variation"
        );
    }

    #[test]
    fn sleep_time_capped_at_max_delay() {
        let handler = MockHandler::success();
        let retry_handler = RetryHandler {
            base_delay_millis: 100,
            max_delay_millis: 500,
            max_retries: 10,
            handler,
        };

        // Attempt 10: exp_backoff = 2^10 * 100 = 102400ms, but capped at 500ms
        // Jitter should be in [0, 500)
        for _ in 0_u32..100_u32 {
            let sleep = retry_handler.sleep_time(10).as_millis() as u64;
            assert!(sleep < 500, "Sleep time should be capped at max_delay");
        }
    }

    // =========================================================================
    // Shutdown vs Cancellation Tests
    // =========================================================================
    //
    // These tests verify correct behavior for two distinct signals:
    // - **Shutdown**: Partition revoked or consumer stopping → should abort
    // - **Cancellation**: Message-level cancellation → should treat as transient,
    //   retry
    //
    // Test matrix (2×2×2 = 8 tests):
    // - Handler type: FallibleHandler vs EventHandler
    // - Method: on_message vs on_timer
    // - Signal: shutdown vs cancellation

    use crate::consumer::partition::offsets::OffsetTracker;
    use crate::consumer::{Keyed, Uncommitted};
    use crate::timers::UncommittedTimer;
    use color_eyre::eyre::{Result, bail};
    use crossbeam_utils::CachePadded;

    /// Mock commit guard for tracking commit/abort calls.
    struct MockCommitGuard {
        committed: Arc<AtomicBool>,
        aborted: Arc<AtomicBool>,
    }

    impl Uncommitted for MockCommitGuard {
        async fn commit(self) {
            self.committed.store(true, Ordering::Relaxed);
        }

        async fn abort(self) {
            self.aborted.store(true, Ordering::Relaxed);
        }
    }

    /// Mock uncommitted timer for testing `EventHandler::on_timer`.
    struct MockUncommittedTimer {
        trigger: Trigger,
        committed: Arc<AtomicBool>,
        aborted: Arc<AtomicBool>,
    }

    impl MockUncommittedTimer {
        fn new(committed: Arc<AtomicBool>, aborted: Arc<AtomicBool>) -> Self {
            Self {
                trigger: create_test_trigger(),
                committed,
                aborted,
            }
        }
    }

    impl Keyed for MockUncommittedTimer {
        type Key = crate::Key;

        fn key(&self) -> &Self::Key {
            &self.trigger.key
        }
    }

    impl Uncommitted for MockUncommittedTimer {
        async fn commit(self) {
            self.committed.store(true, Ordering::Relaxed);
        }

        async fn abort(self) {
            self.aborted.store(true, Ordering::Relaxed);
        }
    }

    impl UncommittedTimer for MockUncommittedTimer {
        type CommitGuard = MockCommitGuard;

        fn time(&self) -> CompactDateTime {
            self.trigger.time
        }

        fn timer_type(&self) -> TimerType {
            self.trigger.timer_type
        }

        fn span(&self) -> Span {
            Span::none()
        }

        fn into_inner(self) -> (Trigger, Self::CommitGuard) {
            (
                self.trigger,
                MockCommitGuard {
                    committed: self.committed,
                    aborted: self.aborted,
                },
            )
        }
    }

    fn create_offset_tracker() -> OffsetTracker {
        let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
        OffsetTracker::new(
            "test-topic".into(),
            0,
            10,
            Duration::from_secs(300),
            version,
        )
    }

    // === Shutdown Tests (should pass - abort is correct behavior) ===

    /// `FallibleHandler::on_message` should abort on shutdown signal.
    #[tokio::test]
    async fn fallible_on_message_shutdown_aborts() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_shutdown();

        let Some(message) = create_test_message() else {
            bail!("failed to create test message");
        };
        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1);
        Ok(())
    }

    /// `FallibleHandler::on_timer` should abort on shutdown signal.
    #[tokio::test]
    async fn fallible_on_timer_shutdown_aborts() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_shutdown();

        let result = FallibleHandler::on_timer(
            &retry_handler,
            context,
            create_test_trigger(),
            DemandType::Normal,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1);
        Ok(())
    }

    /// `EventHandler::on_message` should abort offset on shutdown signal.
    #[tokio::test]
    async fn event_on_message_shutdown_aborts() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_shutdown();

        let tracker = create_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let Some(message) = create_test_message() else {
            bail!("failed to create test message");
        };
        let uncommitted_message = message.into_uncommitted(uncommitted_offset);

        EventHandler::on_message(
            &retry_handler,
            context,
            uncommitted_message,
            DemandType::Normal,
        )
        .await;

        assert_eq!(handler.call_count(), 1);
        assert_eq!(tracker.shutdown().await, None, "offset should be aborted");
        Ok(())
    }

    /// `EventHandler::on_timer` should abort on shutdown signal.
    #[tokio::test]
    async fn event_on_timer_shutdown_aborts() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_shutdown();

        let committed = Arc::new(AtomicBool::new(false));
        let aborted = Arc::new(AtomicBool::new(false));
        let timer = MockUncommittedTimer::new(Arc::clone(&committed), Arc::clone(&aborted));

        EventHandler::on_timer(&retry_handler, context, timer, DemandType::Normal).await;

        assert_eq!(handler.call_count(), 1);
        assert!(aborted.load(Ordering::Relaxed));
        assert!(!committed.load(Ordering::Relaxed));
        Ok(())
    }

    // === Cancellation Tests (treats message cancellation as transient) ===

    /// `FallibleHandler::on_message` should continue retrying on cancellation.
    #[tokio::test]
    async fn fallible_on_message_cancellation_retries() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        context.request_cancellation();

        let Some(message) = create_test_message() else {
            bail!("failed to create test message");
        };
        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 4); // 1 initial + 3 retries
        Ok(())
    }

    /// `FallibleHandler::on_timer` should continue retrying on cancellation.
    #[tokio::test]
    async fn fallible_on_timer_cancellation_retries() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        context.request_cancellation();

        let result = FallibleHandler::on_timer(
            &retry_handler,
            context,
            create_test_trigger(),
            DemandType::Normal,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 4); // 1 initial + 3 retries
        Ok(())
    }

    /// `EventHandler::on_message` should continue retrying on cancellation.
    #[tokio::test]
    async fn event_on_message_cancellation_retries() -> Result<()> {
        let handler = MockHandler::failing_then_success(vec![
            ErrorCategory::Transient,
            ErrorCategory::Transient,
        ]);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_cancellation();

        let tracker = create_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let Some(message) = create_test_message() else {
            bail!("failed to create test message");
        };
        let uncommitted_message = message.into_uncommitted(uncommitted_offset);

        EventHandler::on_message(
            &retry_handler,
            context,
            uncommitted_message,
            DemandType::Normal,
        )
        .await;

        assert_eq!(handler.call_count(), 3); // 2 failures + 1 success
        assert_eq!(
            tracker.shutdown().await,
            Some(0),
            "offset should be committed"
        );
        Ok(())
    }

    /// `EventHandler::on_timer` should continue retrying on cancellation.
    #[tokio::test]
    async fn event_on_timer_cancellation_retries() -> Result<()> {
        let handler = MockHandler::failing_then_success(vec![
            ErrorCategory::Transient,
            ErrorCategory::Transient,
        ]);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_cancellation();

        let committed = Arc::new(AtomicBool::new(false));
        let aborted = Arc::new(AtomicBool::new(false));
        let timer = MockUncommittedTimer::new(Arc::clone(&committed), Arc::clone(&aborted));

        EventHandler::on_timer(&retry_handler, context, timer, DemandType::Normal).await;

        assert_eq!(handler.call_count(), 3); // 2 failures + 1 success
        assert!(committed.load(Ordering::Relaxed));
        assert!(!aborted.load(Ordering::Relaxed));
        Ok(())
    }
}
