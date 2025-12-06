//! Fixed timeout middleware for handler execution.
//!
//! Enforces a fixed timeout on handler invocations to prevent indefinite
//! blocking.
//!
//! # Execution
//!
//! **Request Path:**
//! 1. Race handler execution against configured timeout
//! 2. Return handler result or timeout error
//!
//! # Configuration
//!
//! - `timeout`: Fixed timeout duration (default: 80% of stall threshold)

use std::future::Future;
use std::time::Duration;

use derive_builder::Builder;
use futures::pin_mut;
use thiserror::Error;
use tokio::select;
use tokio::time::sleep;
use validator::{Validate, ValidationErrors};

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::timers::Trigger;
use crate::util::from_option_duration_env;
use crate::{Partition, Topic};

/// Configuration for fixed timeout policy.
#[derive(Builder, Clone, Debug, Validate)]
pub struct TimeoutConfiguration {
    /// Fixed timeout duration for handler execution.
    ///
    /// Environment variable: `PROSODY_TIMEOUT`
    /// Default: 80% of stall threshold (typically 4 minutes when stall
    /// threshold is 5 minutes)
    ///
    /// Set to "none" to use the default (80% of stall threshold).
    #[builder(
        default = "from_option_duration_env(\"PROSODY_TIMEOUT\")?",
        setter(into)
    )]
    pub timeout: Option<Duration>,
}

/// Middleware that applies fixed timeouts to handler execution.
#[derive(Clone, Debug)]
pub struct TimeoutMiddleware {
    timeout: Duration,
}

/// Provider that creates timeout handlers for each partition.
#[derive(Clone, Debug)]
pub struct TimeoutProvider<T> {
    provider: T,
    timeout: Duration,
}

/// Handler wrapper that enforces timeouts on inner handler invocations.
#[derive(Clone, Debug)]
pub struct TimeoutHandler<T> {
    handler: T,
    timeout: Duration,
}

impl<T> TimeoutHandler<T> {
    /// Run an operation with timeout, signaling cancellation if exceeded.
    ///
    /// If the timeout fires before the operation completes, cancellation is
    /// signaled via `context.cancel()` and we continue waiting for the
    /// operation to finish. This ensures the handler has a chance to clean up
    /// before returning its result.
    async fn run_with_timeout<C, F, R, E>(&self, context: C, operation: F) -> Result<R, E>
    where
        C: EventContext,
        F: Future<Output = Result<R, E>>,
    {
        pin_mut!(operation);

        select! {
            result = &mut operation => result,
            () = sleep(self.timeout) => {
                context.cancel();
                operation.await
            }
        }
    }
}

/// Errors that can occur during timeout middleware initialization.
#[derive(Debug, Error)]
pub enum TimeoutInitError {
    /// Configuration validation failed.
    #[error("Invalid configuration: {0:#}")]
    Validation(#[from] ValidationErrors),
}

impl TimeoutConfiguration {
    /// Creates a builder for constructing [`TimeoutConfiguration`].
    #[must_use]
    pub fn builder() -> TimeoutConfigurationBuilder {
        TimeoutConfigurationBuilder::default()
    }
}

impl TimeoutMiddleware {
    /// Creates a new timeout middleware with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the timeout duration
    /// * `stall_threshold` - The stall threshold duration from consumer
    ///   configuration, used to calculate the default timeout (80% of this
    ///   value)
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration validation fails
    pub fn new(
        config: &TimeoutConfiguration,
        stall_threshold: Duration,
    ) -> Result<Self, TimeoutInitError> {
        config.validate()?;
        let timeout = config.timeout.unwrap_or_else(|| stall_threshold * 4 / 5);
        Ok(Self { timeout })
    }
}

impl HandlerMiddleware for TimeoutMiddleware {
    type Provider<T: FallibleHandlerProvider> = TimeoutProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        TimeoutProvider {
            provider,
            timeout: self.timeout,
        }
    }
}

impl<T> FallibleHandlerProvider for TimeoutProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = TimeoutHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        TimeoutHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            timeout: self.timeout,
        }
    }
}

impl<T> FallibleHandler for TimeoutHandler<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        self.run_with_timeout(
            context.clone(),
            self.handler.on_message(context, message, demand_type),
        )
        .await
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
        self.run_with_timeout(
            context.clone(),
            self.handler.on_timer(context, trigger, demand_type),
        )
        .await
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::message::ConsumerMessage;
    use crate::consumer::middleware::{ClassifyError, ErrorCategory};
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use chrono::Utc;
    use futures::stream;
    use serde_json::json;
    use std::convert::Infallible;
    use std::error::Error;
    use std::fmt::{Display, Formatter, Result as FmtResult};
    use std::future::{self, Future};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Semaphore;
    use tokio::task::yield_now;
    use tokio::time::Instant;
    use tracing::Span;

    /// Test error type.
    #[derive(Debug, Clone)]
    struct TestError(&'static str);

    impl Display for TestError {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            write!(f, "test error: {}", self.0)
        }
    }

    impl Error for TestError {}

    impl ClassifyError for TestError {
        fn classify_error(&self) -> ErrorCategory {
            ErrorCategory::Transient
        }
    }

    /// Mock context that satisfies [`EventContext`] and tracks cancellation.
    #[derive(Clone)]
    struct MockContext {
        cancelled: Arc<AtomicBool>,
    }

    impl MockContext {
        fn new() -> Self {
            Self {
                cancelled: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl EventContext for MockContext {
        type Error = Infallible;

        fn should_cancel(&self) -> bool {
            self.cancelled.load(Ordering::Relaxed)
        }

        fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
            future::pending::<()>()
        }

        fn cancel(&self) {
            self.cancelled.store(true, Ordering::Relaxed);
        }

        fn schedule(
            &self,
            _time: CompactDateTime,
            _timer_type: TimerType,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            future::ready(Ok(()))
        }

        fn clear_and_schedule(
            &self,
            _time: CompactDateTime,
            _timer_type: TimerType,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            future::ready(Ok(()))
        }

        fn unschedule(
            &self,
            _time: CompactDateTime,
            _timer_type: TimerType,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            future::ready(Ok(()))
        }

        fn clear_scheduled(
            &self,
            _timer_type: TimerType,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            future::ready(Ok(()))
        }

        fn invalidate(self) {
            self.cancel();
        }

        fn scheduled(
            &self,
            _timer_type: TimerType,
        ) -> impl futures::Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static
        {
            stream::empty()
        }
    }

    /// Mock handler with configurable behavior including delay.
    #[derive(Clone)]
    struct MockHandler {
        call_count: Arc<AtomicUsize>,
        delay: Option<Duration>,
        result: Result<(), TestError>,
    }

    impl MockHandler {
        fn success() -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                delay: None,
                result: Ok(()),
            }
        }

        fn with_delay(delay: Duration) -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                delay: Some(delay),
                result: Ok(()),
            }
        }

        fn failing() -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                delay: None,
                result: Err(TestError("handler failed")),
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::Relaxed)
        }
    }

    impl FallibleHandler for MockHandler {
        type Error = TestError;

        async fn on_message<C>(
            &self,
            context: C,
            _message: ConsumerMessage,
            _demand_type: DemandType,
        ) -> Result<(), Self::Error>
        where
            C: EventContext,
        {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            if let Some(delay) = self.delay {
                // Poll for cancellation during delay to support cooperative cancellation.
                let deadline = Instant::now() + delay;
                while Instant::now() < deadline {
                    if context.should_cancel() {
                        return Err(TestError("cancelled"));
                    }
                    // Yield to allow other tasks (like timeout) to run
                    yield_now().await;
                }
            }
            self.result.clone()
        }

        async fn on_timer<C>(
            &self,
            context: C,
            _trigger: Trigger,
            _demand_type: DemandType,
        ) -> Result<(), Self::Error>
        where
            C: EventContext,
        {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            if let Some(delay) = self.delay {
                // Poll for cancellation during delay to support cooperative cancellation.
                let deadline = Instant::now() + delay;
                while Instant::now() < deadline {
                    if context.should_cancel() {
                        return Err(TestError("cancelled"));
                    }
                    // Yield to allow other tasks (like timeout) to run
                    yield_now().await;
                }
            }
            self.result.clone()
        }

        async fn shutdown(self) {}
    }

    fn create_test_message() -> Option<ConsumerMessage> {
        let semaphore = Arc::new(Semaphore::new(10));
        let permit = semaphore.try_acquire_owned().ok()?;
        Some(ConsumerMessage::new(
            None,
            "test-topic".into(),
            0,
            0,
            "test-key".into(),
            Utc::now(),
            json!({}),
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

    #[tokio::test]
    async fn handler_completes_before_timeout_returns_ok() {
        let handler = MockHandler::success();
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_secs(10),
        };
        let context = MockContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = timeout_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn handler_completes_before_timeout_returns_handler_error() {
        let handler = MockHandler::failing();
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_secs(10),
        };
        let context = MockContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = timeout_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn handler_exceeds_timeout_signals_cancellation_and_waits() {
        // Handler takes 100ms but timeout is 10ms
        // After timeout, cancellation is signaled and we wait for handler
        let handler = MockHandler::with_delay(Duration::from_millis(100));
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_millis(10),
        };
        let context = MockContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = timeout_handler
            .on_message(context.clone(), message, DemandType::Normal)
            .await;

        // Handler should return error after seeing cancellation
        assert!(result.is_err());
        // Handler was invoked and responded to cancellation
        assert_eq!(handler.call_count(), 1);
        // Context should be marked as cancelled
        assert!(context.should_cancel());
    }

    #[tokio::test]
    async fn timer_handler_completes_before_timeout_returns_ok() {
        let handler = MockHandler::success();
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_secs(10),
        };
        let context = MockContext::new();
        let trigger = create_test_trigger();

        let result = timeout_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn timer_handler_exceeds_timeout_signals_cancellation_and_waits() {
        // Timer handler takes 100ms but timeout is 10ms
        let handler = MockHandler::with_delay(Duration::from_millis(100));
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_millis(10),
        };
        let context = MockContext::new();
        let trigger = create_test_trigger();

        let result = timeout_handler
            .on_timer(context.clone(), trigger, DemandType::Normal)
            .await;

        // Handler should return error after seeing cancellation
        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1);
        assert!(context.should_cancel());
    }

    #[tokio::test]
    async fn timer_handler_error_passed_through() {
        let handler = MockHandler::failing();
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_secs(10),
        };
        let context = MockContext::new();
        let trigger = create_test_trigger();

        let result = timeout_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1);
    }
}
