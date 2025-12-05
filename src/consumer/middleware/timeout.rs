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

use std::time::Duration;

use derive_builder::Builder;
use thiserror::Error;
use tokio::select;
use tokio::time::sleep;
use validator::{Validate, ValidationErrors};

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
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

/// Errors that can occur during timeout handling.
#[derive(Debug, Error)]
pub enum TimeoutError<E> {
    /// The inner handler returned an error.
    #[error(transparent)]
    Handler(E),

    /// The operation timed out.
    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),
}

/// Errors that can occur during timeout middleware initialization.
#[derive(Debug, Error)]
pub enum TimeoutInitError {
    /// Configuration validation failed.
    #[error("Invalid configuration: {0:#}")]
    Validation(#[from] ValidationErrors),
}

impl<E> ClassifyError for TimeoutError<E>
where
    E: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            TimeoutError::Handler(error) => error.classify_error(),
            TimeoutError::Timeout(_) => ErrorCategory::Transient,
        }
    }
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
    type Error = TimeoutError<T::Error>;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        select! {
            result = self.handler.on_message(context, message, demand_type) => {
                result.map_err(TimeoutError::Handler)
            }
            () = sleep(self.timeout) => {
                Err(TimeoutError::Timeout(self.timeout))
            }
        }
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
        select! {
            result = self.handler.on_timer(context, trigger, demand_type) => {
                result.map_err(TimeoutError::Handler)
            }
            () = sleep(self.timeout) => {
                Err(TimeoutError::Timeout(self.timeout))
            }
        }
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::message::ConsumerMessage;
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Semaphore;
    use tokio::time::sleep as tokio_sleep;
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

    /// Mock context that satisfies [`EventContext`].
    #[derive(Clone)]
    struct MockContext;

    impl EventContext for MockContext {
        type Error = Infallible;

        fn should_shutdown(&self) -> bool {
            false
        }

        fn should_cancel(&self) -> bool {
            false
        }

        fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
            future::pending::<()>()
        }

        fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
            future::pending::<()>()
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

        fn invalidate(self) {}

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

        fn failing(category: ErrorCategory) -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                delay: None,
                result: Err(TestError(category)),
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    impl FallibleHandler for MockHandler {
        type Error = TestError;

        async fn on_message<C>(
            &self,
            _context: C,
            _message: ConsumerMessage,
            _demand_type: DemandType,
        ) -> Result<(), Self::Error>
        where
            C: EventContext,
        {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            if let Some(delay) = self.delay {
                tokio_sleep(delay).await;
            }
            self.result.clone()
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
            self.call_count.fetch_add(1, Ordering::SeqCst);
            if let Some(delay) = self.delay {
                tokio_sleep(delay).await;
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

    #[test]
    fn timeout_error_classifies_as_transient() {
        let error: TimeoutError<TestError> = TimeoutError::Timeout(Duration::from_secs(10));
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn handler_error_delegates_classification_transient() {
        let error: TimeoutError<TestError> =
            TimeoutError::Handler(TestError(ErrorCategory::Transient));
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn handler_error_delegates_classification_permanent() {
        let error: TimeoutError<TestError> =
            TimeoutError::Handler(TestError(ErrorCategory::Permanent));
        assert!(matches!(error.classify_error(), ErrorCategory::Permanent));
    }

    #[test]
    fn handler_error_delegates_classification_terminal() {
        let error: TimeoutError<TestError> =
            TimeoutError::Handler(TestError(ErrorCategory::Terminal));
        assert!(matches!(error.classify_error(), ErrorCategory::Terminal));
    }

    #[tokio::test]
    async fn handler_completes_before_timeout_returns_ok() {
        let handler = MockHandler::success();
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_secs(10),
        };
        let context = MockContext;
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
        let handler = MockHandler::failing(ErrorCategory::Permanent);
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_secs(10),
        };
        let context = MockContext;
        let Some(message) = create_test_message() else {
            return;
        };

        let result = timeout_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(TimeoutError::Handler(_))));
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn handler_exceeds_timeout_returns_timeout_error() {
        // Handler takes 100ms but timeout is 10ms
        let handler = MockHandler::with_delay(Duration::from_millis(100));
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_millis(10),
        };
        let context = MockContext;
        let Some(message) = create_test_message() else {
            return;
        };

        let result = timeout_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(TimeoutError::Timeout(_))));
        // Handler was invoked (started running)
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn timer_handler_completes_before_timeout_returns_ok() {
        let handler = MockHandler::success();
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_secs(10),
        };
        let context = MockContext;
        let trigger = create_test_trigger();

        let result = timeout_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn timer_handler_exceeds_timeout_returns_timeout_error() {
        let handler = MockHandler::with_delay(Duration::from_millis(100));
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_millis(10),
        };
        let context = MockContext;
        let trigger = create_test_trigger();

        let result = timeout_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(TimeoutError::Timeout(_))));
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn timer_handler_error_wrapped_correctly() {
        let handler = MockHandler::failing(ErrorCategory::Transient);
        let timeout_handler = TimeoutHandler {
            handler: handler.clone(),
            timeout: Duration::from_secs(10),
        };
        let context = MockContext;
        let trigger = create_test_trigger();

        let result = timeout_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(TimeoutError::Handler(_))));
        if let Err(TimeoutError::Handler(inner)) = result {
            assert!(matches!(inner.classify_error(), ErrorCategory::Transient));
        }
    }

    #[test]
    fn timeout_error_message_contains_duration() {
        let duration = Duration::from_secs(30);
        let error: TimeoutError<TestError> = TimeoutError::Timeout(duration);
        let message = error.to_string();
        assert!(
            message.contains("30"),
            "Error message should contain duration: {message}"
        );
    }
}
