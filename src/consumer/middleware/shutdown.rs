//! Graceful shutdown middleware for partition revocation.
//!
//! Monitors partition revocation signals and immediately stops processing new
//! messages when a partition is being revoked. Returns terminal errors to abort
//! processing gracefully.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. **Check shutdown signal** - Return terminal error if partition being
//!    revoked
//! 2. Pass control to inner middleware layers (if not shutting down)
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. Pass result through unchanged
//!
//! # Shutdown Behavior
//!
//! - **Signal Detection**: Monitors [`EventContext::is_shutdown_requested`]
//! - **Immediate Stop**: Returns [`ShutdownError`] when revocation detected
//! - **Terminal Classification**: Error classified as
//!   [`ErrorCategory::Terminal`]
//! - **Graceful Abort**: Allows in-flight operations to complete
//!
//! # Usage
//!
//! Position early in middleware stack to prevent unnecessary processing during
//! shutdown:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::retry::*;
//! # use prosody::consumer::middleware::scheduler::*;
//! # use prosody::consumer::middleware::shutdown::*;
//! # use prosody::consumer::DemandType;
//! # use prosody::consumer::event_context::EventContext;
//! # use prosody::consumer::message::ConsumerMessage;
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
//! # let telemetry = Telemetry::default();
//! # let handler = MyHandler;
//!
//! let provider = SchedulerMiddleware::new(&config, &telemetry).unwrap()
//!     .layer(ShutdownMiddleware) // Check shutdown early
//!     .layer(RetryMiddleware::new(retry_config).unwrap())
//!     .into_provider(handler);
//! ```
//!
//! [`EventContext::is_shutdown_requested`]: crate::consumer::event_context::EventContext
//! [`ErrorCategory::Terminal`]: crate::consumer::middleware::ErrorCategory::Terminal

use thiserror::Error;
use tracing::debug;

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::timers::Trigger;
use crate::{Partition, Topic};

/// Middleware that checks if the partition is shutting down before running the
/// handler, preventing other middleware from delaying the shutdown.
#[derive(Clone, Copy, Debug)]
pub struct ShutdownMiddleware;

/// A provider that wraps handlers with shutdown functionality.
#[derive(Clone, Debug)]
pub struct ShutdownProvider<T> {
    provider: T,
}

/// Wraps a handler with shutdown functionality.
///
/// This struct adds shutdown checks to the wrapped handler's message
/// processing, ensuring that no new messages are processed when a
/// partition is being revoked.
#[derive(Clone, Debug)]
pub struct ShutdownHandler<T> {
    handler: T,
}

impl HandlerMiddleware for ShutdownMiddleware {
    type Provider<T: FallibleHandlerProvider> = ShutdownProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        ShutdownProvider { provider }
    }
}

impl<T> FallibleHandlerProvider for ShutdownProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = ShutdownHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        ShutdownHandler {
            handler: self.provider.handler_for_partition(topic, partition),
        }
    }
}

impl<T> FallibleHandler for ShutdownHandler<T>
where
    T: FallibleHandler,
{
    type Error = ShutdownError<T::Error>;

    /// Processes a message, checking for shutdown conditions.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The message to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or a `ShutdownError`.
    ///
    /// # Errors
    ///
    /// Returns a `ShutdownError::Shutdown` if the partition is being revoked,
    /// or a `ShutdownError::Handler` containing the wrapped handler's error.
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        if context.should_cancel() {
            return Err(ShutdownError::Shutdown);
        }

        self.handler
            .on_message(context, message, demand_type)
            .await
            .map_err(ShutdownError::Handler)
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
        if context.should_cancel() {
            return Err(ShutdownError::Shutdown);
        }
        self.handler
            .on_timer(context, timer, demand_type)
            .await
            .map_err(ShutdownError::Handler)
    }

    async fn shutdown(self) {
        debug!("shutting down shutdown middleware handler");

        // No shutdown-specific state to clean up (signals are external)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
    }
}

/// Represents errors that can occur during shutdown handling.
#[derive(Debug, Error)]
pub enum ShutdownError<T> {
    /// Indicates that the partition is being revoked.
    #[error("partition is being revoked")]
    Shutdown,

    /// Wraps an error from the underlying handler.
    #[error("handler error: {0:#}")]
    Handler(T),
}

impl<T> ClassifyError for ShutdownError<T>
where
    T: ClassifyError,
{
    /// Classifies the shutdown error.
    ///
    /// # Returns
    ///
    /// An `ErrorCategory` indicating the nature of the error:
    /// - `ErrorCategory::Terminal` for `ShutdownError::Shutdown`
    /// - The classification of the wrapped error for `ShutdownError::Handler`
    fn classify_error(&self) -> ErrorCategory {
        match self {
            ShutdownError::Shutdown => ErrorCategory::Terminal,
            ShutdownError::Handler(error) => error.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    use tokio::sync::Semaphore;
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

    /// Mock context with configurable shutdown state.
    #[derive(Clone)]
    struct MockContext {
        shutdown: bool,
    }

    impl MockContext {
        fn new(shutdown: bool) -> Self {
            Self { shutdown }
        }
    }

    impl EventContext for MockContext {
        type Error = Infallible;

        fn should_cancel(&self) -> bool {
            self.shutdown
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

        fn cancel(&self) {}

        fn invalidate(self) {}

        fn scheduled(
            &self,
            _timer_type: TimerType,
        ) -> impl futures::Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static
        {
            stream::empty()
        }
    }

    /// Mock handler with configurable behavior.
    #[derive(Clone)]
    struct MockHandler {
        call_count: Arc<AtomicUsize>,
        result: Result<(), TestError>,
    }

    impl MockHandler {
        fn success() -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                result: Ok(()),
            }
        }

        fn failing(category: ErrorCategory) -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
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
    fn shutdown_error_classifies_as_terminal() {
        let error: ShutdownError<TestError> = ShutdownError::Shutdown;
        assert!(matches!(error.classify_error(), ErrorCategory::Terminal));
    }

    #[test]
    fn handler_error_delegates_classification_transient() {
        let error: ShutdownError<TestError> =
            ShutdownError::Handler(TestError(ErrorCategory::Transient));
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn handler_error_delegates_classification_permanent() {
        let error: ShutdownError<TestError> =
            ShutdownError::Handler(TestError(ErrorCategory::Permanent));
        assert!(matches!(error.classify_error(), ErrorCategory::Permanent));
    }

    #[tokio::test]
    async fn shutdown_signal_returns_shutdown_error() {
        let handler = MockHandler::success();
        let shutdown_handler = ShutdownHandler {
            handler: handler.clone(),
        };
        let context = MockContext::new(true); // shutdown = true
        let Some(message) = create_test_message() else {
            return;
        };

        let result = shutdown_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(ShutdownError::Shutdown)));
        assert_eq!(handler.call_count(), 0, "handler should not be called");
    }

    #[tokio::test]
    async fn no_shutdown_passes_through_to_handler() {
        let handler = MockHandler::success();
        let shutdown_handler = ShutdownHandler {
            handler: handler.clone(),
        };
        let context = MockContext::new(false); // shutdown = false
        let Some(message) = create_test_message() else {
            return;
        };

        let result = shutdown_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1, "handler should be called once");
    }

    #[tokio::test]
    async fn handler_error_wrapped_in_shutdown_error() {
        let handler = MockHandler::failing(ErrorCategory::Transient);
        let shutdown_handler = ShutdownHandler {
            handler: handler.clone(),
        };
        let context = MockContext::new(false);
        let Some(message) = create_test_message() else {
            return;
        };

        let result = shutdown_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(ShutdownError::Handler(_))));
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn timer_shutdown_signal_returns_shutdown_error() {
        let handler = MockHandler::success();
        let shutdown_handler = ShutdownHandler {
            handler: handler.clone(),
        };
        let context = MockContext::new(true);
        let trigger = create_test_trigger();

        let result = shutdown_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(ShutdownError::Shutdown)));
        assert_eq!(handler.call_count(), 0);
    }

    #[tokio::test]
    async fn timer_no_shutdown_passes_through() {
        let handler = MockHandler::success();
        let shutdown_handler = ShutdownHandler {
            handler: handler.clone(),
        };
        let context = MockContext::new(false);
        let trigger = create_test_trigger();

        let result = shutdown_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1);
    }
}
