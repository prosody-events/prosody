//! Cancellation guard middleware for preventing unnecessary work.
//!
//! Checks if the context is already cancelled before passing control to inner
//! middleware. This prevents starting new work when the message has already
//! been cancelled (shutdown or timeout).
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. **Check cancellation signals** - Return appropriate error if cancelled
//! 2. Pass control to inner middleware layers (if not cancelled)
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. Pass result through unchanged
//!
//! # Cancellation Behavior
//!
//! The middleware distinguishes between two types of cancellation:
//!
//! - **Shutdown** (partition revoked): Returns
//!   [`CancellationGuardError::Shutdown`] classified as
//!   [`ErrorCategory::Terminal`]. Processing must stop immediately to release
//!   the partition.
//!
//! - **Message cancellation**: Returns
//!   [`CancellationGuardError::MessageCancelled`] classified as
//!   [`ErrorCategory::Transient`]. The retry middleware will continue retrying
//!   rather than aborting the message.
//!
//! # Usage
//!
//! Position early in middleware stack to prevent unnecessary processing when
//! already cancelled:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::retry::*;
//! # use prosody::consumer::middleware::scheduler::*;
//! # use prosody::consumer::middleware::cancellation::*;
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
//!     .layer(CancellationGuardMiddleware) // Check cancellation early
//!     .layer(RetryMiddleware::new(retry_config).unwrap())
//!     .into_provider(handler);
//! ```
//!
//! [`ErrorCategory::Terminal`]: crate::consumer::middleware::ErrorCategory::Terminal
//! [`ErrorCategory::Transient`]: crate::consumer::middleware::ErrorCategory::Transient

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

/// Middleware that checks if processing is already cancelled before running the
/// handler, preventing unnecessary work when shutdown or cancellation has
/// occurred.
#[derive(Clone, Copy, Debug)]
pub struct CancellationGuardMiddleware;

/// A provider that wraps handlers with cancellation guard functionality.
#[derive(Clone, Debug)]
pub struct CancellationGuardProvider<T> {
    provider: T,
}

/// Wraps a handler with cancellation guard functionality.
///
/// This struct adds cancellation checks to the wrapped handler's message
/// processing, ensuring that no new messages are processed when the context
/// is already cancelled.
#[derive(Clone, Debug)]
pub struct CancellationGuardHandler<T> {
    handler: T,
}

impl HandlerMiddleware for CancellationGuardMiddleware {
    type Provider<T: FallibleHandlerProvider> = CancellationGuardProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        CancellationGuardProvider { provider }
    }
}

impl<T> FallibleHandlerProvider for CancellationGuardProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = CancellationGuardHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        CancellationGuardHandler {
            handler: self.provider.handler_for_partition(topic, partition),
        }
    }
}

impl<T> FallibleHandler for CancellationGuardHandler<T>
where
    T: FallibleHandler,
{
    type Error = CancellationGuardError<T::Error>;

    /// Processes a message, checking for cancellation conditions.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The message to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or a `CancellationGuardError`.
    ///
    /// # Errors
    ///
    /// - `CancellationGuardError::Shutdown` (Terminal) if partition is revoked
    /// - `CancellationGuardError::MessageCancelled` (Transient) if message was
    ///   cancelled
    /// - `CancellationGuardError::Handler` containing the wrapped handler's
    ///   error
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Check shutdown first (Terminal) - must release partition immediately
        if context.is_shutdown() {
            return Err(CancellationGuardError::Shutdown);
        }
        // Check message cancellation (Transient) - retry will continue
        if context.is_message_cancelled() {
            return Err(CancellationGuardError::MessageCancelled);
        }

        self.handler
            .on_message(context, message, demand_type)
            .await
            .map_err(CancellationGuardError::Handler)
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
        // Check shutdown first (Terminal) - must release partition immediately
        if context.is_shutdown() {
            return Err(CancellationGuardError::Shutdown);
        }
        // Check message cancellation (Transient) - retry will continue
        if context.is_message_cancelled() {
            return Err(CancellationGuardError::MessageCancelled);
        }

        self.handler
            .on_timer(context, timer, demand_type)
            .await
            .map_err(CancellationGuardError::Handler)
    }

    async fn shutdown(self) {
        debug!("shutting down cancellation guard handler");

        // No guard-specific state to clean up (signals are external)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
    }
}

/// Represents errors that can occur during cancellation guard handling.
#[derive(Debug, Error)]
pub enum CancellationGuardError<T> {
    /// Indicates shutdown was requested (partition revoked).
    ///
    /// Classified as [`ErrorCategory::Terminal`] - processing must stop
    /// immediately to release the partition.
    #[error("partition is being revoked")]
    Shutdown,

    /// Indicates message processing was cancelled.
    ///
    /// Classified as [`ErrorCategory::Transient`] - retry middleware will
    /// continue retrying rather than aborting the message.
    #[error("message processing was cancelled")]
    MessageCancelled,

    /// Wraps an error from the underlying handler.
    #[error("handler error: {0:#}")]
    Handler(T),
}

impl<T> ClassifyError for CancellationGuardError<T>
where
    T: ClassifyError,
{
    /// Classifies the cancellation guard error.
    ///
    /// # Returns
    ///
    /// An `ErrorCategory` indicating the nature of the error:
    /// - `ErrorCategory::Terminal` for `CancellationGuardError::Shutdown`
    /// - `ErrorCategory::Transient` for
    ///   `CancellationGuardError::MessageCancelled`
    /// - The classification of the wrapped error for
    ///   `CancellationGuardError::Handler`
    fn classify_error(&self) -> ErrorCategory {
        match self {
            CancellationGuardError::Shutdown => ErrorCategory::Terminal,
            CancellationGuardError::MessageCancelled => ErrorCategory::Transient,
            CancellationGuardError::Handler(error) => error.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::middleware::test_support::MockEventContext;
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use chrono::Utc;
    use serde_json::json;
    use std::error::Error;
    use std::fmt::{Display, Formatter, Result as FmtResult};
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
        let error: CancellationGuardError<TestError> = CancellationGuardError::Shutdown;
        assert!(matches!(error.classify_error(), ErrorCategory::Terminal));
    }

    #[test]
    fn message_cancelled_error_classifies_as_transient() {
        let error: CancellationGuardError<TestError> = CancellationGuardError::MessageCancelled;
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn handler_error_delegates_classification_transient() {
        let error: CancellationGuardError<TestError> =
            CancellationGuardError::Handler(TestError(ErrorCategory::Transient));
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn handler_error_delegates_classification_permanent() {
        let error: CancellationGuardError<TestError> =
            CancellationGuardError::Handler(TestError(ErrorCategory::Permanent));
        assert!(matches!(error.classify_error(), ErrorCategory::Permanent));
    }

    #[tokio::test]
    async fn shutdown_returns_terminal_error() {
        let handler = MockHandler::success();
        let guard_handler = CancellationGuardHandler {
            handler: handler.clone(),
        };
        let context = MockEventContext::new().with_shutdown();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = guard_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(CancellationGuardError::Shutdown)));
        assert!(matches!(
            result.as_ref().err().map(ClassifyError::classify_error),
            Some(ErrorCategory::Terminal)
        ));
        assert_eq!(handler.call_count(), 0, "handler should not be called");
    }

    #[tokio::test]
    async fn message_cancelled_returns_transient_error() {
        let handler = MockHandler::success();
        let guard_handler = CancellationGuardHandler {
            handler: handler.clone(),
        };
        let context = MockEventContext::new();
        context.request_cancellation();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = guard_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(matches!(
            result,
            Err(CancellationGuardError::MessageCancelled)
        ));
        assert!(matches!(
            result.as_ref().err().map(ClassifyError::classify_error),
            Some(ErrorCategory::Transient)
        ));
        assert_eq!(handler.call_count(), 0, "handler should not be called");
    }

    #[tokio::test]
    async fn not_cancelled_passes_through_to_handler() {
        let handler = MockHandler::success();
        let guard_handler = CancellationGuardHandler {
            handler: handler.clone(),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = guard_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1, "handler should be called once");
    }

    #[tokio::test]
    async fn handler_error_wrapped_in_guard_error() {
        let handler = MockHandler::failing(ErrorCategory::Transient);
        let guard_handler = CancellationGuardHandler {
            handler: handler.clone(),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = guard_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(CancellationGuardError::Handler(_))));
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn timer_shutdown_returns_terminal_error() {
        let handler = MockHandler::success();
        let guard_handler = CancellationGuardHandler {
            handler: handler.clone(),
        };
        let context = MockEventContext::new().with_shutdown();
        let trigger = create_test_trigger();

        let result = guard_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(CancellationGuardError::Shutdown)));
        assert_eq!(handler.call_count(), 0);
    }

    #[tokio::test]
    async fn timer_message_cancelled_returns_transient_error() {
        let handler = MockHandler::success();
        let guard_handler = CancellationGuardHandler {
            handler: handler.clone(),
        };
        let context = MockEventContext::new();
        context.request_cancellation();
        let trigger = create_test_trigger();

        let result = guard_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(matches!(
            result,
            Err(CancellationGuardError::MessageCancelled)
        ));
        assert_eq!(handler.call_count(), 0);
    }

    #[tokio::test]
    async fn timer_not_cancelled_passes_through() {
        let handler = MockHandler::success();
        let guard_handler = CancellationGuardHandler {
            handler: handler.clone(),
        };
        let context = MockEventContext::new();
        let trigger = create_test_trigger();

        let result = guard_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1);
    }
}
