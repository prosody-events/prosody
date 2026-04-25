//! Cancellation middleware for early exit when already cancelled.
//!
//! Checks cancellation state before invoking inner middleware. Prevents
//! starting new work when shutdown or cancellation has already been signaled.
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
//! - **Shutdown** (partition revoked): Returns [`CancellationError::Shutdown`]
//!   classified as [`ErrorCategory::Terminal`]. Processing must stop
//!   immediately to release the partition.
//!
//! - **Message cancellation**: Returns [`CancellationError::MessageCancelled`]
//!   classified as [`ErrorCategory::Transient`]. The retry middleware will
//!   continue retrying rather than aborting the message.
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
//! #     type Output = ();
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
//!     .layer(CancellationMiddleware)
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

/// Middleware that checks cancellation state before invoking the handler.
#[derive(Clone, Copy, Debug)]
pub struct CancellationMiddleware;

/// Provider that wraps handlers with cancellation checks.
#[derive(Clone, Debug)]
pub struct CancellationProvider<T> {
    provider: T,
}

/// Handler wrapper that checks cancellation before delegating.
#[derive(Clone, Debug)]
pub struct CancellationHandler<T> {
    handler: T,
}

impl<T> CancellationHandler<T> {
    pub(crate) fn new(handler: T) -> Self {
        Self { handler }
    }
}

impl HandlerMiddleware for CancellationMiddleware {
    type Provider<T: FallibleHandlerProvider> = CancellationProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        CancellationProvider { provider }
    }
}

impl<T> FallibleHandlerProvider for CancellationProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = CancellationHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        CancellationHandler::new(self.provider.handler_for_partition(topic, partition))
    }
}

impl<T> FallibleHandler for CancellationHandler<T>
where
    T: FallibleHandler,
{
    type Error = CancellationError<T::Error>;
    type Output = T::Output;

    /// Checks cancellation state, then delegates to inner handler if clear.
    ///
    /// Post-call: Transient errors are promoted to
    /// [`CancellationError::Shutdown`] (Terminal) if shutdown is active
    /// when the handler returns, ensuring outer middleware layers treat the
    /// failure as unrecoverable rather than retrying or deferring.
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        if context.is_shutdown() {
            return Err(CancellationError::Shutdown);
        }
        if context.is_message_cancelled() {
            return Err(CancellationError::MessageCancelled);
        }

        self.handler
            .on_message(context.clone(), message, demand_type)
            .await
            .map_err(|error| {
                if context.is_shutdown()
                    && matches!(error.classify_error(), ErrorCategory::Transient)
                {
                    CancellationError::Shutdown
                } else {
                    CancellationError::Handler(error)
                }
            })
    }

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        if context.is_shutdown() {
            return Err(CancellationError::Shutdown);
        }
        if context.is_message_cancelled() {
            return Err(CancellationError::MessageCancelled);
        }

        self.handler
            .on_timer(context.clone(), timer, demand_type)
            .await
            .map_err(|error| {
                if context.is_shutdown()
                    && matches!(error.classify_error(), ErrorCategory::Transient)
                {
                    CancellationError::Shutdown
                } else {
                    CancellationError::Handler(error)
                }
            })
    }

    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        match result {
            Ok(output) => self.handler.after_commit(context, Ok(output)).await,
            Err(CancellationError::Handler(inner)) => {
                self.handler.after_commit(context, Err(inner)).await;
            }
            // Cancellation/Shutdown originated at this layer; the inner
            // handler did not see them, so there is no inner-typed error to
            // forward.
            Err(CancellationError::Shutdown | CancellationError::MessageCancelled) => {}
        }
    }

    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        match result {
            Ok(output) => self.handler.after_abort(context, Ok(output)).await,
            Err(CancellationError::Handler(inner)) => {
                self.handler.after_abort(context, Err(inner)).await;
            }
            // Cancellation/Shutdown originated here; nothing to forward.
            Err(CancellationError::Shutdown | CancellationError::MessageCancelled) => {}
        }
    }

    async fn shutdown(self) {
        debug!("shutting down cancellation handler");
        self.handler.shutdown().await;
    }
}

/// Errors from the cancellation middleware.
#[derive(Debug, Error)]
pub enum CancellationError<T> {
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

impl<T> ClassifyError for CancellationError<T>
where
    T: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            CancellationError::Shutdown => ErrorCategory::Terminal,
            CancellationError::MessageCancelled => ErrorCategory::Transient,
            CancellationError::Handler(error) => error.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::message::ConsumerMessageValue;
    use crate::consumer::middleware::test_support::MockEventContext;
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
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
        type Output = ();

        async fn on_message<C>(
            &self,
            _context: C,
            _message: ConsumerMessage,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
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
        ) -> Result<Self::Output, Self::Error>
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

    #[test]
    fn shutdown_error_classifies_as_terminal() {
        let error: CancellationError<TestError> = CancellationError::Shutdown;
        assert!(matches!(error.classify_error(), ErrorCategory::Terminal));
    }

    #[test]
    fn message_cancelled_error_classifies_as_transient() {
        let error: CancellationError<TestError> = CancellationError::MessageCancelled;
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn handler_error_delegates_classification_transient() {
        let error: CancellationError<TestError> =
            CancellationError::Handler(TestError(ErrorCategory::Transient));
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn handler_error_delegates_classification_permanent() {
        let error: CancellationError<TestError> =
            CancellationError::Handler(TestError(ErrorCategory::Permanent));
        assert!(matches!(error.classify_error(), ErrorCategory::Permanent));
    }

    #[tokio::test]
    async fn shutdown_returns_terminal_error() {
        let handler = MockHandler::success();
        let guard_handler = CancellationHandler::new(handler.clone());
        let context = MockEventContext::new().with_shutdown();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = guard_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(CancellationError::Shutdown)));
        assert!(matches!(
            result.as_ref().err().map(ClassifyError::classify_error),
            Some(ErrorCategory::Terminal)
        ));
        assert_eq!(handler.call_count(), 0, "handler should not be called");
    }

    #[tokio::test]
    async fn message_cancelled_returns_transient_error() {
        let handler = MockHandler::success();
        let guard_handler = CancellationHandler::new(handler.clone());
        let context = MockEventContext::new();
        context.request_cancellation();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = guard_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(CancellationError::MessageCancelled)));
        assert!(matches!(
            result.as_ref().err().map(ClassifyError::classify_error),
            Some(ErrorCategory::Transient)
        ));
        assert_eq!(handler.call_count(), 0, "handler should not be called");
    }

    #[tokio::test]
    async fn not_cancelled_passes_through_to_handler() {
        let handler = MockHandler::success();
        let guard_handler = CancellationHandler::new(handler.clone());
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
        let guard_handler = CancellationHandler::new(handler.clone());
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = guard_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(CancellationError::Handler(_))));
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn timer_shutdown_returns_terminal_error() {
        let handler = MockHandler::success();
        let guard_handler = CancellationHandler::new(handler.clone());
        let context = MockEventContext::new().with_shutdown();
        let trigger = create_test_trigger();

        let result = guard_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(CancellationError::Shutdown)));
        assert_eq!(handler.call_count(), 0);
    }

    #[tokio::test]
    async fn timer_message_cancelled_returns_transient_error() {
        let handler = MockHandler::success();
        let guard_handler = CancellationHandler::new(handler.clone());
        let context = MockEventContext::new();
        context.request_cancellation();
        let trigger = create_test_trigger();

        let result = guard_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(CancellationError::MessageCancelled)));
        assert_eq!(handler.call_count(), 0);
    }

    #[tokio::test]
    async fn timer_not_cancelled_passes_through() {
        let handler = MockHandler::success();
        let guard_handler = CancellationHandler::new(handler.clone());
        let context = MockEventContext::new();
        let trigger = create_test_trigger();

        let result = guard_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1);
    }

    /// Mock handler that triggers shutdown mid-execution and returns a
    /// configurable result. Used to simulate a handler that fails while
    /// shutdown is concurrently signaled.
    #[derive(Clone)]
    struct ShutdownTriggerHandler {
        ctx: MockEventContext,
        result: Result<(), TestError>,
    }

    impl ShutdownTriggerHandler {
        fn new(ctx: MockEventContext, result: Result<(), TestError>) -> Self {
            Self { ctx, result }
        }
    }

    impl FallibleHandler for ShutdownTriggerHandler {
        type Error = TestError;
        type Output = ();

        async fn on_message<C>(
            &self,
            _context: C,
            _message: ConsumerMessage,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext,
        {
            self.ctx.request_shutdown();
            self.result.clone()
        }

        async fn on_timer<C>(
            &self,
            _context: C,
            _trigger: Trigger,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext,
        {
            self.ctx.request_shutdown();
            self.result.clone()
        }

        async fn shutdown(self) {}
    }

    #[tokio::test]
    async fn shutdown_during_message_converts_transient_to_terminal() {
        let context = MockEventContext::new();
        let handler =
            ShutdownTriggerHandler::new(context.clone(), Err(TestError(ErrorCategory::Transient)));
        let guard_handler = CancellationHandler::new(handler);
        let Some(message) = create_test_message() else {
            return;
        };

        let result = guard_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(CancellationError::Shutdown)));
        assert!(matches!(
            result.as_ref().err().map(ClassifyError::classify_error),
            Some(ErrorCategory::Terminal)
        ));
    }

    #[tokio::test]
    async fn shutdown_during_message_preserves_non_transient_error() {
        let context = MockEventContext::new();
        let handler =
            ShutdownTriggerHandler::new(context.clone(), Err(TestError(ErrorCategory::Permanent)));
        let guard_handler = CancellationHandler::new(handler);
        let Some(message) = create_test_message() else {
            return;
        };

        let result = guard_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        // Permanent errors are NOT promoted to Shutdown even during shutdown
        assert!(matches!(result, Err(CancellationError::Handler(_))));
        assert!(matches!(
            result.as_ref().err().map(ClassifyError::classify_error),
            Some(ErrorCategory::Permanent)
        ));
    }

    #[tokio::test]
    async fn shutdown_during_timer_converts_transient_to_terminal() {
        let context = MockEventContext::new();
        let handler =
            ShutdownTriggerHandler::new(context.clone(), Err(TestError(ErrorCategory::Transient)));
        let guard_handler = CancellationHandler::new(handler);
        let trigger = create_test_trigger();

        let result = guard_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(matches!(result, Err(CancellationError::Shutdown)));
        assert!(matches!(
            result.as_ref().err().map(ClassifyError::classify_error),
            Some(ErrorCategory::Terminal)
        ));
    }

    #[tokio::test]
    async fn shutdown_during_timer_preserves_non_transient_error() {
        let context = MockEventContext::new();
        let handler =
            ShutdownTriggerHandler::new(context.clone(), Err(TestError(ErrorCategory::Permanent)));
        let guard_handler = CancellationHandler::new(handler);
        let trigger = create_test_trigger();

        let result = guard_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        // Permanent errors are NOT promoted to Shutdown even during shutdown
        assert!(matches!(result, Err(CancellationError::Handler(_))));
        assert!(matches!(
            result.as_ref().err().map(ClassifyError::classify_error),
            Some(ErrorCategory::Permanent)
        ));
    }
}
