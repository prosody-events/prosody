//! Handler lifecycle telemetry middleware.
//!
//! Records handler invocation events for observability and monitoring. Captures
//! metrics like execution time, success/failure rates, and error
//! classifications without affecting the processing flow.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. **Record handler invocation** - Log start of processing
//! 2. Pass control to inner middleware layers
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. **Record handler completion** - Log success/failure and timing
//! 3. Pass original result through unchanged
//!
//! # Telemetry Events
//!
//! - **Handler Invoked**: When processing begins
//! - **Handler Succeeded**: When processing completes successfully
//! - **Handler Failed**: When processing fails (with error category)
//! - **Execution Time**: Duration of processing
//! - **Partition Context**: Which topic-partition was processed
//!
//! # Usage
//!
//! Position for comprehensive visibility across the processing pipeline:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::retry::*;
//! # use prosody::consumer::middleware::scheduler::*;
//! # use prosody::consumer::middleware::shutdown::*;
//! # use prosody::consumer::middleware::telemetry::*;
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
//!     .layer(TelemetryMiddleware::new(telemetry)) // Monitor entire pipeline
//!     .layer(ShutdownMiddleware)
//!     .layer(RetryMiddleware::new(retry_config).unwrap())
//!     .into_provider(handler);
//! ```

use tracing::debug;

use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::telemetry::{Telemetry, partition::TelemetryPartitionSender};
use crate::timers::Trigger;
use crate::{Partition, Topic};

/// Middleware that records telemetry events during message processing.
#[derive(Clone, Debug)]
pub struct TelemetryMiddleware {
    telemetry: Telemetry,
}

/// A provider that records telemetry events during message processing.
#[derive(Clone, Debug)]
pub struct TelemetryProvider<T> {
    provider: T,
    telemetry: Telemetry,
}

/// A handler that records telemetry events during message processing.
///
/// Wraps another handler and adds telemetry recording capabilities while
/// preserving the original processing behavior and error handling.
#[derive(Clone, Debug)]
pub struct TelemetryHandler<T> {
    handler: T,
    sender: TelemetryPartitionSender,
}

impl TelemetryMiddleware {
    /// Creates a new `TelemetryMiddleware` with the provided telemetry system.
    ///
    /// # Arguments
    ///
    /// * `telemetry` - The telemetry system for creating partition senders
    #[must_use]
    pub fn new(telemetry: Telemetry) -> Self {
        Self { telemetry }
    }
}

impl HandlerMiddleware for TelemetryMiddleware {
    type Provider<T: FallibleHandlerProvider> = TelemetryProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        TelemetryProvider {
            provider,
            telemetry: self.telemetry.clone(),
        }
    }
}

impl<T> FallibleHandlerProvider for TelemetryProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = TelemetryHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        let partition_sender = self.telemetry.partition_sender(topic, partition);
        TelemetryHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            sender: partition_sender,
        }
    }
}

impl<T> FallibleHandler for TelemetryHandler<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;

    /// Processes a message and records telemetry events for handler lifecycle.
    ///
    /// Records the following events:
    /// - `HandlerInvoked` when the handler is called
    /// - `HandlerSucceeded` when the handler completes successfully
    /// - `HandlerFailed` when the handler returns an error
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed
    /// * `message` - The message to process
    ///
    /// # Errors
    ///
    /// Returns the original error from the wrapped handler
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let key = message.key().clone();

        // Record handler invocation
        self.sender.handler_invoked(key.clone(), demand_type);

        // Process the message with the wrapped handler
        let result = self.handler.on_message(context, message, demand_type).await;

        // Record success or failure
        match &result {
            Ok(()) => self.sender.handler_succeeded(key, demand_type),
            Err(_) => self.sender.handler_failed(key, demand_type),
        }

        result
    }

    /// Processes a timer and records telemetry events for handler lifecycle.
    ///
    /// Records the following events:
    /// - `HandlerInvoked` when the handler is called
    /// - `HandlerSucceeded` when the handler completes successfully
    /// - `HandlerFailed` when the handler returns an error
    ///
    /// # Arguments
    ///
    /// * `context` - The context for timer processing
    /// * `trigger` - The timer trigger to process
    ///
    /// # Errors
    ///
    /// Returns the original error from the wrapped handler
    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let key = trigger.key.clone();

        // Record handler invocation
        self.sender.handler_invoked(key.clone(), demand_type);

        // Process the timer with the wrapped handler
        let result = self.handler.on_timer(context, trigger, demand_type).await;

        // Record success or failure
        match &result {
            Ok(()) => self.sender.handler_succeeded(key, demand_type),
            Err(_) => self.sender.handler_failed(key, demand_type),
        }

        result
    }

    async fn shutdown(self) {
        debug!("shutting down telemetry handler");

        // No telemetry-specific state to clean up (sender is shared)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::message::ConsumerMessage;
    use crate::consumer::middleware::test_support::MockEventContext;
    use crate::consumer::middleware::{ClassifyError, ErrorCategory, FallibleCloneProvider};
    use crate::telemetry::event::{Data, KeyState};
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use chrono::Utc;
    use serde_json::json;
    use std::error::Error;
    use std::fmt::{Display, Formatter, Result as FmtResult};
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

    // === Handler Pass-Through Tests ===

    #[tokio::test]
    async fn success_result_passed_through_unchanged() {
        let telemetry = Telemetry::new();
        let handler = MockHandler::success();
        let telemetry_handler = TelemetryHandler {
            handler: handler.clone(),
            sender: telemetry.partition_sender("test-topic".into(), 0),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = telemetry_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn error_result_passed_through_unchanged() {
        let telemetry = Telemetry::new();
        let handler = MockHandler::failing(ErrorCategory::Permanent);
        let telemetry_handler = TelemetryHandler {
            handler: handler.clone(),
            sender: telemetry.partition_sender("test-topic".into(), 0),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result = telemetry_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn timer_success_passed_through() {
        let telemetry = Telemetry::new();
        let handler = MockHandler::success();
        let telemetry_handler = TelemetryHandler {
            handler: handler.clone(),
            sender: telemetry.partition_sender("test-topic".into(), 0),
        };
        let context = MockEventContext::new();
        let trigger = create_test_trigger();

        let result = telemetry_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn timer_error_passed_through() {
        let telemetry = Telemetry::new();
        let handler = MockHandler::failing(ErrorCategory::Transient);
        let telemetry_handler = TelemetryHandler {
            handler: handler.clone(),
            sender: telemetry.partition_sender("test-topic".into(), 0),
        };
        let context = MockEventContext::new();
        let trigger = create_test_trigger();

        let result = telemetry_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1);
    }

    // === Telemetry Event Recording Tests ===

    #[tokio::test]
    async fn message_success_emits_invoked_and_succeeded_events() {
        let telemetry = Telemetry::new();
        let mut rx = telemetry.subscribe();
        let handler = MockHandler::success();
        let telemetry_handler = TelemetryHandler {
            handler,
            sender: telemetry.partition_sender("test-topic".into(), 0),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let _ = telemetry_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        // Give events time to propagate
        tokio_sleep(Duration::from_millis(10)).await;

        // Collect events
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }

        // Should have invoked and succeeded events
        let has_invoked = events.iter().any(
            |e| matches!(&e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerInvoked)),
        );
        let has_succeeded = events.iter().any(
            |e| matches!(&e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerSucceeded)),
        );

        assert!(has_invoked, "Should emit HandlerInvoked event");
        assert!(has_succeeded, "Should emit HandlerSucceeded event");
    }

    #[tokio::test]
    async fn message_failure_emits_invoked_and_failed_events() {
        let telemetry = Telemetry::new();
        let mut rx = telemetry.subscribe();
        let handler = MockHandler::failing(ErrorCategory::Permanent);
        let telemetry_handler = TelemetryHandler {
            handler,
            sender: telemetry.partition_sender("test-topic".into(), 0),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let _ = telemetry_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        // Give events time to propagate
        tokio_sleep(Duration::from_millis(10)).await;

        // Collect events
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }

        // Should have invoked and failed events
        let has_invoked = events.iter().any(
            |e| matches!(&e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerInvoked)),
        );
        let has_failed = events.iter().any(
            |e| matches!(&e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerFailed)),
        );

        assert!(has_invoked, "Should emit HandlerInvoked event");
        assert!(has_failed, "Should emit HandlerFailed event");
    }

    #[tokio::test]
    async fn timer_success_emits_events() {
        let telemetry = Telemetry::new();
        let mut rx = telemetry.subscribe();
        let handler = MockHandler::success();
        let telemetry_handler = TelemetryHandler {
            handler,
            sender: telemetry.partition_sender("test-topic".into(), 0),
        };
        let context = MockEventContext::new();
        let trigger = create_test_trigger();

        let _ = telemetry_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        tokio_sleep(Duration::from_millis(10)).await;

        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }

        let has_invoked = events.iter().any(
            |e| matches!(&e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerInvoked)),
        );
        let has_succeeded = events.iter().any(
            |e| matches!(&e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerSucceeded)),
        );

        assert!(has_invoked, "Should emit HandlerInvoked event");
        assert!(has_succeeded, "Should emit HandlerSucceeded event");
    }

    // === Middleware Composition Tests ===

    #[test]
    fn middleware_creates_provider() {
        let telemetry = Telemetry::new();
        let middleware = TelemetryMiddleware::new(telemetry);
        let inner_provider = FallibleCloneProvider::new(MockHandler::success());
        let _provider = middleware.with_provider(inner_provider);
        // If this compiles, middleware composition works
    }
}
