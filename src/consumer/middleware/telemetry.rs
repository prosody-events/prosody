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
//! 2. **Record handler completion** - Log success/failure and timing of the
//!    handler invocation as observed at this layer
//! 3. Pass original result through unchanged
//!
//! Note: the `Succeeded`/`Failed` events recorded here reflect the outcome of
//! the wrapped `on_message` / `on_timer` invocation as it returns to this
//! layer. They are work-level signals only; they do NOT indicate whether the
//! framework will treat the dispatch as final (commit) or non-final (retry of
//! the same logical event). The framework's `FallibleHandler` invariant
//! guarantees that for every `on_message`/`on_timer` call that runs and
//! returns, exactly one of `after_commit` or `after_abort` will subsequently
//! fire on the same handler instance — `after_commit` marking the dispatch as
//! final, `after_abort` marking it as non-final with a retry to come.
//! `TelemetryHandler` forwards both apply hooks verbatim to the inner handler
//! and does not currently emit dedicated events distinguishing the two. The
//! inner is invoked at most once per call; per-invocation invariant trivially
//! upheld.
//!
//! # Telemetry Events
//!
//! - **Handler Invoked**: When processing begins
//! - **Handler Succeeded**: When the handler invocation returns `Ok` at this
//!   layer (work-level outcome; not a commit/abort signal)
//! - **Handler Failed**: When the handler invocation returns `Err` at this
//!   layer, with error category (work-level outcome; not a commit/abort signal)
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
//! # use prosody::consumer::middleware::cancellation::CancellationMiddleware;
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
//!     .layer(TelemetryMiddleware::new(telemetry, std::sync::Arc::from("my-group"))) // Monitor entire pipeline
//!     .layer(CancellationMiddleware)
//!     .layer(RetryMiddleware::new(retry_config).unwrap())
//!     .into_provider(handler);
//! ```

use tracing::debug;

use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::error::ClassifyError;
use crate::telemetry::event::TimerEventType;
use crate::telemetry::{Telemetry, partition::TelemetryPartitionSender};
use crate::timers::Trigger;
use crate::{Partition, Topic};
use std::sync::Arc;

/// Middleware that records telemetry events during message processing.
#[derive(Clone, Debug)]
pub struct TelemetryMiddleware {
    telemetry: Telemetry,
    source: Arc<str>,
}

/// A provider that records telemetry events during message processing.
#[derive(Clone, Debug)]
pub struct TelemetryProvider<T> {
    provider: T,
    telemetry: Telemetry,
    source: Arc<str>,
}

/// A handler that records telemetry events during message processing.
///
/// Wraps another handler and adds telemetry recording capabilities while
/// preserving the original processing behavior and error handling.
#[derive(Clone, Debug)]
pub struct TelemetryHandler<T> {
    handler: T,
    sender: TelemetryPartitionSender,
    source: Arc<str>,
}

impl TelemetryMiddleware {
    /// Creates a new `TelemetryMiddleware` with the provided telemetry system.
    ///
    /// # Arguments
    ///
    /// * `telemetry` - The telemetry system for creating partition senders
    /// * `source` - The consumer `group_id` used as source identifier in events
    #[must_use]
    pub fn new(telemetry: Telemetry, source: Arc<str>) -> Self {
        Self { telemetry, source }
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
            source: self.source.clone(),
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
            source: self.source.clone(),
        }
    }
}

impl<T> FallibleHandler for TelemetryHandler<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;
    type Output = T::Output;

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
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        let key = message.key().clone();
        let offset = message.offset();

        // Record handler invocation
        self.sender.handler_invoked(key.clone(), demand_type);

        // Emit message dispatched
        self.sender
            .message_dispatched(key.clone(), offset, demand_type, self.source.clone());

        // Process the message with the wrapped handler
        let result = self.handler.on_message(context, message, demand_type).await;

        // Record success or failure
        match &result {
            Ok(_) => {
                self.sender.handler_succeeded(key.clone(), demand_type);
                self.sender
                    .message_succeeded(key, offset, demand_type, self.source.clone());
            }
            Err(e) => {
                self.sender.handler_failed(key.clone(), demand_type);
                self.sender.message_failed(
                    key,
                    offset,
                    demand_type,
                    self.source.clone(),
                    e.classify_error(),
                    format!("{e:?}").into_boxed_str(),
                );
            }
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
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        let key = trigger.key.clone();
        let scheduled_time = trigger.time;
        let timer_type = trigger.timer_type;

        // Record handler invocation
        self.sender.handler_invoked(key.clone(), demand_type);

        // Emit timer dispatched
        self.sender.timer_dispatched(
            key.clone(),
            scheduled_time,
            timer_type,
            demand_type,
            self.source.clone(),
        );

        // Process the timer with the wrapped handler
        let result = self.handler.on_timer(context, trigger, demand_type).await;

        // Record success or failure
        match &result {
            Ok(_) => {
                self.sender.handler_succeeded(key.clone(), demand_type);
                self.sender.timer_succeeded(
                    key,
                    scheduled_time,
                    timer_type,
                    demand_type,
                    self.source.clone(),
                );
            }
            Err(e) => {
                self.sender.handler_failed(key.clone(), demand_type);
                self.sender.emit_timer(
                    TimerEventType::Failed {
                        demand_type,
                        error_category: e.classify_error(),
                        exception: format!("{e:?}").into_boxed_str(),
                    },
                    key,
                    scheduled_time,
                    timer_type,
                    self.source.clone(),
                );
            }
        }

        result
    }

    /// Pass-through forwarder for the framework's terminal apply hook.
    ///
    /// `TelemetryHandler` is a pure pass-through middleware
    /// (`Output = T::Output`, `Error = T::Error`) and therefore cannot change
    /// the dispatch's final outcome. The framework calls this hook to mark
    /// the dispatch as FINAL: no retry of the same logical event will follow
    /// through this consumer. Per the `FallibleHandler` invariant, exactly
    /// one of `after_commit` or `after_abort` fires for each `on_message` /
    /// `on_timer` call that runs and returns; this layer simply forwards the
    /// hook verbatim so the inner handler observes the framework-level
    /// commit/abort decision unchanged.
    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        self.handler.after_commit(context, result).await;
    }

    /// Pass-through forwarder for the framework's non-terminal apply hook.
    ///
    /// `TelemetryHandler` is a pure pass-through middleware
    /// (`Output = T::Output`, `Error = T::Error`) and therefore cannot change
    /// the dispatch's final outcome. The framework calls this hook to mark
    /// the dispatch as NOT final: a retry of the same logical event is coming
    /// through this consumer. Per the `FallibleHandler` invariant, exactly
    /// one of `after_commit` or `after_abort` fires for each `on_message` /
    /// `on_timer` call that runs and returns; this layer simply forwards the
    /// hook verbatim so the inner handler observes the framework-level
    /// commit/abort decision unchanged.
    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        self.handler.after_abort(context, result).await;
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
    use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
    use crate::consumer::middleware::FallibleCloneProvider;
    use crate::consumer::middleware::test_support::MockEventContext;
    use crate::error::{ClassifyError, ErrorCategory};
    use crate::telemetry::event::{
        Data, KeyState, MessageEventType, MessageTelemetryEvent, TelemetryEvent, TimerEventType,
        TimerTelemetryEvent,
    };
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use std::error::Error;
    use std::fmt::{Display, Formatter, Result as FmtResult};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Semaphore;
    use tokio::sync::broadcast;
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

    // === Handler Pass-Through Tests ===

    #[tokio::test]
    async fn success_result_passed_through_unchanged() {
        let telemetry = Telemetry::new();
        let handler = MockHandler::success();
        let telemetry_handler = TelemetryHandler {
            handler: handler.clone(),
            sender: telemetry.partition_sender("test-topic".into(), 0),
            source: Arc::from("test-group"),
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
            source: Arc::from("test-group"),
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
            source: Arc::from("test-group"),
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
            source: Arc::from("test-group"),
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
            source: Arc::from("test-group"),
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
            |e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerInvoked)),
        );
        let has_succeeded = events.iter().any(
            |e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerSucceeded)),
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
            source: Arc::from("test-group"),
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
            |e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerInvoked)),
        );
        let has_failed = events.iter().any(
            |e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerFailed)),
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
            source: Arc::from("test-group"),
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
            |e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerInvoked)),
        );
        let has_succeeded = events.iter().any(
            |e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerSucceeded)),
        );

        assert!(has_invoked, "Should emit HandlerInvoked event");
        assert!(has_succeeded, "Should emit HandlerSucceeded event");
    }

    // === Middleware Composition Tests ===

    #[test]
    fn middleware_creates_provider() {
        let telemetry = Telemetry::new();
        let middleware = TelemetryMiddleware::new(telemetry, Arc::from("test-group"));
        let inner_provider = FallibleCloneProvider::new(MockHandler::success());
        let _provider = middleware.with_provider(inner_provider);
        // If this compiles, middleware composition works
    }

    // === Data::Message Event Tests ===

    fn create_test_message_with_fields(key: &str, offset: i64) -> Option<ConsumerMessage> {
        let semaphore = Arc::new(Semaphore::new(10));
        let permit = semaphore.try_acquire_owned().ok()?;
        Some(ConsumerMessage::new(
            ConsumerMessageValue {
                key: Arc::from(key),
                offset,
                ..ConsumerMessageValue::default()
            },
            Span::current(),
            permit,
        ))
    }

    fn collect_events(rx: &mut broadcast::Receiver<TelemetryEvent>) -> Vec<TelemetryEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    #[tokio::test]
    async fn on_message_success_emits_message_dispatched_and_succeeded() {
        let telemetry = Telemetry::new();
        let mut rx = telemetry.subscribe();
        let handler = MockHandler::success();
        let telemetry_handler = TelemetryHandler {
            handler,
            sender: telemetry.partition_sender("test-topic".into(), 0),
            source: Arc::from("test-group"),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message_with_fields("my-key", 42) else {
            return;
        };

        let _ = telemetry_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        let events = collect_events(&mut rx);

        let dispatched = events.iter().find_map(|e| match &*e.data {
            Data::Message(
                m @ MessageTelemetryEvent {
                    event_type: MessageEventType::Dispatched { .. },
                    ..
                },
            ) => Some(m),
            _ => None,
        });
        let succeeded = events.iter().find_map(|e| match &*e.data {
            Data::Message(
                m @ MessageTelemetryEvent {
                    event_type: MessageEventType::Succeeded { .. },
                    ..
                },
            ) => Some(m),
            _ => None,
        });

        assert!(dispatched.is_some(), "Should emit Message(Dispatched)");
        assert!(succeeded.is_some(), "Should emit Message(Succeeded)");

        let d = dispatched.as_ref();
        assert_eq!(d.map(|m| m.key.as_ref()), Some("my-key"));
        assert_eq!(d.map(|m| m.offset), Some(42));
        assert!(
            matches!(
                d.map(|m| &m.event_type),
                Some(MessageEventType::Dispatched {
                    demand_type: DemandType::Normal
                })
            ),
            "Dispatched should have Normal demand_type"
        );
    }

    #[tokio::test]
    async fn on_message_failure_emits_message_dispatched_and_failed() {
        let telemetry = Telemetry::new();
        let mut rx = telemetry.subscribe();
        let handler = MockHandler::failing(ErrorCategory::Permanent);
        let telemetry_handler = TelemetryHandler {
            handler,
            sender: telemetry.partition_sender("test-topic".into(), 0),
            source: Arc::from("test-group"),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message_with_fields("fail-key", 99) else {
            return;
        };

        let _ = telemetry_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        let events = collect_events(&mut rx);

        let failed = events.iter().find_map(|e| match &*e.data {
            Data::Message(
                m @ MessageTelemetryEvent {
                    event_type: MessageEventType::Failed { .. },
                    ..
                },
            ) => Some(m),
            _ => None,
        });

        assert!(failed.is_some(), "Should emit Message(Failed)");
        let f = failed.as_ref();
        assert_eq!(f.map(|m| m.key.as_ref()), Some("fail-key"));
        assert_eq!(f.map(|m| m.offset), Some(99));
        assert!(
            matches!(
                f.map(|m| &m.event_type),
                Some(MessageEventType::Failed {
                    error_category: ErrorCategory::Permanent,
                    ..
                })
            ),
            "Failed should have Permanent error_category"
        );
        // exception should be non-empty
        if let Some(MessageEventType::Failed { exception, .. }) = f.map(|m| &m.event_type) {
            assert!(!exception.is_empty(), "exception should be non-empty");
        }
    }

    #[tokio::test]
    async fn on_message_transient_error_category_correct() {
        let telemetry = Telemetry::new();
        let mut rx = telemetry.subscribe();
        let handler = MockHandler::failing(ErrorCategory::Transient);
        let telemetry_handler = TelemetryHandler {
            handler,
            sender: telemetry.partition_sender("test-topic".into(), 0),
            source: Arc::from("test-group"),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message_with_fields("transient-key", 7) else {
            return;
        };

        let _ = telemetry_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        let events = collect_events(&mut rx);

        let failed = events.iter().find_map(|e| match &*e.data {
            Data::Message(
                m @ MessageTelemetryEvent {
                    event_type: MessageEventType::Failed { .. },
                    ..
                },
            ) => Some(m),
            _ => None,
        });

        assert!(
            matches!(
                failed.map(|m| &m.event_type),
                Some(MessageEventType::Failed {
                    error_category: ErrorCategory::Transient,
                    ..
                })
            ),
            "ErrorCategory::Transient should be propagated in Failed event"
        );
    }

    // === Data::Timer Event Tests ===

    fn create_test_trigger_with_fields(key: &str, time: u32, timer_type: TimerType) -> Trigger {
        Trigger::for_testing(Arc::from(key), CompactDateTime::from(time), timer_type)
    }

    #[tokio::test]
    async fn on_timer_success_emits_timer_dispatched_and_succeeded() {
        let telemetry = Telemetry::new();
        let mut rx = telemetry.subscribe();
        let handler = MockHandler::success();
        let telemetry_handler = TelemetryHandler {
            handler,
            sender: telemetry.partition_sender("test-topic".into(), 0),
            source: Arc::from("test-group"),
        };
        let context = MockEventContext::new();
        let trigger = create_test_trigger_with_fields("timer-key", 5000, TimerType::Application);

        let _ = telemetry_handler
            .on_timer(context, trigger, DemandType::Normal)
            .await;

        let events = collect_events(&mut rx);

        let dispatched = events.iter().find_map(|e| match &*e.data {
            Data::Timer(
                t @ TimerTelemetryEvent {
                    event_type: TimerEventType::Dispatched { .. },
                    ..
                },
            ) => Some(t),
            _ => None,
        });
        let succeeded = events.iter().find_map(|e| match &*e.data {
            Data::Timer(
                t @ TimerTelemetryEvent {
                    event_type: TimerEventType::Succeeded { .. },
                    ..
                },
            ) => Some(t),
            _ => None,
        });

        assert!(dispatched.is_some(), "Should emit Timer(Dispatched)");
        assert!(succeeded.is_some(), "Should emit Timer(Succeeded)");

        let d = dispatched.as_ref();
        assert_eq!(d.map(|t| t.key.as_ref()), Some("timer-key"));
        assert_eq!(
            d.map(|t| t.scheduled_time),
            Some(CompactDateTime::from(5000_u32))
        );
        assert_eq!(d.map(|t| t.timer_type), Some(TimerType::Application));
    }

    #[tokio::test]
    async fn on_timer_failure_emits_timer_failed_with_error_fields() {
        let telemetry = Telemetry::new();
        let mut rx = telemetry.subscribe();
        let handler = MockHandler::failing(ErrorCategory::Transient);
        let telemetry_handler = TelemetryHandler {
            handler,
            sender: telemetry.partition_sender("test-topic".into(), 0),
            source: Arc::from("test-group"),
        };
        let context = MockEventContext::new();
        let trigger =
            create_test_trigger_with_fields("fail-timer", 9000, TimerType::DeferredMessage);

        let _ = telemetry_handler
            .on_timer(context, trigger, DemandType::Failure)
            .await;

        let events = collect_events(&mut rx);

        let failed = events.iter().find_map(|e| match &*e.data {
            Data::Timer(
                t @ TimerTelemetryEvent {
                    event_type: TimerEventType::Failed { .. },
                    ..
                },
            ) => Some(t),
            _ => None,
        });

        assert!(failed.is_some(), "Should emit Timer(Failed)");
        let f = failed.as_ref();
        assert_eq!(f.map(|t| t.key.as_ref()), Some("fail-timer"));
        assert!(
            matches!(
                f.map(|t| &t.event_type),
                Some(TimerEventType::Failed {
                    demand_type: DemandType::Failure,
                    error_category: ErrorCategory::Transient,
                    ..
                })
            ),
            "Failed should have correct demand_type and error_category"
        );
        if let Some(TimerEventType::Failed { exception, .. }) = f.map(|t| &t.event_type) {
            assert!(!exception.is_empty(), "exception should be non-empty");
        }
    }

    #[tokio::test]
    async fn on_message_emits_both_key_and_message_events() {
        let telemetry = Telemetry::new();
        let mut rx = telemetry.subscribe();
        let handler = MockHandler::success();
        let telemetry_handler = TelemetryHandler {
            handler,
            sender: telemetry.partition_sender("test-topic".into(), 0),
            source: Arc::from("test-group"),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message_with_fields("dual-key", 10) else {
            return;
        };

        let _ = telemetry_handler
            .on_message(context, message, DemandType::Normal)
            .await;

        let events = collect_events(&mut rx);

        // Should have 4 total events:
        // Key(HandlerInvoked), Message(Dispatched), Key(HandlerSucceeded),
        // Message(Succeeded)
        assert_eq!(events.len(), 4, "Should emit exactly 4 events");

        let key_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(&*e.data, Data::Key(_)))
            .collect();
        let message_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(&*e.data, Data::Message(_)))
            .collect();

        assert_eq!(key_events.len(), 2, "Should emit 2 Key events");
        assert_eq!(message_events.len(), 2, "Should emit 2 Message events");

        // Verify Key event states
        assert!(
            key_events.iter().any(
                |e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerInvoked))
            ),
            "Should have Key(HandlerInvoked)"
        );
        assert!(
            key_events.iter().any(
                |e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerSucceeded))
            ),
            "Should have Key(HandlerSucceeded)"
        );

        // Verify Message event types
        assert!(
            message_events.iter().any(
                |e| matches!(&*e.data, Data::Message(m) if matches!(m.event_type, MessageEventType::Dispatched { .. }))
            ),
            "Should have Message(Dispatched)"
        );
        assert!(
            message_events.iter().any(
                |e| matches!(&*e.data, Data::Message(m) if matches!(m.event_type, MessageEventType::Succeeded { .. }))
            ),
            "Should have Message(Succeeded)"
        );
    }
}
