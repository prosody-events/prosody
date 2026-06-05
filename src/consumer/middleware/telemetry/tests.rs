use super::*;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::consumer::middleware::FallibleCloneProvider;
use crate::consumer::middleware::tests::test_support::MockEventContext;
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
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
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
        C: EventContext<Payload = Self::Payload>,
    {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        self.result.clone()
    }

    async fn shutdown(self) {}
}

fn create_test_message() -> Option<ConsumerMessage<serde_json::Value>> {
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
    let has_invoked = events
        .iter()
        .any(|e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerInvoked)));
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
    let has_invoked = events
        .iter()
        .any(|e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerInvoked)));
    let has_failed = events
        .iter()
        .any(|e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerFailed)));

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

    let has_invoked = events
        .iter()
        .any(|e| matches!(&*e.data, Data::Key(ke) if matches!(ke.state, KeyState::HandlerInvoked)));
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

fn create_test_message_with_fields(
    key: &str,
    offset: i64,
) -> Option<ConsumerMessage<serde_json::Value>> {
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
    let trigger = create_test_trigger_with_fields("fail-timer", 9000, TimerType::DeferredMessage);

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
