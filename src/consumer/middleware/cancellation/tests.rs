use super::*;
use crate::consumer::message::ConsumerMessageValue;
use crate::consumer::middleware::tests::test_support::MockEventContext;
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
        C: EventContext<Payload = Self::Payload>,
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

    // The inner ran and returned a Transient err while shutdown was
    // signaled; we promote it to Terminal but keep the inner err so
    // its apply hook (`after_abort`) can still fire.
    assert!(matches!(
        result,
        Err(CancellationError::ShutdownAfterInner(_))
    ));
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

    assert!(matches!(
        result,
        Err(CancellationError::ShutdownAfterInner(_))
    ));
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

/// Records results passed to `after_commit` / `after_abort`.
#[derive(Clone)]
struct RecordingHandler {
    commit_calls: Arc<parking_lot::Mutex<Vec<Result<(), TestError>>>>,
    abort_calls: Arc<parking_lot::Mutex<Vec<Result<(), TestError>>>>,
}

impl RecordingHandler {
    fn new() -> Self {
        Self {
            commit_calls: Arc::new(parking_lot::Mutex::new(Vec::new())),
            abort_calls: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }
    }
}

impl FallibleHandler for RecordingHandler {
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
        Ok(())
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
        Ok(())
    }

    async fn after_commit<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.commit_calls.lock().push(result);
    }

    async fn after_abort<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.abort_calls.lock().push(result);
    }

    async fn shutdown(self) {}
}

#[tokio::test]
async fn after_abort_forwards_inner_error_when_shutdown_promoted() {
    let recorder = RecordingHandler::new();
    let guard_handler = CancellationHandler::new(recorder.clone());
    let context = MockEventContext::new();

    let promoted: Result<(), CancellationError<TestError>> = Err(
        CancellationError::ShutdownAfterInner(TestError(ErrorCategory::Transient)),
    );

    guard_handler.after_abort(context, promoted).await;

    let abort_calls = recorder.abort_calls.lock();
    assert_eq!(
        abort_calls.len(),
        1,
        "inner after_abort must fire exactly once"
    );
    assert!(matches!(
        &abort_calls[0],
        Err(TestError(ErrorCategory::Transient))
    ));
    assert!(
        recorder.commit_calls.lock().is_empty(),
        "after_commit must not fire on the abort path"
    );
}

#[tokio::test]
async fn apply_hooks_suppressed_when_inner_did_not_run() {
    let recorder = RecordingHandler::new();
    let guard_handler = CancellationHandler::new(recorder.clone());
    let context = MockEventContext::new();

    let pre_call: Result<(), CancellationError<TestError>> = Err(CancellationError::Shutdown);
    guard_handler.after_abort(context.clone(), pre_call).await;

    let pre_call_cancel: Result<(), CancellationError<TestError>> =
        Err(CancellationError::MessageCancelled);
    guard_handler.after_abort(context, pre_call_cancel).await;

    assert!(recorder.abort_calls.lock().is_empty());
    assert!(recorder.commit_calls.lock().is_empty());
}
