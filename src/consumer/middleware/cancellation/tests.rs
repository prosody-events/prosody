use super::*;
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, ScriptedHandler, TestError, create_test_message, create_test_trigger,
};
use std::sync::Arc;

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
async fn shutdown_returns_terminal_error() -> color_eyre::Result<()> {
    let handler = ScriptedHandler::success();
    let guard_handler = CancellationHandler::new(handler.clone());
    let context = MockEventContext::new().with_shutdown();
    let message = create_test_message()?;

    let result = guard_handler
        .on_message(context, message, DemandType::Normal)
        .await;

    assert!(matches!(result, Err(CancellationError::Shutdown)));
    assert!(matches!(
        result.as_ref().err().map(ClassifyError::classify_error),
        Some(ErrorCategory::Terminal)
    ));
    assert_eq!(handler.call_count(), 0, "handler should not be called");
    Ok(())
}

#[tokio::test]
async fn message_cancelled_returns_transient_error() -> color_eyre::Result<()> {
    let handler = ScriptedHandler::success();
    let guard_handler = CancellationHandler::new(handler.clone());
    let context = MockEventContext::new();
    context.request_cancellation();
    let message = create_test_message()?;

    let result = guard_handler
        .on_message(context, message, DemandType::Normal)
        .await;

    assert!(matches!(result, Err(CancellationError::MessageCancelled)));
    assert!(matches!(
        result.as_ref().err().map(ClassifyError::classify_error),
        Some(ErrorCategory::Transient)
    ));
    assert_eq!(handler.call_count(), 0, "handler should not be called");
    Ok(())
}

#[tokio::test]
async fn not_cancelled_passes_through_to_handler() -> color_eyre::Result<()> {
    let handler = ScriptedHandler::success();
    let guard_handler = CancellationHandler::new(handler.clone());
    let context = MockEventContext::new();
    let message = create_test_message()?;

    let result = guard_handler
        .on_message(context, message, DemandType::Normal)
        .await;

    assert!(result.is_ok());
    assert_eq!(handler.call_count(), 1, "handler should be called once");
    Ok(())
}

#[tokio::test]
async fn handler_error_wrapped_in_guard_error() -> color_eyre::Result<()> {
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
    let guard_handler = CancellationHandler::new(handler.clone());
    let context = MockEventContext::new();
    let message = create_test_message()?;

    let result = guard_handler
        .on_message(context, message, DemandType::Normal)
        .await;

    assert!(matches!(result, Err(CancellationError::Handler(_))));
    assert_eq!(handler.call_count(), 1);
    Ok(())
}

#[tokio::test]
async fn timer_shutdown_returns_terminal_error() {
    let handler = ScriptedHandler::success();
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
    let handler = ScriptedHandler::success();
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
    let handler = ScriptedHandler::success();
    let guard_handler = CancellationHandler::new(handler.clone());
    let context = MockEventContext::new();
    let trigger = create_test_trigger();

    let result = guard_handler
        .on_timer(context, trigger, DemandType::Normal)
        .await;

    assert!(result.is_ok());
    assert_eq!(handler.call_count(), 1);
}

#[tokio::test]
async fn shutdown_during_message_converts_transient_to_terminal() -> color_eyre::Result<()> {
    let context = MockEventContext::new();
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient)
        .with_shutdown_on_call(context.clone());
    let guard_handler = CancellationHandler::new(handler);
    let message = create_test_message()?;

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
    Ok(())
}

#[tokio::test]
async fn shutdown_during_message_preserves_non_transient_error() -> color_eyre::Result<()> {
    let context = MockEventContext::new();
    let handler = ScriptedHandler::always_failing(ErrorCategory::Permanent)
        .with_shutdown_on_call(context.clone());
    let guard_handler = CancellationHandler::new(handler);
    let message = create_test_message()?;

    let result = guard_handler
        .on_message(context, message, DemandType::Normal)
        .await;

    // Permanent errors are NOT promoted to Shutdown even during shutdown
    assert!(matches!(result, Err(CancellationError::Handler(_))));
    assert!(matches!(
        result.as_ref().err().map(ClassifyError::classify_error),
        Some(ErrorCategory::Permanent)
    ));
    Ok(())
}

#[tokio::test]
async fn shutdown_during_timer_converts_transient_to_terminal() {
    let context = MockEventContext::new();
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient)
        .with_shutdown_on_call(context.clone());
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
    let handler = ScriptedHandler::always_failing(ErrorCategory::Permanent)
        .with_shutdown_on_call(context.clone());
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

/// The settlement classification table: inner-ran rows delegate (including
/// `ShutdownAfterInner`, which carries the inner's own error); the pre-inner
/// admission rejections are `Bypassed`. Delegation is proven against a
/// `Bypassed`-classifying probe.
#[test]
fn settlement_classification_table() {
    use crate::consumer::middleware::tests::test_support::BypassedHandler;
    use crate::consumer::middleware::{Settlement, SettlementHandler};

    type Subject = CancellationHandler<ScriptedHandler>;
    type Probe = CancellationHandler<BypassedHandler>;
    type Err_ = CancellationError<TestError>;

    let rows: Vec<(&str, Result<(), Err_>, Settlement)> = vec![
        (
            "Ok delegates to the leaf's Final",
            Ok(()),
            Settlement::Final,
        ),
        (
            "Handler delegates to the leaf's Final",
            Err(CancellationError::Handler(TestError(
                ErrorCategory::Permanent,
            ))),
            Settlement::Final,
        ),
        (
            "ShutdownAfterInner delegates on the carried inner error",
            Err(CancellationError::ShutdownAfterInner(TestError(
                ErrorCategory::Transient,
            ))),
            Settlement::Final,
        ),
        (
            "Shutdown (pre-inner) is Bypassed",
            Err(CancellationError::Shutdown),
            Settlement::Bypassed,
        ),
        (
            "MessageCancelled (pre-inner) is Bypassed",
            Err(CancellationError::MessageCancelled),
            Settlement::Bypassed,
        ),
    ];
    for (label, result, expected) in rows {
        assert_eq!(Subject::settlement(result.as_ref()), expected, "{label}");
    }

    // Delegation proof: over a Bypassed-classifying inner the delegating
    // rows stay Bypassed.
    let ok: Result<(), Err_> = Ok(());
    assert_eq!(Probe::settlement(ok.as_ref()), Settlement::Bypassed);
    let inner_err: Result<(), Err_> = Err(CancellationError::Handler(TestError(
        ErrorCategory::Permanent,
    )));
    assert_eq!(Probe::settlement(inner_err.as_ref()), Settlement::Bypassed);
}
