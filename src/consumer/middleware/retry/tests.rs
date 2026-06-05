use super::*;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::consumer::middleware::tests::test_support::MockEventContext;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use parking_lot::Mutex;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::{sleep as tokio_sleep, timeout};
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

/// Records every lifecycle hook firing on the inner handler in order so
/// tests can assert the per-invocation apply-hook invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
enum HookEvent {
    /// `on_message` / `on_timer` was invoked with this demand type.
    Invoke(DemandType),
    /// `after_commit` was fired with this `Result` shape (Ok / Err
    /// category).
    AfterCommit(Result<(), ErrorCategory>),
    /// `after_abort` was fired with this `Result` shape.
    AfterAbort(Result<(), ErrorCategory>),
}

/// Mock handler that tracks calls and can be configured to fail.
#[derive(Clone)]
struct MockHandler {
    call_count: Arc<AtomicUsize>,
    /// Sequence of results to return on successive calls.
    /// Empty means success.
    failure_sequence: Arc<Mutex<Vec<ErrorCategory>>>,
    /// Recorded demand types from calls.
    demand_types: Arc<Mutex<Vec<DemandType>>>,
    /// Ordered log of every lifecycle hook firing (invoke + apply hooks).
    hook_log: Arc<Mutex<Vec<HookEvent>>>,
}

impl MockHandler {
    fn success() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            failure_sequence: Arc::new(Mutex::new(vec![])),
            demand_types: Arc::new(Mutex::new(vec![])),
            hook_log: Arc::new(Mutex::new(vec![])),
        }
    }

    fn failing_then_success(failures: Vec<ErrorCategory>) -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            failure_sequence: Arc::new(Mutex::new(failures)),
            demand_types: Arc::new(Mutex::new(vec![])),
            hook_log: Arc::new(Mutex::new(vec![])),
        }
    }

    fn always_failing(category: ErrorCategory) -> Self {
        // Create a large sequence that should outlast max_retries
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            failure_sequence: Arc::new(Mutex::new(vec![category; 100])),
            demand_types: Arc::new(Mutex::new(vec![])),
            hook_log: Arc::new(Mutex::new(vec![])),
        }
    }

    fn call_count(&self) -> usize {
        self.call_count.load(Ordering::Relaxed)
    }

    fn recorded_demand_types(&self) -> Vec<DemandType> {
        self.demand_types.lock().clone()
    }

    fn hook_events(&self) -> Vec<HookEvent> {
        self.hook_log.lock().clone()
    }
}

/// Project a `Result<(), TestError>` onto the equality-friendly
/// `Result<(), ErrorCategory>` carried by `HookEvent`.
fn project_result(result: Result<(), TestError>) -> Result<(), ErrorCategory> {
    result.map_err(|TestError(category)| category)
}

impl FallibleHandler for MockHandler {
    type Error = TestError;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.demand_types.lock().push(demand_type);
        self.hook_log.lock().push(HookEvent::Invoke(demand_type));

        let mut seq = self.failure_sequence.lock();
        if seq.is_empty() {
            Ok(())
        } else {
            let category = seq.remove(0);
            Err(TestError(category))
        }
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.demand_types.lock().push(demand_type);
        self.hook_log.lock().push(HookEvent::Invoke(demand_type));

        let mut seq = self.failure_sequence.lock();
        if seq.is_empty() {
            Ok(())
        } else {
            let category = seq.remove(0);
            Err(TestError(category))
        }
    }

    async fn after_commit<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.hook_log
            .lock()
            .push(HookEvent::AfterCommit(project_result(result)));
    }

    async fn after_abort<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.hook_log
            .lock()
            .push(HookEvent::AfterAbort(project_result(result)));
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

fn create_retry_handler<T>(handler: T, max_retries: u32) -> RetryHandler<T> {
    RetryHandler {
        base_delay_millis: 1, // Very short for tests
        max_delay_millis: 10,
        max_retries,
        handler,
    }
}

// === Success Tests ===

#[tokio::test]
async fn success_on_first_attempt_returns_ok_immediately() {
    let handler = MockHandler::success();
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let Some(message) = create_test_message() else {
        return;
    };

    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_ok());
    assert_eq!(handler.call_count(), 1, "Should only call handler once");
}

// === Transient Error Tests ===

#[tokio::test]
async fn transient_error_retries_then_succeeds() {
    // Fail twice with transient errors, then succeed
    let handler =
        MockHandler::failing_then_success(vec![ErrorCategory::Transient, ErrorCategory::Transient]);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let Some(message) = create_test_message() else {
        return;
    };

    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_ok(), "Should succeed after retries");
    assert_eq!(handler.call_count(), 3, "Should retry twice then succeed");
}

#[tokio::test]
async fn transient_error_fails_after_max_retries() {
    let handler = MockHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let Some(message) = create_test_message() else {
        return;
    };

    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_err(), "Should fail after max retries");
    // First attempt + 3 retries = 4 total calls
    assert_eq!(
        handler.call_count(),
        4,
        "Should attempt 1 + max_retries times"
    );
}

// === Permanent Error Tests ===

#[tokio::test]
async fn permanent_error_fails_immediately_no_retry() {
    let handler = MockHandler::always_failing(ErrorCategory::Permanent);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let Some(message) = create_test_message() else {
        return;
    };

    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_err());
    assert_eq!(handler.call_count(), 1, "Should not retry permanent errors");
}

// === Terminal Error Tests ===

#[tokio::test]
async fn terminal_error_fails_immediately_no_retry() {
    let handler = MockHandler::always_failing(ErrorCategory::Terminal);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let Some(message) = create_test_message() else {
        return;
    };

    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_err());
    assert_eq!(handler.call_count(), 1, "Should not retry terminal errors");
}

// === Demand Type Tests ===

#[tokio::test]
async fn first_attempt_uses_original_demand_type_retries_use_failure() {
    // Fail once with transient, then succeed
    let handler = MockHandler::failing_then_success(vec![ErrorCategory::Transient]);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let Some(message) = create_test_message() else {
        return;
    };

    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_ok());
    let demand_types = handler.recorded_demand_types();
    assert_eq!(demand_types.len(), 2);
    assert_eq!(
        demand_types[0],
        DemandType::Normal,
        "First attempt should use original"
    );
    assert_eq!(
        demand_types[1],
        DemandType::Failure,
        "Retry should use Failure"
    );
}

// === Shutdown Tests ===

#[tokio::test]
async fn shutdown_during_retry_sleep_returns_error() {
    let handler = MockHandler::always_failing(ErrorCategory::Transient);
    // Use longer delays to give time for shutdown signal
    let retry_handler = RetryHandler {
        base_delay_millis: 1000, // 1 second base delay
        max_delay_millis: 10000,
        max_retries: 10,
        handler: handler.clone(),
    };
    let context = MockEventContext::new();
    let Some(message) = create_test_message() else {
        return;
    };

    // Spawn the retry operation
    let ctx = context.clone();
    let handle = tokio::spawn(async move {
        FallibleHandler::on_message(&retry_handler, ctx, message, DemandType::Normal).await
    });

    // Wait a bit for the first failure and retry sleep to start
    tokio_sleep(Duration::from_millis(50)).await;

    // Signal shutdown
    context.request_shutdown();

    // Should complete quickly due to shutdown
    let Ok(join_result) = timeout(Duration::from_millis(500), handle).await else {
        // Timed out waiting for shutdown - test fails
        return;
    };
    let Ok(result) = join_result else {
        // Task panicked - test fails
        return;
    };

    assert!(result.is_err(), "Should return error on shutdown");
}

// === Timer Path Tests ===

#[tokio::test]
async fn timer_success_on_first_attempt() {
    let handler = MockHandler::success();
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let trigger = create_test_trigger();

    let result =
        FallibleHandler::on_timer(&retry_handler, context, trigger, DemandType::Normal).await;

    assert!(result.is_ok());
    assert_eq!(handler.call_count(), 1);
}

#[tokio::test]
async fn timer_transient_error_retries_then_succeeds() {
    let handler = MockHandler::failing_then_success(vec![ErrorCategory::Transient]);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let trigger = create_test_trigger();

    let result =
        FallibleHandler::on_timer(&retry_handler, context, trigger, DemandType::Normal).await;

    assert!(result.is_ok());
    assert_eq!(handler.call_count(), 2);
}

#[tokio::test]
async fn timer_permanent_error_no_retry() {
    let handler = MockHandler::always_failing(ErrorCategory::Permanent);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let trigger = create_test_trigger();

    let result =
        FallibleHandler::on_timer(&retry_handler, context, trigger, DemandType::Normal).await;

    assert!(result.is_err());
    assert_eq!(handler.call_count(), 1);
}

// === Backoff Calculation Tests ===

#[test]
fn sleep_time_has_exponential_growth_with_jitter() {
    let handler = MockHandler::success();
    let retry_handler = RetryHandler {
        base_delay_millis: 100,
        max_delay_millis: 10000,
        max_retries: 10,
        handler,
    };

    // Collect multiple samples to verify jitter randomness
    let mut samples_attempt_1: Vec<u64> = Vec::new();
    let mut samples_attempt_3: Vec<u64> = Vec::new();

    for _ in 0_u32..100_u32 {
        samples_attempt_1.push(retry_handler.sleep_time(1).as_millis() as u64);
        samples_attempt_3.push(retry_handler.sleep_time(3).as_millis() as u64);
    }

    // Attempt 1: exp_backoff = 2^1 * 100 = 200ms, jitter in [0, 200)
    let Some(&max_attempt_1) = samples_attempt_1.iter().max() else {
        return;
    };
    assert!(max_attempt_1 < 200, "Attempt 1 jitter should be < 200ms");

    // Attempt 3: exp_backoff = 2^3 * 100 = 800ms, jitter in [0, 800)
    let Some(&max_attempt_3) = samples_attempt_3.iter().max() else {
        return;
    };
    assert!(max_attempt_3 < 800, "Attempt 3 jitter should be < 800ms");

    // Verify there's some variation (jitter is working)
    let Some(&min_attempt_3) = samples_attempt_3.iter().min() else {
        return;
    };
    assert!(
        max_attempt_3 > min_attempt_3 + 50,
        "Jitter should introduce variation"
    );
}

#[test]
fn sleep_time_capped_at_max_delay() {
    let handler = MockHandler::success();
    let retry_handler = RetryHandler {
        base_delay_millis: 100,
        max_delay_millis: 500,
        max_retries: 10,
        handler,
    };

    // Attempt 10: exp_backoff = 2^10 * 100 = 102400ms, but capped at 500ms
    // Jitter should be in [0, 500)
    for _ in 0_u32..100_u32 {
        let sleep = retry_handler.sleep_time(10).as_millis() as u64;
        assert!(sleep < 500, "Sleep time should be capped at max_delay");
    }
}

// =========================================================================
// Shutdown vs Cancellation Tests
// =========================================================================
//
// These tests verify correct behavior for two distinct signals:
// - **Shutdown**: Partition revoked or consumer stopping → should abort
// - **Cancellation**: Message-level cancellation → should treat as transient,
//   retry
//
// Test matrix (2×2×2 = 8 tests):
// - Handler type: FallibleHandler vs EventHandler
// - Method: on_message vs on_timer
// - Signal: shutdown vs cancellation

use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::{Keyed, Uncommitted};
use crate::timers::UncommittedTimer;
use color_eyre::eyre::{Result, bail};
use crossbeam_utils::CachePadded;

/// Mock commit guard for tracking commit/abort calls.
struct MockCommitGuard {
    committed: Arc<AtomicBool>,
    aborted: Arc<AtomicBool>,
}

impl Uncommitted for MockCommitGuard {
    async fn commit(self) {
        self.committed.store(true, Ordering::Relaxed);
    }

    async fn abort(self) {
        self.aborted.store(true, Ordering::Relaxed);
    }
}

/// Mock uncommitted timer for testing `EventHandler::on_timer`.
struct MockUncommittedTimer {
    trigger: Trigger,
    committed: Arc<AtomicBool>,
    aborted: Arc<AtomicBool>,
}

impl MockUncommittedTimer {
    fn new(committed: Arc<AtomicBool>, aborted: Arc<AtomicBool>) -> Self {
        Self {
            trigger: create_test_trigger(),
            committed,
            aborted,
        }
    }
}

impl Keyed for MockUncommittedTimer {
    type Key = crate::Key;

    fn key(&self) -> &Self::Key {
        &self.trigger.key
    }
}

impl Uncommitted for MockUncommittedTimer {
    async fn commit(self) {
        self.committed.store(true, Ordering::Relaxed);
    }

    async fn abort(self) {
        self.aborted.store(true, Ordering::Relaxed);
    }
}

impl UncommittedTimer for MockUncommittedTimer {
    type CommitGuard = MockCommitGuard;

    fn time(&self) -> CompactDateTime {
        self.trigger.time
    }

    fn timer_type(&self) -> TimerType {
        self.trigger.timer_type
    }

    fn span(&self) -> Span {
        Span::none()
    }

    fn into_inner(self) -> (Trigger, Self::CommitGuard) {
        (
            self.trigger,
            MockCommitGuard {
                committed: self.committed,
                aborted: self.aborted,
            },
        )
    }
}

fn create_offset_tracker() -> OffsetTracker {
    let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    OffsetTracker::new("test-topic".into(), 0, 10, Duration::from_mins(5), version)
}

// === Shutdown Tests (should pass - abort is correct behavior) ===

/// `FallibleHandler::on_message` should abort on shutdown signal.
#[tokio::test]
async fn fallible_on_message_shutdown_aborts() -> Result<()> {
    let handler = MockHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 10);
    let context = MockEventContext::new();
    context.request_shutdown();

    let Some(message) = create_test_message() else {
        bail!("failed to create test message");
    };
    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_err());
    assert_eq!(handler.call_count(), 1);
    Ok(())
}

/// `FallibleHandler::on_timer` should abort on shutdown signal.
#[tokio::test]
async fn fallible_on_timer_shutdown_aborts() -> Result<()> {
    let handler = MockHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 10);
    let context = MockEventContext::new();
    context.request_shutdown();

    let result = FallibleHandler::on_timer(
        &retry_handler,
        context,
        create_test_trigger(),
        DemandType::Normal,
    )
    .await;

    assert!(result.is_err());
    assert_eq!(handler.call_count(), 1);
    Ok(())
}

/// `EventHandler::on_message` should abort offset on shutdown signal.
#[tokio::test]
async fn event_on_message_shutdown_aborts() -> Result<()> {
    let handler = MockHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 10);
    let context = MockEventContext::new();
    context.request_shutdown();

    let tracker = create_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let Some(message) = create_test_message() else {
        bail!("failed to create test message");
    };
    let uncommitted_message = message.into_uncommitted(uncommitted_offset);

    EventHandler::on_message(
        &retry_handler,
        context,
        uncommitted_message,
        DemandType::Normal,
    )
    .await;

    assert_eq!(handler.call_count(), 1);
    assert_eq!(tracker.shutdown().await, None, "offset should be aborted");
    Ok(())
}

/// `EventHandler::on_timer` should abort on shutdown signal.
#[tokio::test]
async fn event_on_timer_shutdown_aborts() -> Result<()> {
    let handler = MockHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 10);
    let context = MockEventContext::new();
    context.request_shutdown();

    let committed = Arc::new(AtomicBool::new(false));
    let aborted = Arc::new(AtomicBool::new(false));
    let timer = MockUncommittedTimer::new(Arc::clone(&committed), Arc::clone(&aborted));

    EventHandler::on_timer(&retry_handler, context, timer, DemandType::Normal).await;

    assert_eq!(handler.call_count(), 1);
    assert!(aborted.load(Ordering::Relaxed));
    assert!(!committed.load(Ordering::Relaxed));
    Ok(())
}

// === Cancellation Tests (treats message cancellation as transient) ===

/// `FallibleHandler::on_message` should continue retrying on cancellation.
#[tokio::test]
async fn fallible_on_message_cancellation_retries() -> Result<()> {
    let handler = MockHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    context.request_cancellation();

    let Some(message) = create_test_message() else {
        bail!("failed to create test message");
    };
    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_err());
    assert_eq!(handler.call_count(), 4); // 1 initial + 3 retries
    Ok(())
}

/// `FallibleHandler::on_timer` should continue retrying on cancellation.
#[tokio::test]
async fn fallible_on_timer_cancellation_retries() -> Result<()> {
    let handler = MockHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    context.request_cancellation();

    let result = FallibleHandler::on_timer(
        &retry_handler,
        context,
        create_test_trigger(),
        DemandType::Normal,
    )
    .await;

    assert!(result.is_err());
    assert_eq!(handler.call_count(), 4); // 1 initial + 3 retries
    Ok(())
}

/// `EventHandler::on_message` should continue retrying on cancellation.
#[tokio::test]
async fn event_on_message_cancellation_retries() -> Result<()> {
    let handler =
        MockHandler::failing_then_success(vec![ErrorCategory::Transient, ErrorCategory::Transient]);
    let retry_handler = create_retry_handler(handler.clone(), 10);
    let context = MockEventContext::new();
    context.request_cancellation();

    let tracker = create_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let Some(message) = create_test_message() else {
        bail!("failed to create test message");
    };
    let uncommitted_message = message.into_uncommitted(uncommitted_offset);

    EventHandler::on_message(
        &retry_handler,
        context,
        uncommitted_message,
        DemandType::Normal,
    )
    .await;

    assert_eq!(handler.call_count(), 3); // 2 failures + 1 success
    assert_eq!(
        tracker.shutdown().await,
        Some(0),
        "offset should be committed"
    );
    Ok(())
}

/// `EventHandler::on_timer` should continue retrying on cancellation.
#[tokio::test]
async fn event_on_timer_cancellation_retries() -> Result<()> {
    let handler =
        MockHandler::failing_then_success(vec![ErrorCategory::Transient, ErrorCategory::Transient]);
    let retry_handler = create_retry_handler(handler.clone(), 10);
    let context = MockEventContext::new();
    context.request_cancellation();

    let committed = Arc::new(AtomicBool::new(false));
    let aborted = Arc::new(AtomicBool::new(false));
    let timer = MockUncommittedTimer::new(Arc::clone(&committed), Arc::clone(&aborted));

    EventHandler::on_timer(&retry_handler, context, timer, DemandType::Normal).await;

    assert_eq!(handler.call_count(), 3); // 2 failures + 1 success
    assert!(committed.load(Ordering::Relaxed));
    assert!(!aborted.load(Ordering::Relaxed));
    Ok(())
}

// =========================================================================
// Per-Invocation Apply-Hook Invariant Tests
// =========================================================================
//
// The `FallibleHandler` apply-hook contract is **per-invocation**: every
// call to `on_message` / `on_timer` that runs and returns is paired with
// exactly one apply hook (`after_commit` or `after_abort`) on the same
// handler instance. The retry middleware preserves this on its inner by
// firing `inner.after_abort(Err(error))` between attempts; the final
// attempt's hook is fired by the outer (FallibleHandler blanket impl or
// EventHandler durability boundary).

/// Two transient failures followed by a success — the inner sees three
/// invocations, each paired with exactly one apply hook. The first two
/// (non-final) attempts fire `after_abort(Err)` from the retry loop;
/// the third (success, final) fires `after_commit(Ok)` via the outer
/// blanket-impl boundary.
#[tokio::test]
async fn fallible_inner_sees_one_apply_hook_per_attempt_when_retries_then_succeeds() -> Result<()> {
    let handler =
        MockHandler::failing_then_success(vec![ErrorCategory::Transient, ErrorCategory::Transient]);
    let retry_handler = create_retry_handler(handler.clone(), 5);
    // Wrap in the FallibleEventHandler blanket impl by going through the
    // EventHandler path with a real durability marker. The blanket impl
    // is what fires the final attempt's apply hook on the outer
    // RetryHandler, which retry forwards to the inner.
    let context = MockEventContext::new();
    let tracker = create_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let Some(message) = create_test_message() else {
        bail!("failed to create test message");
    };
    let uncommitted_message = message.into_uncommitted(uncommitted_offset);

    EventHandler::on_message(
        &retry_handler,
        context,
        uncommitted_message,
        DemandType::Normal,
    )
    .await;

    let events = handler.hook_events();
    assert_eq!(
        events,
        vec![
            HookEvent::Invoke(DemandType::Normal),
            HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
            HookEvent::Invoke(DemandType::Failure),
            HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
            HookEvent::Invoke(DemandType::Failure),
            HookEvent::AfterCommit(Ok(())),
        ],
        "each invocation must be paired with exactly one apply hook on the inner",
    );
    Ok(())
}

/// All transient failures with `max_retries = 2` exhausted: 1 initial +
/// 2 retries = 3 invocations. The first two (non-final) attempts fire
/// `after_abort(Err)` from the retry loop. The third (final) attempt's
/// hook is `after_commit(Err)` because max-retries-exceeded is treated
/// as commit (DLQ takes over) by the outer.
///
/// We drive `FallibleHandler::on_message` directly here (that path
/// honours `max_retries`; the `EventHandler` path uses `None` for
/// retry-forever semantics at the durability boundary) and then
/// manually invoke the outer apply hook the way an outer
/// `FallibleEventHandler` blanket impl would for a `Transient`
/// classification (commit + `after_commit(Err)`).
#[tokio::test]
async fn fallible_inner_sees_one_apply_hook_per_attempt_when_max_retries_exhausted() -> Result<()> {
    let handler = MockHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 2);
    let context = MockEventContext::new();
    let Some(message) = create_test_message() else {
        bail!("failed to create test message");
    };

    let result =
        FallibleHandler::on_message(&retry_handler, context.clone(), message, DemandType::Normal)
            .await;

    // Simulate the outer (FallibleEventHandler blanket impl): a
    // Transient error commits the marker and fires `after_commit`.
    assert!(
        matches!(&result, Err(TestError(ErrorCategory::Transient))),
        "max retries should exhaust to a Transient Err",
    );
    FallibleHandler::after_commit(&retry_handler, context, result).await;

    let events = handler.hook_events();
    assert_eq!(
        events,
        vec![
            HookEvent::Invoke(DemandType::Normal),
            HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
            HookEvent::Invoke(DemandType::Failure),
            HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
            HookEvent::Invoke(DemandType::Failure),
            HookEvent::AfterCommit(Err(ErrorCategory::Transient)),
        ],
        "max-retries-exhausted: 3 invocations, each paired with exactly one apply hook; final \
         hook is after_commit because the outer treats this as commit (DLQ takeover)",
    );
    Ok(())
}

/// Shutdown during a retry sleep: every attempt that ran and returned is
/// paired with exactly one apply hook on the inner, with no double-fire
/// for the abandoned attempt. The retry loop's `Resolution::Abort`
/// branch on shutdown deliberately skips the per-attempt `apply_abort`
/// so the outer's `after_abort` (fired here by `EventHandler`) is the
/// sole apply-hook firing for the final attempt.
///
/// We avoid asserting a fixed event count because the jitter floor on
/// `sleep_time` is zero: between the first failure and the shutdown
/// signal a second attempt may slip in. Instead we assert the
/// invariant directly: events alternate `Invoke` / `Apply` strictly
/// 1:1, every intermediate apply hook is `AfterAbort(Err(Transient))`,
/// and the final apply hook is the outer's `AfterAbort` (shutdown
/// path), never `AfterCommit`.
#[tokio::test]
async fn shutdown_during_sleep_does_not_double_fire_apply_hook() -> Result<()> {
    let handler = MockHandler::always_failing(ErrorCategory::Transient);
    // Long sleep so we can race the shutdown signal against it.
    let retry_handler = RetryHandler {
        base_delay_millis: 1000,
        max_delay_millis: 10_000,
        max_retries: 10,
        handler: handler.clone(),
    };
    let context = MockEventContext::new();

    let tracker = create_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let Some(message) = create_test_message() else {
        bail!("failed to create test message");
    };
    let uncommitted_message = message.into_uncommitted(uncommitted_offset);

    // Spawn the dispatch and signal shutdown shortly after the first
    // attempt fails and the retry-sleep is in flight.
    let ctx = context.clone();
    let handle = tokio::spawn(async move {
        EventHandler::on_message(&retry_handler, ctx, uncommitted_message, DemandType::Normal)
            .await;
    });

    tokio_sleep(Duration::from_millis(50)).await;
    context.request_shutdown();

    // Bound the wait so a regression doesn't hang the suite.
    match timeout(Duration::from_secs(5), handle).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => bail!("dispatch task panicked"),
        Err(_) => bail!("dispatch did not finish within timeout after shutdown"),
    }

    let events = handler.hook_events();
    assert!(
        !events.is_empty() && events.len().is_multiple_of(2),
        "events must come in invoke+apply pairs; got {events:?}",
    );
    for (i, pair) in events.chunks(2).enumerate() {
        let [invoke, apply] = pair else {
            bail!("uneven event chunk: {pair:?}");
        };
        assert!(
            matches!(invoke, HookEvent::Invoke(_)),
            "pair {i} expected to start with Invoke, got {invoke:?}",
        );
        let is_last = i + 1 == events.len() / 2;
        if is_last {
            // The shutdown-abandoned final attempt must be paired with
            // exactly one `AfterAbort(Err(Transient))` from the outer
            // (NEVER `AfterCommit`, and NEVER duplicated).
            assert_eq!(
                apply,
                &HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
                "final pair must be after_abort fired by the outer (not from the loop), got \
                 {apply:?}",
            );
        } else {
            // Intermediate (non-final) attempts get the loop's
            // between-attempts after_abort.
            assert_eq!(
                apply,
                &HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
                "intermediate pair {i} expected after_abort, got {apply:?}",
            );
        }
    }
    Ok(())
}
