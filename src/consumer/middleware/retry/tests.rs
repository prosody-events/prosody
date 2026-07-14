use super::*;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, ScriptedHandler, ScriptedHook, TestError, create_test_message,
    create_test_trigger,
};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use quickcheck::{QuickCheck, TestResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::{sleep as tokio_sleep, timeout};
use tracing::Span;

fn create_retry_handler<T>(handler: T, max_retries: u32) -> RetryHandler<T> {
    RetryHandler {
        base_delay_millis: 1, // Very short for tests
        max_delay_millis: 10,
        max_retries,
        handler,
    }
}

// === Classification Property ===
//
// `RetryHandler::run` makes one decision per attempt: retry (Transient,
// attempts left), stop-with-error (Permanent/Terminal, or Transient with
// attempts exhausted), or stop-with-success. `expected_outcome` walks a
// scripted failure sequence through that same decision and predicts the
// call count, the final Ok/Err, and the demand-type sequence
// (`Normal` on attempt 1, `Failure` on every retry) — one property replaces
// five single-path examples (success-first-try, transient-then-succeeds,
// permanent-immediate, terminal-immediate, first-attempt-demand-type).
// `transient_error_fails_after_max_retries` stays as a literal anchor.

#[test]
fn prop_retry_classification_arithmetic() {
    /// Predicts `(call_count, is_err, demand_types)` for a scripted
    /// `failures` sequence and `max_retries`, mirroring `RetryHandler::run`'s
    /// per-attempt classification exactly.
    fn expected_outcome(
        failures: &[ErrorCategory],
        max_retries: u32,
    ) -> (usize, bool, Vec<DemandType>) {
        let mut demand_types = Vec::new();
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            demand_types.push(if attempt == 1 {
                DemandType::Normal
            } else {
                DemandType::Failure
            });
            let Some(&category) = failures.get((attempt - 1) as usize) else {
                return (attempt as usize, false, demand_types);
            };
            match category {
                ErrorCategory::Transient if attempt <= max_retries => {}
                _ => return (attempt as usize, true, demand_types),
            }
        }
    }

    fn property(raw_failures: Vec<u8>, max_retries_raw: u8) -> TestResult {
        // Bound both axes so each iteration's paused-clock retry loop stays
        // fast while still crossing the zero/non-zero and
        // exhausted/not-exhausted boundaries.
        let failures: Vec<ErrorCategory> = raw_failures
            .into_iter()
            .take(6)
            .map(|b| match b % 3 {
                0 => ErrorCategory::Transient,
                1 => ErrorCategory::Permanent,
                _ => ErrorCategory::Terminal,
            })
            .collect();
        let max_retries = u32::from(max_retries_raw % 4);
        let (expected_calls, expected_err, expected_demand_types) =
            expected_outcome(&failures, max_retries);

        let runtime = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build();
        let Ok(runtime) = runtime else {
            return TestResult::error("failed to build paused runtime");
        };
        runtime.block_on(async move {
            let handler = ScriptedHandler::failing_then_success(failures);
            let retry_handler = create_retry_handler(handler.clone(), max_retries);
            let context = MockEventContext::new();
            let message = match create_test_message() {
                Ok(message) => message,
                Err(error) => return TestResult::error(format!("create_test_message: {error}")),
            };

            let result =
                FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal)
                    .await;

            if result.is_err() != expected_err {
                return TestResult::error(format!(
                    "result.is_err()={} expected={expected_err}",
                    result.is_err(),
                ));
            }
            if handler.call_count() != expected_calls {
                return TestResult::error(format!(
                    "call_count={} expected={expected_calls}",
                    handler.call_count(),
                ));
            }
            let demand_types = handler.recorded_demand_types();
            if demand_types != expected_demand_types {
                return TestResult::error(format!(
                    "demand_types={demand_types:?} expected={expected_demand_types:?}",
                ));
            }
            TestResult::passed()
        })
    }

    QuickCheck::new().quickcheck(property as fn(Vec<u8>, u8) -> TestResult);
}

/// Named anchor: pins the exact "1 + `max_retries`" call count as a literal,
/// independent of `prop_retry_classification_arithmetic`'s model.
#[tokio::test]
async fn transient_error_fails_after_max_retries() -> Result<()> {
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let message = create_test_message()?;

    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_err(), "Should fail after max retries");
    // First attempt + 3 retries = 4 total calls
    assert_eq!(
        handler.call_count(),
        4,
        "Should attempt 1 + max_retries times"
    );
    Ok(())
}

// === Shutdown Tests ===

#[tokio::test]
async fn shutdown_during_retry_sleep_returns_error() -> Result<()> {
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
    // Use longer delays to give time for shutdown signal
    let retry_handler = RetryHandler {
        base_delay_millis: 1000, // 1 second base delay
        max_delay_millis: 10000,
        max_retries: 10,
        handler: handler.clone(),
    };
    let context = MockEventContext::new();
    let message = create_test_message()?;

    // Spawn the retry operation
    let ctx = context.clone();
    let handle = tokio::spawn(async move {
        FallibleHandler::on_message(&retry_handler, ctx, message, DemandType::Normal).await
    });

    // Wait a bit for the first failure and retry sleep to start
    tokio_sleep(Duration::from_millis(50)).await;

    // Signal shutdown
    context.request_shutdown();

    // Should complete quickly due to shutdown; the deadline is a hang-guard,
    // not the assertion.
    let Ok(join_result) = timeout(Duration::from_millis(500), handle).await else {
        bail!("retry loop did not observe the shutdown signal within 500ms");
    };
    let result = join_result?;

    assert!(result.is_err(), "Should return error on shutdown");
    Ok(())
}

// === Timer Path Smoke ===
//
// The timer path shares `RetryHandler::run` verbatim with the message path
// (`on_timer` differs only in what it invokes); the classification
// arithmetic is proved once above. This smoke just confirms the timer
// dispatch wires into that shared loop at all.

#[tokio::test]
async fn timer_transient_error_retries_then_succeeds() {
    let handler = ScriptedHandler::failing_then_success(vec![ErrorCategory::Transient]);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let trigger = create_test_trigger();

    let result =
        FallibleHandler::on_timer(&retry_handler, context, trigger, DemandType::Normal).await;

    assert!(result.is_ok());
    assert_eq!(handler.call_count(), 2);
}

// === Backoff Calculation Tests ===

#[test]
fn sleep_time_has_exponential_growth_with_jitter() {
    let handler = ScriptedHandler::success();
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
    let max_attempt_1 = samples_attempt_1.iter().max().copied().unwrap_or(u64::MAX);
    assert!(max_attempt_1 < 200, "Attempt 1 jitter should be < 200ms");

    // Attempt 3: exp_backoff = 2^3 * 100 = 800ms, jitter in [0, 800)
    let max_attempt_3 = samples_attempt_3.iter().max().copied().unwrap_or(u64::MAX);
    assert!(max_attempt_3 < 800, "Attempt 3 jitter should be < 800ms");

    // Verify there's some variation (jitter is working)
    let min_attempt_3 = samples_attempt_3.iter().min().copied().unwrap_or(u64::MAX);
    assert!(
        max_attempt_3 > min_attempt_3 + 50,
        "Jitter should introduce variation"
    );
}

#[test]
fn sleep_time_survives_zero_backoff() {
    // A sub-millisecond base delay truncates exp_backoff to 0; the jitter
    // bound must clamp instead of panicking on an empty range.
    let retry_handler = RetryHandler {
        base_delay_millis: 0,
        max_delay_millis: 0,
        max_retries: 10,
        handler: ScriptedHandler::success(),
    };

    assert_eq!(retry_handler.sleep_time(1), Duration::ZERO);
}

#[test]
fn sleep_time_capped_at_max_delay() {
    let handler = ScriptedHandler::success();
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
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 10);
    let context = MockEventContext::new();
    context.request_shutdown();

    let message = create_test_message()?;
    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_err());
    assert_eq!(handler.call_count(), 1);
    Ok(())
}

/// `FallibleHandler::on_timer` should abort on shutdown signal.
#[tokio::test]
async fn fallible_on_timer_shutdown_aborts() -> Result<()> {
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
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
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 10);
    let context = MockEventContext::new();
    context.request_shutdown();

    let tracker = create_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let message = create_test_message()?;
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
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
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
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    context.request_cancellation();

    let message = create_test_message()?;
    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_err());
    assert_eq!(handler.call_count(), 4); // 1 initial + 3 retries
    Ok(())
}

/// `FallibleHandler::on_timer` should continue retrying on cancellation.
#[tokio::test]
async fn fallible_on_timer_cancellation_retries() -> Result<()> {
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
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
    let handler = ScriptedHandler::failing_then_success(vec![
        ErrorCategory::Transient,
        ErrorCategory::Transient,
    ]);
    let retry_handler = create_retry_handler(handler.clone(), 10);
    let context = MockEventContext::new();
    context.request_cancellation();

    let tracker = create_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let message = create_test_message()?;
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
    let handler = ScriptedHandler::failing_then_success(vec![
        ErrorCategory::Transient,
        ErrorCategory::Transient,
    ]);
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
    let handler = ScriptedHandler::failing_then_success(vec![
        ErrorCategory::Transient,
        ErrorCategory::Transient,
    ]);
    let retry_handler = create_retry_handler(handler.clone(), 5);
    // Wrap in the FallibleEventHandler blanket impl by going through the
    // EventHandler path with a real durability marker. The blanket impl
    // is what fires the final attempt's apply hook on the outer
    // RetryHandler, which retry forwards to the inner.
    let context = MockEventContext::new();
    let tracker = create_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let message = create_test_message()?;
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
            ScriptedHook::Invoke(DemandType::Normal),
            ScriptedHook::AfterAbort(Err(ErrorCategory::Transient)),
            ScriptedHook::Invoke(DemandType::Failure),
            ScriptedHook::AfterAbort(Err(ErrorCategory::Transient)),
            ScriptedHook::Invoke(DemandType::Failure),
            ScriptedHook::AfterCommit(Ok(())),
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
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 2);
    let context = MockEventContext::new();
    let message = create_test_message()?;

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
            ScriptedHook::Invoke(DemandType::Normal),
            ScriptedHook::AfterAbort(Err(ErrorCategory::Transient)),
            ScriptedHook::Invoke(DemandType::Failure),
            ScriptedHook::AfterAbort(Err(ErrorCategory::Transient)),
            ScriptedHook::Invoke(DemandType::Failure),
            ScriptedHook::AfterCommit(Err(ErrorCategory::Transient)),
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
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
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
    let message = create_test_message()?;
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
            matches!(invoke, ScriptedHook::Invoke(_)),
            "pair {i} expected to start with Invoke, got {invoke:?}",
        );
        let is_last = i + 1 == events.len() / 2;
        if is_last {
            // The shutdown-abandoned final attempt must be paired with
            // exactly one `AfterAbort(Err(Transient))` from the outer
            // (NEVER `AfterCommit`, and NEVER duplicated).
            assert_eq!(
                apply,
                &ScriptedHook::AfterAbort(Err(ErrorCategory::Transient)),
                "final pair must be after_abort fired by the outer (not from the loop), got \
                 {apply:?}",
            );
        } else {
            // Intermediate (non-final) attempts get the loop's
            // between-attempts after_abort.
            assert_eq!(
                apply,
                &ScriptedHook::AfterAbort(Err(ErrorCategory::Transient)),
                "intermediate pair {i} expected after_abort, got {apply:?}",
            );
        }
    }
    Ok(())
}

// =========================================================================
// Attempt-boundary fencing over a real keyed-state session
// =========================================================================
//
// The retry loop's attempt boundary runs the `next_attempt` verb, whose
// `reset` transition discards the failed attempt's buffered dirty ops (under
// the session gate) and bumps the attempt epoch before the next attempt runs.
// Two things follow, both pinned here over a **real** session end to end
// through `settle` (every other retry test drives the inert `UnavailableState`
// session, whose `finalize` is always `Clean`):
//
//  * a later successful attempt stages only its own writes, and the boundary
//    records the event's marker exactly once, for the final attempt (the
//    isolation the discard once owned, now the reset's);
//  * the epoch fence: a hook that reads state after a (possibly nested) retry
//    sees the settled state via the settle stamp, while the intermediate
//    `after_abort` fired between attempts holds the EXPIRED pre-verb context
//    and its state ops error `Terminated`.

mod attempt_boundary {
    use super::*;
    use crate::codec::{JsonCodec, JsonCodecError};
    use crate::consumer::event_context::StateAccessError;
    use crate::consumer::middleware::tests::test_support::{
        RecordingOracle, RecordingSession, committed_value, recording_session,
    };
    use crate::consumer::middleware::{Settlement, SettlementHandler};
    use crate::state::descriptor::{CellStateError, Registered, ValueDescriptor, value_state};
    use crate::state::memory::MemoryCellStore;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::{EventRef, StateKey, StoreOutcome};
    use parking_lot::Mutex;
    use serde_json::{Value, json};
    use uuid::Uuid;

    /// What a keyed-state read observed inside an apply hook.
    #[derive(Debug, Clone, PartialEq)]
    enum ReadObs {
        /// The read succeeded, carrying the visible value.
        Value(Option<Value>),
        /// The read was fenced (`StateAccessError::Terminated`).
        Terminated,
        /// Any other error (fails the assertions that expect the two above).
        Other(String),
    }

    /// What a mid-handler `commit()` observed inside an apply hook.
    #[derive(Debug, Clone, PartialEq)]
    enum CommitObs {
        /// The commit ran, carrying its outcome.
        Outcome(StoreOutcome),
        /// The commit was fenced (`StateAccessError::Terminated`).
        Terminated,
        /// Any other error.
        Other(String),
    }

    fn classify_read(r: Result<Option<Value>, CellStateError<JsonCodecError>>) -> ReadObs {
        match r {
            Ok(value) => ReadObs::Value(value),
            Err(CellStateError::Access(StateAccessError::Terminated)) => ReadObs::Terminated,
            Err(other) => ReadObs::Other(format!("{other}")),
        }
    }

    fn classify_commit(r: Result<StoreOutcome, CellStateError<JsonCodecError>>) -> CommitObs {
        match r {
            Ok(outcome) => CommitObs::Outcome(outcome),
            Err(CellStateError::Access(StateAccessError::Terminated)) => CommitObs::Terminated,
            Err(other) => CommitObs::Other(format!("{other}")),
        }
    }

    /// The message's dedup id on the session `EventRef` — the one marker the
    /// boundary may record, once, for the final attempt.
    const MSG_DEDUP_ID: Uuid = Uuid::from_u128(0xA11);

    fn cart() -> ValueDescriptor {
        value_state::<JsonCodec>("cart")
    }

    fn wishlist() -> ValueDescriptor {
        value_state::<JsonCodec>("wishlist")
    }

    /// Fails attempt 1 (`Normal`) after staging `cart`; succeeds attempt 2
    /// (`Failure`) after staging `wishlist`. The two attempts touch
    /// **disjoint** collections, so a leaked attempt-1 write would surface
    /// as a committed `cart`.
    #[derive(Clone)]
    struct AttemptAwareHandler {
        calls: Arc<AtomicUsize>,
    }

    impl FallibleHandler for AttemptAwareHandler {
        type Error = TestError;
        type Output = ();
        type Payload = Value;

        async fn on_message<C>(
            &self,
            context: C,
            _message: ConsumerMessage<Self::Payload>,
            demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext<Payload = Self::Payload>,
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match demand_type {
                DemandType::Normal => {
                    let handle = context
                        .state(Registered::new(cart()))
                        .map_err(|_| TestError(ErrorCategory::Terminal))?;
                    handle
                        .set(json!({ "attempt": 1_i32 }))
                        .await
                        .map_err(|_| TestError(ErrorCategory::Terminal))?;
                    Err(TestError(ErrorCategory::Transient))
                }
                DemandType::Failure => {
                    let handle = context
                        .state(Registered::new(wishlist()))
                        .map_err(|_| TestError(ErrorCategory::Terminal))?;
                    handle
                        .set(json!({ "attempt": 2_i32 }))
                        .await
                        .map_err(|_| TestError(ErrorCategory::Terminal))?;
                    Ok(())
                }
            }
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

        async fn shutdown(self) {}
    }

    impl SettlementHandler for AttemptAwareHandler {
        fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
            Settlement::Final
        }
    }

    /// Retry over a real session: attempt 1 stages `cart` then fails
    /// Transient; the `next_attempt` verb's `reset` discards it (and bumps the
    /// epoch); attempt 2 stages `wishlist` and succeeds; `settle` then
    /// certifies only attempt 2's work and records the event's marker
    /// **exactly once** (a retried event yields one marker — the final
    /// attempt's). The committed state must show `wishlist` present and
    /// `cart` **absent**.
    ///
    /// Removing the reset's discard fails this: attempt 1's `cart` write would
    /// survive in the dirty overlay, `finalize` would stage it alongside
    /// `wishlist`, and the read-back would find `cart` committed to attempt 1's
    /// value — precisely the leak the reset exists to prevent.
    #[tokio::test]
    async fn retry_isolates_dirty_between_attempts_and_records_one_marker() -> Result<()> {
        let mut registry = CollectionDefRegistry::default();
        registry.register(&cart(), CollectionDef::new(None))?;
        registry.register(&wishlist(), CollectionDef::new(None))?;
        let state_key = StateKey::new(Uuid::from_u128(0xE), Arc::from("user-1"));
        let (session, cell_store, _dirty, recorded) = recording_session(
            registry,
            state_key.clone(),
            EventRef::Message {
                dedup_id: MSG_DEDUP_ID,
            },
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = AttemptAwareHandler {
            calls: calls.clone(),
        };
        let retry_handler = create_retry_handler(handler, 10);
        let context = MockEventContext::new().with_session(session);

        let tracker = create_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let message = create_test_message()?;
        let uncommitted_message = message.into_uncommitted(uncommitted_offset);

        EventHandler::on_message(
            &retry_handler,
            context,
            uncommitted_message,
            DemandType::Normal,
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "one failed then one ok attempt"
        );
        assert_eq!(
            committed_value(&cell_store, state_key.clone(), "wishlist").await?,
            Some(json!({ "attempt": 2_i32 })),
            "attempt 2's write must be committed",
        );
        assert_eq!(
            committed_value(&cell_store, state_key.clone(), "cart").await?,
            None,
            "attempt 1's discarded write must NOT leak into the committed state",
        );
        assert_eq!(
            recorded.lock().clone(),
            vec![MSG_DEDUP_ID],
            "a retried event records exactly one marker — the final attempt's",
        );
        assert_eq!(
            tracker.shutdown().await,
            Some(0),
            "the offset commits after the successful retry",
        );
        Ok(())
    }

    // --- (j) hook-view arms: the settle stamp and the expired intermediate ---

    /// Fails attempt 1 (`Normal`) Transient; on attempt 2 (`Failure`) stages
    /// `wishlist = {attempt:2}` and succeeds. Its `after_commit` re-reads
    /// `wishlist` through the (stamped) hook context and records the outcome.
    #[derive(Clone)]
    struct FinalHookReadHandler {
        read: Arc<Mutex<Option<ReadObs>>>,
    }

    impl FallibleHandler for FinalHookReadHandler {
        type Error = TestError;
        type Output = ();
        type Payload = Value;

        async fn on_message<C>(
            &self,
            context: C,
            _message: ConsumerMessage<Self::Payload>,
            demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext<Payload = Self::Payload>,
        {
            match demand_type {
                DemandType::Normal => Err(TestError(ErrorCategory::Transient)),
                DemandType::Failure => {
                    let handle = context
                        .state(Registered::new(wishlist()))
                        .map_err(|_| TestError(ErrorCategory::Terminal))?;
                    handle
                        .set(json!({ "attempt": 2_i32 }))
                        .await
                        .map_err(|_| TestError(ErrorCategory::Terminal))?;
                    Ok(())
                }
            }
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

        async fn after_commit<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
        where
            C: EventContext<Payload = Self::Payload>,
        {
            let obs = match context.state(Registered::new(wishlist())) {
                Ok(handle) => classify_read(handle.get().await),
                Err(e) => ReadObs::Other(format!("bind: {e}")),
            };
            *self.read.lock() = Some(obs);
        }

        async fn shutdown(self) {}
    }

    impl SettlementHandler for FinalHookReadHandler {
        fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
            Settlement::Final
        }
    }

    /// Builds the recording-session fixture and mock context the hook-view
    /// pins share; returns the pieces they assert on.
    fn hook_fixture(
        handler_registry: impl FnOnce(&mut CollectionDefRegistry) -> Result<()>,
    ) -> Result<(
        MockEventContext<Value, RecordingSession>,
        MemoryCellStore<RecordingOracle>,
        StateKey,
    )> {
        let mut registry = CollectionDefRegistry::default();
        handler_registry(&mut registry)?;
        let state_key = StateKey::new(Uuid::from_u128(0xF00), Arc::from("user-1"));
        let (session, cell_store, _dirty, _recorded) = recording_session(
            registry,
            state_key.clone(),
            EventRef::Message {
                dedup_id: MSG_DEDUP_ID,
            },
        );
        Ok((
            MockEventContext::new().with_session(session),
            cell_store,
            state_key,
        ))
    }

    /// (j-final), simple: after one retry the final `after_commit` reads
    /// attempt 2's `wishlist` — the post-settle read contract survives a
    /// retry. (The final context is already current here, so this arm is the
    /// green baseline; the stamp's load-bearing case is the nested arm.)
    #[tokio::test]
    async fn final_hook_reads_settled_state_after_retry() -> Result<()> {
        let read = Arc::new(Mutex::new(None));
        let handler = FinalHookReadHandler { read: read.clone() };
        let retry_handler = create_retry_handler(handler, 10);
        let (context, cell_store, state_key) =
            hook_fixture(|r| Ok(r.register(&wishlist(), CollectionDef::new(None))?))?;

        let tracker = create_offset_tracker();
        let uncommitted = create_test_message()?.into_uncommitted(tracker.take(0).await?);
        EventHandler::on_message(&retry_handler, context, uncommitted, DemandType::Normal).await;

        assert_eq!(
            read.lock().clone(),
            Some(ReadObs::Value(Some(json!({ "attempt": 2_i32 })))),
            "the final after_commit reads attempt 2's committed wishlist",
        );
        assert_eq!(
            committed_value(&cell_store, state_key, "wishlist").await?,
            Some(json!({ "attempt": 2_i32 })),
            "wishlist committed",
        );
        let _ = tracker.shutdown().await;
        Ok(())
    }

    /// (j-final), nested: an inner retry bumps the epoch during the outer
    /// attempt, leaving the outer's final context pinned stale; the settle
    /// stamp re-pins it current so `after_commit`'s read still sees the
    /// settled `wishlist`. Dropping the `redispatch` stamp in `fire_apply_hook`
    /// makes this read `Terminated` (the stale pin no longer matches the
    /// inner-bumped epoch).
    #[tokio::test]
    async fn final_hook_reads_settled_state_under_nested_retry() -> Result<()> {
        let read = Arc::new(Mutex::new(None));
        let handler = FinalHookReadHandler { read: read.clone() };
        // Inner retry (FallibleHandler) drives the attempts and bumps the
        // shared epoch; the outer retry (EventHandler) settles on its own
        // attempt-1 context, now stale relative to the inner's bumps.
        let nested = create_retry_handler(create_retry_handler(handler, 10), 10);
        let (context, cell_store, state_key) =
            hook_fixture(|r| Ok(r.register(&wishlist(), CollectionDef::new(None))?))?;

        let tracker = create_offset_tracker();
        let uncommitted = create_test_message()?.into_uncommitted(tracker.take(0).await?);
        EventHandler::on_message(&nested, context, uncommitted, DemandType::Normal).await;

        assert_eq!(
            read.lock().clone(),
            Some(ReadObs::Value(Some(json!({ "attempt": 2_i32 })))),
            "the stamp re-pins the final hook current, so its read sees the settled wishlist \
             despite the inner retry's epoch bump",
        );
        assert_eq!(
            committed_value(&cell_store, state_key, "wishlist").await?,
            Some(json!({ "attempt": 2_i32 })),
            "wishlist committed",
        );
        let _ = tracker.shutdown().await;
        Ok(())
    }

    /// Fails attempts until `succeed_on`, staging `cart = {attempt:n}` each
    /// attempt; its intermediate `after_abort` (fired between attempts with the
    /// EXPIRED pre-verb context) tries to read and `commit()` `cart`, recording
    /// both observations.
    #[derive(Clone)]
    struct IntermediateHookHandler {
        calls: Arc<AtomicUsize>,
        succeed_on: usize,
        reads: Arc<Mutex<Vec<ReadObs>>>,
        commits: Arc<Mutex<Vec<CommitObs>>>,
    }

    impl FallibleHandler for IntermediateHookHandler {
        type Error = TestError;
        type Output = ();
        type Payload = Value;

        async fn on_message<C>(
            &self,
            context: C,
            _message: ConsumerMessage<Self::Payload>,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext<Payload = Self::Payload>,
        {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let handle = context
                .state(Registered::new(cart()))
                .map_err(|_| TestError(ErrorCategory::Terminal))?;
            handle
                .set(json!({ "attempt": n as i32 }))
                .await
                .map_err(|_| TestError(ErrorCategory::Terminal))?;
            if n >= self.succeed_on {
                Ok(())
            } else {
                Err(TestError(ErrorCategory::Transient))
            }
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

        async fn after_abort<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
        where
            C: EventContext<Payload = Self::Payload>,
        {
            let read = match context.state(Registered::new(cart())) {
                Ok(handle) => classify_read(handle.get().await),
                Err(e) => ReadObs::Other(format!("bind: {e}")),
            };
            self.reads.lock().push(read);
            let commit = match context.state(Registered::new(cart())) {
                Ok(handle) => classify_commit(handle.commit().await),
                Err(e) => CommitObs::Other(format!("bind: {e}")),
            };
            self.commits.lock().push(commit);
        }

        async fn shutdown(self) {}
    }

    impl SettlementHandler for IntermediateHookHandler {
        fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
            Settlement::Final
        }
    }

    /// (j-intermediate): the between-attempts `after_abort` holds the EXPIRED
    /// pre-verb context (pinned at the failed attempt, the epoch already
    /// bumped), so both its `get()` and its `commit()` error `Terminated` and
    /// its `commit()` has zero durable effect — a mid-loop commit of the failed
    /// attempt's overlay is impossible. Covers the N≥3 case (attempts 1 and 2
    /// fail, so two intermediate hooks fire). Passing the LIVE post-verb
    /// context to the hook instead of the expired clone makes the reads
    /// succeed (`Value`, not `Terminated`), failing this pin.
    #[tokio::test]
    async fn intermediate_hook_is_state_dead() -> Result<()> {
        let reads = Arc::new(Mutex::new(Vec::new()));
        let commits = Arc::new(Mutex::new(Vec::new()));
        let handler = IntermediateHookHandler {
            calls: Arc::new(AtomicUsize::new(0)),
            succeed_on: 3,
            reads: reads.clone(),
            commits: commits.clone(),
        };
        let retry_handler = create_retry_handler(handler, 10);
        let (context, cell_store, state_key) =
            hook_fixture(|r| Ok(r.register(&cart(), CollectionDef::new(None))?))?;

        let tracker = create_offset_tracker();
        let uncommitted = create_test_message()?.into_uncommitted(tracker.take(0).await?);
        EventHandler::on_message(&retry_handler, context, uncommitted, DemandType::Normal).await;

        assert_eq!(
            reads.lock().clone(),
            vec![ReadObs::Terminated, ReadObs::Terminated],
            "both intermediate after_abort reads are fenced (N>=3: two hooks fire)",
        );
        assert_eq!(
            commits.lock().clone(),
            vec![CommitObs::Terminated, CommitObs::Terminated],
            "both intermediate after_abort commits are fenced",
        );
        assert_eq!(
            committed_value(&cell_store, state_key, "cart").await?,
            Some(json!({ "attempt": 3_i32 })),
            "only the successful attempt 3's cart commits; the fenced intermediate commits added \
             nothing durable",
        );
        let _ = tracker.shutdown().await;
        Ok(())
    }
}
