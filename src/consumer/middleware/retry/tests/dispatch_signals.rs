use super::*;

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
use crate::consumer::receipted_sealed;
use crate::consumer::{Keyed, Receipted, ReceiptedSource, Redelivery, Uncommitted};
use crate::timers::UncommittedTimer;
use color_eyre::eyre::{Result, bail};
use crossbeam_utils::CachePadded;
use std::future::ready;

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

impl receipted_sealed::Sealed for MockCommitGuard {}

impl Receipted for MockCommitGuard {
    type Source = Self;

    fn redelivery(&self) -> impl Future<Output = Redelivery> + Send {
        ready(Redelivery::Sweeps)
    }

    fn receipt(self) -> impl Future<Output = Self::Source> + Send {
        ready(self)
    }
}

impl ReceiptedSource for MockCommitGuard {
    async fn retire(self) {
        self.commit().await;
    }

    async fn keep(self) {}
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

pub(super) fn create_offset_tracker() -> OffsetTracker {
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
