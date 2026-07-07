//! Integration tests for defer middleware using the test harness.
//!
//! These tests verify specific behavioral scenarios of the
//! `MessageDeferHandler` middleware using deterministic traces, complementing
//! the property-based tests in `properties.rs`.

use super::MockEventContext;
use super::TEST_RUNTIME;
use super::context::{KeyedCapturingContext, TimerCapture};
use super::handler::{HandlerOutcome, OutcomeHandler};
use super::harness::TestHarness;
use super::types::{MessageEvent, MessageOutcome, TimerEvent, TimerOutcome};
use crate::Offset;
use crate::consumer::DemandType;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::cancellation::CancellationHandler;
use crate::consumer::middleware::defer::DeferConfiguration;
use crate::consumer::middleware::defer::decider::TraceBasedDecider;
use crate::consumer::middleware::defer::message::handler::MessageDeferHandler;
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::consumer::middleware::defer::message::store::memory::MemoryMessageDeferStore;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::telemetry::Telemetry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger};
use crate::tracing::init_test_logging;
use crate::{Key, Partition, Topic};
use color_eyre::eyre::{bail, eyre};
use std::sync::Arc;
use std::time::Duration;

/// Defers offset 1 on key 0 with a 60-second transient backoff.
async fn defer_single_message(harness: &mut TestHarness) -> color_eyre::Result<()> {
    let msg = MessageEvent {
        key_idx: 0,
        offset: Offset::from(1_i64),
        outcome: MessageOutcome::Transient {
            max_backoff: CompactDuration::new(60),
            defer: true,
        },
    };
    harness.execute_message(&msg).await
}

/// Defers offset 1 on key 0, then queues offset 2 behind it.
async fn defer_then_queue_second_message(harness: &mut TestHarness) -> color_eyre::Result<()> {
    defer_single_message(harness).await?;

    let msg2 = MessageEvent {
        key_idx: 0,
        offset: Offset::from(2_i64),
        outcome: MessageOutcome::Queued,
    };
    harness.execute_message(&msg2).await
}

#[test]
fn simple_defer_and_retry_succeeds() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1)?;

        // Message arrives and is deferred
        defer_single_message(&mut harness).await?;

        // Key should be deferred
        let retry_count = harness
            .get_retry_count(0)
            .await?
            .ok_or_else(|| eyre!("expected key to be deferred"))?;
        assert_eq!(retry_count, 0);

        // Timer should be active
        let key = harness.key(0).clone();
        assert!(harness.capture().has_active_timer(&key));

        // Timer fires successfully
        let timer = TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Success,
        };
        harness.execute_timer(&timer).await?;

        // Key should not be deferred
        let retry_count = harness.get_retry_count(0).await?;
        assert!(retry_count.is_none());

        // Timer should be cleared
        assert!(!harness.capture().has_active_timer(&key));

        Ok(())
    })
}

#[test]
fn queues_messages_while_key_deferred() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1)?;

        // First message defers, second queues behind it
        defer_then_queue_second_message(&mut harness).await?;

        // Timer fires for first message
        let timer1 = TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Success,
        };
        harness.execute_timer(&timer1).await?;

        // Key should still be deferred (has second message)
        let retry_count = harness.get_retry_count(0).await?;
        assert!(retry_count.is_some());

        // Timer should still be active
        let key = harness.key(0).clone();
        assert!(harness.capture().has_active_timer(&key));

        Ok(())
    })
}

#[test]
fn increments_retry_count_on_transient_failure() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1)?;

        // Message defers
        defer_single_message(&mut harness).await?;

        // Initial retry count is 0
        let retry_count = harness
            .get_retry_count(0)
            .await?
            .ok_or_else(|| eyre!("expected key to be deferred"))?;
        assert_eq!(retry_count, 0);

        // Timer fires with transient failure
        let timer = TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Transient {
                max_backoff: CompactDuration::new(120),
            },
        };
        harness.execute_timer(&timer).await?;

        // Retry count should be incremented
        let retry_count = harness
            .get_retry_count(0)
            .await?
            .ok_or_else(|| eyre!("expected key to still be deferred"))?;
        assert_eq!(retry_count, 1);

        Ok(())
    })
}

/// Root-cause pin for a flaky `tests/defer_middleware.rs` quiet-window
/// failure: after a message's immediate retry fails transiently, its next
/// dispatch is driven by exactly **one** standing retry timer scheduled no
/// sooner than the base backoff. The integration test's former 500ms
/// "expect no event" window started at event-*drain* time while this timer
/// arms at *failure* time, so under load the (legal) backoff retry landed
/// inside the window — a timing race, not a duplicate dispatch. This test
/// replays that scenario with manually driven dispatch (no clock in the
/// loop) and pins both halves: the retry timer is a `clear_and_schedule`
/// singleton (a plain `schedule` regression would leave two timers here),
/// and its fire time respects the backoff floor that dooms any sub-backoff
/// quiet window.
#[test]
fn redeferred_retry_is_a_single_timer_at_the_backoff_floor() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1)?;
        let key = harness.key(0).clone();

        // Attempt 1 fails transiently and defers (retry_count=0 → an
        // immediate retry timer).
        defer_single_message(&mut harness).await?;
        assert_eq!(harness.capture().key_timer_count(&key), 1);

        // The immediate retry fires and fails transiently again.
        let before_refire = CompactDateTime::now()?;
        let timer = TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Transient {
                max_backoff: CompactDuration::new(60),
            },
        };
        harness.execute_timer(&timer).await?;

        // Re-deferred at retry_count=1 with exactly one standing timer...
        let retry_count = harness
            .get_retry_count(0)
            .await?
            .ok_or_else(|| eyre!("expected key to remain deferred"))?;
        assert_eq!(retry_count, 1);
        assert_eq!(
            harness.capture().key_timer_count(&key),
            1,
            "the retry timer must be a singleton"
        );

        // ...scheduled no sooner than the base backoff after the failure:
        // the third dispatch cannot legally occur before this fire time.
        let fire = harness
            .capture()
            .get_timer_time(&key)
            .ok_or_else(|| eyre!("expected a standing retry timer"))?;
        let floor =
            before_refire.add_duration(CompactDuration::new(super::TEST_BASE_BACKOFF_SECS))?;
        assert!(
            fire >= floor,
            "retry fire {fire} must respect the backoff floor {floor}"
        );

        // The backoff retry succeeds and fully drains the key.
        let timer = TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Success,
        };
        harness.execute_timer(&timer).await?;
        assert!(harness.get_retry_count(0).await?.is_none());
        assert_eq!(harness.capture().key_timer_count(&key), 0);

        // Exactly the three trace-driven dispatches reached the inner
        // handler — dispatch is test-controlled, so nothing fires between
        // the trace events.
        assert_eq!(harness.processed_messages().len(), 3);
        Ok(())
    })
}

/// Tests that transient errors are ALWAYS re-deferred, regardless of decider
/// state.
///
/// This is critical for maintaining ordering invariants: once a message is
/// committed to the defer queue, it cannot be dropped on transient failure. The
/// decider only gates *initial* deferral, not re-deferral of already-queued
/// messages.
#[test]
fn transient_errors_always_redeferred_ignoring_decider() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1)?;

        // First message defers, second queues behind it
        defer_then_queue_second_message(&mut harness).await?;

        let key = harness.key(0).clone();

        // Verify both messages are deferred and timer is active
        assert!(harness.capture().has_active_timer(&key));
        let state = harness
            .store()
            .get_next_deferred_message(&key)
            .await?
            .ok_or_else(|| eyre!("expected a deferred message"))?;
        assert_eq!(state.0, Offset::from(1_i64));
        assert_eq!(state.1, 0, "Initial retry count should be 0");

        // Set decider to false - this should NOT affect re-deferral
        harness.decider.set_next(false);
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);

        // Create context and trigger manually
        let trigger_time = harness
            .capture()
            .get_timer_time(&key)
            .ok_or_else(|| eyre!("expected an active timer for the deferred key"))?;
        let key_context = KeyedCapturingContext::new(key.clone(), harness.capture().clone());
        let trigger = Trigger::for_testing(key.clone(), trigger_time, TimerType::DeferredMessage);

        // Transient failures are ALWAYS re-deferred (decider is ignored for retry path)
        let result = harness
            .handler
            .on_timer(key_context, trigger, DemandType::Normal)
            .await;

        // Should succeed (re-deferred) even though decider returned false
        assert!(
            result.is_ok(),
            "Transient errors must always be re-deferred"
        );

        // Timer should still be active for the SAME message (re-deferred)
        assert!(
            harness.capture().has_active_timer(&key),
            "Timer should be rescheduled for re-deferred message"
        );

        // Store should still show offset 1 (NOT advanced to 2) with incremented retry
        // count
        let next_state = harness.store().get_next_deferred_message(&key).await?;
        assert_eq!(
            next_state,
            Some((Offset::from(1_i64), 1)),
            "Same message should be re-deferred with retry_count incremented to 1"
        );

        Ok(())
    })
}

/// Tests that permanent error on timer retry schedules timer for next queued
/// message.
#[test]
fn permanent_error_schedules_timer_for_next_message() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1)?;

        // First message defers, second queues behind it
        defer_then_queue_second_message(&mut harness).await?;

        let key = harness.key(0).clone();

        // Timer fires with permanent failure
        let timer = TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Permanent,
        };
        harness.execute_timer(&timer).await?;

        // Timer should still be active for the next queued message
        assert!(
            harness.capture().has_active_timer(&key),
            "Timer should be scheduled for next queued message after permanent error"
        );

        // Store should show offset 2 as next
        let next_state = harness.store().get_next_deferred_message(&key).await?;
        assert_eq!(
            next_state,
            Some((Offset::from(2_i64), 0)),
            "Next message should be offset 2 with retry_count reset to 0"
        );

        Ok(())
    })
}

// ============================================================================
// Shutdown proof tests
//
// These tests verify the composition: cancellation middleware (inside) +
// defer middleware (outside). On shutdown, cancellation converts Transient
// errors to Terminal so defer's existing guard prevents any store write.
// ============================================================================

/// The stacked handler type used in shutdown proof tests:
/// defer(cancellation(inner)), mirroring production middleware ordering.
type ShutdownProofHandler = MessageDeferHandler<
    CancellationHandler<OutcomeHandler>,
    MemoryMessageDeferStore,
    MemoryLoader<serde_json::Value>,
    TraceBasedDecider,
>;

/// Builds a [`ShutdownProofHandler`] for shutdown proof tests.
///
/// Returns `(handler, store, loader, inner_handler, capture, decider)` so
/// callers can control outcomes and inspect store state after each call.
fn build_shutdown_proof_stack(
    topic: Topic,
    partition: Partition,
) -> color_eyre::Result<(
    ShutdownProofHandler,
    MemoryMessageDeferStore,
    MemoryLoader<serde_json::Value>,
    OutcomeHandler,
    TimerCapture,
    TraceBasedDecider,
)> {
    let inner = OutcomeHandler::new();
    let cancellation = CancellationHandler::new(inner.clone());
    let store = MemoryMessageDeferStore::new();
    let loader = MemoryLoader::new();
    let capture = TimerCapture::new();
    let decider = TraceBasedDecider::new();

    let config = DeferConfiguration::builder()
        .base(Duration::from_secs(1))
        .max_delay(Duration::from_hours(1))
        .failure_threshold(0.9_f64)
        .build()?;

    let telemetry = Telemetry::new();
    let sender = telemetry.partition_sender(topic, partition);

    let handler = MessageDeferHandler {
        handler: cancellation,
        loader: loader.clone(),
        store: store.clone(),
        decider: decider.clone(),
        config,
        topic,
        partition,
        sender,
        source: Arc::from("test"),
    };

    Ok((handler, store, loader, inner, capture, decider))
}

/// Stores a test payload in the loader and builds the matching consumer
/// message.
fn seed_message(
    loader: &MemoryLoader<serde_json::Value>,
    topic: Topic,
    partition: Partition,
    offset: Offset,
    key: &Key,
) -> color_eyre::Result<ConsumerMessage<serde_json::Value>> {
    let payload = serde_json::json!({"test": true});
    loader.store_message(topic, partition, offset, key.clone(), payload.clone());
    ConsumerMessage::for_testing(topic, partition, offset, key.clone(), payload)
}

/// Proves that a transient handler error is NOT deferred when the partition
/// is shutting down.
///
/// The cancellation middleware (inside defer) converts Transient → Terminal on
/// shutdown. Defer's existing guard (`if !Transient { return Err }`) then
/// propagates the error without writing to the store, preventing tombstones.
#[test]
fn transient_error_not_deferred_on_shutdown() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);
        let key: Key = Arc::from("test-key");

        let (handler, store, loader, inner, _capture, decider) =
            build_shutdown_proof_stack(topic, partition)?;

        let message = seed_message(&loader, topic, partition, Offset::from(1_i64), &key)?;

        inner.set_outcome(HandlerOutcome::Transient);
        decider.set_next(true); // decider would allow deferral if not for shutdown

        // Shutdown is signaled by the inner handler mid-execution, not before
        // the call. This exercises the post-call promotion path in
        // CancellationHandler rather than the pre-call short-circuit.
        let ctx = MockEventContext::new();
        inner.set_shutdown_trigger(ctx.clone());

        let result = handler.on_message(ctx, message, DemandType::Normal).await;

        // Error must propagate; the middleware must not absorb it.
        let Err(err) = result else {
            bail!("expected on_message to fail during shutdown, got Ok");
        };

        // Must classify as Terminal so FallibleEventHandler aborts the offset,
        // letting the incoming consumer replay the message after rebalance.
        assert!(
            matches!(err.classify_error(), ErrorCategory::Terminal),
            "Shutdown-aborted deferral must be Terminal so the offset is aborted"
        );

        // No row must appear in the defer store.
        let deferred = store.is_deferred(&key).await?;
        assert!(
            deferred.is_none(),
            "Key must not be deferred when shutdown is in progress"
        );

        Ok(())
    })
}

/// Proves that a transient handler error during timer retry is NOT re-deferred
/// when the partition is shutting down.
///
/// The message is already committed to the defer store from a prior deferral.
/// On retry, if the handler fails transiently while shutdown is active, the
/// cancellation middleware converts the error to Terminal. Defer's retry path
/// propagates Terminal errors without modifying the store, so `retry_count` is
/// unchanged and no timer is rescheduled — the existing row stays in place for
/// the new consumer to pick up after rebalance.
#[test]
fn transient_retry_not_redeferred_on_shutdown() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);
        let key: Key = Arc::from("test-key");

        let (handler, store, loader, inner, capture, decider) =
            build_shutdown_proof_stack(topic, partition)?;

        // Pre-store message so the loader can serve it during timer retry.
        let message = seed_message(&loader, topic, partition, Offset::from(1_i64), &key)?;

        // --- Step 1: Defer normally (no shutdown) ---
        inner.set_outcome(HandlerOutcome::Transient);
        decider.set_next(true);

        let key_ctx = KeyedCapturingContext::new(key.clone(), capture.clone());
        handler
            .on_message(key_ctx, message, DemandType::Normal)
            .await?;

        // Confirm deferred with retry_count == 0.
        let retry_count_before = store
            .is_deferred(&key)
            .await?
            .ok_or_else(|| eyre!("expected key to be deferred"))?;
        assert_eq!(retry_count_before, 0);

        // --- Step 2: Fire timer, triggering shutdown mid-execution ---
        // Shutdown is signaled by the inner handler during the retry call,
        // exercising the post-call promotion path in CancellationHandler.
        let trigger_time = capture
            .get_timer_time(&key)
            .ok_or_else(|| eyre!("expected an active timer for the deferred key"))?;
        inner.set_outcome(HandlerOutcome::Transient);

        let ctx = MockEventContext::new();
        inner.set_shutdown_trigger(ctx.clone());

        let trigger = Trigger::for_testing(key.clone(), trigger_time, TimerType::DeferredMessage);
        let result = handler.on_timer(ctx, trigger, DemandType::Normal).await;

        // Error must propagate as Terminal; no re-deferral must happen.
        let Err(err) = result else {
            bail!("expected on_timer to fail during shutdown, got Ok");
        };
        assert!(
            matches!(err.classify_error(), ErrorCategory::Terminal),
            "Shutdown-aborted re-deferral must be Terminal so the timer is aborted"
        );

        // Retry count must be unchanged — no increment_retry_count call.
        let state_after = store.get_next_deferred_message(&key).await?;
        assert_eq!(
            state_after,
            Some((Offset::from(1_i64), 0)),
            "Retry count must not be incremented during shutdown"
        );

        Ok(())
    })
}
