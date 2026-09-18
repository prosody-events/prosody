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
use tracing::subscriber::with_default;

/// Defers offset 1 on key 0.
async fn defer_single_message(harness: &mut TestHarness) -> color_eyre::Result<()> {
    let msg = MessageEvent {
        fault: None,
        key_idx: 0,
        offset: Offset::from(1_i64),
        outcome: MessageOutcome::Transient { defer: true },
    };
    harness.execute_message(&msg).await
}

/// Defers offset 1 on key 0, then queues offset 2 behind it.
async fn defer_then_queue_second_message(harness: &mut TestHarness) -> color_eyre::Result<()> {
    defer_single_message(harness).await?;

    let msg2 = MessageEvent {
        fault: None,
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
            fault: None,
            key_idx: 0,
            outcome: TimerOutcome::Success,
        };
        harness.execute_timer(&timer, DemandType::Normal).await?;

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
            fault: None,
            key_idx: 0,
            outcome: TimerOutcome::Success,
        };
        harness.execute_timer(&timer1, DemandType::Normal).await?;

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
            fault: None,
            key_idx: 0,
            outcome: TimerOutcome::Transient,
        };
        harness.execute_timer(&timer, DemandType::Normal).await?;

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
        let mut calls = harness.passes[0].calls.len();
        assert_eq!(harness.capture().key_timer_count(&key), 1);

        // The immediate retry fires and fails transiently again.
        let before_refire = CompactDateTime::now()?;
        let timer = TimerEvent {
            fault: None,
            key_idx: 0,
            outcome: TimerOutcome::Transient,
        };
        harness.execute_timer(&timer, DemandType::Normal).await?;
        calls += harness.passes[0].calls.len();

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
            fault: None,
            key_idx: 0,
            outcome: TimerOutcome::Success,
        };
        harness.execute_timer(&timer, DemandType::Normal).await?;
        calls += harness.passes[0].calls.len();
        assert!(harness.get_retry_count(0).await?.is_none());
        assert_eq!(harness.capture().key_timer_count(&key), 0);

        // Exactly the three trace-driven dispatches reached the inner
        // handler — dispatch is test-controlled, so nothing fires between
        // the trace events.
        assert_eq!(calls, 3);
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
            fault: None,
            key_idx: 0,
            outcome: TimerOutcome::Permanent,
        };
        harness.execute_timer(&timer, DemandType::Normal).await?;

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

#[test]
fn retried_message_handler_runs_inside_the_load_span() -> color_eyre::Result<()> {
    // The message-defer retry dispatch instruments the inner call with the
    // reloaded message's span, so a retried handler observes it as the
    // ambient span (`Span::current()`). A registry is installed so spans get
    // real ids — the `is_some` guard fails, rather than passing vacuously,
    // if spans are disabled.
    with_default(tracing_subscriber::registry(), || {
        TEST_RUNTIME.block_on(async {
            let mut harness = TestHarness::new(1)?;

            defer_single_message(&mut harness).await?;

            let timer = TimerEvent {
                fault: None,
                key_idx: 0,
                outcome: TimerOutcome::Success,
            };
            harness.execute_timer(&timer, DemandType::Normal).await?;

            // The second dispatch is the retry: its ambient span must be the
            // reloaded message's own span, by id.
            let pairs = harness.inner_handler.ambient_pairs();
            let (ambient, load) = pairs
                .get(1)
                .ok_or_else(|| eyre!("retry dispatch was not recorded"))?;
            assert!(ambient.is_some(), "spans must be enabled for this pin");
            assert_eq!(
                ambient, load,
                "retried handler must run inside the reloaded message's span"
            );

            Ok(())
        })
    })
}

mod shutdown;
