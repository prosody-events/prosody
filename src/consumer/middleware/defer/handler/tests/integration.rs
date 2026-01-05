//! Integration tests for defer middleware using the test harness.
//!
//! These tests verify specific behavioral scenarios of the
//! `MessageDeferHandler` middleware using deterministic traces, complementing
//! the property-based tests in `properties.rs`.

use super::TEST_RUNTIME;
use super::harness::TestHarness;
use super::types::{MessageEvent, MessageOutcome, TimerEvent, TimerOutcome};
use crate::Offset;
use crate::consumer::DemandType;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::defer::handler::tests::context::KeyedCapturingContext;
use crate::consumer::middleware::defer::store::MessageDeferStore;
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger};
use crate::tracing::init_test_logging;

#[test]
fn simple_defer_and_retry_succeeds() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1).ok()?;

        // Message arrives and is deferred
        let msg = MessageEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: MessageOutcome::Transient {
                max_backoff: CompactDuration::new(60),
                defer: true,
            },
        };
        harness.execute_message(&msg).await.ok()?;

        // Key should be deferred
        let retry_count = harness.get_retry_count(0).await.ok()??;
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
        harness.execute_timer(&timer).await.ok()?;

        // Key should not be deferred
        let retry_count = harness.get_retry_count(0).await.ok()?;
        assert!(retry_count.is_none());

        // Timer should be cleared
        assert!(!harness.capture().has_active_timer(&key));

        Some(())
    });
}

#[test]
fn queues_messages_while_key_deferred() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1).ok()?;

        // First message defers
        let msg1 = MessageEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: MessageOutcome::Transient {
                max_backoff: CompactDuration::new(60),
                defer: true,
            },
        };
        harness.execute_message(&msg1).await.ok()?;

        // Second message queues
        let msg2 = MessageEvent {
            key_idx: 0,
            offset: Offset::from(2_i64),
            outcome: MessageOutcome::Queued,
        };
        harness.execute_message(&msg2).await.ok()?;

        // Timer fires for first message
        let timer1 = TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Success,
        };
        harness.execute_timer(&timer1).await.ok()?;

        // Key should still be deferred (has second message)
        let retry_count = harness.get_retry_count(0).await.ok()?;
        assert!(retry_count.is_some());

        // Timer should still be active
        let key = harness.key(0).clone();
        assert!(harness.capture().has_active_timer(&key));

        Some(())
    });
}

#[test]
fn increments_retry_count_on_transient_failure() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1).ok()?;

        // Message defers
        let msg = MessageEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: MessageOutcome::Transient {
                max_backoff: CompactDuration::new(60),
                defer: true,
            },
        };
        harness.execute_message(&msg).await.ok()?;

        // Initial retry count is 0
        let retry_count = harness.get_retry_count(0).await.ok()??;
        assert_eq!(retry_count, 0);

        // Timer fires with transient failure
        let timer = TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Transient {
                max_backoff: CompactDuration::new(120),
            },
        };
        harness.execute_timer(&timer).await.ok()?;

        // Retry count should be incremented
        let retry_count = harness.get_retry_count(0).await.ok()??;
        assert_eq!(retry_count, 1);

        Some(())
    });
}

/// Tests that transient errors are ALWAYS re-deferred, regardless of decider
/// state.
///
/// This is critical for maintaining ordering invariants: once a message is
/// committed to the defer queue, it cannot be dropped on transient failure. The
/// decider only gates *initial* deferral, not re-deferral of already-queued
/// messages.
#[test]
fn transient_errors_always_redeferred_ignoring_decider() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1).ok()?;

        // First message defers
        let msg1 = MessageEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: MessageOutcome::Transient {
                max_backoff: CompactDuration::new(60),
                defer: true,
            },
        };
        harness.execute_message(&msg1).await.ok()?;

        // Second message queues (arrives while first is deferred)
        let msg2 = MessageEvent {
            key_idx: 0,
            offset: Offset::from(2_i64),
            outcome: MessageOutcome::Queued,
        };
        harness.execute_message(&msg2).await.ok()?;

        let key = harness.key(0).clone();

        // Verify both messages are deferred and timer is active
        assert!(harness.capture().has_active_timer(&key));
        let state = harness
            .store()
            .get_next_deferred_message(&key)
            .await
            .ok()??;
        assert_eq!(state.0, Offset::from(1_i64));
        assert_eq!(state.1, 0, "Initial retry count should be 0");

        // Set decider to false - this should NOT affect re-deferral
        harness.decider.set_next(false);
        harness
            .inner_handler
            .set_outcome(super::handler::HandlerOutcome::Transient);

        // Create context and trigger manually
        let trigger_time = harness.capture().get_timer_time(&key)?;
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
        let next_state = harness.store().get_next_deferred_message(&key).await.ok()?;
        assert_eq!(
            next_state,
            Some((Offset::from(1_i64), 1)),
            "Same message should be re-deferred with retry_count incremented to 1"
        );

        Some(())
    });
}

/// Tests that permanent error on timer retry schedules timer for next queued
/// message.
#[test]
fn permanent_error_schedules_timer_for_next_message() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1).ok()?;

        // First message defers
        let msg1 = MessageEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: MessageOutcome::Transient {
                max_backoff: CompactDuration::new(60),
                defer: true,
            },
        };
        harness.execute_message(&msg1).await.ok()?;

        // Second message queues
        let msg2 = MessageEvent {
            key_idx: 0,
            offset: Offset::from(2_i64),
            outcome: MessageOutcome::Queued,
        };
        harness.execute_message(&msg2).await.ok()?;

        let key = harness.key(0).clone();

        // Timer fires with permanent failure
        let timer = TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Permanent,
        };
        harness.execute_timer(&timer).await.ok()?;

        // Timer should still be active for the next queued message
        assert!(
            harness.capture().has_active_timer(&key),
            "Timer should be scheduled for next queued message after permanent error"
        );

        // Store should show offset 2 as next
        let next_state = harness.store().get_next_deferred_message(&key).await.ok()?;
        assert_eq!(
            next_state,
            Some((Offset::from(2_i64), 0)),
            "Next message should be offset 2 with retry_count reset to 0"
        );

        Some(())
    });
}
