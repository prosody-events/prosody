//! Integration tests for defer middleware using the test harness.
//!
//! These tests verify specific behavioral scenarios of the `DeferHandler`
//! middleware using deterministic traces, complementing the property-based
//! tests in `properties.rs`.

use super::TEST_RUNTIME;
use super::harness::TestHarness;
use super::types::{MessageEvent, MessageOutcome, TimerEvent, TimerOutcome};
use crate::Offset;
use crate::consumer::DemandType;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::defer::handler::tests::context::KeyedCapturingContext;
use crate::consumer::middleware::defer::store::DeferStore;
use crate::timers::datetime::CompactDateTime;
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

/// Tests that when deferral is disabled mid-retry (due to high failure rate),
/// remaining queued messages still get timer coverage.
#[test]
fn deferral_disabled_preserves_timer_for_remaining_messages() {
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

        // Now simulate deferral being disabled (decider returns false)
        // Timer fires with transient failure but deferral is disabled
        harness.decider.set_next(false);
        harness
            .inner_handler
            .set_outcome(super::handler::HandlerOutcome::Transient);

        // Create context and trigger manually since execute_timer expects success
        let trigger_time = harness.capture().get_timer_time(&key)?;
        let key_context = KeyedCapturingContext::new(key.clone(), harness.capture().clone());
        let trigger = Trigger::for_testing(key.clone(), trigger_time, TimerType::DeferRetry);

        // This should fail (deferral disabled) but schedule timer for next message
        let result = harness
            .handler
            .on_timer(key_context, trigger, DemandType::Normal)
            .await;

        // Should return error (deferral disabled)
        assert!(result.is_err());

        // BUT: timer should still be active for the next queued message
        assert!(
            harness.capture().has_active_timer(&key),
            "Timer should be scheduled for next queued message"
        );

        // And the store should show offset 2 as next (offset 1 was removed)
        let next_state = harness.store().get_next_deferred_message(&key).await.ok()?;
        assert_eq!(
            next_state,
            Some((Offset::from(2_i64), 0)),
            "Next message should be offset 2 with retry_count reset to 0"
        );

        Some(())
    });
}

/// Tests that when a timer fires but no deferred message is found (e.g.,
/// expired or already processed), the timer is properly cleared.
#[test]
fn timer_cleared_when_message_not_found() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new(1).ok()?;
        let key = harness.key(0).clone();

        // Manually set up a timer without a corresponding store entry
        // This simulates the case where the message was already processed or expired
        let time = CompactDateTime::now().ok()?;
        harness.capture().record_schedule(key.clone(), time);

        // Verify timer is active but store is empty
        assert!(harness.capture().has_active_timer(&key));
        let store_state = harness.store().get_next_deferred_message(&key).await.ok()?;
        assert!(store_state.is_none(), "Store should be empty");

        // Create context and fire the timer
        let key_context = KeyedCapturingContext::new(key.clone(), harness.capture().clone());
        let trigger = Trigger::for_testing(key.clone(), time, TimerType::DeferRetry);

        // Timer should complete successfully (no error) but clear the timer
        let result = harness
            .handler
            .on_timer(key_context, trigger, DemandType::Normal)
            .await;

        assert!(result.is_ok(), "Should succeed even with no message found");

        // Timer should now be cleared
        assert!(
            !harness.capture().has_active_timer(&key),
            "Timer should be cleared when no deferred message found"
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
