//! Integration tests for timer defer middleware using the test harness.
//!
//! These tests verify specific behavioral scenarios of the
//! `TimerDeferHandler` middleware using deterministic traces.

use super::*;
use crate::consumer::middleware::defer::error::DeferError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::tracing::init_test_logging;

/// Returns the retry count for `key`, failing the test if it isn't deferred.
async fn expect_deferred(harness: &TestHarness, key: &str) -> color_eyre::Result<u32> {
    harness
        .get_retry_count(key)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("Key `{key}` should be deferred"))
}

#[test]
fn simple_defer_and_retry_succeeds() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new()?;

        // Set handler to return transient error
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        harness.decider.set_next(true);

        // Create and process application timer
        let trigger = TestHarness::create_trigger("test-key", 1000);
        let result = harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger.clone(),
                DemandType::Normal,
            )
            .await;

        // Should succeed (error absorbed, timer deferred)
        assert!(result.is_ok(), "Defer should absorb transient error");

        // Key should be deferred with retry_count = 0
        assert_eq!(expect_deferred(&harness, "test-key").await?, 0);

        // DeferredTimer should be scheduled
        assert!(
            !harness
                .context()
                .scheduled(TimerType::DeferredTimer)
                .await?
                .is_empty(),
            "DeferredTimer should be scheduled"
        );

        // Inner handler should have been called once
        assert_eq!(harness.inner_handler.timer_calls().len(), 1);

        // Now simulate retry timer firing - set handler to succeed
        harness.inner_handler.set_outcome(HandlerOutcome::Success);
        harness.context().clear_operations();

        let retry_trigger = TestHarness::create_deferred_timer_trigger("test-key", 1001);
        let result = harness
            .handler
            .on_timer(harness.context().clone(), retry_trigger, DemandType::Normal)
            .await;

        // Should succeed
        assert!(result.is_ok(), "Retry should succeed");

        // Key should not be deferred anymore
        let retry_count = harness.get_retry_count("test-key").await?;
        assert!(
            retry_count.is_none(),
            "Key should not be deferred after success"
        );

        // Inner handler should have been called again (total 2)
        assert_eq!(harness.inner_handler.timer_calls().len(), 2);

        Ok(())
    })
}

#[test]
fn queues_timers_while_key_deferred() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new()?;

        // First timer defers
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        harness.decider.set_next(true);

        let trigger1 = TestHarness::create_trigger("test-key", 1000);
        harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger1.clone(),
                DemandType::Normal,
            )
            .await?;

        // Second timer arrives while key is deferred - should queue
        let trigger2 = TestHarness::create_trigger("test-key", 2000);
        let result = harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger2.clone(),
                DemandType::Normal,
            )
            .await;

        // Should succeed (queued behind first)
        assert!(result.is_ok(), "Second timer should queue");

        // Queueing behind an already-deferred key short-circuits before the
        // is_deferred check reaches config.enabled or the inner handler: the
        // retry_count is untouched and the inner handler is not invoked.
        assert_eq!(
            expect_deferred(&harness, "test-key").await?,
            0,
            "Queueing should not change the original retry_count"
        );
        assert_eq!(
            harness.inner_handler.timer_calls().len(),
            1,
            "Inner handler should not be called for a queued timer"
        );

        // First timer's retry succeeds
        harness.inner_handler.set_outcome(HandlerOutcome::Success);
        let retry_trigger = TestHarness::create_deferred_timer_trigger("test-key", 1001);
        harness
            .handler
            .on_timer(harness.context().clone(), retry_trigger, DemandType::Normal)
            .await?;

        // Key should still be deferred (has second timer)
        let retry_count = harness.get_retry_count("test-key").await?;
        assert!(
            retry_count.is_some(),
            "Key should still be deferred with queued timer"
        );

        Ok(())
    })
}

#[test]
fn increments_retry_count_on_transient_failure() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new()?;

        // Initial defer
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        harness.decider.set_next(true);

        let trigger = TestHarness::create_trigger("test-key", 1000);
        harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger.clone(),
                DemandType::Normal,
            )
            .await?;

        // Verify initial retry count
        assert_eq!(expect_deferred(&harness, "test-key").await?, 0);

        // Retry fires, handler fails transiently again
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        let retry_trigger = TestHarness::create_deferred_timer_trigger("test-key", 1001);
        let result = harness
            .handler
            .on_timer(harness.context().clone(), retry_trigger, DemandType::Normal)
            .await;

        // Should succeed (re-deferred)
        assert!(result.is_ok(), "Re-defer should succeed");

        // Retry count should be incremented
        assert_eq!(
            expect_deferred(&harness, "test-key").await?,
            1,
            "Retry count should be incremented"
        );

        Ok(())
    })
}

#[test]
fn partial_failure_orphaned_timer_cleanup() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new()?;

        // Simulate orphaned state: DeferredTimer fires but store is empty
        // This can happen if timer scheduled but store write failed
        let retry_trigger = TestHarness::create_deferred_timer_trigger("orphan-key", 1000);
        let result = harness
            .handler
            .on_timer(harness.context().clone(), retry_trigger, DemandType::Normal)
            .await;

        // Should succeed (orphan cleaned up)
        assert!(result.is_ok(), "Orphan cleanup should succeed");

        // Key should not be deferred
        let retry_count = harness.get_retry_count("orphan-key").await?;
        assert!(retry_count.is_none(), "Orphaned key should be cleaned up");

        Ok(())
    })
}

#[test]
fn decider_gates_initial_deferral_only() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new()?;

        // Decider says don't defer
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        harness.decider.set_next(false);

        let trigger = TestHarness::create_trigger("test-key", 1000);
        let result = harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger.clone(),
                DemandType::Normal,
            )
            .await;

        // Should fail (deferral rejected)
        assert!(result.is_err(), "Deferral should be rejected by decider");

        // Key should not be deferred
        let retry_count = harness.get_retry_count("test-key").await?;
        assert!(
            retry_count.is_none(),
            "Key should not be deferred when decider rejects"
        );

        Ok(())
    })
}

#[test]
fn re_deferral_ignores_decider() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new()?;

        // Initial defer succeeds
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        harness.decider.set_next(true);

        let trigger = TestHarness::create_trigger("test-key", 1000);
        harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger.clone(),
                DemandType::Normal,
            )
            .await?;

        // Now decider says don't defer - but re-deferral should ignore this
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        harness.decider.set_next(false);

        let retry_trigger = TestHarness::create_deferred_timer_trigger("test-key", 1001);
        let result = harness
            .handler
            .on_timer(harness.context().clone(), retry_trigger, DemandType::Normal)
            .await;

        // Should succeed (re-deferred despite decider)
        assert!(
            result.is_ok(),
            "Re-deferral should succeed despite decider returning false"
        );

        // Key should still be deferred
        assert_eq!(
            expect_deferred(&harness, "test-key").await?,
            1,
            "Key should be re-deferred with incremented retry count"
        );

        Ok(())
    })
}

// Note: There's no "System" timer type - only Application, DeferredMessage, and
// DeferredTimer. DeferredMessage and DeferredTimer are handled specially by
// their respective middlewares. Application timers are the only ones that can
// be deferred by TimerDeferHandler.
//
// The handler passes non-Application timers through to the inner handler, but
// the only other timer types are the deferred retry timers themselves. We test
// DeferredTimer handling in the retry tests above.

#[test]
fn permanent_error_schedules_timer_for_next() -> color_eyre::Result<()> {
    // When a permanent error occurs during deferred timer retry, the queue
    // advances and a DeferredTimer is scheduled for the NEXT timer.
    //
    // The queue advances (timer removed, next timer scheduled if any), then the
    // error is wrapped in DeferError::Handler and propagated.
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new()?;

        // First timer defers
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        harness.decider.set_next(true);

        let trigger1 = TestHarness::create_trigger("perm-error-key", 1000);
        harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger1.clone(),
                DemandType::Normal,
            )
            .await?;

        // Second timer queues behind first
        let trigger2 = TestHarness::create_trigger("perm-error-key", 2000);
        harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger2.clone(),
                DemandType::Normal,
            )
            .await?;

        // Clear context to track new scheduling
        harness.context().clear_operations();

        // Retry fires with permanent failure
        harness.inner_handler.set_outcome(HandlerOutcome::Permanent);
        let retry_trigger = TestHarness::create_deferred_timer_trigger("perm-error-key", 1001);
        let result = harness
            .handler
            .on_timer(harness.context().clone(), retry_trigger, DemandType::Normal)
            .await;

        // Should fail (permanent errors propagate)
        assert!(
            result.is_err(),
            "Permanent error should propagate after queue advancement"
        );

        // Retry count resets to 0 for the next timer in queue.
        assert_eq!(
            harness.get_retry_count("perm-error-key").await?,
            Some(0),
            "Retry count should be reset for next timer"
        );

        // DeferredTimer should be scheduled for the NEXT timer in queue
        assert!(
            !harness
                .context()
                .scheduled(TimerType::DeferredTimer)
                .await?
                .is_empty(),
            "DeferredTimer should be scheduled for next timer after permanent error"
        );

        // Verify the next timer can now be processed (first timer was removed)
        harness.context().clear_operations();
        harness.inner_handler.set_outcome(HandlerOutcome::Success);
        let retry_trigger2 = TestHarness::create_deferred_timer_trigger("perm-error-key", 2001);
        let result = harness
            .handler
            .on_timer(
                harness.context().clone(),
                retry_trigger2,
                DemandType::Normal,
            )
            .await;

        // Should succeed (second timer was next in queue)
        assert!(
            result.is_ok(),
            "Second timer should succeed after advancement"
        );

        // Key should no longer be deferred (queue is now empty)
        let retry_count = harness.get_retry_count("perm-error-key").await?;
        assert!(
            retry_count.is_none(),
            "Key should not be deferred after queue empties"
        );

        Ok(())
    })
}

#[test]
fn permanent_error_propagates_wrapped() -> color_eyre::Result<()> {
    // Permanent errors are properly wrapped in DeferError::Handler and propagated
    // up the middleware stack for observability.
    //
    // The CommittingHandler at the top of the stack calls on_timer_error() for
    // observability (logging, metrics) and commits the timer (marking it as
    // processed).
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new()?;

        // Defer a timer
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        harness.decider.set_next(true);

        let trigger = TestHarness::create_trigger("propagate-key", 1000);
        harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger.clone(),
                DemandType::Normal,
            )
            .await?;

        // Retry fires with permanent failure
        harness.inner_handler.set_outcome(HandlerOutcome::Permanent);
        let retry_trigger = TestHarness::create_deferred_timer_trigger("propagate-key", 1001);
        let result = harness
            .handler
            .on_timer(harness.context().clone(), retry_trigger, DemandType::Normal)
            .await;

        // Should be DeferError::Handler wrapping the permanent error
        let err = result
            .err()
            .ok_or_else(|| color_eyre::eyre::eyre!("Expected error"))?;
        assert!(
            matches!(err, DeferError::Handler(_)),
            "Error should be wrapped in DeferError::Handler, got: {err:?}"
        );

        // Verify the error classifies as Permanent
        assert!(
            matches!(err.classify_error(), ErrorCategory::Permanent),
            "Wrapped error should classify as Permanent"
        );

        // The queue should have advanced (timer removed)
        let retry_count = harness.get_retry_count("propagate-key").await?;
        assert!(
            retry_count.is_none(),
            "Queue should be empty after permanent error on single timer"
        );

        Ok(())
    })
}

mod errors;
mod spans;
