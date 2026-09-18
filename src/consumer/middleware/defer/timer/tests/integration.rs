//! Integration tests for timer defer middleware using the test harness.
//!
//! These tests verify specific behavioral scenarios of the
//! `TimerDeferHandler` middleware using deterministic traces.

use super::*;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::CassandraDeferStoreError;
use crate::consumer::middleware::defer::error::DeferError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::KafkaLoaderError;
use crate::tracing::init_test_logging;
use scylla::errors::ExecutionError;
use tracing::subscriber::with_default;

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
            harness.has_deferred_timer(),
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
fn store_write_failure_retries_via_retry_middleware() {
    // This test verifies the error classification chain that enables the
    // composition where timer defer is inside message defer to work correctly.
    //
    // When a timer defer store operation fails with a transient error (e.g.,
    // Cassandra timeout), the error must:
    // 1. Be wrapped as DeferError::Store by TimerDeferHandler
    // 2. Classify as Transient (delegating to inner error classification)
    // 3. Enable MessageDeferHandler to catch and defer via message-based retry
    //
    // This test validates the classification chain using CassandraStoreError
    // which can produce transient errors (resource exhaustion, timeouts).
    init_test_logging();

    // Simulate a transient Cassandra error (no nodes available in plan)
    // This represents cluster unavailability during partitions or maintenance
    let execution_error = ExecutionError::EmptyPlan;
    let cassandra_store_error = CassandraStoreError::from(execution_error);

    // Verify Cassandra store error classifies as transient
    let cassandra_classification = cassandra_store_error.classify_error();
    assert!(
        matches!(cassandra_classification, ErrorCategory::Transient),
        "EmptyPlan should classify as transient"
    );

    // Wrap in CassandraDeferStoreError (unified error type)
    let timer_store_error = CassandraDeferStoreError::Cassandra(cassandra_store_error);
    assert!(
        matches!(timer_store_error.classify_error(), ErrorCategory::Transient),
        "CassandraDeferStoreError should delegate to inner Cassandra classification"
    );

    // Wrap in DeferError::Store (as TimerDeferHandler does)
    let defer_error: DeferError<CassandraDeferStoreError, OutcomeError, KafkaLoaderError> =
        DeferError::Store(timer_store_error);

    // Final verification: DeferError::Store classifies as transient
    assert!(
        matches!(defer_error.classify_error(), ErrorCategory::Transient),
        "DeferError::Store with transient Cassandra error should classify as transient, enabling \
         message defer middleware to handle via message-based retry"
    );

    // Verify permanent handler errors still propagate correctly
    let permanent_error: DeferError<CassandraDeferStoreError, OutcomeError, KafkaLoaderError> =
        DeferError::Handler(OutcomeError::Permanent);
    assert!(
        matches!(permanent_error.classify_error(), ErrorCategory::Permanent),
        "DeferError::Handler with permanent error should classify as permanent"
    );
}

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
            harness.has_deferred_timer(),
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

#[test]
fn span_restored_on_retry() -> color_eyre::Result<()> {
    // When a timer is deferred and later retried, the span context is properly
    // restored. The inner handler should receive a trigger with a span that
    // links back to the original trace, maintaining distributed trace linkage.
    //
    // This tests the round-trip:
    // 1. Application timer fires with a span (from Span::current())
    // 2. Timer defers, span context stored via propagator.inject_context()
    // 3. DeferredTimer fires, span context restored via propagator.extract()
    // 4. A reload span is built from the restored context per the configured
    //    relation and installed as the trigger's live dispatch span (reload time is
    //    dispatch time on the defer path)
    // 5. Inner handler receives the retry trigger carrying that span, whose context
    //    chains back to the original trace
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new()?;

        // Create an active span to serve as the parent context
        let parent_span = tracing::info_span!("test_parent_span", test_key = "span-test-key");
        let _guard = parent_span.enter();

        // Set handler to return transient error so timer gets deferred
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        harness.decider.set_next(true);

        // Create trigger with the current span (which has parent_span as context)
        let trigger = TestHarness::create_trigger("span-test-key", 1000);

        // Process the application timer - should be deferred
        let result = harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger.clone(),
                DemandType::Normal,
            )
            .await;

        assert!(result.is_ok(), "Deferral should succeed");

        // Verify the timer is deferred
        assert_eq!(
            expect_deferred(&harness, "span-test-key").await?,
            0,
            "Initial retry count should be 0"
        );

        // Now trigger the retry - set handler to succeed
        harness.inner_handler.set_outcome(HandlerOutcome::Success);
        harness.context().clear_operations();

        let retry_trigger = TestHarness::create_deferred_timer_trigger("span-test-key", 1001);
        let result = harness
            .handler
            .on_timer(harness.context().clone(), retry_trigger, DemandType::Normal)
            .await;

        assert!(result.is_ok(), "Retry should succeed");

        // Verify the inner handler was called (it received the trigger with span)
        let calls = harness.inner_handler.timer_calls();
        assert!(
            calls.len() >= 2,
            "Handler should be called at least twice (initial + retry)"
        );

        // The key should have been in the retry call
        let retry_key_found = calls.iter().any(|k| k.as_ref() == "span-test-key");
        assert!(
            retry_key_found,
            "Handler should be called with the deferred key during retry"
        );

        // Key should no longer be deferred
        let retry_count = harness.get_retry_count("span-test-key").await?;
        assert!(
            retry_count.is_none(),
            "Key should not be deferred after successful retry"
        );

        Ok(())
    })
}

#[test]
fn span_extraction_failure_fallback() -> color_eyre::Result<()> {
    // When span extraction fails (e.g., corrupt or empty span data), the system
    // gracefully falls back to using Span::current() rather than failing.
    //
    // If span extraction from the database fails (invalid/corrupt data), log at
    // debug level (matching existing timer store pattern) and use Span::current()
    // as fallback. This ensures timer processing continues even with degraded
    // tracing.
    //
    // The MemoryTimerDeferStore stores the Context directly, so we can't easily
    // simulate corrupt data. However, we can verify that when a timer is stored
    // and retrieved, processing continues even without an active tracing context.
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new()?;

        // Use Span::none() as parent - simulates no active trace context
        // This tests the fallback behavior when there's no parent to restore
        let _guard = tracing::Span::none().entered();

        // Set handler to return transient error
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        harness.decider.set_next(true);

        // Create trigger with no meaningful span context
        let trigger = TestHarness::create_trigger("fallback-test-key", 1000);

        // Process - should defer
        let result = harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger.clone(),
                DemandType::Normal,
            )
            .await;

        assert!(
            result.is_ok(),
            "Deferral should succeed even with Span::none() context"
        );

        // Retry should succeed - the system should gracefully handle
        // empty/missing span context
        harness.inner_handler.set_outcome(HandlerOutcome::Success);
        harness.context().clear_operations();

        let retry_trigger = TestHarness::create_deferred_timer_trigger("fallback-test-key", 1001);
        let result = harness
            .handler
            .on_timer(harness.context().clone(), retry_trigger, DemandType::Normal)
            .await;

        assert!(
            result.is_ok(),
            "Retry should succeed even with degraded span context - system should use fallback"
        );

        // Verify the inner handler was called (processing continued)
        let calls = harness.inner_handler.timer_calls();
        assert!(
            calls.len() >= 2,
            "Handler should be called despite no span context: initial + retry"
        );

        Ok(())
    })
}

#[test]
fn disabled_config_propagates_errors_no_deferral() -> color_eyre::Result<()> {
    // When `enabled: false`, transient errors propagate to the caller instead of
    // being absorbed by deferral. No deferral occurs.
    //
    // New failures propagate to retry middleware (no deferral for either messages
    // or timers).
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        // Create harness with disabled configuration
        let harness = TestHarness::with_enabled(false)?;

        // Set handler to return transient error
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        // Decider would say yes, but config.enabled=false takes precedence
        harness.decider.set_next(true);

        let trigger = TestHarness::create_trigger("disabled-test-key", 1000);
        let result = harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger.clone(),
                DemandType::Normal,
            )
            .await;

        // Should fail - error propagates instead of being absorbed
        assert!(
            result.is_err(),
            "With enabled=false, transient error should propagate"
        );

        // Verify it's a Handler error (the transient error wrapped)
        let err = result
            .err()
            .ok_or_else(|| color_eyre::eyre::eyre!("Expected error"))?;
        assert!(
            matches!(err, DeferError::Handler(_)),
            "Error should be DeferError::Handler containing the transient error"
        );

        // Key should NOT be deferred
        let retry_count = harness.get_retry_count("disabled-test-key").await?;
        assert!(
            retry_count.is_none(),
            "Key should NOT be deferred when config.enabled=false"
        );

        // No DeferredTimer should be scheduled
        assert!(
            !harness.has_deferred_timer(),
            "No DeferredTimer should be scheduled when disabled"
        );

        // Inner handler should have been called exactly once
        assert_eq!(
            harness.inner_handler.timer_calls().len(),
            1,
            "Inner handler should be called once"
        );

        Ok(())
    })
}

#[test]
fn retry_handler_runs_inside_the_reload_span() -> color_eyre::Result<()> {
    // The defer-retry dispatch instruments the inner call with the reload
    // trigger's span, so a retried handler observes it as the ambient span
    // (`Span::current()`). A registry (not the global ERROR-filtered test
    // subscriber) is installed so spans get real ids — the `is_some` guard
    // below fails, rather than passing vacuously, if spans are disabled.
    with_default(tracing_subscriber::registry(), || {
        TEST_RUNTIME.block_on(async {
            let harness = TestHarness::new()?;

            harness.inner_handler.set_outcome(HandlerOutcome::Transient);
            harness.decider.set_next(true);
            let trigger = TestHarness::create_trigger("ambient-key", 1000);
            let result = harness
                .handler
                .on_timer(harness.context().clone(), trigger, DemandType::Normal)
                .await;
            assert!(result.is_ok(), "Defer should absorb transient error");

            harness.inner_handler.set_outcome(HandlerOutcome::Success);
            let retry = TestHarness::create_deferred_timer_trigger("ambient-key", 1001);
            let result = harness
                .handler
                .on_timer(harness.context().clone(), retry, DemandType::Normal)
                .await;
            assert!(result.is_ok(), "Retry should succeed");

            // The second dispatch is the retry: its ambient span must be the
            // reload trigger's own span, by id.
            let pairs = harness.inner_handler.ambient_pairs();
            let (ambient, reload) = pairs
                .get(1)
                .ok_or_else(|| color_eyre::eyre::eyre!("retry dispatch was not recorded"))?;
            assert!(ambient.is_some(), "spans must be enabled for this pin");
            assert_eq!(
                ambient, reload,
                "retried handler must run inside the reload trigger's span"
            );

            Ok(())
        })
    })
}
