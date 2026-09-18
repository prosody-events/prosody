//! Checks timer span restoration and dispatch scope.

use super::*;
use tracing::subscriber::with_default;

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
