//! Checks active and deferred timer operations.

use super::*;

/// `schedule()` when NOT deferred delegates to inner context.
#[test]
fn schedule_when_not_deferred_adds_to_active() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");
        let context = harness.create_wrapped_context();

        // Key is not deferred
        assert!(!harness.is_deferred().await?);

        // Schedule via wrapped context
        let time = CompactDateTime::from(1000_u32);
        context.schedule(time, TimerType::Application).await?;

        // Should have been added to inner context's active timers
        let active = harness.inner_context.active_application_timers();
        assert!(active.contains(&time), "Timer should be in active store");

        // Should NOT be in defer store
        let deferred = harness.store.get_next_deferred_timer(harness.key()).await?;
        assert!(deferred.is_none(), "Timer should not be in defer store");

        Ok(())
    })
}

/// `schedule()` when deferred appends to defer store.
#[test]
fn schedule_when_deferred_appends_to_store() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // First, defer a timer to make the key deferred
        harness.defer_timer(1000).await?;
        assert!(harness.is_deferred().await?);

        let context = harness.create_wrapped_context();

        // Schedule a NEW timer via wrapped context
        let time = CompactDateTime::from(2000_u32);
        context.schedule(time, TimerType::Application).await?;

        // Timer should be appended to defer store (not inner context)
        let deferred = harness.deferred_times().await?;
        assert!(
            deferred.contains(&time),
            "Timer should be in defer store; got: {deferred:?}"
        );

        // Should NOT be in inner context's active timers
        let active = harness.inner_context.active_application_timers();
        assert!(
            !active.contains(&time),
            "Timer should NOT be in active store when key is deferred"
        );

        // CLIENT INVARIANT: Timer must appear in scheduled() regardless of internal
        // storage
        let scheduled: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;
        assert!(
            scheduled.contains(&time),
            "Timer scheduled while deferred must appear in scheduled(); got: {scheduled:?}"
        );

        Ok(())
    })
}

/// `unschedule()` removes from both stores when deferred.
#[test]
fn unschedule_removes_from_both_stores() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timer to active store first
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await?;

        // Defer timers at 500 and 1500 to make key deferred
        harness.defer_timer(500).await?;
        harness.defer_additional_timer(1500).await?;

        let context = harness.create_wrapped_context();

        // Unschedule the timer at 1000 (in active store)
        context
            .unschedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await?;

        // Should be removed from active store
        let active = harness.inner_context.active_application_timers();
        assert!(
            !active.contains(&CompactDateTime::from(1000_u32)),
            "Timer should be removed from active store"
        );

        // Unschedule the timer at 500 (in defer store)
        context
            .unschedule(CompactDateTime::from(500_u32), TimerType::Application)
            .await?;

        // Should be removed from defer store (only 1500 remains)
        let deferred = harness.deferred_times().await?;
        assert!(
            !deferred.contains(&CompactDateTime::from(500_u32)),
            "Timer at 500 should be removed from defer store"
        );
        assert!(
            deferred.contains(&CompactDateTime::from(1500_u32)),
            "Timer at 1500 should still be in defer store"
        );

        // CLIENT INVARIANT: scheduled() must reflect unschedule operations
        let scheduled: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;
        assert!(
            !scheduled.contains(&CompactDateTime::from(500_u32)),
            "Unscheduled timer must not appear in scheduled()"
        );
        assert!(
            !scheduled.contains(&CompactDateTime::from(1000_u32)),
            "Unscheduled timer must not appear in scheduled()"
        );
        assert!(
            scheduled.contains(&CompactDateTime::from(1500_u32)),
            "Remaining timer must appear in scheduled()"
        );

        Ok(())
    })
}

/// `clear_scheduled()` clears both stores and cancels `DeferredTimer`.
#[test]
fn clear_scheduled_clears_both_stores_and_cancels_deferred_timer() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timer to active store
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await?;

        // Defer a timer
        harness.defer_timer(500).await?;

        // Schedule a DeferredTimer (simulates the retry timer)
        harness
            .inner_context
            .schedule(CompactDateTime::from(600_u32), TimerType::DeferredTimer)
            .await?;

        let context = harness.create_wrapped_context();

        // Clear all Application timers
        context.clear_scheduled(TimerType::Application).await?;

        // Active store should have no Application timers
        let active = harness.inner_context.active_application_timers();
        assert!(
            active.is_empty(),
            "Active Application timers should be cleared"
        );

        // Defer store should be cleared (key deleted)
        let deferred = harness.deferred_times().await?;
        assert!(
            deferred.is_empty(),
            "Defer store should be cleared; got: {deferred:?}"
        );

        // Key should no longer be deferred
        assert!(
            !harness.is_deferred().await?,
            "Key should not be deferred after clear_scheduled"
        );

        // DeferredTimer should be cancelled
        let deferred_timers = harness.inner_context.active_deferred_timers();
        assert!(
            deferred_timers.is_empty(),
            "DeferredTimer should be cancelled; got: {deferred_timers:?}"
        );

        Ok(())
    })
}

/// `scheduled()` merges both stores sorted and deduplicated.
#[test]
fn scheduled_merges_both_stores_sorted_deduplicated() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timers to active store: 1000, 3000
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await?;
        harness
            .inner_context
            .schedule(CompactDateTime::from(3000_u32), TimerType::Application)
            .await?;

        // Defer timers at 500, 2000
        harness.defer_timer(500).await?;
        harness.defer_additional_timer(2000).await?;

        let context = harness.create_wrapped_context();

        // Get scheduled times - should be merged and sorted
        let times: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;

        // Should be sorted: 500, 1000, 2000, 3000
        assert_eq!(
            times,
            vec![
                CompactDateTime::from(500_u32),
                CompactDateTime::from(1000_u32),
                CompactDateTime::from(2000_u32),
                CompactDateTime::from(3000_u32),
            ],
            "Times should be merged and sorted"
        );

        Ok(())
    })
}

/// `scheduled()` deduplicates when same time exists in both stores.
#[test]
fn scheduled_deduplicates_when_same_time_in_both_stores() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timer at 1000 to active store
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await?;

        // Defer timer at the SAME time 1000
        harness.defer_timer(1000).await?;

        let context = harness.create_wrapped_context();

        // Get scheduled times - should be deduplicated
        let times: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;

        // Should have only ONE entry at 1000
        assert_eq!(times.len(), 1, "Duplicate time should appear only once");
        assert_eq!(times[0], CompactDateTime::from(1000_u32));

        Ok(())
    })
}

/// `clear_and_schedule()` makes key not deferred after completion.
#[test]
fn clear_and_schedule_makes_key_not_deferred() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Defer a timer
        harness.defer_timer(1000).await?;
        assert!(harness.is_deferred().await?, "Key should be deferred");

        let context = harness.create_wrapped_context();

        // Clear and schedule a new timer
        let new_time = CompactDateTime::from(2000_u32);
        context
            .clear_and_schedule(new_time, TimerType::Application)
            .await?;

        // Key should no longer be deferred
        assert!(
            !harness.is_deferred().await?,
            "Key should not be deferred after clear_and_schedule"
        );

        // New timer should be in active store
        let active = harness.inner_context.active_application_timers();
        assert!(
            active.contains(&new_time),
            "New timer should be in active store"
        );

        // DeferredTimer should be cleared
        let deferred_timers = harness.inner_context.active_deferred_timers();
        assert!(
            deferred_timers.is_empty(),
            "DeferredTimer should be cleared"
        );

        Ok(())
    })
}

/// Client invariant: schedule/unschedule while deferred behaves identically to
/// not deferred.
///
/// This test verifies the key behavioral equivalence requirement: from the
/// client's perspective, timer operations should work the same regardless of
/// internal deferral state. The client should be able to:
/// 1. Schedule a timer and see it in `scheduled()`
/// 2. Unschedule that timer and no longer see it in `scheduled()`
#[test]
fn client_operations_work_identically_when_deferred() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Make key deferred by deferring an initial timer
        harness.defer_timer(1000).await?;
        assert!(harness.is_deferred().await?);

        let context = harness.create_wrapped_context();

        // STEP 1: Schedule a new timer while deferred
        let client_timer = CompactDateTime::from(2000_u32);
        context
            .schedule(client_timer, TimerType::Application)
            .await?;

        // CLIENT INVARIANT: Scheduled timer must be visible
        let scheduled: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;
        assert!(
            scheduled.contains(&client_timer),
            "schedule() while deferred: timer must appear in scheduled()"
        );

        // STEP 2: Unschedule the timer
        context
            .unschedule(client_timer, TimerType::Application)
            .await?;

        // CLIENT INVARIANT: Unscheduled timer must be gone
        let scheduled: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;
        assert!(
            !scheduled.contains(&client_timer),
            "unschedule() while deferred: timer must not appear in scheduled()"
        );

        // Original deferred timer should still be there
        assert!(
            scheduled.contains(&CompactDateTime::from(1000_u32)),
            "Original deferred timer should remain"
        );

        // STEP 3: Schedule again and use clear_and_schedule to replace
        context
            .schedule(client_timer, TimerType::Application)
            .await?;
        let replacement = CompactDateTime::from(3000_u32);
        context
            .clear_and_schedule(replacement, TimerType::Application)
            .await?;

        // CLIENT INVARIANT: Only the replacement timer should exist
        let scheduled: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;
        assert_eq!(
            scheduled,
            vec![replacement],
            "clear_and_schedule() should leave only the new timer"
        );

        // Key should no longer be deferred (clear_and_schedule deletes the key)
        assert!(
            !harness.is_deferred().await?,
            "clear_and_schedule should un-defer the key"
        );

        Ok(())
    })
}

/// Non-Application timer types pass through to inner context.
#[test]
fn non_application_timers_pass_through() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Defer a timer to make key deferred
        harness.defer_timer(1000).await?;
        assert!(harness.is_deferred().await?);

        let context = harness.create_wrapped_context();

        // Schedule a DeferredMessage timer (internal type)
        let time = CompactDateTime::from(2000_u32);
        context.schedule(time, TimerType::DeferredMessage).await?;

        // Should go directly to inner context, not defer store
        let has_deferred_msg = harness
            .inner_context
            .active_timers
            .lock()
            .iter()
            .any(|(t, tt)| *t == time && *tt == TimerType::DeferredMessage);
        assert!(
            has_deferred_msg,
            "DeferredMessage should pass through to inner context"
        );

        Ok(())
    })
}
