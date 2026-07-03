use super::*;
use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use tokio::test;

#[test]
async fn test_insert_and_contains() {
    let active_triggers = ActiveTriggers::default();

    let key = Key::from("test-key");
    let time = CompactDateTime::from(12345u32);
    let trigger = Trigger::new(
        key.clone(),
        time,
        TimerType::Application,
        tracing::Span::current(),
    );

    // Initially, the trigger should not be present
    assert!(
        !active_triggers
            .contains(&key, time, TimerType::Application)
            .await
    );
    assert!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await
            .is_none()
    );

    // Insert the trigger
    active_triggers.insert(trigger.clone()).await;

    // Now, the trigger should be present with Scheduled state
    assert!(
        active_triggers
            .contains(&key, time, TimerType::Application)
            .await
    );
    assert_eq!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await,
        Some(TimerState::Scheduled)
    );
}

#[test]
async fn test_remove() {
    let active_triggers = ActiveTriggers::default();

    let key = Key::from("test-key");
    let time = CompactDateTime::from(12345u32);
    let trigger = Trigger::new(
        key.clone(),
        time,
        TimerType::Application,
        tracing::Span::current(),
    );

    // Insert the trigger
    active_triggers.insert(trigger.clone()).await;

    // Verify it exists
    assert!(
        active_triggers
            .contains(&key, time, TimerType::Application)
            .await
    );

    // Remove the trigger
    active_triggers
        .remove(&key, time, TimerType::Application)
        .await;

    // Verify it no longer exists
    assert!(
        !active_triggers
            .contains(&key, time, TimerType::Application)
            .await
    );
}

#[test]
async fn test_multiple_triggers_same_key() {
    let active_triggers = ActiveTriggers::default();

    let key = Key::from("shared-key");
    let time1 = CompactDateTime::from(12345u32);
    let time2 = CompactDateTime::from(67890u32);

    let trigger1 = Trigger::new(
        key.clone(),
        time1,
        TimerType::Application,
        tracing::Span::current(),
    );
    let trigger2 = Trigger::new(
        key.clone(),
        time2,
        TimerType::Application,
        tracing::Span::current(),
    );

    // Insert both triggers
    active_triggers.insert(trigger1.clone()).await;
    active_triggers.insert(trigger2.clone()).await;

    // Verify both triggers exist with correct state
    assert_eq!(
        active_triggers
            .get_state(&key, time1, TimerType::Application)
            .await,
        Some(TimerState::Scheduled)
    );
    assert_eq!(
        active_triggers
            .get_state(&key, time2, TimerType::Application)
            .await,
        Some(TimerState::Scheduled)
    );

    // Remove one trigger
    active_triggers
        .remove(&key, time1, TimerType::Application)
        .await;

    // Verify only the second trigger exists with correct state
    assert!(
        active_triggers
            .get_state(&key, time1, TimerType::Application)
            .await
            .is_none()
    );
    assert_eq!(
        active_triggers
            .get_state(&key, time2, TimerType::Application)
            .await,
        Some(TimerState::Scheduled)
    );

    // Remove the second trigger
    active_triggers
        .remove(&key, time2, TimerType::Application)
        .await;

    // Verify no triggers exist for the key
    assert!(
        active_triggers
            .get_state(&key, time1, TimerType::Application)
            .await
            .is_none()
    );
    assert!(
        active_triggers
            .get_state(&key, time2, TimerType::Application)
            .await
            .is_none()
    );
}

#[test]
async fn test_multiple_keys() {
    let active_triggers = ActiveTriggers::default();

    let key1 = Key::from("key-1");
    let key2 = Key::from("key-2");
    let time1 = CompactDateTime::from(11111u32);
    let time2 = CompactDateTime::from(22222u32);

    let trigger1 = Trigger::new(
        key1.clone(),
        time1,
        TimerType::Application,
        tracing::Span::current(),
    );
    let trigger2 = Trigger::new(
        key2.clone(),
        time2,
        TimerType::Application,
        tracing::Span::current(),
    );

    // Insert triggers for different keys
    active_triggers.insert(trigger1.clone()).await;
    active_triggers.insert(trigger2.clone()).await;

    // Verify both triggers exist under their respective keys
    assert!(
        active_triggers
            .contains(&key1, time1, TimerType::Application)
            .await
    );
    assert!(
        active_triggers
            .contains(&key2, time2, TimerType::Application)
            .await
    );

    // Verify cross-key isolation
    assert!(
        !active_triggers
            .contains(&key1, time2, TimerType::Application)
            .await
    );
    assert!(
        !active_triggers
            .contains(&key2, time1, TimerType::Application)
            .await
    );

    // Remove one trigger
    active_triggers
        .remove(&key1, time1, TimerType::Application)
        .await;

    // Verify only the second trigger remains
    assert!(
        !active_triggers
            .contains(&key1, time1, TimerType::Application)
            .await
    );
    assert!(
        active_triggers
            .contains(&key2, time2, TimerType::Application)
            .await
    );
}

#[test]
async fn test_scan_active_times() {
    let active_triggers = ActiveTriggers::default();

    let key1 = Key::from("key-1");
    let key2 = Key::from("key-2");
    let time1 = CompactDateTime::from(10000u32);
    let time2 = CompactDateTime::from(20000u32);
    let time3 = CompactDateTime::from(30000u32);

    // Insert multiple triggers
    active_triggers
        .insert(Trigger::new(
            key1.clone(),
            time1,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;

    active_triggers
        .insert(Trigger::new(
            key1.clone(),
            time2,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;

    active_triggers
        .insert(Trigger::new(
            key2.clone(),
            time3,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;

    // Collect all active times using scan_active_times
    let mut collected_times = Vec::new();
    active_triggers
        .scan_active_times(|time, _timer_type| {
            collected_times.push(time);
        })
        .await;

    // Sort for consistent comparison
    collected_times.sort();
    let mut expected_times = vec![time1, time2, time3];
    expected_times.sort();

    assert_eq!(collected_times, expected_times);
}

#[test]
async fn test_edge_cases() {
    let active_triggers = ActiveTriggers::default();
    let key = Key::from("test-key");
    let time = CompactDateTime::from(12345u32);

    // Test contains on empty state
    assert!(
        !active_triggers
            .contains(&key, time, TimerType::Application)
            .await
    );

    // Test remove on non-existent key
    active_triggers
        .remove(&key, time, TimerType::Application)
        .await; // Should not panic

    // Test remove on non-existent time for existing key
    let trigger = Trigger::new(
        key.clone(),
        time,
        TimerType::Application,
        tracing::Span::current(),
    );
    active_triggers.insert(trigger).await;

    let other_time = CompactDateTime::from(99999u32);
    active_triggers
        .remove(&key, other_time, TimerType::Application)
        .await; // Should not panic

    // Original trigger should still exist
    assert!(
        active_triggers
            .contains(&key, time, TimerType::Application)
            .await
    );
}

#[test]
async fn test_scan_active_times_empty() {
    let active_triggers = ActiveTriggers::default();

    let mut call_count = 0_i32;
    active_triggers
        .scan_active_times(|_, _| {
            call_count += 1_i32;
        })
        .await;

    // Should not call the function for empty state
    assert_eq!(call_count, 0_i32);
}

#[test]
async fn test_same_time_different_types() {
    let active_triggers = ActiveTriggers::default();
    let key = Key::from("key-1");
    let time = CompactDateTime::from(1000u32);

    // Insert both types at same time
    let app = Trigger::new(
        key.clone(),
        time,
        TimerType::Application,
        tracing::Span::current(),
    );
    let retry = Trigger::new(
        key.clone(),
        time,
        TimerType::DeferredMessage,
        tracing::Span::current(),
    );

    active_triggers.insert(app).await;
    active_triggers.insert(retry).await;

    // Both should coexist
    assert!(
        active_triggers
            .contains(&key, time, TimerType::Application)
            .await
    );
    assert!(
        active_triggers
            .contains(&key, time, TimerType::DeferredMessage)
            .await
    );

    // Remove one type
    active_triggers
        .remove(&key, time, TimerType::Application)
        .await;

    // Only retry type remains
    assert!(
        !active_triggers
            .contains(&key, time, TimerType::Application)
            .await
    );
    assert!(
        active_triggers
            .contains(&key, time, TimerType::DeferredMessage)
            .await
    );
}

#[test]
async fn test_remove_respects_type() {
    let active_triggers = ActiveTriggers::default();
    let key = Key::from("key-1");
    let time = CompactDateTime::from(2000u32);

    // Insert both types
    let app = Trigger::new(
        key.clone(),
        time,
        TimerType::Application,
        tracing::Span::current(),
    );
    let retry = Trigger::new(
        key.clone(),
        time,
        TimerType::DeferredMessage,
        tracing::Span::current(),
    );

    active_triggers.insert(app).await;
    active_triggers.insert(retry).await;

    // Remove Application type
    active_triggers
        .remove(&key, time, TimerType::Application)
        .await;

    // DeferredMessage should still exist
    assert!(
        !active_triggers
            .contains(&key, time, TimerType::Application)
            .await
    );
    assert!(
        active_triggers
            .contains(&key, time, TimerType::DeferredMessage)
            .await
    );

    // Remove DeferredMessage type
    active_triggers
        .remove(&key, time, TimerType::DeferredMessage)
        .await;

    // Both should be gone
    assert!(
        !active_triggers
            .contains(&key, time, TimerType::Application)
            .await
    );
    assert!(
        !active_triggers
            .contains(&key, time, TimerType::DeferredMessage)
            .await
    );
}

#[test]
async fn test_scan_includes_all_types() {
    let active_triggers = ActiveTriggers::default();
    let key = Key::from("key-1");
    let time1 = CompactDateTime::from(1000u32);
    let time2 = CompactDateTime::from(2000u32);

    // Insert Application at time1
    active_triggers
        .insert(Trigger::new(
            key.clone(),
            time1,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;

    // Insert DeferredMessage at time1 (same time, different type)
    active_triggers
        .insert(Trigger::new(
            key.clone(),
            time1,
            TimerType::DeferredMessage,
            tracing::Span::current(),
        ))
        .await;

    // Insert DeferredMessage at time2
    active_triggers
        .insert(Trigger::new(
            key.clone(),
            time2,
            TimerType::DeferredMessage,
            tracing::Span::current(),
        ))
        .await;

    // Collect all (time, type) tuples
    let mut collected = Vec::new();
    active_triggers
        .scan_active_times(|time, timer_type| {
            collected.push((time, timer_type));
        })
        .await;

    // Should have 3 entries
    assert_eq!(collected.len(), 3);

    // Verify all combinations exist
    assert!(collected.contains(&(time1, TimerType::Application)));
    assert!(collected.contains(&(time1, TimerType::DeferredMessage)));
    assert!(collected.contains(&(time2, TimerType::DeferredMessage)));
}

#[test]
async fn test_state_transitions() {
    let active_triggers = ActiveTriggers::default();
    let key = Key::from("state-key");
    let time = CompactDateTime::from(12345u32);
    let trigger = Trigger::new(
        key.clone(),
        time,
        TimerType::Application,
        tracing::Span::current(),
    );

    // Insert trigger - should be Scheduled
    active_triggers.insert(trigger).await;
    assert_eq!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await,
        Some(TimerState::Scheduled)
    );

    // Transition to Firing
    assert!(
        active_triggers
            .set_state(&key, time, TimerType::Application, TimerState::Firing)
            .await
    );
    assert_eq!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await,
        Some(TimerState::Firing)
    );

    // Transition to FiringRescheduled
    assert!(
        active_triggers
            .set_state(
                &key,
                time,
                TimerType::Application,
                TimerState::FiringRescheduled
            )
            .await
    );
    assert_eq!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await,
        Some(TimerState::FiringRescheduled)
    );

    // Transition to Aborted
    assert!(
        active_triggers
            .set_state(&key, time, TimerType::Application, TimerState::Aborted)
            .await
    );
    assert_eq!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await,
        Some(TimerState::Aborted)
    );

    // Transition back to Scheduled (after commit with reschedule)
    assert!(
        active_triggers
            .set_state(&key, time, TimerType::Application, TimerState::Scheduled)
            .await
    );
    assert_eq!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await,
        Some(TimerState::Scheduled)
    );
}

#[test]
async fn test_get_state() {
    let active_triggers = ActiveTriggers::default();
    let key = Key::from("get-state-key");
    let time = CompactDateTime::from(11111u32);
    let trigger = Trigger::new(
        key.clone(),
        time,
        TimerType::Application,
        tracing::Span::current(),
    );

    // Get state on non-existent trigger
    assert!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await
            .is_none()
    );

    // Insert and verify Scheduled state
    active_triggers.insert(trigger).await;
    assert_eq!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await,
        Some(TimerState::Scheduled)
    );

    // Set to Firing and verify
    active_triggers
        .set_state(&key, time, TimerType::Application, TimerState::Firing)
        .await;
    assert_eq!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await,
        Some(TimerState::Firing)
    );

    // Set to FiringRescheduled and verify
    active_triggers
        .set_state(
            &key,
            time,
            TimerType::Application,
            TimerState::FiringRescheduled,
        )
        .await;
    assert_eq!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await,
        Some(TimerState::FiringRescheduled)
    );

    // Remove and verify None
    active_triggers
        .remove(&key, time, TimerType::Application)
        .await;
    assert!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await
            .is_none()
    );
}

#[test]
async fn test_is_scheduled() {
    let active_triggers = ActiveTriggers::default();
    let key = Key::from("is-scheduled-key");
    let time = CompactDateTime::from(22222u32);
    let trigger = Trigger::new(
        key.clone(),
        time,
        TimerType::Application,
        tracing::Span::current(),
    );

    // Not present - is_scheduled returns false
    assert!(
        !active_triggers
            .is_scheduled(&key, time, TimerType::Application)
            .await
    );

    // Insert (Scheduled state) - is_scheduled returns true
    active_triggers.insert(trigger).await;
    assert!(
        active_triggers
            .is_scheduled(&key, time, TimerType::Application)
            .await
    );

    // Transition to Firing - is_scheduled returns false
    active_triggers
        .set_state(&key, time, TimerType::Application, TimerState::Firing)
        .await;
    assert!(
        !active_triggers
            .is_scheduled(&key, time, TimerType::Application)
            .await
    );

    // Transition to FiringRescheduled - is_scheduled returns true
    active_triggers
        .set_state(
            &key,
            time,
            TimerType::Application,
            TimerState::FiringRescheduled,
        )
        .await;
    assert!(
        active_triggers
            .is_scheduled(&key, time, TimerType::Application)
            .await
    );

    // Transition to Aborted - is_scheduled returns false
    active_triggers
        .set_state(&key, time, TimerType::Application, TimerState::Aborted)
        .await;
    assert!(
        !active_triggers
            .is_scheduled(&key, time, TimerType::Application)
            .await
    );

    // Transition back to Scheduled - is_scheduled returns true
    active_triggers
        .set_state(&key, time, TimerType::Application, TimerState::Scheduled)
        .await;
    assert!(
        active_triggers
            .is_scheduled(&key, time, TimerType::Application)
            .await
    );
}

#[test]
async fn test_set_state_nonexistent() {
    let active_triggers = ActiveTriggers::default();
    let key = Key::from("nonexistent-key");
    let time = CompactDateTime::from(33333u32);

    // set_state on non-existent trigger should return false
    assert!(
        !active_triggers
            .set_state(&key, time, TimerType::Application, TimerState::Firing)
            .await
    );

    // Still should not exist
    assert!(
        active_triggers
            .get_state(&key, time, TimerType::Application)
            .await
            .is_none()
    );
}

// =========================================================================
// TimerSnapshot tests
// =========================================================================

#[test]
async fn test_snapshot_empty() {
    let active_triggers = ActiveTriggers::default();
    let now = CompactDateTime::from(10000u32);
    let s = active_triggers.snapshot(now).await;
    assert_eq!(s.active, 0);
    assert_eq!(s.in_flight, 0);
    assert_eq!(s.overdue, 0);
    assert_eq!(s.oldest_overdue_secs, 0);
}

#[test]
async fn test_snapshot_scheduled_future() {
    let active_triggers = ActiveTriggers::default();
    let now = CompactDateTime::from(1000u32);
    let future_time = CompactDateTime::from(2000u32);
    let key = Key::from("k");

    active_triggers
        .insert(Trigger::new(
            key,
            future_time,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;

    let s = active_triggers.snapshot(now).await;
    assert_eq!(s.active, 1);
    assert_eq!(s.in_flight, 0);
    assert_eq!(s.overdue, 0);
    assert_eq!(s.oldest_overdue_secs, 0);
}

#[test]
async fn test_snapshot_scheduled_overdue() {
    let active_triggers = ActiveTriggers::default();
    let past_time = CompactDateTime::from(1000u32);
    let now = CompactDateTime::from(1030u32);
    let key = Key::from("k");

    active_triggers
        .insert(Trigger::new(
            key,
            past_time,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;

    let s = active_triggers.snapshot(now).await;
    assert_eq!(s.active, 1);
    assert_eq!(s.in_flight, 0);
    assert_eq!(s.overdue, 1);
    assert_eq!(s.oldest_overdue_secs, 30);
}

#[test]
async fn test_snapshot_firing_counts_in_flight_and_overdue() {
    let active_triggers = ActiveTriggers::default();
    let time = CompactDateTime::from(1000u32);
    let now = CompactDateTime::from(1000u32);
    let key = Key::from("k");

    active_triggers
        .insert(Trigger::new(
            key.clone(),
            time,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;
    active_triggers
        .set_state(&key, time, TimerType::Application, TimerState::Firing)
        .await;

    let s = active_triggers.snapshot(now).await;
    assert_eq!(s.active, 1);
    assert_eq!(s.in_flight, 1);
    assert_eq!(s.overdue, 1);
    assert_eq!(s.oldest_overdue_secs, 0);
}

#[test]
async fn test_snapshot_mixed_states() {
    let active_triggers = ActiveTriggers::default();
    let now = CompactDateTime::from(2000u32);
    // overdue scheduled
    let past = CompactDateTime::from(1900u32);
    // future scheduled
    let future = CompactDateTime::from(3000u32);
    // overdue firing
    let past_firing = CompactDateTime::from(1800u32);

    let k1 = Key::from("k1");
    let k2 = Key::from("k2");
    let k3 = Key::from("k3");

    active_triggers
        .insert(Trigger::new(
            k1.clone(),
            past,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;
    active_triggers
        .insert(Trigger::new(
            k2,
            future,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;
    active_triggers
        .insert(Trigger::new(
            k3.clone(),
            past_firing,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;
    active_triggers
        .set_state(&k3, past_firing, TimerType::Application, TimerState::Firing)
        .await;

    let s = active_triggers.snapshot(now).await;
    assert_eq!(s.active, 3);
    assert_eq!(s.in_flight, 1);
    assert_eq!(s.overdue, 2); // past + past_firing
    assert_eq!(s.oldest_overdue_secs, 200); // now(2000) - past_firing(1800)
}

#[test]
async fn test_snapshot_firing_rescheduled_counts_in_flight() {
    let active_triggers = ActiveTriggers::default();
    let time = CompactDateTime::from(1000u32);
    let now = CompactDateTime::from(1000u32);
    let key = Key::from("k");

    active_triggers
        .insert(Trigger::new(
            key.clone(),
            time,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;
    active_triggers
        .set_state(
            &key,
            time,
            TimerType::Application,
            TimerState::FiringRescheduled,
        )
        .await;

    let s = active_triggers.snapshot(now).await;
    assert_eq!(s.active, 1);
    assert_eq!(s.in_flight, 1);
    assert_eq!(s.overdue, 1);
}

#[test]
async fn test_snapshot_aborted_counts_active_and_overdue_not_in_flight() {
    let active_triggers = ActiveTriggers::default();
    let time = CompactDateTime::from(1000u32);
    let now = CompactDateTime::from(1030u32);
    let key = Key::from("k");

    active_triggers
        .insert(Trigger::new(
            key.clone(),
            time,
            TimerType::Application,
            tracing::Span::current(),
        ))
        .await;
    active_triggers
        .set_state(&key, time, TimerType::Application, TimerState::Aborted)
        .await;

    let s = active_triggers.snapshot(now).await;
    assert_eq!(s.active, 1);
    assert_eq!(s.in_flight, 0);
    assert_eq!(s.overdue, 1);
    assert_eq!(s.oldest_overdue_secs, 30);
}
