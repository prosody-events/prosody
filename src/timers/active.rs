//! Active trigger registry with explicit timer lifecycle states.
//!
//! Provides [`ActiveTriggers`], a thread-safe registry tracking timer
//! triggers currently loaded in the scheduler. Each trigger is identified
//! by a [`Key`], scheduled [`CompactDateTime`], and [`TimerType`].
//!
//! # Timer State Machine
//!
//! Each timer transitions through these states:
//!
//! ```text
//! UNSCHEDULED ──schedule()──► SCHEDULED ──fire()──► FIRING
//!      ▲                          │                   │ │
//!      │                   unschedule()          commit() schedule(same)
//!      │                          │                   │      │
//!      │                          ▼                   ▼      ▼
//!      └──────────────────── UNSCHEDULED      UNSCHEDULED  FIRING_RESCHEDULED
//!                                                    ▲            │ │
//!                                               commit()    abort() unschedule(same)
//!                                                    │            │ │
//!                                                    └─SCHEDULED◄─┘ │
//!                                                         ▲         │
//!                                                         └─────────┘
//! ```
//!
//! # State Definitions
//!
//! | State                | `ActiveTriggers`    | `DelayQueue` | `Database` |
//! |----------------------|---------------------|--------------|------------|
//! | `UNSCHEDULED`        | absent              | -            | -          |
//! | `SCHEDULED`          | `Scheduled`         | ✓            | ✓          |
//! | `FIRING`             | `Firing`            | -            | ✓          |
//! | `FIRING_RESCHEDULED` | `FiringRescheduled` | ✓            | ✓          |
//!
//! # Transition Effects
//!
//! | From                 | Operation          | To                   | `ActiveTriggers`      | `DelayQueue`   | `Database` |
//! |----------------------|--------------------|----------------------|-----------------------|----------------|------------|
//! | `UNSCHEDULED`        | `schedule()`       | `SCHEDULED`          | insert `Scheduled`    | insert         | insert     |
//! | `SCHEDULED`          | `unschedule()`     | `UNSCHEDULED`        | remove                | remove         | delete     |
//! | `SCHEDULED`          | `fire()`           | `FIRING`             | → `Firing`            | (auto-removed) | -          |
//! | `FIRING`             | `schedule(same)`   | `FIRING_RESCHEDULED` | → `FiringRescheduled` | insert         | -          |
//! | `FIRING`             | `unschedule(same)` | `FIRING`             | -                     | -              | -          |
//! | `FIRING`             | `commit()`         | `UNSCHEDULED`        | remove                | -              | delete     |
//! | `FIRING`             | `abort()`          | `SCHEDULED`*         | remove                | -              | -          |
//! | `FIRING_RESCHEDULED` | `unschedule(same)` | `FIRING`             | → `Firing`            | remove         | -          |
//! | `FIRING_RESCHEDULED` | `commit()`         | `SCHEDULED`          | → `Scheduled`         | -              | -          |
//! | `FIRING_RESCHEDULED` | `abort()`          | `SCHEDULED`          | → `Scheduled`         | -              | -          |
//!
//! *Database row preserved; slab loader restores on restart.
//!
//! # API Behavior by State
//!
//! | Operation            | `SCHEDULED` | `FIRING`   | `FIRING_RESCHEDULED` |
//! |----------------------|-------------|------------|----------------------|
//! | `schedule(T)`        | no-op       | reschedule | no-op (idempotent)   |
//! | `unschedule(T)`      | remove      | no-op      | cancel reschedule    |
//! | `scheduled_times()`  | include     | exclude    | include              |
//! | `commit(T)`          | N/A         | delete DB  | keep DB              |

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use scc::hash_map::Entry;
use std::sync::Arc;

/// Maps (time, type) tuples to their lifecycle state for a single key.
type TriggerStateMap = HashMap<(CompactDateTime, TimerType), TimerState>;

/// Lifecycle state of a timer in the in-memory scheduler.
///
/// Each timer in [`ActiveTriggers`] is in exactly one of these states.
/// The state determines how operations like `schedule()`, `unschedule()`,
/// `commit()`, and `abort()` behave. See `data-model.md` for the full
/// state machine diagram.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TimerState {
    /// Timer is in the `DelayQueue`, waiting to fire.
    ///
    /// This is the default state for newly scheduled timers.
    #[default]
    Scheduled,

    /// Handler is processing this timer; timer has not been rescheduled.
    ///
    /// In this state:
    /// - `schedule(same)` transitions to `FiringRescheduled`
    /// - `unschedule(same)` is a no-op
    /// - `commit()` deletes the database row and removes from `ActiveTriggers`
    /// - `abort()` keeps the database row and removes from `ActiveTriggers`
    Firing,

    /// Handler is processing this timer; timer will fire again after commit.
    ///
    /// In this state:
    /// - `schedule(same)` is idempotent (no-op)
    /// - `unschedule(same)` transitions back to `Firing`
    /// - `commit()` keeps the database row and transitions to `Scheduled`
    /// - `abort()` keeps the database row and transitions to `Scheduled`
    FiringRescheduled,
}

/// A concurrent registry of active timer triggers.
///
/// Maps each [`Key`] to a [`TriggerStateMap`] of `(CompactDateTime, TimerType)`
/// to [`TimerState`]. This allows timers of different types to coexist at the
/// same (key, time). Cloning shares the same underlying registry.
#[derive(Clone, Debug, Default)]
pub struct ActiveTriggers(Arc<scc::HashMap<Key, TriggerStateMap>>);

impl ActiveTriggers {
    /// Inserts a trigger into the active registry with
    /// [`TimerState::Scheduled`] state.
    ///
    /// Creates a new map of (time, type) to state if no entry exists for the
    /// trigger's key. Duplicate insertions are ignored if the trigger already
    /// exists.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The [`Trigger`] containing the key, time, and type to
    ///   insert.
    pub async fn insert(&self, trigger: Trigger) {
        // Obtain or create the entry for this key, then insert the (time, type) with
        // Scheduled state.
        self.0
            .entry_async(trigger.key)
            .await
            .or_default()
            .get_mut()
            .entry((trigger.time, trigger.timer_type))
            .or_insert(TimerState::Scheduled);
    }

    /// Removes a trigger time for a specific key and timer type.
    ///
    /// Removes the key from the registry if removing the (time, type) entry
    /// leaves the map empty. Removing non-existent keys or entries has no
    /// effect.
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] from which to remove the trigger.
    /// * `time` - The [`CompactDateTime`] to remove.
    /// * `timer_type` - The [`TimerType`] to remove.
    pub async fn remove(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        // Look up the entry; if it exists, remove the (time, type) entry and clean up.
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await {
            let states = occupied.get_mut();
            states.remove(&(time, timer_type));

            if states.is_empty() {
                let _ = occupied.remove();
            }
        }
    }

    /// Returns the state of a given trigger time and type for a key.
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] to query.
    /// * `time` - The [`CompactDateTime`] to check.
    /// * `timer_type` - The [`TimerType`] to check.
    ///
    /// # Returns
    ///
    /// `Some(TimerState)` if the registry contains the specified (time, type)
    /// entry for the key, `None` otherwise.
    pub async fn get_state(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<TimerState> {
        // Read the entry for the key, returning None if absent.
        self.0
            .read_async(key, |_, states| states.get(&(time, timer_type)).copied())
            .await
            .flatten()
    }

    /// Checks whether a given trigger time and type is active for a key.
    ///
    /// This is a convenience wrapper around [`get_state`](Self::get_state)
    /// that returns `true` if any state exists.
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] to query.
    /// * `time` - The [`CompactDateTime`] to check.
    /// * `timer_type` - The [`TimerType`] to check.
    ///
    /// # Returns
    ///
    /// `true` if the registry contains the specified (time, type) entry for the
    /// key.
    pub async fn contains(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) -> bool {
        self.get_state(key, time, timer_type).await.is_some()
    }

    /// Checks whether a trigger is considered scheduled (will fire in the
    /// future).
    ///
    /// A timer is scheduled if it is in [`TimerState::Scheduled`] or
    /// [`TimerState::FiringRescheduled`] state. Timers in
    /// [`TimerState::Firing`] are currently being processed but not scheduled
    /// to fire again.
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] to query.
    /// * `time` - The [`CompactDateTime`] to check.
    /// * `timer_type` - The [`TimerType`] to check.
    ///
    /// # Returns
    ///
    /// `true` if the timer is in `Scheduled` or `FiringRescheduled` state.
    pub async fn is_scheduled(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> bool {
        self.get_state(key, time, timer_type)
            .await
            .is_some_and(|state| {
                matches!(state, TimerState::Scheduled | TimerState::FiringRescheduled)
            })
    }

    /// Atomically sets the state of a timer.
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] of the timer.
    /// * `time` - The [`CompactDateTime`] of the timer.
    /// * `timer_type` - The [`TimerType`] of the timer.
    /// * `state` - The new [`TimerState`] to set.
    ///
    /// # Returns
    ///
    /// `true` if the state was set (timer exists), `false` otherwise.
    pub async fn set_state(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
        state: TimerState,
    ) -> bool {
        // Update the state if the entry exists.
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await {
            let states = occupied.get_mut();
            if let Some(current_state) = states.get_mut(&(time, timer_type)) {
                *current_state = state;
                return true;
            }
        }
        false
    }

    /// Invokes a closure for every active trigger time and type in the
    /// registry.
    ///
    /// The closure is called once for each stored `(CompactDateTime,
    /// TimerType)` tuple across all keys. Iteration order depends on
    /// internal hash map ordering.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that takes a [`CompactDateTime`] and [`TimerType`].
    pub async fn scan_active_times<F>(&self, mut f: F)
    where
        F: FnMut(CompactDateTime, TimerType),
    {
        // For each key and its map of (time, type) → state, apply the callback.
        self.0
            .iter_async(|_, states| {
                for &(time, timer_type) in states.keys() {
                    f(time, timer_type);
                }
                true // Continue iteration over all entries
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
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
            TimerType::DeferRetry,
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
                .contains(&key, time, TimerType::DeferRetry)
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
                .contains(&key, time, TimerType::DeferRetry)
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
            TimerType::DeferRetry,
            tracing::Span::current(),
        );

        active_triggers.insert(app).await;
        active_triggers.insert(retry).await;

        // Remove Application type
        active_triggers
            .remove(&key, time, TimerType::Application)
            .await;

        // DeferRetry should still exist
        assert!(
            !active_triggers
                .contains(&key, time, TimerType::Application)
                .await
        );
        assert!(
            active_triggers
                .contains(&key, time, TimerType::DeferRetry)
                .await
        );

        // Remove DeferRetry type
        active_triggers
            .remove(&key, time, TimerType::DeferRetry)
            .await;

        // Both should be gone
        assert!(
            !active_triggers
                .contains(&key, time, TimerType::Application)
                .await
        );
        assert!(
            !active_triggers
                .contains(&key, time, TimerType::DeferRetry)
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

        // Insert DeferRetry at time1 (same time, different type)
        active_triggers
            .insert(Trigger::new(
                key.clone(),
                time1,
                TimerType::DeferRetry,
                tracing::Span::current(),
            ))
            .await;

        // Insert DeferRetry at time2
        active_triggers
            .insert(Trigger::new(
                key.clone(),
                time2,
                TimerType::DeferRetry,
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
        assert!(collected.contains(&(time1, TimerType::DeferRetry)));
        assert!(collected.contains(&(time2, TimerType::DeferRetry)));
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
}
