//! Active trigger registry.
//!
//! Provides [`ActiveTriggers`], a thread-safe registry tracking timer
//! triggers currently loaded in the scheduler. Each trigger is identified
//! by a [`Key`], scheduled [`CompactDateTime`], and [`TimerType`]. Supports
//! concurrent insertion, removal, membership checks, and scanning.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use ahash::HashSet;
use scc::hash_map::Entry;
use std::sync::Arc;

/// A concurrent registry of active timer triggers.
///
/// Maps each [`Key`] to a [`HashSet`] of `(CompactDateTime, TimerType)` tuples.
/// This allows timers of different types to coexist at the same (key, time).
/// Cloning shares the same underlying registry.
#[derive(Clone, Debug, Default)]
pub struct ActiveTriggers(Arc<scc::HashMap<Key, HashSet<(CompactDateTime, TimerType)>>>);

impl ActiveTriggers {
    /// Inserts a trigger into the active registry.
    ///
    /// Creates a new set of (time, type) tuples if no entry exists for the
    /// trigger's key. Duplicate insertions are ignored.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The [`Trigger`] containing the key, time, and type to
    ///   insert.
    pub async fn insert(&self, trigger: Trigger) {
        // Obtain or create the entry for this key, then insert the (time, type) tuple.
        self.0
            .entry_async(trigger.key)
            .await
            .or_default()
            .get_mut()
            .insert((trigger.time, trigger.timer_type));
    }

    /// Removes a trigger time for a specific key and timer type.
    ///
    /// Removes the key from the registry if removing the (time, type) tuple
    /// leaves the set empty. Removing non-existent keys or tuples has no
    /// effect.
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] from which to remove the trigger.
    /// * `time` - The [`CompactDateTime`] to remove.
    /// * `timer_type` - The [`TimerType`] to remove.
    pub async fn remove(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        // Look up the entry; if it exists, remove the (time, type) tuple and clean up.
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await {
            let times = occupied.get_mut();
            times.remove(&(time, timer_type));

            if times.is_empty() {
                let _ = occupied.remove();
            }
        }
    }

    /// Checks whether a given trigger time and type is active for a key.
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] to query.
    /// * `time` - The [`CompactDateTime`] to check.
    /// * `timer_type` - The [`TimerType`] to check.
    ///
    /// # Returns
    ///
    /// `true` if the registry contains the specified (time, type) tuple for the
    /// key.
    pub async fn contains(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) -> bool {
        // Read the entry for the key, returning false if absent.
        self.0
            .read_async(key, |_, times| times.contains(&(time, timer_type)))
            .await
            .unwrap_or_default()
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
        // For each key and its set of (time, type) tuples, apply the callback.
        self.0
            .iter_async(|_, times| {
                for &(time, timer_type) in times {
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

        // Insert the trigger
        active_triggers.insert(trigger.clone()).await;

        // Now, the trigger should be present
        assert!(
            active_triggers
                .contains(&key, time, TimerType::Application)
                .await
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

        // Verify both triggers exist
        assert!(
            active_triggers
                .contains(&key, time1, TimerType::Application)
                .await
        );
        assert!(
            active_triggers
                .contains(&key, time2, TimerType::Application)
                .await
        );

        // Remove one trigger
        active_triggers
            .remove(&key, time1, TimerType::Application)
            .await;

        // Verify only the second trigger exists
        assert!(
            !active_triggers
                .contains(&key, time1, TimerType::Application)
                .await
        );
        assert!(
            active_triggers
                .contains(&key, time2, TimerType::Application)
                .await
        );

        // Remove the second trigger
        active_triggers
            .remove(&key, time2, TimerType::Application)
            .await;

        // Verify no triggers exist for the key
        assert!(
            !active_triggers
                .contains(&key, time1, TimerType::Application)
                .await
        );
        assert!(
            !active_triggers
                .contains(&key, time2, TimerType::Application)
                .await
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
}
