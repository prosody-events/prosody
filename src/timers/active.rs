//! Active trigger registry.
//!
//! Provides [`ActiveTriggers`], a thread-safe registry tracking timer
//! triggers currently loaded in the scheduler. Each trigger is identified
//! by a [`Key`] and scheduled [`CompactDateTime`]. Supports concurrent
//! insertion, removal, membership checks, and scanning.

use crate::Key;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use ahash::HashSet;
use scc::hash_map::Entry;
use std::sync::Arc;

/// A concurrent registry of active timer triggers.
///
/// Maps each [`Key`] to a [`HashSet`] of [`CompactDateTime`] values.
/// Cloning shares the same underlying registry.
#[derive(Clone, Debug, Default)]
pub struct ActiveTriggers(Arc<scc::HashMap<Key, HashSet<CompactDateTime>>>);

impl ActiveTriggers {
    /// Inserts a trigger into the active registry.
    ///
    /// Creates a new set of times if no entry exists for the trigger's key.
    /// Duplicate insertions are ignored.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The [`Trigger`] containing the key and time to insert.
    pub async fn insert(&self, trigger: Trigger) {
        // Obtain or create the entry for this key, then insert the time.
        self.0
            .entry_async(trigger.key)
            .await
            .or_default()
            .get_mut()
            .insert(trigger.time);
    }

    /// Removes a trigger time for a specific key.
    ///
    /// Removes the key from the registry if removing the time leaves the
    /// set empty. Removing non-existent keys or times has no effect.
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] from which to remove the time.
    /// * `time` - The [`CompactDateTime`] to remove.
    pub async fn remove(&self, key: &Key, time: CompactDateTime) {
        // Look up the entry; if it exists, remove the time and clean up.
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await {
            let times = occupied.get_mut();
            times.remove(&time);

            if times.is_empty() {
                let _ = occupied.remove();
            }
        }
    }

    /// Checks whether a given trigger time is active for a key.
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] to query.
    /// * `time` - The [`CompactDateTime`] to check.
    ///
    /// # Returns
    ///
    /// `true` if the registry contains the specified time for the key.
    pub async fn contains(&self, key: &Key, time: CompactDateTime) -> bool {
        // Read the entry for the key, returning false if absent.
        self.0
            .read_async(key, |_, times| times.contains(&time))
            .await
            .unwrap_or_default()
    }

    /// Invokes a closure for every active trigger time in the registry.
    ///
    /// The closure is called once for each stored [`CompactDateTime`] across
    /// all keys. Iteration order depends on internal hash map ordering.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that takes a [`CompactDateTime`].
    pub async fn scan_active_times<F>(&self, mut f: F)
    where
        F: FnMut(CompactDateTime),
    {
        // For each key and its set of times, apply the callback.
        self.0
            .iter_async(|_, times| {
                for &time in times {
                    f(time);
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
    use crate::timers::Trigger;
    use crate::timers::datetime::CompactDateTime;
    use tokio::test;

    #[test]
    async fn test_insert_and_contains() {
        let active_triggers = ActiveTriggers::default();

        let key = Key::from("test-key");
        let time = CompactDateTime::from(12345u32);
        let trigger = Trigger {
            key: key.clone(),
            time,
            span: tracing::Span::current(),
        };

        // Initially, the trigger should not be present
        assert!(!active_triggers.contains(&key, time).await);

        // Insert the trigger
        active_triggers.insert(trigger.clone()).await;

        // Now, the trigger should be present
        assert!(active_triggers.contains(&key, time).await);
    }

    #[test]
    async fn test_remove() {
        let active_triggers = ActiveTriggers::default();

        let key = Key::from("test-key");
        let time = CompactDateTime::from(12345u32);
        let trigger = Trigger {
            key: key.clone(),
            time,
            span: tracing::Span::current(),
        };

        // Insert the trigger
        active_triggers.insert(trigger.clone()).await;

        // Verify it exists
        assert!(active_triggers.contains(&key, time).await);

        // Remove the trigger
        active_triggers.remove(&key, time).await;

        // Verify it no longer exists
        assert!(!active_triggers.contains(&key, time).await);
    }

    #[test]
    async fn test_multiple_triggers_same_key() {
        let active_triggers = ActiveTriggers::default();

        let key = Key::from("shared-key");
        let time1 = CompactDateTime::from(12345u32);
        let time2 = CompactDateTime::from(67890u32);

        let trigger1 = Trigger {
            key: key.clone(),
            time: time1,
            span: tracing::Span::current(),
        };
        let trigger2 = Trigger {
            key: key.clone(),
            time: time2,
            span: tracing::Span::current(),
        };

        // Insert both triggers
        active_triggers.insert(trigger1.clone()).await;
        active_triggers.insert(trigger2.clone()).await;

        // Verify both triggers exist
        assert!(active_triggers.contains(&key, time1).await);
        assert!(active_triggers.contains(&key, time2).await);

        // Remove one trigger
        active_triggers.remove(&key, time1).await;

        // Verify only the second trigger exists
        assert!(!active_triggers.contains(&key, time1).await);
        assert!(active_triggers.contains(&key, time2).await);

        // Remove the second trigger
        active_triggers.remove(&key, time2).await;

        // Verify no triggers exist for the key
        assert!(!active_triggers.contains(&key, time1).await);
        assert!(!active_triggers.contains(&key, time2).await);
    }

    #[test]
    async fn test_multiple_keys() {
        let active_triggers = ActiveTriggers::default();

        let key1 = Key::from("key-1");
        let key2 = Key::from("key-2");
        let time1 = CompactDateTime::from(11111u32);
        let time2 = CompactDateTime::from(22222u32);

        let trigger1 = Trigger {
            key: key1.clone(),
            time: time1,
            span: tracing::Span::current(),
        };
        let trigger2 = Trigger {
            key: key2.clone(),
            time: time2,
            span: tracing::Span::current(),
        };

        // Insert triggers for different keys
        active_triggers.insert(trigger1.clone()).await;
        active_triggers.insert(trigger2.clone()).await;

        // Verify both triggers exist under their respective keys
        assert!(active_triggers.contains(&key1, time1).await);
        assert!(active_triggers.contains(&key2, time2).await);

        // Verify cross-key isolation
        assert!(!active_triggers.contains(&key1, time2).await);
        assert!(!active_triggers.contains(&key2, time1).await);

        // Remove one trigger
        active_triggers.remove(&key1, time1).await;

        // Verify only the second trigger remains
        assert!(!active_triggers.contains(&key1, time1).await);
        assert!(active_triggers.contains(&key2, time2).await);
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
            .insert(Trigger {
                key: key1.clone(),
                time: time1,
                span: tracing::Span::current(),
            })
            .await;

        active_triggers
            .insert(Trigger {
                key: key1.clone(),
                time: time2,
                span: tracing::Span::current(),
            })
            .await;

        active_triggers
            .insert(Trigger {
                key: key2.clone(),
                time: time3,
                span: tracing::Span::current(),
            })
            .await;

        // Collect all active times using scan_active_times
        let mut collected_times = Vec::new();
        active_triggers
            .scan_active_times(|time| {
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
        assert!(!active_triggers.contains(&key, time).await);

        // Test remove on non-existent key
        active_triggers.remove(&key, time).await; // Should not panic

        // Test remove on non-existent time for existing key
        let trigger = Trigger {
            key: key.clone(),
            time,
            span: tracing::Span::current(),
        };
        active_triggers.insert(trigger).await;

        let other_time = CompactDateTime::from(99999u32);
        active_triggers.remove(&key, other_time).await; // Should not panic

        // Original trigger should still exist
        assert!(active_triggers.contains(&key, time).await);
    }

    #[test]
    async fn test_scan_active_times_empty() {
        let active_triggers = ActiveTriggers::default();

        let mut call_count = 0_i32;
        active_triggers
            .scan_active_times(|_| {
                call_count += 1_i32;
            })
            .await;

        // Should not call the function for empty state
        assert_eq!(call_count, 0_i32);
    }
}
