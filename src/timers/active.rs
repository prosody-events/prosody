use crate::Key;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use scc::hash_map::Entry;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct ActiveTriggers(Arc<scc::HashMap<Key, BTreeSet<CompactDateTime>>>);

impl ActiveTriggers {
    pub async fn insert(&self, trigger: Trigger) {
        self.0
            .entry_async(trigger.key)
            .await
            .or_default()
            .get_mut()
            .insert(trigger.time);
    }

    pub async fn remove(&self, key: &Key, time: CompactDateTime) {
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await {
            let times = occupied.get_mut();
            times.remove(&time);
            if times.is_empty() {
                let _ = occupied.remove();
            }
        }
    }

    pub async fn contains(&self, key: &Key, time: CompactDateTime) -> bool {
        self.0
            .read_async(key, |_, v| v.contains(&time))
            .await
            .unwrap_or_default()
    }

    pub async fn scan_active_times<F>(&self, mut f: F)
    where
        F: FnMut(CompactDateTime),
    {
        self.0
            .scan_async(|_, times| {
                for &time in times {
                    f(time);
                }
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
}
