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

    pub async fn remove(&self, trigger: &Trigger) {
        if let Entry::Occupied(mut occupied) = self.0.entry_async(trigger.key.clone()).await {
            let times = occupied.get_mut();
            times.remove(&trigger.time);
            if times.is_empty() {
                let _ = occupied.remove();
            }
        }
    }

    pub async fn contains(&self, trigger: &Trigger) -> bool {
        self.0
            .read_async(&trigger.key, |_, v| v.contains(&trigger.time))
            .await
            .unwrap_or_default()
    }

    pub async fn key_times(&self, key: Key) -> BTreeSet<CompactDateTime> {
        self.0
            .read_async(&key, |_, v| v.clone())
            .await
            .unwrap_or_default()
    }
}
