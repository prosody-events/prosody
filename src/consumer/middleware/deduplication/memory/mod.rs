//! In-memory deduplication store for testing.
//!
//! Uses [`scc::HashSet`] for lock-free concurrent access. All data is volatile.

use super::store::{DeduplicationStore, DeduplicationStoreProvider};
use crate::{Partition, Topic};
use ahash::RandomState;
use scc::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use uuid::Uuid;

/// In-memory deduplication store backed by a lock-free hash set.
#[derive(Clone, Debug)]
pub struct MemoryDeduplicationStore {
    set: Arc<HashSet<Uuid, RandomState>>,
}

impl MemoryDeduplicationStore {
    /// Creates a new empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            set: Arc::new(HashSet::with_hasher(RandomState::new())),
        }
    }
}

impl Default for MemoryDeduplicationStore {
    fn default() -> Self {
        Self::new()
    }
}

impl DeduplicationStore for MemoryDeduplicationStore {
    type Error = Infallible;

    async fn exists(&self, id: Uuid) -> Result<bool, Self::Error> {
        Ok(self.set.contains_async(&id).await)
    }

    async fn insert(&self, id: Uuid) -> Result<(), Self::Error> {
        let _ = self.set.insert_async(id).await;
        Ok(())
    }
}

/// Hands out clones of one shared in-memory deduplication store.
///
/// Every `create_store` call returns a clone of the **same** `Arc`-backed
/// store, so the deduplication middleware (writer) and the keyed-state commit
/// oracle (reader) observe the same rows for a partition — exactly as the
/// Cassandra provider does by sharing its session and cache. A fresh store per
/// call would split-brain the oracle: it would never see the middleware's
/// inserts and message recovery would silently fail. The dedup UUID encodes
/// topic/partition, so one shared set across partitions cannot collide.
#[derive(Clone, Debug, Default)]
pub struct MemoryDeduplicationStoreProvider {
    store: MemoryDeduplicationStore,
}

impl MemoryDeduplicationStoreProvider {
    /// Creates a new provider backed by one fresh shared store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            store: MemoryDeduplicationStore::new(),
        }
    }
}

impl DeduplicationStoreProvider for MemoryDeduplicationStoreProvider {
    type Store = MemoryDeduplicationStore;

    fn create_store(
        &self,
        _topic: Topic,
        _partition: Partition,
        _consumer_group: &str,
    ) -> Self::Store {
        self.store.clone()
    }
}

#[cfg(test)]
mod prop_tests {
    use std::convert::Infallible;

    use super::MemoryDeduplicationStore;

    crate::dedup_store_tests!(async { Ok::<_, Infallible>(MemoryDeduplicationStore::new()) });
}

#[cfg(test)]
mod tests;
