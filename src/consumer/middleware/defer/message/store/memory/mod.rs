//! In-memory message defer store for testing.
//!
//! Uses [`scc::HashMap`] for lock-free concurrent access. All data is volatile.

use super::MessageDeferStore;
use super::provider::MessageDeferStoreProvider;
use crate::{Key, Offset, Partition, Topic};

#[cfg(test)]
use crate::defer_store_tests;
use ahash::RandomState;
use scc::HashMap;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::sync::Arc;

/// In-memory message defer store.
///
/// Lock-free via [`scc::HashMap`]. Each key maps to a `BTreeSet<Offset>`
/// (sorted queue) plus a shared retry counter. Thread-safe and cheap to clone.
///
/// Each store instance is scoped to a segment; partition isolation comes from
/// creating separate instances per partition.
#[derive(Clone, Debug)]
pub struct MemoryMessageDeferStore {
    inner: Arc<Inner>,
}

impl MemoryMessageDeferStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }
}

impl Default for MemoryMessageDeferStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Storage: `key` → (`sorted offsets`, `retry_count`).
#[derive(Debug)]
struct Inner {
    deferred: HashMap<Key, (BTreeSet<Offset>, u32), RandomState>,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            deferred: HashMap::with_hasher(RandomState::new()),
        }
    }
}

impl MessageDeferStore for MemoryMessageDeferStore {
    type Error = Infallible;

    async fn defer_first_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.inner
            .deferred
            .entry_async(Arc::clone(key))
            .await
            .and_modify(|(offsets, retry_count)| {
                offsets.insert(offset);
                *retry_count = 0;
            })
            .or_insert_with(|| {
                let mut offsets = BTreeSet::new();
                offsets.insert(offset);
                (offsets, 0)
            });

        Ok(())
    }

    async fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        let result = self
            .inner
            .deferred
            .get_async(key.as_ref())
            .await
            .and_then(|entry| {
                let (offsets, retry_count) = entry.get();
                offsets.first().map(|&offset| (offset, *retry_count))
            });

        Ok(result)
    }

    async fn append_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.inner
            .deferred
            .entry_async(Arc::clone(key))
            .await
            .and_modify(|(offsets, _)| {
                offsets.insert(offset);
            })
            .or_insert_with(|| {
                // Shouldn't happen (should use defer_first_message first)
                // but handle gracefully with retry_count=0
                let mut offsets = BTreeSet::new();
                offsets.insert(offset);
                (offsets, 0)
            });

        Ok(())
    }

    async fn remove_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        // Drop the entry when its last offset is removed. Once all deferred
        // messages for a key are processed, the entry is dead state
        // (retry_count = 0 ≡ retry_count absent), matching Cassandra's
        // delete_key on min-only-row removal. Atomic via remove_if_async.
        let _ = self
            .inner
            .deferred
            .remove_if_async(key.as_ref(), |(offsets, _)| {
                offsets.remove(&offset);
                offsets.is_empty()
            })
            .await;

        Ok(())
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        // No-op on a key with no offsets. Production only calls this with an
        // active deferred message present; creating an entry here would leave
        // an orphan, violating "no entry after all messages are processed."
        let _ = self
            .inner
            .deferred
            .entry_async(Arc::clone(key))
            .await
            .and_modify(|(_, current)| {
                *current = retry_count;
            });

        Ok(())
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        self.inner.deferred.remove_async(key.as_ref()).await;
        Ok(())
    }
}

/// Creates isolated in-memory stores per partition.
#[derive(Clone, Debug, Default)]
pub struct MemoryMessageDeferStoreProvider;

impl MemoryMessageDeferStoreProvider {
    /// Creates a new provider.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl MessageDeferStoreProvider for MemoryMessageDeferStoreProvider {
    type Store = MemoryMessageDeferStore;

    fn create_store(
        &self,
        _topic: Topic,
        _partition: Partition,
        _consumer_group: &str,
        _cache_size: usize,
    ) -> Self::Store {
        MemoryMessageDeferStore::new()
    }
}

#[cfg(test)]
mod tests;
