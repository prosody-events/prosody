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
        let _ = self
            .inner
            .deferred
            .entry_async(Arc::clone(key))
            .await
            .and_modify(|(offsets, _)| {
                offsets.remove(&offset);
            });

        Ok(())
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        self.inner
            .deferred
            .entry_async(Arc::clone(key))
            .await
            .and_modify(|(_, current)| {
                *current = retry_count;
            })
            .or_insert_with(|| (BTreeSet::new(), retry_count));

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
mod tests {
    use super::*;
    use crate::Key;

    fn create_test_store() -> MemoryMessageDeferStore {
        MemoryMessageDeferStore::new()
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() -> color_eyre::Result<()> {
        let store = create_test_store();
        let key: Key = Arc::from("test-key-1");

        let result = store.get_next_deferred_message(&key).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_append_and_get() -> color_eyre::Result<()> {
        let store = create_test_store();
        let key: Key = Arc::from("test-key-1");
        let offset = Offset::from(42_i64);

        // Use defer_first_message for first failure
        store.defer_first_message(&key, offset).await?;

        let result = store.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((offset, 0)));
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_offsets_returns_oldest() -> color_eyre::Result<()> {
        let store = create_test_store();
        let key: Key = Arc::from("test-key-1");

        // Defer first message
        store
            .defer_first_message(&key, Offset::from(100_i64))
            .await?;
        // Append additional messages
        store
            .append_deferred_message(&key, Offset::from(50_i64))
            .await?;
        store
            .append_deferred_message(&key, Offset::from(150_i64))
            .await?;

        // Should return the oldest (smallest) offset
        let result = store.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((Offset::from(50_i64), 0)));
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_offset() -> color_eyre::Result<()> {
        let store = create_test_store();
        let key: Key = Arc::from("test-key-1");
        let offset = Offset::from(42_i64);

        store.defer_first_message(&key, offset).await?;
        store.remove_deferred_message(&key, offset).await?;

        let result = store.get_next_deferred_message(&key).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_nonexistent() -> color_eyre::Result<()> {
        let store = create_test_store();
        let key: Key = Arc::from("test-key-1");

        // Should not error
        store
            .remove_deferred_message(&key, Offset::from(42_i64))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_set_retry_count() -> color_eyre::Result<()> {
        let store = create_test_store();
        let key: Key = Arc::from("test-key-1");
        let offset = Offset::from(42_i64);

        // Defer first message
        store.defer_first_message(&key, offset).await?;

        // Update retry_count to 5
        store.set_retry_count(&key, 5).await?;

        let result = store.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((offset, 5)));
        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_access() -> color_eyre::Result<()> {
        let store = create_test_store();

        let key1: Key = Arc::from("test-key-1");
        let key2: Key = Arc::from("test-key-2");

        let store_clone = store.clone();
        let k1 = key1.clone();
        let handle1 = tokio::spawn(async move {
            store_clone
                .defer_first_message(&k1, Offset::from(1_i64))
                .await
        });

        let store_clone = store.clone();
        let k2 = key2.clone();
        let handle2 = tokio::spawn(async move {
            store_clone
                .defer_first_message(&k2, Offset::from(2_i64))
                .await
        });

        assert!(handle1.await.is_ok());
        assert!(handle2.await.is_ok());

        let result1 = store.get_next_deferred_message(&key1).await?;
        assert_eq!(result1, Some((Offset::from(1_i64), 0)));

        let result2 = store.get_next_deferred_message(&key2).await?;
        assert_eq!(result2, Some((Offset::from(2_i64), 0)));

        Ok(())
    }

    // Property-based tests using model equivalence
    defer_store_tests!(async { Ok::<_, color_eyre::Report>(MemoryMessageDeferStore::new()) });
}
