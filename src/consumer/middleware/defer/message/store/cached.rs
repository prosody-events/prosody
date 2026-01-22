//! Write-through cache for message defer stores.

use super::{MessageDeferStore, MessageRetryCompletionResult};
use crate::{Key, Offset};
use quick_cache::sync::Cache;
use std::sync::Arc;

/// Cache: `key` → `Option<(next_offset, retry_count)>`.
type DeferCache = Cache<Key, Option<(Offset, u32)>>;

#[cfg(test)]
use crate::defer_store_tests;

/// Write-through cache for any [`MessageDeferStore`].
///
/// Caches `get_next_deferred_message` results. Writes go to store first, then
/// update or conservatively invalidate cache. Uses smart invalidation:
/// monotonic appends preserve cache; out-of-order appends update to new
/// minimum.
#[derive(Clone)]
pub struct CachedDeferStore<S> {
    store: S,
    cache: Arc<DeferCache>,
}

impl<S> CachedDeferStore<S>
where
    S: MessageDeferStore,
{
    /// Wraps a store with a cache of the given capacity.
    #[must_use]
    pub fn new(store: S, capacity: usize) -> Self {
        Self {
            store,
            cache: Arc::new(Cache::new(capacity)),
        }
    }

    /// Smart cache update: preserve if monotonic, update if new minimum,
    /// invalidate if uncertain.
    fn update_cache_after_append(&self, key: &Key, offset: Offset) {
        if let Some(cached) = self.cache.get(key.as_ref())
            && let Some((min_offset, retry_count)) = cached
        {
            if offset >= min_offset {
                // Monotonic append - cache stays valid
                return;
            }
            // New minimum - update cache
            self.cache
                .insert(Arc::clone(key), Some((offset, retry_count)));
            return;
        }

        // Unknown state - invalidate
        self.cache.remove(key.as_ref());
    }
}

impl<S> MessageDeferStore for CachedDeferStore<S>
where
    S: MessageDeferStore,
{
    type Error = S::Error;

    async fn defer_first_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.store.defer_first_message(key, offset).await?;

        // Populate cache only if we knew key was empty
        match self.cache.get(key.as_ref()) {
            Some(None) => self.cache.insert(Arc::clone(key), Some((offset, 0))),
            Some(Some(_)) | None => {
                self.cache.remove(key.as_ref());
            }
        }

        Ok(())
    }

    async fn defer_additional_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.store.defer_additional_message(key, offset).await?;
        self.update_cache_after_append(key, offset);
        Ok(())
    }

    async fn complete_retry_success(
        &self,
        key: &Key,
        offset: Offset,
    ) -> Result<MessageRetryCompletionResult, Self::Error> {
        let result = self.store.complete_retry_success(key, offset).await?;

        // Result tells us exact new state
        match result {
            MessageRetryCompletionResult::MoreMessages { next_offset } => {
                self.cache.insert(Arc::clone(key), Some((next_offset, 0)));
            }
            MessageRetryCompletionResult::Completed => {
                self.cache.insert(Arc::clone(key), None);
            }
        }

        Ok(result)
    }

    async fn increment_retry_count(
        &self,
        key: &Key,
        current_retry_count: u32,
    ) -> Result<u32, Self::Error> {
        let new_count = self
            .store
            .increment_retry_count(key, current_retry_count)
            .await?;

        // Update in-place if cached (retry count is orthogonal to offset)
        if let Some(cached) = self.cache.get(key.as_ref())
            && let Some((offset, _)) = cached
        {
            self.cache
                .insert(Arc::clone(key), Some((offset, new_count)));
            return Ok(new_count);
        }

        self.cache.remove(key.as_ref());
        Ok(new_count)
    }

    async fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        if let Some(cached) = self.cache.get(key.as_ref()) {
            return Ok(cached);
        }

        let result = self.store.get_next_deferred_message(key).await?;
        self.cache.insert(Arc::clone(key), result);
        Ok(result)
    }

    async fn append_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.store.append_deferred_message(key, offset).await?;
        self.update_cache_after_append(key, offset);
        Ok(())
    }

    async fn remove_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.store.remove_deferred_message(key, offset).await?;
        // Can't know next offset - invalidate
        self.cache.remove(key.as_ref());
        Ok(())
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        self.store.set_retry_count(key, retry_count).await?;

        // Update in-place if cached (retry count is orthogonal to offset)
        if let Some(cached) = self.cache.get(key.as_ref())
            && let Some((offset, _)) = cached
        {
            self.cache
                .insert(Arc::clone(key), Some((offset, retry_count)));
            return Ok(());
        }

        self.cache.remove(key.as_ref());
        Ok(())
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        self.store.delete_key(key).await?;
        self.cache.insert(Arc::clone(key), None);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Key;
    use crate::consumer::middleware::defer::message::store::memory::MemoryMessageDeferStore;

    fn create_test_store() -> MemoryMessageDeferStore {
        MemoryMessageDeferStore::new()
    }

    #[tokio::test]
    async fn test_cache_hit_on_repeated_get() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedDeferStore::new(store, 100);
        let key: Key = Arc::from("test-key-1");
        let offset = Offset::from(42_i64);

        // First defer
        cached_store.defer_first_message(&key, offset).await?;

        // First get (cache miss, populates cache)
        let result1 = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(result1, Some((offset, 0)));

        // Second get (cache hit - should return same result without querying store)
        let result2 = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(result2, Some((offset, 0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_update_on_increment() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedDeferStore::new(store, 100);
        let key: Key = Arc::from("test-key-1");
        let offset = Offset::from(42_i64);

        // Defer and populate cache
        cached_store.defer_first_message(&key, offset).await?;
        cached_store.get_next_deferred_message(&key).await?;

        // Increment retry count (should update cache in-place)
        let new_count = cached_store.increment_retry_count(&key, 0).await?;
        assert_eq!(new_count, 1);

        // Next get should see updated retry_count (cache hit, no store query)
        let result = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((offset, 1)));

        // Increment again to verify it keeps working
        let new_count = cached_store.increment_retry_count(&key, 1).await?;
        assert_eq!(new_count, 2);

        let result = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((offset, 2)));

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_update_on_complete_success() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedDeferStore::new(store, 100);
        let key: Key = Arc::from("test-key-1");

        // Defer two messages
        cached_store
            .defer_first_message(&key, Offset::from(100_i64))
            .await?;
        cached_store
            .defer_additional_message(&key, Offset::from(200_i64))
            .await?;

        // Complete first message
        let result = cached_store
            .complete_retry_success(&key, Offset::from(100_i64))
            .await?;

        // Should return MoreMessages with next offset
        assert!(matches!(
            result,
            MessageRetryCompletionResult::MoreMessages { next_offset } if next_offset == Offset::from(200_i64)
        ));

        // Cache should be updated to point to next message with retry_count=0
        let cached = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(cached, Some((Offset::from(200_i64), 0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_cleared_on_complete_last_message() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedDeferStore::new(store, 100);
        let key: Key = Arc::from("test-key-1");
        let offset = Offset::from(42_i64);

        // Defer one message
        cached_store.defer_first_message(&key, offset).await?;

        // Complete it
        let result = cached_store.complete_retry_success(&key, offset).await?;

        assert!(matches!(result, MessageRetryCompletionResult::Completed));

        // Cache should show None (key is not deferred)
        let cached = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(cached, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_defer_additional_out_of_order() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedDeferStore::new(store, 100);
        let key: Key = Arc::from("test-key-1");

        // Defer first message - cache pre-populated
        cached_store
            .defer_first_message(&key, Offset::from(100_i64))
            .await?;
        // No need to call get_next - defer_first_message pre-warms cache

        // Defer additional message with SMALLER offset (out-of-order scenario)
        // Smart invalidation should update cache with new minimum
        cached_store
            .defer_additional_message(&key, Offset::from(50_i64))
            .await?;

        // Next read should see offset 50 (the smaller one) as next (cache hit!)
        let after = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(after, Some((Offset::from(50_i64), 0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_defer_additional_monotonic() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedDeferStore::new(store, 100);
        let key: Key = Arc::from("test-key-1");

        // Defer first message - cache pre-populated
        cached_store
            .defer_first_message(&key, Offset::from(100_i64))
            .await?;

        // Append monotonically increasing offsets - cache should be preserved
        cached_store
            .defer_additional_message(&key, Offset::from(200_i64))
            .await?;

        // Cache should still have offset 100 as minimum (not invalidated!)
        let result = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((Offset::from(100_i64), 0)));

        // Append another - still monotonic
        cached_store
            .defer_additional_message(&key, Offset::from(300_i64))
            .await?;

        // Still cached at 100
        let result = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((Offset::from(100_i64), 0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_negative_results() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedDeferStore::new(store, 100);
        let key: Key = Arc::from("test-key-1");

        // Query non-existent key (should cache None)
        let result1 = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(result1, None);

        // Second query should hit cache
        let result2 = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(result2, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_key_updates_cache() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedDeferStore::new(store, 100);
        let key: Key = Arc::from("test-key-1");
        let offset = Offset::from(42_i64);

        // Defer message and populate cache
        cached_store.defer_first_message(&key, offset).await?;
        cached_store.get_next_deferred_message(&key).await?;

        // Delete key (should update cache to None)
        cached_store.delete_key(&key).await?;

        // Cache should show None
        let result = cached_store.get_next_deferred_message(&key).await?;
        assert_eq!(result, None);

        Ok(())
    }

    // Property-based tests using model equivalence with underlying memory store
    defer_store_tests!(async {
        Ok::<_, color_eyre::Report>(CachedDeferStore::new(create_test_store(), 1000))
    });
}
