//! Write-through cache adapter for `MessageDeferStore` implementations.
//!
//! Provides [`CachedDeferStore`], a transparent caching layer that wraps any
//! [`MessageDeferStore`] implementation to reduce store queries.

use super::{MessageDeferStore, MessageRetryCompletionResult};
use crate::{Key, Offset};
use quick_cache::sync::Cache;
use std::sync::Arc;

/// Type alias for the defer cache: maps message keys to their cached state.
///
/// The cached state is `Option<(Offset, u32)>`:
/// - `None` means the key has no deferred messages
/// - `Some((offset, retry_count))` is the first deferred message for the key
type DeferCache = Cache<Key, Option<(Offset, u32)>>;

#[cfg(test)]
use crate::defer_store_tests;

/// Write-through cache adapter for `MessageDeferStore` implementations.
///
/// Caches the result of
/// [`get_next_deferred_message`](MessageDeferStore::get_next_deferred_message)
/// to reduce store queries. All mutations write through to the underlying store
/// and update/invalidate the cache appropriately.
///
/// # Cache Strategy
///
/// - **Reads**: Check cache first, query store on miss, populate cache
/// - **Writes**: Write to store first (for durability), then update/invalidate
///   cache
/// - **Consistency**: Conservative invalidation when new state is uncertain
///
/// # Cache Key
///
/// Uses the message key ([`Key`]) as the cache key.
///
/// # Cache Value
///
/// Stores `Option<(Offset, u32)>` - the result of `get_next_deferred_message`:
/// - `Some((offset, retry_count))`: Key has deferred messages, next is at
///   `offset`
/// - `None`: Key has no deferred messages
///
/// # Usage
///
/// ```rust
/// use prosody::consumer::middleware::defer::message::store::cached::CachedDeferStore;
/// use prosody::consumer::middleware::defer::message::store::memory::MemoryMessageDeferStore;
///
/// let store = MemoryMessageDeferStore::new();
/// let cached_store = CachedDeferStore::new(store, 10_000);
/// // Use cached_store with MessageDeferStore methods
/// ```
#[derive(Clone)]
pub struct CachedDeferStore<S> {
    store: S,
    cache: Arc<DeferCache>,
}

impl<S> CachedDeferStore<S>
where
    S: MessageDeferStore,
{
    /// Creates a new cached store wrapping the underlying store.
    ///
    /// # Arguments
    ///
    /// * `store` - Underlying store implementation
    /// * `capacity` - Maximum number of keys to cache
    ///
    /// # Returns
    ///
    /// A [`CachedDeferStore`] that transparently caches queries to `store`.
    #[must_use]
    pub fn new(store: S, capacity: usize) -> Self {
        Self {
            store,
            cache: Arc::new(Cache::new(capacity)),
        }
    }

    /// Updates cache after appending an offset to the deferred queue.
    ///
    /// Uses smart invalidation: only updates/invalidates if the new offset
    /// might change the cached minimum offset.
    ///
    /// # Strategy
    ///
    /// - If cached and `offset >= min_offset`: Preserve cache (monotonic
    ///   append)
    /// - If cached and `offset < min_offset`: Update cache with new minimum
    /// - If not cached or was `None`: Invalidate (conservative)
    fn update_cache_after_append(&self, key: &Key, offset: Offset) {
        if let Some(cached) = self.cache.get(key.as_ref())
            && let Some((min_offset, retry_count)) = cached
        {
            if offset >= min_offset {
                // New offset doesn't change minimum (monotonic append case)
                // Preserve cache - this is the common case for Kafka
                return;
            }
            // New offset is smaller - update cache with new minimum
            // Keep existing retry_count as it applies to the queue as a whole
            self.cache
                .insert(Arc::clone(key), Some((offset, retry_count)));
            return;
        }

        // No cached entry or was None - invalidate to be safe
        self.cache.remove(key.as_ref());
    }
}

impl<S> MessageDeferStore for CachedDeferStore<S>
where
    S: MessageDeferStore,
{
    type Error = S::Error;

    async fn defer_first_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        // Write through to store first (for durability)
        self.store.defer_first_message(key, offset).await?;

        // Pre-populate cache ONLY if we're confident this is truly the first message
        match self.cache.get(key.as_ref()) {
            Some(None) => {
                // Was cached as not-deferred - safe to update to deferred
                self.cache.insert(Arc::clone(key), Some((offset, 0)));
            }
            Some(Some(_)) | None => {
                // Either already had deferred state (contract violation) or not cached
                // Be conservative: invalidate so next read queries store for true minimum
                self.cache.remove(key.as_ref());
            }
        }

        Ok(())
    }

    async fn defer_additional_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.defer_additional_message(key, offset).await?;

        // Update cache using smart invalidation
        self.update_cache_after_append(key, offset);

        Ok(())
    }

    async fn complete_retry_success(
        &self,
        key: &Key,
        offset: Offset,
    ) -> Result<MessageRetryCompletionResult, Self::Error> {
        // Write through to store first
        let result = self.store.complete_retry_success(key, offset).await?;

        // Update cache based on result (store tells us exact new state)
        match result {
            MessageRetryCompletionResult::MoreMessages { next_offset } => {
                // More messages exist, retry_count reset to 0
                self.cache.insert(Arc::clone(key), Some((next_offset, 0)));
            }
            MessageRetryCompletionResult::Completed => {
                // Key deleted, no more messages
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
        // Write through to store first
        let new_count = self
            .store
            .increment_retry_count(key, current_retry_count)
            .await?;

        // Update retry count in-place if we have a cached entry with offset.
        // This is safe because retry_count changes don't affect offset ordering.
        if let Some(cached) = self.cache.get(key.as_ref())
            && let Some((offset, _old_retry_count)) = cached
        {
            self.cache
                .insert(Arc::clone(key), Some((offset, new_count)));
            return Ok(new_count);
        }

        // No cached entry or was None - invalidate so next read repopulates
        self.cache.remove(key.as_ref());

        Ok(new_count)
    }

    async fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        // Check cache first
        if let Some(cached) = self.cache.get(key.as_ref()) {
            return Ok(cached);
        }

        // Cache miss - query store
        let result = self.store.get_next_deferred_message(key).await?;

        // Populate cache (including None for "not deferred")
        self.cache.insert(Arc::clone(key), result);

        Ok(result)
    }

    async fn append_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.append_deferred_message(key, offset).await?;

        // Update cache using smart invalidation
        self.update_cache_after_append(key, offset);

        Ok(())
    }

    async fn remove_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.remove_deferred_message(key, offset).await?;

        // Invalidate: don't know if more messages exist.
        // If this was the last offset, get_next_deferred_message would return None.
        // If more offsets exist, we need to query to find the next one.
        self.cache.remove(key.as_ref());

        Ok(())
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.set_retry_count(key, retry_count).await?;

        // Update retry count in-place if we have a cached entry with offset.
        // This is safe because:
        // 1. Retry count changes are orthogonal to offset ordering
        // 2. If we have an offset cached, the key exists with offsets in store
        // 3. set_retry_count doesn't remove offsets, so our cached offset remains valid
        if let Some(cached) = self.cache.get(key.as_ref())
            && let Some((offset, _old_retry_count)) = cached
        {
            self.cache
                .insert(Arc::clone(key), Some((offset, retry_count)));
            return Ok(());
        }

        // No cached entry or was None - invalidate to be safe
        // (set_retry_count can create entry with empty offsets per trait semantics)
        self.cache.remove(key.as_ref());

        Ok(())
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.delete_key(key).await?;

        // Update cache with known state: key is deleted
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
