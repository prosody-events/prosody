//! Write-through cache adapter for `DeferStore` implementations.
//!
//! Provides [`CachedDeferStore`], a transparent caching layer that wraps any
//! [`DeferStore`] implementation to reduce store queries.

use super::{DeferStore, RetryCompletionResult};
use crate::Offset;
use crate::timers::datetime::CompactDateTime;
use quick_cache::sync::Cache;
use std::sync::Arc;
use uuid::Uuid;

#[cfg(test)]
use crate::defer_store_tests;

/// Write-through cache adapter for `DeferStore` implementations.
///
/// Caches the result of
/// [`get_next_deferred_message`](DeferStore::get_next_deferred_message)
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
/// Uses `Uuid` (`key_id`) as the cache key, matching the trait interface.
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
/// ```rust,no_run
/// use prosody::consumer::middleware::defer::store::cached::CachedDeferStore;
/// use prosody::consumer::middleware::defer::store::memory::MemoryDeferStore;
///
/// let store = MemoryDeferStore::new();
/// let cached_store = CachedDeferStore::new(store, 10_000);
/// // Use cached_store with DeferStore methods
/// ```
#[derive(Clone)]
pub struct CachedDeferStore<S> {
    store: S,
    cache: Arc<Cache<Uuid, Option<(Offset, u32)>>>,
}

impl<S> CachedDeferStore<S>
where
    S: DeferStore,
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
}

impl<S> DeferStore for CachedDeferStore<S>
where
    S: DeferStore,
{
    type Error = S::Error;

    async fn defer_first_message(
        &self,
        key_id: &Uuid,
        offset: Offset,
        expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        // Write through to store first (for durability)
        self.store
            .defer_first_message(key_id, offset, expected_retry_time)
            .await?;

        // Invalidate cache: While the contract says this should only be called
        // for new keys, the store implementations handle it gracefully even if
        // the key already has offsets (e.g., if defer_additional was called first).
        // To ensure correctness in all cases, we invalidate and let the next read
        // query the store for the actual next offset.
        self.cache.remove(key_id);

        Ok(())
    }

    async fn defer_additional_message(
        &self,
        key_id: &Uuid,
        offset: Offset,
        expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        // Write through to store first
        self.store
            .defer_additional_message(key_id, offset, expected_retry_time)
            .await?;

        // Invalidate cache: the appended offset might be smaller than the current
        // "next" offset (if offsets arrive out-of-order), which would make the
        // cached next offset stale.
        // In practice, Kafka offsets are monotonic per partition, but the property
        // tests (and correct general behavior) require handling arbitrary offsets.
        self.cache.remove(key_id);

        Ok(())
    }

    async fn complete_retry_success(
        &self,
        key_id: &Uuid,
        offset: Offset,
    ) -> Result<RetryCompletionResult, Self::Error> {
        // Write through to store first
        let result = self.store.complete_retry_success(key_id, offset).await?;

        // Update cache based on result (store tells us exact new state)
        match result {
            RetryCompletionResult::MoreMessages { next_offset } => {
                // More messages exist, retry_count reset to 0
                self.cache.insert(*key_id, Some((next_offset, 0)));
            }
            RetryCompletionResult::Completed => {
                // Key deleted, no more messages
                self.cache.insert(*key_id, None);
            }
        }

        Ok(result)
    }

    async fn increment_retry_count(
        &self,
        key_id: &Uuid,
        current_retry_count: u32,
    ) -> Result<u32, Self::Error> {
        // Write through to store first
        let new_count = self
            .store
            .increment_retry_count(key_id, current_retry_count)
            .await?;

        // Invalidate cache - next read will populate with correct retry_count.
        // Conservative strategy: simpler and safer than in-place update.
        self.cache.remove(key_id);

        Ok(new_count)
    }

    async fn get_next_deferred_message(
        &self,
        key_id: &Uuid,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        // Check cache first
        if let Some(cached) = self.cache.get(key_id) {
            return Ok(cached);
        }

        // Cache miss - query store
        let result = self.store.get_next_deferred_message(key_id).await?;

        // Populate cache (including None for "not deferred")
        self.cache.insert(*key_id, result);

        Ok(result)
    }

    async fn append_deferred_message(
        &self,
        key_id: &Uuid,
        offset: Offset,
        expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        // Write through to store first
        self.store
            .append_deferred_message(key_id, offset, expected_retry_time)
            .await?;

        // Invalidate cache for the same reason as defer_additional_message:
        // the appended offset might be smaller than the current next offset.
        self.cache.remove(key_id);

        Ok(())
    }

    async fn remove_deferred_message(
        &self,
        key_id: &Uuid,
        offset: Offset,
    ) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.remove_deferred_message(key_id, offset).await?;

        // Invalidate: don't know if more messages exist.
        // If this was the last offset, get_next_deferred_message would return None.
        // If more offsets exist, we need to query to find the next one.
        self.cache.remove(key_id);

        Ok(())
    }

    async fn set_retry_count(&self, key_id: &Uuid, retry_count: u32) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.set_retry_count(key_id, retry_count).await?;

        // Invalidate cache - next read will populate with correct state.
        // Low-level primitive, safest to invalidate.
        self.cache.remove(key_id);

        Ok(())
    }

    async fn delete_key(&self, key_id: &Uuid) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.delete_key(key_id).await?;

        // Update cache with known state: key is deleted
        self.cache.insert(*key_id, None);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::middleware::defer::store::memory::MemoryDeferStore;
    use crate::timers::datetime::CompactDateTime;

    fn test_key_id() -> Uuid {
        Uuid::new_v4()
    }

    #[tokio::test]
    async fn test_cache_hit_on_repeated_get() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let cached_store = CachedDeferStore::new(store, 100);
        let key_id = test_key_id();
        let offset = Offset::from(42_i64);
        let retry_time = CompactDateTime::now()?;

        // First defer
        cached_store
            .defer_first_message(&key_id, offset, retry_time)
            .await?;

        // First get (cache miss, populates cache)
        let result1 = cached_store.get_next_deferred_message(&key_id).await?;
        assert_eq!(result1, Some((offset, 0)));

        // Second get (cache hit - should return same result without querying store)
        let result2 = cached_store.get_next_deferred_message(&key_id).await?;
        assert_eq!(result2, Some((offset, 0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_invalidation_on_increment() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let cached_store = CachedDeferStore::new(store, 100);
        let key_id = test_key_id();
        let offset = Offset::from(42_i64);
        let retry_time = CompactDateTime::now()?;

        // Defer and populate cache
        cached_store
            .defer_first_message(&key_id, offset, retry_time)
            .await?;
        cached_store.get_next_deferred_message(&key_id).await?;

        // Increment retry count (should invalidate cache)
        let new_count = cached_store.increment_retry_count(&key_id, 0).await?;
        assert_eq!(new_count, 1);

        // Next get should see updated retry_count
        let result = cached_store.get_next_deferred_message(&key_id).await?;
        assert_eq!(result, Some((offset, 1)));

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_update_on_complete_success() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let cached_store = CachedDeferStore::new(store, 100);
        let key_id = test_key_id();
        let retry_time = CompactDateTime::now()?;

        // Defer two messages
        cached_store
            .defer_first_message(&key_id, Offset::from(100_i64), retry_time)
            .await?;
        cached_store
            .defer_additional_message(&key_id, Offset::from(200_i64), retry_time)
            .await?;

        // Complete first message
        let result = cached_store
            .complete_retry_success(&key_id, Offset::from(100_i64))
            .await?;

        // Should return MoreMessages with next offset
        assert!(matches!(
            result,
            RetryCompletionResult::MoreMessages { next_offset } if next_offset == Offset::from(200_i64)
        ));

        // Cache should be updated to point to next message with retry_count=0
        let cached = cached_store.get_next_deferred_message(&key_id).await?;
        assert_eq!(cached, Some((Offset::from(200_i64), 0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_cleared_on_complete_last_message() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let cached_store = CachedDeferStore::new(store, 100);
        let key_id = test_key_id();
        let offset = Offset::from(42_i64);
        let retry_time = CompactDateTime::now()?;

        // Defer one message
        cached_store
            .defer_first_message(&key_id, offset, retry_time)
            .await?;

        // Complete it
        let result = cached_store.complete_retry_success(&key_id, offset).await?;

        assert!(matches!(result, RetryCompletionResult::Completed));

        // Cache should show None (key is not deferred)
        let cached = cached_store.get_next_deferred_message(&key_id).await?;
        assert_eq!(cached, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_defer_additional_out_of_order() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let cached_store = CachedDeferStore::new(store, 100);
        let key_id = test_key_id();
        let retry_time = CompactDateTime::now()?;

        // Defer first message and populate cache
        cached_store
            .defer_first_message(&key_id, Offset::from(100_i64), retry_time)
            .await?;
        let before = cached_store.get_next_deferred_message(&key_id).await?;
        assert_eq!(before, Some((Offset::from(100_i64), 0)));

        // Defer additional message with SMALLER offset (out-of-order scenario)
        // This should invalidate cache since the new offset becomes the "next"
        cached_store
            .defer_additional_message(&key_id, Offset::from(50_i64), retry_time)
            .await?;

        // Next read should see offset 50 (the smaller one) as next
        let after = cached_store.get_next_deferred_message(&key_id).await?;
        assert_eq!(after, Some((Offset::from(50_i64), 0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_negative_results() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let cached_store = CachedDeferStore::new(store, 100);
        let key_id = test_key_id();

        // Query non-existent key (should cache None)
        let result1 = cached_store.get_next_deferred_message(&key_id).await?;
        assert_eq!(result1, None);

        // Second query should hit cache
        let result2 = cached_store.get_next_deferred_message(&key_id).await?;
        assert_eq!(result2, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_key_updates_cache() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let cached_store = CachedDeferStore::new(store, 100);
        let key_id = test_key_id();
        let offset = Offset::from(42_i64);
        let retry_time = CompactDateTime::now()?;

        // Defer message and populate cache
        cached_store
            .defer_first_message(&key_id, offset, retry_time)
            .await?;
        cached_store.get_next_deferred_message(&key_id).await?;

        // Delete key (should update cache to None)
        cached_store.delete_key(&key_id).await?;

        // Cache should show None
        let result = cached_store.get_next_deferred_message(&key_id).await?;
        assert_eq!(result, None);

        Ok(())
    }

    // Property-based tests using model equivalence with underlying memory store
    defer_store_tests!(async {
        Ok::<_, color_eyre::Report>(CachedDeferStore::new(MemoryDeferStore::new(), 1000))
    });
}
