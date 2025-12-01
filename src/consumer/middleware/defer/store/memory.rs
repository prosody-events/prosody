//! In-memory implementation of `DeferStore` for testing.
//!
//! Provides [`MemoryDeferStore`], a lock-free, concurrent implementation
//! of the [`DeferStore`] trait using [`scc::HashMap`].
//!
//! # Usage
//!
//! ```rust,no_run
//! use prosody::consumer::middleware::defer::store::memory::MemoryDeferStoreProvider;
//! use prosody::consumer::middleware::defer::store::DeferStoreProvider;
//! use prosody::{Partition, Topic};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = MemoryDeferStoreProvider::new();
//! let store = provider
//!     .create_store(Topic::from("test"), Partition::from(0), "consumer-group")
//!     .await?;
//! // Use store with DeferStore methods
//! # Ok(())
//! # }
//! ```
//!
//! All data is held in memory and lost on process exit. Not suitable for
//! production use where persistence across restarts is required.

use super::{DeferStore, DeferStoreProvider};
use crate::timers::datetime::CompactDateTime;
use crate::{Key, Offset, Partition, Topic};

#[cfg(test)]
use crate::defer_store_tests;
use scc::HashMap;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// In-memory implementation of [`DeferStore`] for testing and development.
///
/// Uses [`scc::HashMap`] for lock-free concurrent access. Stores multiple
/// offsets per key (queue) with a shared retry counter. All data is
/// volatile - deferred state is lost when the process exits.
///
/// # Thread Safety
///
/// Safe to clone and use from multiple threads. All operations are atomic
/// per message key.
///
/// # Design
///
/// Each store instance is created for a specific segment
/// (`topic/partition/consumer_group`). The internal `HashMap` keys by message
/// key only, with segment isolation handled by having separate store instances
/// per partition.
#[derive(Clone, Debug)]
pub struct MemoryDeferStore {
    inner: Arc<Inner>,
}

impl MemoryDeferStore {
    /// Creates a new empty in-memory defer store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }
}

impl Default for MemoryDeferStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal state for [`MemoryDeferStore`].
///
/// Maps each message key to:
/// - `BTreeMap<Offset, Instant>`: Sorted offsets (oldest first) with TTL
///   simulation
/// - `u32`: Shared retry counter for this key
#[derive(Debug, Default)]
struct Inner {
    /// Storage: `message_key` -> (`offsets_with_expiry`, `retry_count`)
    deferred: HashMap<Key, (BTreeMap<Offset, Instant>, u32)>,
}

impl DeferStore for MemoryDeferStore {
    type Error = Infallible;

    async fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        let now = Instant::now();

        // Get the entry for this key
        let result = self
            .inner
            .deferred
            .get_async(key.as_ref())
            .await
            .and_then(|entry| {
                let (offsets, retry_count) = entry.get();

                // Find the oldest (first) non-expired offset
                offsets
                    .iter()
                    .find(|&(_, expiry)| *expiry > now)
                    .map(|(&offset, _)| (offset, *retry_count))
            });

        Ok(result)
    }

    async fn defer_first_message(
        &self,
        key: &Key,
        offset: Offset,
        _expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        // Calculate expiry time (TTL simulation)
        let expiry = Instant::now() + Duration::from_secs(3600);

        // Set retry_count=0 and append offset
        // Match Cassandra: INSERT doesn't delete existing offsets
        self.inner
            .deferred
            .entry_async(Arc::clone(key))
            .await
            .and_modify(|(offsets, retry_count)| {
                offsets.insert(offset, expiry);
                *retry_count = 0;
            })
            .or_insert_with(|| {
                let mut offsets = BTreeMap::new();
                offsets.insert(offset, expiry);
                (offsets, 0)
            });

        Ok(())
    }

    async fn append_deferred_message(
        &self,
        key: &Key,
        offset: Offset,
        _expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        // Calculate expiry time (TTL simulation)
        let expiry = Instant::now() + Duration::from_secs(3600);

        // Insert offset without modifying retry_count
        self.inner
            .deferred
            .entry_async(Arc::clone(key))
            .await
            .and_modify(|(offsets, _retry_count)| {
                offsets.insert(offset, expiry);
            })
            .or_insert_with(|| {
                // Shouldn't happen (should use defer_first_message first)
                // but handle gracefully with retry_count=0
                let mut offsets = BTreeMap::new();
                offsets.insert(offset, expiry);
                (offsets, 0)
            });

        Ok(())
    }

    async fn remove_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        // Remove the specific offset
        // Note: Unlike Cassandra's DELETE on clustering column, we don't remove the
        // entry entirely when offsets become empty. This preserves retry_count
        // (static column equivalent), matching Cassandra behavior where static
        // columns persist after deleting all clustering rows.
        let _ = self
            .inner
            .deferred
            .entry_async(Arc::clone(key))
            .await
            .and_modify(|(offsets, _retry_count)| {
                offsets.remove(&offset);
            });

        Ok(())
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        // Match Cassandra: UPDATE creates partition with static column even if no
        // offsets exist
        self.inner
            .deferred
            .entry_async(Arc::clone(key))
            .await
            .and_modify(|(_offsets, current_retry_count)| {
                *current_retry_count = retry_count;
            })
            .or_insert_with(|| {
                // Create entry with empty offsets and the specified retry_count
                (BTreeMap::new(), retry_count)
            });

        Ok(())
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        self.inner.deferred.remove_async(key.as_ref()).await;
        Ok(())
    }
}

/// Provider for creating [`MemoryDeferStore`] instances.
///
/// Simple provider that creates isolated in-memory stores for each partition.
/// Each store instance has its own `HashMap`, ensuring partition isolation.
#[derive(Clone, Debug, Default)]
pub struct MemoryDeferStoreProvider {
    /// Shared inner state (empty, just for consistency with pattern)
    _inner: Arc<()>,
}

impl MemoryDeferStoreProvider {
    /// Creates a new memory defer store provider.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl DeferStoreProvider for MemoryDeferStoreProvider {
    type Error = Infallible;
    type Store = MemoryDeferStore;

    async fn create_store(
        &self,
        _topic: Topic,
        _partition: Partition,
        _consumer_group: &str,
    ) -> Result<Self::Store, Self::Error> {
        Ok(MemoryDeferStore {
            inner: Arc::new(Inner::default()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timers::datetime::CompactDateTime;
    use crate::{Key, Partition, Topic};

    async fn create_test_store() -> MemoryDeferStore {
        let provider = MemoryDeferStoreProvider::new();
        provider
            .create_store(
                Topic::from("test-topic"),
                Partition::from(0_i32),
                "test-group",
            )
            .await
            .expect("store creation is infallible")
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() -> color_eyre::Result<()> {
        let store = create_test_store().await;
        let key: Key = Arc::from("test-key-1");

        let result = store.get_next_deferred_message(&key).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_append_and_get() -> color_eyre::Result<()> {
        let store = create_test_store().await;
        let key: Key = Arc::from("test-key-1");
        let offset = Offset::from(42_i64);
        let retry_time = CompactDateTime::now()?;

        // Use defer_first_message for first failure
        store.defer_first_message(&key, offset, retry_time).await?;

        let result = store.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((offset, 0)));
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_offsets_returns_oldest() -> color_eyre::Result<()> {
        let store = create_test_store().await;
        let key: Key = Arc::from("test-key-1");
        let retry_time = CompactDateTime::now()?;

        // Defer first message
        store
            .defer_first_message(&key, Offset::from(100_i64), retry_time)
            .await?;
        // Append additional messages
        store
            .append_deferred_message(&key, Offset::from(50_i64), retry_time)
            .await?;
        store
            .append_deferred_message(&key, Offset::from(150_i64), retry_time)
            .await?;

        // Should return the oldest (smallest) offset
        let result = store.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((Offset::from(50_i64), 0)));
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_offset() -> color_eyre::Result<()> {
        let store = create_test_store().await;
        let key: Key = Arc::from("test-key-1");
        let offset = Offset::from(42_i64);
        let retry_time = CompactDateTime::now()?;

        store.defer_first_message(&key, offset, retry_time).await?;
        store.remove_deferred_message(&key, offset).await?;

        let result = store.get_next_deferred_message(&key).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_nonexistent() -> color_eyre::Result<()> {
        let store = create_test_store().await;
        let key: Key = Arc::from("test-key-1");

        // Should not error
        store
            .remove_deferred_message(&key, Offset::from(42_i64))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_set_retry_count() -> color_eyre::Result<()> {
        let store = create_test_store().await;
        let key: Key = Arc::from("test-key-1");
        let offset = Offset::from(42_i64);
        let retry_time = CompactDateTime::now()?;

        // Defer first message
        store.defer_first_message(&key, offset, retry_time).await?;

        // Update retry_count to 5
        store.set_retry_count(&key, 5).await?;

        let result = store.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((offset, 5)));
        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_access() -> color_eyre::Result<()> {
        let store = create_test_store().await;
        let retry_time = CompactDateTime::now()?;

        let key1: Key = Arc::from("test-key-1");
        let key2: Key = Arc::from("test-key-2");

        let store_clone = store.clone();
        let k1 = key1.clone();
        let handle1 = tokio::spawn(async move {
            store_clone
                .defer_first_message(&k1, Offset::from(1_i64), retry_time)
                .await
        });

        let store_clone = store.clone();
        let k2 = key2.clone();
        let handle2 = tokio::spawn(async move {
            store_clone
                .defer_first_message(&k2, Offset::from(2_i64), retry_time)
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
    defer_store_tests!(async { Ok::<_, color_eyre::Report>(create_test_store().await) });
}
