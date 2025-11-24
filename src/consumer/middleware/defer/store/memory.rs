//! In-memory implementation of `DeferStore` for testing.
//!
//! Provides [`MemoryDeferStore`], a lock-free, concurrent implementation
//! of the [`DeferStore`] trait using [`scc::HashMap`].
//!
//! # Usage
//!
//! ```rust,no_run
//! use prosody::consumer::middleware::defer::store::memory::MemoryDeferStore;
//!
//! let store = MemoryDeferStore::new();
//! // Use store with DeferStore methods
//! ```
//!
//! All data is held in memory and lost on process exit. Not suitable for
//! production use where persistence across restarts is required.

use super::DeferStore;
use crate::Offset;
use crate::timers::datetime::CompactDateTime;

#[cfg(test)]
use crate::defer_store_tests;
use scc::HashMap;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// In-memory implementation of [`DeferStore`] for testing and development.
///
/// Uses [`scc::HashMap`] for lock-free concurrent access. Stores multiple
/// offsets per key (queue) with a shared retry counter. All data is
/// volatile - deferred state is lost when the process exits.
///
/// # Thread Safety
///
/// Safe to clone and use from multiple threads. All operations are atomic
/// per `defer_key`.
#[derive(Clone, Debug, Default)]
pub struct MemoryDeferStore(Arc<Inner>);

/// Internal state for [`MemoryDeferStore`].
///
/// Maps each UUID to:
/// - `BTreeMap<Offset, Instant>`: Sorted offsets (oldest first) with TTL
///   simulation
/// - `u32`: Shared retry counter for this key
#[derive(Debug, Default)]
struct Inner {
    /// Storage: `defer_key` -> (`offsets_with_expiry`, `retry_count`)
    deferred: HashMap<Uuid, (BTreeMap<Offset, Instant>, u32)>,
}

impl MemoryDeferStore {
    /// Creates a new, empty in-memory defer store.
    ///
    /// # Returns
    ///
    /// A ready-to-use [`MemoryDeferStore`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl DeferStore for MemoryDeferStore {
    type Error = Infallible;

    async fn get_next_deferred_message(
        &self,
        defer_key: &Uuid,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        let now = Instant::now();

        // Get the entry for this key
        let result = self
            .0
            .deferred
            .get_async(defer_key)
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
        defer_key: &Uuid,
        offset: Offset,
        _expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        // Calculate expiry time (TTL simulation)
        let expiry = Instant::now() + Duration::from_secs(3600);

        // Set retry_count=0 and append offset
        // Match Cassandra: INSERT doesn't delete existing offsets
        self.0
            .deferred
            .entry_async(*defer_key)
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
        defer_key: &Uuid,
        offset: Offset,
        _expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        // Calculate expiry time (TTL simulation)
        let expiry = Instant::now() + Duration::from_secs(3600);

        // Insert offset without modifying retry_count
        self.0
            .deferred
            .entry_async(*defer_key)
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

    async fn remove_deferred_message(
        &self,
        defer_key: &Uuid,
        offset: Offset,
    ) -> Result<(), Self::Error> {
        // Remove the specific offset
        // Note: Unlike Cassandra's DELETE on clustering column, we don't remove the
        // entry entirely when offsets become empty. This preserves retry_count
        // (static column equivalent), matching Cassandra behavior where static
        // columns persist after deleting all clustering rows.
        let _ =
            self.0
                .deferred
                .entry_async(*defer_key)
                .await
                .and_modify(|(offsets, _retry_count)| {
                    offsets.remove(&offset);
                });

        Ok(())
    }

    async fn set_retry_count(&self, defer_key: &Uuid, retry_count: u32) -> Result<(), Self::Error> {
        // Match Cassandra: UPDATE creates partition with static column even if no
        // offsets exist
        self.0
            .deferred
            .entry_async(*defer_key)
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

    async fn delete_key(&self, defer_key: &Uuid) -> Result<(), Self::Error> {
        self.0.deferred.remove_async(defer_key).await;
        Ok(())
    }
}

/// Creates a new in-memory defer store.
///
/// Convenience function that returns the store wrapped for use as a
/// trait object. Equivalent to [`MemoryDeferStore::new()`].
///
/// # Returns
///
/// A new [`MemoryDeferStore`] instance.
#[must_use]
pub fn memory_defer_store() -> MemoryDeferStore {
    MemoryDeferStore::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timers::datetime::CompactDateTime;

    fn test_defer_key() -> Uuid {
        Uuid::new_v4()
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let defer_key = test_defer_key();

        let result = store.get_next_deferred_message(&defer_key).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_append_and_get() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let defer_key = test_defer_key();
        let offset = Offset::from(42_i64);
        let retry_time = CompactDateTime::now()?;

        // Use defer_first_message for first failure
        store
            .defer_first_message(&defer_key, offset, retry_time)
            .await?;

        let result = store.get_next_deferred_message(&defer_key).await?;
        assert_eq!(result, Some((offset, 0)));
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_offsets_returns_oldest() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let defer_key = test_defer_key();
        let retry_time = CompactDateTime::now()?;

        // Defer first message
        store
            .defer_first_message(&defer_key, Offset::from(100_i64), retry_time)
            .await?;
        // Append additional messages
        store
            .append_deferred_message(&defer_key, Offset::from(50_i64), retry_time)
            .await?;
        store
            .append_deferred_message(&defer_key, Offset::from(150_i64), retry_time)
            .await?;

        // Should return the oldest (smallest) offset
        let result = store.get_next_deferred_message(&defer_key).await?;
        assert_eq!(result, Some((Offset::from(50_i64), 0)));
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_offset() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let defer_key = test_defer_key();
        let offset = Offset::from(42_i64);
        let retry_time = CompactDateTime::now()?;

        store
            .defer_first_message(&defer_key, offset, retry_time)
            .await?;
        store.remove_deferred_message(&defer_key, offset).await?;

        let result = store.get_next_deferred_message(&defer_key).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_nonexistent() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let defer_key = test_defer_key();

        // Should not error
        store
            .remove_deferred_message(&defer_key, Offset::from(42_i64))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_set_retry_count() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let defer_key = test_defer_key();
        let offset = Offset::from(42_i64);
        let retry_time = CompactDateTime::now()?;

        // Defer first message
        store
            .defer_first_message(&defer_key, offset, retry_time)
            .await?;

        // Update retry_count to 5
        store.set_retry_count(&defer_key, 5).await?;

        let result = store.get_next_deferred_message(&defer_key).await?;
        assert_eq!(result, Some((offset, 5)));
        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_access() -> color_eyre::Result<()> {
        let store = MemoryDeferStore::new();
        let key1 = Uuid::new_v4();
        let key2 = Uuid::new_v4();
        let retry_time = CompactDateTime::now()?;

        let store_clone = store.clone();
        let handle1 = tokio::spawn(async move {
            store_clone
                .defer_first_message(&key1, Offset::from(1_i64), retry_time)
                .await
        });

        let store_clone = store.clone();
        let handle2 = tokio::spawn(async move {
            store_clone
                .defer_first_message(&key2, Offset::from(2_i64), retry_time)
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
    defer_store_tests!(async { Ok::<_, color_eyre::Report>(MemoryDeferStore::new()) });
}
