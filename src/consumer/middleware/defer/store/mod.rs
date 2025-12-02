//! Storage trait and implementations for defer middleware.

pub mod cached;
pub mod cassandra;
pub mod lazy;
pub mod memory;
pub mod provider;

#[cfg(test)]
pub mod tests;

use crate::consumer::middleware::ClassifyError;
use crate::{Key, Offset};
use std::error::Error;
use std::future::Future;

pub use cached::CachedDeferStore;
pub use cassandra::{CassandraDeferStore, CassandraDeferStoreProvider};
pub use lazy::{LazyStore, StoreFactory};
pub use memory::{MemoryDeferStore, MemoryDeferStoreProvider};
pub use provider::DeferStoreProvider;

/// Result of [`DeferStore::complete_retry_success`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryCompletionResult {
    /// More messages remain; `retry_count` has been reset to 0.
    MoreMessages {
        /// The next (oldest) offset to retry.
        next_offset: Offset,
    },

    /// No more messages; key has been deleted from storage.
    Completed,
}

/// Storage backend for deferred message offsets.
///
/// Manages a FIFO queue of offsets per key with a shared `retry_count`. The
/// segment context (`topic/partition/consumer_group`) is established at
/// construction via [`DeferStoreProvider`].
///
/// # Invariants
///
/// - **First vs. additional**: Use
///   [`defer_first_message`](Self::defer_first_message) for the first failure
///   (initializes `retry_count`),
///   [`defer_additional_message`](Self::defer_additional_message) for
///   subsequent messages. Mixing these corrupts state.
/// - **FIFO ordering**: Messages retry in offset order; `retry_count` applies
///   to the head.
/// - **Cleanup**: [`complete_retry_success`](Self::complete_retry_success)
///   handles queue progression and key deletion when empty.
pub trait DeferStore: Clone + Send + Sync + 'static {
    /// Error type. Must implement [`ClassifyError`] for retry decisions.
    type Error: Error + ClassifyError + Send + Sync + 'static;

    // ─────────────────────────────────────────────────────────────────────────
    // Compound Operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Defers a message for the first time on this key.
    ///
    /// Appends the offset and initializes `retry_count = 0`.
    ///
    /// # Precondition
    ///
    /// Key must not already have deferred messages. See trait-level invariants.
    fn defer_first_message(
        &self,
        key: &Key,
        offset: Offset,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Defers an additional message for an already-deferred key.
    ///
    /// Appends the offset without modifying `retry_count`. Queued messages
    /// retry in FIFO order after the head succeeds.
    ///
    /// # Precondition
    ///
    /// Key must already have deferred messages. See trait-level invariants.
    fn defer_additional_message(
        &self,
        key: &Key,
        offset: Offset,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { self.append_deferred_message(key, offset).await }
    }

    /// Completes a successful retry and advances or clears the queue.
    ///
    /// Removes `offset`, then either resets `retry_count = 0` for the next
    /// message or deletes the key entirely if the queue is empty.
    fn complete_retry_success(
        &self,
        key: &Key,
        offset: Offset,
    ) -> impl Future<Output = Result<RetryCompletionResult, Self::Error>> + Send {
        async move {
            self.remove_deferred_message(key, offset).await?;

            if let Some((next_offset, _)) = self.get_next_deferred_message(key).await? {
                self.set_retry_count(key, 0).await?;
                Ok(RetryCompletionResult::MoreMessages { next_offset })
            } else {
                self.delete_key(key).await?;
                Ok(RetryCompletionResult::Completed)
            }
        }
    }

    /// Increments retry count after a failed retry attempt.
    ///
    /// Returns the new count for backoff calculation.
    fn increment_retry_count(
        &self,
        key: &Key,
        current_retry_count: u32,
    ) -> impl Future<Output = Result<u32, Self::Error>> + Send {
        async move {
            let new_count = current_retry_count.saturating_add(1);
            self.set_retry_count(key, new_count).await?;
            Ok(new_count)
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Queries
    // ─────────────────────────────────────────────────────────────────────────

    /// Returns the oldest deferred offset and current retry count, or `None`.
    fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<Option<(Offset, u32)>, Self::Error>> + Send;

    /// Returns `retry_count` if key is deferred, `None` otherwise.
    ///
    /// Prefer over
    /// [`get_next_deferred_message`](Self::get_next_deferred_message)
    /// when you don't need the offset.
    fn is_deferred(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<Option<u32>, Self::Error>> + Send {
        async move {
            Ok(self
                .get_next_deferred_message(key)
                .await?
                .map(|(_, count)| count))
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Primitives (prefer compound operations above)
    // ─────────────────────────────────────────────────────────────────────────

    /// Appends an offset without modifying `retry_count`.
    ///
    /// Prefer [`defer_first_message`](Self::defer_first_message) or
    /// [`defer_additional_message`](Self::defer_additional_message).
    fn append_deferred_message(
        &self,
        key: &Key,
        offset: Offset,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes an offset without modifying `retry_count` or deleting the key.
    ///
    /// Prefer [`complete_retry_success`](Self::complete_retry_success).
    fn remove_deferred_message(
        &self,
        key: &Key,
        offset: Offset,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // ─────────────────────────────────────────────────────────────────────────
    // Internal (used by default implementations)
    // ─────────────────────────────────────────────────────────────────────────

    /// Sets `retry_count` directly. Used by default implementations.
    fn set_retry_count(
        &self,
        key: &Key,
        retry_count: u32,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes all data for a key. Used by default implementations.
    fn delete_key(&self, key: &Key) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
