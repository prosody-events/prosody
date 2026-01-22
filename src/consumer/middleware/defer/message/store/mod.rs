//! Message defer storage backend.
//!
//! Manages per-key FIFO queues of deferred message offsets with shared retry
//! counters. The [`MessageDeferStore`] trait abstracts over in-memory and
//! Cassandra implementations.
//!
//! # Key Concepts
//!
//! - **Offset queue**: Each key maintains a FIFO queue of Kafka offsets
//! - **Retry count**: Shared counter per key, reset on successful processing
//! - **Segment isolation**: Stores are scoped to
//!   `{topic}/{partition}:{consumer_group}`

pub mod cached;
pub mod cassandra;
pub mod memory;
pub mod provider;

#[cfg(test)]
pub mod tests;

use crate::error::ClassifyError;
use crate::{Key, Offset};
use std::error::Error;
use std::future::Future;

pub use cached::CachedDeferStore;
pub use cassandra::{CassandraMessageDeferStore, CassandraMessageDeferStoreProvider};
pub use memory::{MemoryMessageDeferStore, MemoryMessageDeferStoreProvider};
pub use provider::MessageDeferStoreProvider;

/// Outcome of completing a successful retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageRetryCompletionResult {
    /// Queue has more messages; retry count reset to 0.
    MoreMessages {
        /// Next offset to retry (oldest in queue).
        next_offset: Offset,
    },

    /// Queue empty; key deleted from storage.
    Completed,
}

/// Storage backend for deferred message offsets.
///
/// Each key maintains a FIFO queue of Kafka offsets with a shared retry
/// counter. Segment context (`topic/partition/consumer_group`) is established
/// at store construction.
///
/// # Invariants
///
/// - Use [`defer_first_message`](Self::defer_first_message) for first failure,
///   [`defer_additional_message`](Self::defer_additional_message) for
///   subsequent
/// - Messages retry in offset order; retry count applies to queue head
/// - [`complete_retry_success`](Self::complete_retry_success) advances queue or
///   deletes key
pub trait MessageDeferStore: Clone + Send + Sync + 'static {
    /// Error type. Must implement [`ClassifyError`] for retry decisions.
    type Error: Error + ClassifyError + Send + Sync + 'static;

    /// Defers a message on a fresh key (initializes retry count to 0).
    ///
    /// # Precondition
    ///
    /// Key must not already have deferred messages.
    fn defer_first_message(
        &self,
        key: &Key,
        offset: Offset,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Appends offset to an existing deferred key (preserves retry count).
    ///
    /// # Precondition
    ///
    /// Key must already have deferred messages.
    fn defer_additional_message(
        &self,
        key: &Key,
        offset: Offset,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { self.append_deferred_message(key, offset).await }
    }

    /// Removes offset after successful processing; resets retry count or
    /// deletes key.
    fn complete_retry_success(
        &self,
        key: &Key,
        offset: Offset,
    ) -> impl Future<Output = Result<MessageRetryCompletionResult, Self::Error>> + Send {
        async move {
            self.remove_deferred_message(key, offset).await?;

            if let Some((next_offset, _)) = self.get_next_deferred_message(key).await? {
                self.set_retry_count(key, 0).await?;
                Ok(MessageRetryCompletionResult::MoreMessages { next_offset })
            } else {
                self.delete_key(key).await?;
                Ok(MessageRetryCompletionResult::Completed)
            }
        }
    }

    /// Increments retry count after failed attempt; returns new count.
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

    /// Returns oldest offset and retry count, or `None` if key not deferred.
    fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<Option<(Offset, u32)>, Self::Error>> + Send;

    /// Returns retry count if deferred, `None` otherwise. Cheaper than
    /// [`get_next_deferred_message`](Self::get_next_deferred_message).
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

    /// Low-level: appends offset without touching retry count.
    fn append_deferred_message(
        &self,
        key: &Key,
        offset: Offset,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Low-level: removes offset without cleanup.
    fn remove_deferred_message(
        &self,
        key: &Key,
        offset: Offset,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Internal: sets retry count directly.
    fn set_retry_count(
        &self,
        key: &Key,
        retry_count: u32,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Internal: deletes all data for a key.
    fn delete_key(&self, key: &Key) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
