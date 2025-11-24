//! Storage trait and implementations for defer middleware.

pub mod cached;
pub mod cassandra;
pub mod memory;

#[cfg(test)]
pub mod tests;

use crate::Offset;
use crate::consumer::middleware::ClassifyError;
use crate::timers::datetime::CompactDateTime;
use std::error::Error;
use std::future::Future;
use uuid::Uuid;

pub use cached::CachedDeferStore;
pub use cassandra::CassandraDeferStore;
pub use memory::MemoryDeferStore;

/// Result of completing a successful retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryCompletionResult {
    /// More messages remain in the deferred queue.
    ///
    /// The `retry_count` has been reset to 0 for the next message.
    /// Caller should:
    /// 1. Update cache: `DeferState::Deferred { retry_count: 0 }`
    /// 2. Schedule timer with backoff(0) for immediate retry
    MoreMessages {
        /// The next (oldest) offset to be retried.
        next_offset: Offset,
    },

    /// No more messages remain in the deferred queue.
    ///
    /// The key has been completely cleaned up (deleted from storage).
    /// Caller should:
    /// 1. Remove from cache entirely
    /// 2. No timer needed
    Completed,
}

/// Storage backend for deferred message offsets.
///
/// Tracks which keys have deferred messages using UUIDs that encode
/// consumer group, topic, partition, and key. Supports multiple offsets
/// per key (FIFO queue) with a shared retry counter.
///
/// # Usage Pattern
///
/// ```text
/// 1. Message fails (permanent error)
///    → defer_first_message(key, offset, time)
///    → Schedule timer with backoff(0)
///
/// 2. More messages arrive for same key
///    → defer_additional_message(key, offset, time)
///    → No new timer (existing timer will retry)
///
/// 3. Timer fires
///    → get_next_deferred_message(key) → Some((offset, retry_count))
///    → Load and retry message
///
///    Success:
///      → complete_retry_success(key, offset)
///      → If MoreMessages: schedule timer with backoff(0)
///      → If Completed: cleanup cache
///
///    Failure:
///      → increment_retry_count(key, retry_count) → new_count
///      → Schedule timer with backoff(new_count)
/// ```
///
/// # Implementation Requirements
///
/// - **Atomicity**: Compound operations should minimize round-trips
/// - **Idempotency**: Repeated operations must be safe
/// - **TTL**: Use `expected_retry_time` for automatic cleanup
/// - **Static column semantics**: See notes on `defer_first_message`
pub trait DeferStore: Clone + Send + Sync + 'static {
    /// Error type for storage operations.
    ///
    /// Must implement [`ClassifyError`] for proper retry logic.
    type Error: Error + ClassifyError + Send + Sync + 'static;

    // ============================================================================
    // Compound Operations (Primary Interface)
    // ============================================================================

    /// Defers a message for the first time on this key.
    ///
    /// Appends the offset to the deferred queue and initializes `retry_count`
    /// to 0. After calling this, the caller should schedule a timer with
    /// backoff(0).
    ///
    /// # Panics
    ///
    /// MUST NOT be called if key already has deferred messages. Use
    /// [`defer_additional_message`] instead. Behavior is implementation-defined
    /// (may corrupt `retry_count`).
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    fn defer_first_message(
        &self,
        defer_key: &Uuid,
        offset: Offset,
        expected_retry_time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Defers an additional message for an already-deferred key.
    ///
    /// Appends the offset without modifying `retry_count`. The new message will
    /// be retried after all earlier messages succeed (FIFO).
    ///
    /// # Default Implementation
    ///
    /// Delegates to [`append_deferred_message`]. Backends can override for
    /// optimization.
    ///
    /// # Panics
    ///
    /// MUST NOT be called for the first message on a key. Use
    /// [`defer_first_message`] instead.
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    fn defer_additional_message(
        &self,
        defer_key: &Uuid,
        offset: Offset,
        expected_retry_time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            self.append_deferred_message(defer_key, offset, expected_retry_time)
                .await
        }
    }

    /// Completes successful retry by cleaning up and preparing for next
    /// message.
    ///
    /// Performs atomically (or with minimal operations):
    /// 1. Removes the offset from the deferred queue
    /// 2. Checks if more messages exist for this key
    /// 3. If yes: resets `retry_count` to 0 (for next message's first retry)
    /// 4. If no: deletes the entire key (cleanup orphaned static column)
    ///
    /// Returns status indicating next action for caller.
    ///
    /// # Default Implementation
    ///
    /// Uses primitives: [`remove_deferred_message`],
    /// [`get_next_deferred_message`], [`set_retry_count`], and
    /// [`delete_key`]. Backends can override for atomic implementation.
    ///
    /// # Errors
    ///
    /// Returns error if any operation fails. State may be partially updated.
    fn complete_retry_success(
        &self,
        defer_key: &Uuid,
        offset: Offset,
    ) -> impl Future<Output = Result<RetryCompletionResult, Self::Error>> + Send {
        async move {
            // Step 1: Remove the successfully processed offset
            self.remove_deferred_message(defer_key, offset).await?;

            // Step 2: Check if more messages exist for this key
            let next_message = self.get_next_deferred_message(defer_key).await?;

            if let Some((next_offset, _retry_count)) = next_message {
                // Step 3a: More messages exist - reset retry_count to 0 for next message
                self.set_retry_count(defer_key, 0).await?;
                Ok(RetryCompletionResult::MoreMessages { next_offset })
            } else {
                // Step 3b: No more messages - delete entire key
                self.delete_key(defer_key).await?;
                Ok(RetryCompletionResult::Completed)
            }
        }
    }

    /// Increments retry count after failed retry.
    ///
    /// Takes the current `retry_count` (from [`get_next_deferred_message`]) and
    /// updates it to current + 1. Returns the new count for scheduling backoff.
    ///
    /// # Default Implementation
    ///
    /// Computes `new_count = current.saturating_add(1)` and calls internal
    /// set operation. Backends can override for atomic increment if supported.
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    fn increment_retry_count(
        &self,
        defer_key: &Uuid,
        current_retry_count: u32,
    ) -> impl Future<Output = Result<u32, Self::Error>> + Send {
        async move {
            let new_count = current_retry_count.saturating_add(1);
            self.set_retry_count(defer_key, new_count).await?;
            Ok(new_count)
        }
    }

    // ============================================================================
    // Queries
    // ============================================================================

    /// Gets the next (oldest) deferred message and current retry count.
    ///
    /// Returns `None` if no messages are deferred for this key.
    ///
    /// Use this when you need the specific offset (e.g., timer retries).
    /// For existence checks, prefer [`is_deferred`] (may be more efficient).
    ///
    /// # Errors
    ///
    /// Returns error if query fails.
    fn get_next_deferred_message(
        &self,
        defer_key: &Uuid,
    ) -> impl Future<Output = Result<Option<(Offset, u32)>, Self::Error>> + Send;

    /// Checks if this key has any deferred messages.
    ///
    /// Returns the `retry_count` if deferred, `None` if not.
    ///
    /// More efficient than [`get_next_deferred_message`] when you only need
    /// existence check and retry count (don't need specific offset).
    ///
    /// # Default Implementation
    ///
    /// Calls [`get_next_deferred_message`] and discards offset. Backends can
    /// override to query only `retry_count`.
    ///
    /// # Errors
    ///
    /// Returns error if query fails.
    fn is_deferred(
        &self,
        defer_key: &Uuid,
    ) -> impl Future<Output = Result<Option<u32>, Self::Error>> + Send {
        async move {
            Ok(self
                .get_next_deferred_message(defer_key)
                .await?
                .map(|(_, count)| count))
        }
    }

    // ============================================================================
    // Low-Level Mutations (For Edge Cases)
    // ============================================================================

    /// Appends an offset to the deferred queue (low-level primitive).
    ///
    /// Does NOT modify `retry_count`. Use compound operations instead:
    /// - [`defer_first_message`] - for first failure (sets `retry_count=0`)
    /// - [`defer_additional_message`] - for additional messages
    ///
    /// Direct usage is only for edge cases (e.g., manual data repair).
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    fn append_deferred_message(
        &self,
        defer_key: &Uuid,
        offset: Offset,
        expected_retry_time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes a specific offset from the deferred queue (low-level primitive).
    ///
    /// Does NOT modify `retry_count` or delete key. Use compound operations:
    /// - [`complete_retry_success`] - for successful retries (handles cleanup)
    ///
    /// Direct usage is only for edge cases (e.g., manual cleanup).
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    fn remove_deferred_message(
        &self,
        defer_key: &Uuid,
        offset: Offset,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // ============================================================================
    // Internal Operations (Not for direct use)
    // ============================================================================

    /// Sets `retry_count` to an explicit value (internal primitive).
    ///
    /// Used internally by [`increment_retry_count`] and
    /// [`complete_retry_success`]. Should not be called directly by users.
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    #[doc(hidden)]
    fn set_retry_count(
        &self,
        defer_key: &Uuid,
        retry_count: u32,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes entire key partition (internal primitive).
    ///
    /// Used internally by [`complete_retry_success`]. Should not be called
    /// directly by users.
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    #[doc(hidden)]
    fn delete_key(&self, defer_key: &Uuid) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
