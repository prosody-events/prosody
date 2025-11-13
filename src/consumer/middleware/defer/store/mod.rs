//! Storage trait and implementations for defer middleware.

pub mod memory;

use crate::Offset;
use crate::consumer::middleware::ClassifyError;
use crate::timers::datetime::CompactDateTime;
use std::error::Error;
use std::future::Future;
use uuid::Uuid;

/// Storage backend for deferred message offsets.
///
/// Tracks which keys have deferred messages using UUIDs that encode
/// consumer group, topic, partition, and key. Supports multiple offsets
/// per key (queue of deferred messages) with a shared retry counter.
///
/// # Design Pattern
///
/// Follows the same pattern as `TriggerStore`:
/// - Minimal trait with only essential operations
/// - TTL support for automatic cleanup
/// - Error associated type for storage-specific errors
///
/// # Implementation Requirements
///
/// - **Atomicity**: Updates to defer state should be atomic per `key_id`
/// - **Idempotency**: Repeated operations should be safe
/// - **TTL**: Use `expected_retry_time` for TTL calculation
/// - **Static `retry_count`**: Shared across all offsets for a `key_id`
///
/// # Error Handling
///
/// All operations return `Result<T, Self::Error>` where `Error` is a
/// storage-specific error type (e.g., Cassandra query errors, I/O errors).
pub trait DeferStore: Clone + Send + Sync + 'static {
    /// Error type for storage operations.
    ///
    /// Must implement `ClassifyError` to enable proper error handling and retry
    /// logic.
    type Error: Error + ClassifyError + Send + Sync + 'static;

    /// Get the next (oldest) deferred offset and current retry count for a key.
    ///
    /// Returns `None` if no messages are deferred for this key.
    /// Returns `Some((offset, retry_count))` with the oldest offset and the
    /// shared retry counter (from Cassandra static column).
    ///
    /// # Arguments
    ///
    /// * `key_id` - The UUID identifying the deferred key
    ///
    /// # Returns
    ///
    /// * `None` - No deferred messages for this key
    /// * `Some((offset, retry_count))` - Oldest offset and current retry
    ///   counter
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend fails.
    fn get_next_deferred_message(
        &self,
        key_id: &Uuid,
    ) -> impl Future<Output = Result<Option<(Offset, u32)>, Self::Error>> + Send;

    /// Append a new offset to the deferred queue for a key.
    ///
    /// # TTL Calculation
    ///
    /// The `expected_retry_time` parameter is used to calculate the Cassandra
    /// TTL:
    /// ```text
    /// TTL = (expected_retry_time - now) + base_ttl
    /// ```
    /// This ensures the entry expires shortly after it would be retried,
    /// preventing unbounded growth. The calculation uses
    /// `CassandraStore::calculate_ttl()`.
    ///
    /// # Retry Count Update
    ///
    /// If `retry_count` is `Some(value)`, also updates the static `retry_count`
    /// column. This allows combining offset insert with `retry_count`
    /// update in a single query for the first failure case.
    ///
    /// # Arguments
    ///
    /// * `key_id` - The UUID identifying the deferred key
    /// * `offset` - The Kafka offset to defer
    /// * `expected_retry_time` - When this message is expected to be retried
    ///   (for TTL calculation)
    /// * `retry_count` - If Some, also sets the `retry_count` static column
    ///   (for first failure)
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend fails.
    fn append_deferred_message(
        &self,
        key_id: &Uuid,
        offset: Offset,
        expected_retry_time: CompactDateTime,
        retry_count: Option<u32>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Remove an offset after successful retry.
    ///
    /// Called after successfully processing a deferred message to remove it
    /// from the queue.
    ///
    /// # Arguments
    ///
    /// * `key_id` - The UUID identifying the deferred key
    /// * `offset` - The offset to remove
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend fails.
    fn remove_deferred_message(
        &self,
        key_id: &Uuid,
        offset: Offset,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Set the retry counter to an explicit value.
    ///
    /// Called when we need to update `retry_count` without inserting an offset:
    /// - After a deferred retry fails: set to `retry_count + 1`
    /// - After success with more messages: set to `0` (reset for next message)
    ///
    /// The caller always knows the exact value since they just read it from
    /// `get_next_deferred_message`.
    ///
    /// # Arguments
    ///
    /// * `key_id` - The UUID identifying the deferred key
    /// * `retry_count` - The new retry count value
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend fails.
    fn set_retry_count(
        &self,
        key_id: &Uuid,
        retry_count: u32,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
