//! Storage trait for defer segment metadata.
//!
//! Provides abstraction for persisting and retrieving segment metadata.
//! The segment store is shared by both message and timer defer middlewares.

use super::{Segment, SegmentId};
use crate::consumer::middleware::{ClassifyError, ErrorCategory};
use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::sync::Arc;

/// Storage backend for defer segment metadata.
///
/// Manages the `deferred_segments` table which stores metadata about each
/// segment (topic, partition, `consumer_group`). This metadata is shared
/// between message and timer defer stores.
///
/// # Lifecycle
///
/// 1. When `MessageDeferMiddleware` is created for a partition, call
///    [`get_or_create_segment`](Self::get_or_create_segment)
/// 2. The returned [`Segment`] is passed to both message and timer defer stores
/// 3. Both stores use `segment.id()` as their partition key prefix
///
/// # Implementations
///
/// - [`CassandraSegmentStore`](super::CassandraSegmentStore): Production
///   implementation using Cassandra
/// - [`MemorySegmentStore`]: In-memory implementation for testing
pub trait SegmentStore: Clone + Send + Sync + 'static {
    /// Error type for segment operations.
    type Error: Error + ClassifyError + Send + Sync + 'static;

    /// Gets or creates a segment for the given topic, partition, and consumer
    /// group.
    ///
    /// This is the primary entry point for segment management. It:
    /// 1. Computes the segment ID from the inputs
    /// 2. Inserts segment metadata if not already present (idempotent)
    /// 3. Returns the [`Segment`] with computed ID and metadata
    ///
    /// # Arguments
    ///
    /// * `segment` - Segment metadata to persist
    ///
    /// # Returns
    ///
    /// The segment with its computed ID.
    ///
    /// # Errors
    ///
    /// Returns error if segment metadata insertion fails.
    fn get_or_create_segment(
        &self,
        segment: Segment,
    ) -> impl Future<Output = Result<Segment, Self::Error>> + Send;

    /// Retrieves segment metadata by ID.
    ///
    /// Used for diagnostics and observability. Returns `None` if the segment
    /// doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment ID to look up
    ///
    /// # Returns
    ///
    /// `Some(Segment)` if found, `None` otherwise.
    fn get_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send;
}

/// In-memory implementation of [`SegmentStore`] for testing.
///
/// Stores segments in a concurrent hash map. All data is volatile and lost
/// on process exit.
#[derive(Clone, Debug, Default)]
pub struct MemorySegmentStore {
    segments: Arc<scc::HashMap<SegmentId, Segment, ahash::RandomState>>,
}

impl MemorySegmentStore {
    /// Creates a new empty memory segment store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl SegmentStore for MemorySegmentStore {
    type Error = MemorySegmentStoreError;

    async fn get_or_create_segment(&self, segment: Segment) -> Result<Segment, Self::Error> {
        let segment_id = segment.id();

        // Try to insert, or return existing
        self.segments
            .entry_async(segment_id)
            .await
            .or_insert(segment.clone());

        Ok(segment)
    }

    async fn get_segment(&self, segment_id: &SegmentId) -> Result<Option<Segment>, Self::Error> {
        Ok(self
            .segments
            .get_async(segment_id)
            .await
            .map(|entry| entry.get().clone()))
    }
}

/// Error type for [`MemorySegmentStore`] operations.
///
/// This store is infallible, so this enum has no variants and can never
/// be constructed. It exists only to satisfy the `SegmentStore::Error` bound.
#[derive(Debug, thiserror::Error)]
pub enum MemorySegmentStoreError {}

impl ClassifyError for MemorySegmentStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match *self {}
    }
}

impl From<MemorySegmentStoreError> for Infallible {
    fn from(err: MemorySegmentStoreError) -> Self {
        match err {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConsumerGroup, Partition, Topic};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_memory_get_or_create_new_segment() -> color_eyre::Result<()> {
        let store = MemorySegmentStore::new();
        let segment = Segment::new(
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-group") as ConsumerGroup,
        );
        let segment_id = segment.id();

        let returned = store.get_or_create_segment(segment.clone()).await?;

        assert_eq!(returned.id(), segment_id);
        assert_eq!(returned, segment);
        Ok(())
    }

    #[tokio::test]
    async fn test_memory_get_or_create_existing_segment() -> color_eyre::Result<()> {
        let store = MemorySegmentStore::new();
        let segment = Segment::new(
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-group") as ConsumerGroup,
        );

        // Create first time
        store.get_or_create_segment(segment.clone()).await?;

        // Create again - should be idempotent
        let returned = store.get_or_create_segment(segment.clone()).await?;

        assert_eq!(returned, segment);
        Ok(())
    }

    #[tokio::test]
    async fn test_memory_get_segment_existing() -> color_eyre::Result<()> {
        let store = MemorySegmentStore::new();
        let segment = Segment::new(
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-group") as ConsumerGroup,
        );
        let segment_id = segment.id();

        // Create segment
        store.get_or_create_segment(segment.clone()).await?;

        // Get by ID
        let result = store.get_segment(&segment_id).await?;

        assert_eq!(result, Some(segment));
        Ok(())
    }

    #[tokio::test]
    async fn test_memory_get_segment_nonexistent() -> color_eyre::Result<()> {
        let store = MemorySegmentStore::new();
        let segment_id = uuid::Uuid::new_v4();

        let result = store.get_segment(&segment_id).await?;

        assert_eq!(result, None);
        Ok(())
    }
}
