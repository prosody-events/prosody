//! Segment persistence trait and in-memory implementation.

use super::{Segment, SegmentId};
use crate::error::{ClassifyError, ErrorCategory};
use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::sync::Arc;

/// Storage backend for segment metadata (topic, partition, consumer group).
///
/// Used via [`LazySegment`](super::LazySegment) to defer persistence until
/// first access.
pub trait SegmentStore: Clone + Send + Sync + 'static {
    /// Error type for segment operations.
    type Error: Error + ClassifyError + Send + Sync + 'static;

    /// Persists segment metadata (idempotent).
    fn get_or_create_segment(
        &self,
        segment: Segment,
    ) -> impl Future<Output = Result<Segment, Self::Error>> + Send;

    /// Retrieves segment metadata by ID (for diagnostics).
    fn get_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send;
}

/// In-memory segment store for testing.
#[derive(Clone, Debug, Default)]
pub struct MemorySegmentStore {
    segments: Arc<scc::HashMap<SegmentId, Segment, ahash::RandomState>>,
}

impl MemorySegmentStore {
    /// Creates an empty store.
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

/// Infallible error type (store never fails).
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
