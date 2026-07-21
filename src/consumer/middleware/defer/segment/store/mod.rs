//! Segment persistence trait and in-memory implementation.

use super::Segment;
use crate::SegmentId;
use crate::error::ClassifyError;
use std::error::Error;
use std::future::Future;

#[cfg(test)]
use std::convert::Infallible;
#[cfg(test)]
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
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub struct MemorySegmentStore {
    segments: Arc<scc::HashMap<SegmentId, Segment, ahash::RandomState>>,
}

#[cfg(test)]
impl MemorySegmentStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
impl SegmentStore for MemorySegmentStore {
    type Error = Infallible;

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

#[cfg(test)]
mod tests;
