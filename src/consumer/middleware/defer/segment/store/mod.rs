//! Segment persistence trait and in-memory implementation.

use super::Segment;
use crate::SegmentId;
use crate::error::ClassifyError;
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

/// Memory mode's registry of deferred segments, the twin of the Cassandra
/// `deferred_segments` table.
///
/// One registry serves both memory defer providers, as one
/// [`CassandraSegmentStore`](super::CassandraSegmentStore) serves both
/// Cassandra providers. Cloning shares the map.
#[derive(Clone, Debug, Default)]
pub struct MemorySegmentStore {
    segments: Arc<scc::HashMap<SegmentId, Segment, ahash::RandomState>>,
}

impl MemorySegmentStore {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every segment registered so far. A snapshot.
    ///
    /// The map has no removal path because `deferred_segments` has none
    /// either: a segment that ever deferred stays in the registry. It is
    /// bounded by the number of partitions the providers served, and it drops
    /// with the registry.
    pub(crate) async fn segments(&self) -> Vec<Segment> {
        let mut out = Vec::new();
        self.segments
            .iter_async(|_, segment| {
                out.push(segment.clone());
                true
            })
            .await;
        out
    }
}

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
