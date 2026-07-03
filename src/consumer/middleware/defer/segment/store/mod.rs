//! Segment persistence trait and in-memory implementation.

use super::Segment;
use crate::SegmentId;
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
mod tests;
