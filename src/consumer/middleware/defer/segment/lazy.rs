//! Lazy segment initialization for deferred I/O.

use super::{Segment, SegmentStore};
use crate::{ConsumerGroup, Partition, Topic};
use std::fmt;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Defers segment creation/persistence until first access.
///
/// Cheap to clone (`Arc` internally). Multiple clones share the segment;
/// persistence happens exactly once. Thread-safe concurrent access.
#[derive(Clone)]
pub struct LazySegment<S> {
    inner: Arc<LazySegmentInner<S>>,
}

struct LazySegmentInner<S> {
    cell: OnceCell<Segment>,
    store: S,
    topic: Topic,
    partition: Partition,
    consumer_group: ConsumerGroup,
}

impl<S> LazySegment<S>
where
    S: SegmentStore,
{
    /// Creates a lazy segment (no I/O until [`get`](Self::get)).
    #[must_use]
    pub fn new(
        store: S,
        topic: Topic,
        partition: Partition,
        consumer_group: ConsumerGroup,
    ) -> Self {
        Self {
            inner: Arc::new(LazySegmentInner {
                cell: OnceCell::new(),
                store,
                topic,
                partition,
                consumer_group,
            }),
        }
    }

    /// Returns the segment, persisting on first call. Errors are not cached.
    ///
    /// # Errors
    ///
    /// Returns error if segment persistence fails.
    pub async fn get(&self) -> Result<&Segment, S::Error> {
        self.inner
            .cell
            .get_or_try_init(|| async {
                let segment = Segment::new(
                    self.inner.topic,
                    self.inner.partition,
                    self.inner.consumer_group.clone(),
                );
                self.inner
                    .store
                    .get_or_create_segment(segment.clone())
                    .await?;
                Ok(segment)
            })
            .await
    }

    /// Whether the segment has been initialized.
    #[must_use]
    pub fn is_initialized(&self) -> bool {
        self.inner.cell.initialized()
    }
}

impl<S> fmt::Debug for LazySegment<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazySegment")
            .field("topic", &self.inner.topic)
            .field("partition", &self.inner.partition)
            .field("consumer_group", &self.inner.consumer_group)
            .field("initialized", &self.inner.cell.initialized())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::middleware::defer::segment::MemorySegmentStore;
    use color_eyre::eyre::bail;

    #[tokio::test]
    async fn test_lazy_segment_persists_on_first_access() -> color_eyre::Result<()> {
        let store = MemorySegmentStore::new();
        let segment = LazySegment::new(
            store.clone(),
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-group") as ConsumerGroup,
        );

        // Not initialized yet
        assert!(!segment.is_initialized());

        // First access creates and persists
        let seg = segment.get().await?;
        assert!(segment.is_initialized());

        // Verify it was persisted
        let Some(stored) = store.get_segment(&seg.id()).await? else {
            bail!("segment should exist");
        };
        assert_eq!(stored.id(), seg.id());
        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_segment_clone_shares_state() {
        let store = MemorySegmentStore::new();
        let segment1 = LazySegment::new(
            store,
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-group") as ConsumerGroup,
        );
        let segment2 = segment1.clone();

        // Initialize via first clone
        let seg1 = segment1.get().await.expect("should succeed");

        // Second clone sees initialized state
        assert!(segment2.is_initialized());
        let seg2 = segment2.get().await.expect("should succeed");

        // Same segment
        assert_eq!(seg1.id(), seg2.id());
    }

    #[tokio::test]
    async fn test_lazy_segment_idempotent() {
        let store = MemorySegmentStore::new();
        let segment = LazySegment::new(
            store,
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-group") as ConsumerGroup,
        );

        // Multiple calls return same segment
        let seg1 = segment.get().await.expect("should succeed");
        let seg2 = segment.get().await.expect("should succeed");
        assert_eq!(seg1.id(), seg2.id());
    }
}
