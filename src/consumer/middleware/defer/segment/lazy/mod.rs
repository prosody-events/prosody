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
mod tests;
