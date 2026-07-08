//! Segment identity for defer stores.
//!
//! A **segment** is a `UUIDv5` hash of `{topic}/{partition}:{consumer_group}`,
//! serving as the partition key prefix in Cassandra. This provides:
//!
//! - **Data locality**: All deferred items for a partition colocate
//! - **Isolation**: Different consumer groups don't conflict
//! - **Stability**: Deterministic IDs survive restarts
//!
//! Message and timer stores share segment IDs but use separate tables.
//! [`LazySegment`] defers persistence until first I/O, enabling synchronous
//! store creation in `handler_for_partition`.
//!
//! The id type and its derivation live in the crate-internal `segment`
//! module; this module only names a partition through that shared source.

mod cassandra;
mod lazy;
mod store;

use crate::segment::partition_segment_id;
use crate::{ConsumerGroup, Partition, SegmentId, Topic};

// `CassandraSegmentStore` stays `pub`: `tests/defer_middleware.rs` constructs
// it directly as an integration test, so it must remain reachable outside the
// crate even though nothing else outside `defer::segment` needs it.
pub use cassandra::CassandraSegmentStore;
// `LazySegment`/`SegmentStore` also stay `pub` (and `Segment` below stays
// `pub` because `SegmentStore`'s methods return it): demoting `SegmentStore`
// turns its `get_segment` method (and the `Segment::with_id`/`LazySegment::
// is_initialized`/`SegmentQueries::get_segment` chain it pulls in) into
// production dead code whose only callers are its own tests — deleting it
// would mean deleting those tests, out of scope for a visibility-only wave.
pub use lazy::LazySegment;
pub use store::SegmentStore;

// Re-export MemorySegmentStore for testing only
#[cfg(test)]
pub use store::MemorySegmentStore;

/// Segment metadata: ID plus source context (topic, partition, consumer group).
///
/// Cheap to clone; internal types use `Intern<str>` and `Arc<str>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    id: SegmentId,
    topic: Topic,
    partition: Partition,
    consumer_group: ConsumerGroup,
}

impl Segment {
    /// Creates a segment with computed ID.
    #[must_use]
    pub fn new(topic: Topic, partition: Partition, consumer_group: ConsumerGroup) -> Self {
        let id = partition_segment_id(topic, partition, &consumer_group);
        Self {
            id,
            topic,
            partition,
            consumer_group,
        }
    }

    /// Creates a segment from existing ID (used when loading from storage).
    #[must_use]
    pub(crate) fn with_id(
        id: SegmentId,
        topic: Topic,
        partition: Partition,
        consumer_group: ConsumerGroup,
    ) -> Self {
        Self {
            id,
            topic,
            partition,
            consumer_group,
        }
    }

    /// Segment ID (`UUIDv5`).
    #[must_use]
    pub fn id(&self) -> SegmentId {
        self.id
    }

    /// Kafka topic.
    #[must_use]
    pub fn topic(&self) -> &Topic {
        &self.topic
    }

    /// Kafka partition.
    #[must_use]
    pub fn partition(&self) -> Partition {
        self.partition
    }

    /// Consumer group ID.
    #[must_use]
    pub fn consumer_group(&self) -> &ConsumerGroup {
        &self.consumer_group
    }
}

#[cfg(test)]
mod tests;
