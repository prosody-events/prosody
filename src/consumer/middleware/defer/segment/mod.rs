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

pub mod cassandra;
pub mod lazy;
pub mod store;

use crate::segment::partition_segment_id;
use crate::{ConsumerGroup, Partition, SegmentId, Topic};

pub use cassandra::{CassandraSegmentStore, CassandraSegmentStoreError};
pub use lazy::LazySegment;
pub use store::SegmentStore;

// Re-export MemorySegmentStore for testing only
#[cfg(test)]
pub use store::{MemorySegmentStore, MemorySegmentStoreError};

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
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_segment_new_computes_correct_id() {
        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);
        let consumer_group: ConsumerGroup = Arc::from("test-group");

        let segment = Segment::new(topic, partition, consumer_group.clone());
        let expected_id = partition_segment_id(topic, partition, &consumer_group);

        assert_eq!(segment.id(), expected_id);
    }

    #[test]
    fn test_segment_accessors() {
        let topic = Topic::from("test-topic");
        let partition = Partition::from(42_i32);
        let consumer_group: ConsumerGroup = Arc::from("test-group");

        let segment = Segment::new(topic, partition, consumer_group.clone());

        assert_eq!(segment.topic(), &topic);
        assert_eq!(segment.partition(), partition);
        assert_eq!(segment.consumer_group(), &consumer_group);
    }
}
