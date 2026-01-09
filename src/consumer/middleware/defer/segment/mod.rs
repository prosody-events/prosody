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

pub mod cassandra;
pub mod lazy;
pub mod store;

use crate::{ConsumerGroup, Partition, Topic};
use uuid::Uuid;

pub use cassandra::{CassandraSegmentStore, CassandraSegmentStoreError};
pub use lazy::LazySegment;
pub use store::SegmentStore;

// Re-export MemorySegmentStore for testing only
#[cfg(test)]
pub use store::{MemorySegmentStore, MemorySegmentStoreError};

/// `UUIDv5` identifier for a `{topic}/{partition}:{consumer_group}` context.
pub type SegmentId = Uuid;

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
        let id = compute_segment_id(topic, partition, &consumer_group);
        Self {
            id,
            topic,
            partition,
            consumer_group,
        }
    }

    /// Creates a segment from existing ID (used when loading from storage).
    #[must_use]
    pub fn with_id(
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

/// Computes `UUIDv5(OID, "{topic}/{partition}:{consumer_group}")`.
#[must_use]
pub fn compute_segment_id(topic: Topic, partition: Partition, consumer_group: &str) -> SegmentId {
    let input = format!("{topic}/{partition}:{consumer_group}");
    Uuid::new_v5(&Uuid::NAMESPACE_OID, input.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_segment_id_deterministic() {
        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);
        let consumer_group = "test-group";

        let id1 = compute_segment_id(topic, partition, consumer_group);
        let id2 = compute_segment_id(topic, partition, consumer_group);

        assert_eq!(id1, id2);
    }

    #[test]
    fn test_segment_id_differs_by_topic() {
        let partition = Partition::from(0_i32);
        let consumer_group = "test-group";

        let id1 = compute_segment_id(Topic::from("topic-a"), partition, consumer_group);
        let id2 = compute_segment_id(Topic::from("topic-b"), partition, consumer_group);

        assert_ne!(id1, id2);
    }

    #[test]
    fn test_segment_id_differs_by_partition() {
        let topic = Topic::from("test-topic");
        let consumer_group = "test-group";

        let id1 = compute_segment_id(topic, Partition::from(0_i32), consumer_group);
        let id2 = compute_segment_id(topic, Partition::from(1_i32), consumer_group);

        assert_ne!(id1, id2);
    }

    #[test]
    fn test_segment_id_differs_by_consumer_group() {
        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);

        let id1 = compute_segment_id(topic, partition, "group-a");
        let id2 = compute_segment_id(topic, partition, "group-b");

        assert_ne!(id1, id2);
    }

    #[test]
    fn test_segment_new_computes_correct_id() {
        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);
        let consumer_group: ConsumerGroup = Arc::from("test-group");

        let segment = Segment::new(topic, partition, consumer_group.clone());
        let expected_id = compute_segment_id(topic, partition, &consumer_group);

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
