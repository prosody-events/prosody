//! Segment infrastructure for defer middleware.
//!
//! Provides shared segment identification and management for both message
//! and timer defer stores. A segment represents the context of a specific
//! `topic/partition/consumer_group` combination.
//!
//! # Architecture
//!
//! Both message defer and timer defer share the same segment concept:
//! - **Same segment ID**: Both compute identical `UUIDv5` from
//!   `{topic}/{partition}:{consumer_group}`
//! - **Different data tables**: `deferred_offsets` for messages,
//!   `deferred_timers` for timers
//!
//! # Lifecycle
//!
//! 1. [`CassandraSegmentStore`] is created once with prepared queries
//! 2. Each Cassandra defer store receives a clone and wraps it in
//!    [`LazySegment`]
//! 3. On first store access, the segment is persisted to Cassandra
//! 4. Stores use `segment.id()` for their storage operations

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

/// Unique identifier for a defer segment.
///
/// A segment represents the context of `{topic}/{partition}:{consumer_group}`.
/// All deferred messages and timers for a given segment share the same
/// partition key prefix in storage.
pub type SegmentId = Uuid;

/// Segment metadata for observability and reconstruction.
///
/// Stores the context needed to identify and manage a defer segment.
/// The segment ID is computed deterministically from the topic, partition,
/// and consumer group.
///
/// # Shared Ownership
///
/// `Segment` is designed to be cloned and shared between message and timer
/// defer handlers. The internal types (`Topic`, `ConsumerGroup`) are cheap
/// to clone (`Intern<str>`, `Arc<str>`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    /// Unique segment identifier (`UUIDv5` hash).
    id: SegmentId,

    /// Kafka topic.
    topic: Topic,

    /// Kafka partition.
    partition: Partition,

    /// Consumer group ID.
    consumer_group: ConsumerGroup,
}

impl Segment {
    /// Creates a new segment with computed ID.
    ///
    /// The segment ID is computed deterministically from the topic, partition,
    /// and consumer group using [`compute_segment_id`].
    ///
    /// # Arguments
    ///
    /// * `topic` - Kafka topic
    /// * `partition` - Kafka partition
    /// * `consumer_group` - Consumer group ID
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

    /// Creates a segment from existing ID and metadata.
    ///
    /// Used when loading segment metadata from storage.
    ///
    /// # Arguments
    ///
    /// * `id` - Pre-computed segment ID
    /// * `topic` - Kafka topic
    /// * `partition` - Kafka partition
    /// * `consumer_group` - Consumer group ID
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

    /// Returns the segment's unique identifier.
    #[must_use]
    pub fn id(&self) -> SegmentId {
        self.id
    }

    /// Returns the Kafka topic.
    #[must_use]
    pub fn topic(&self) -> &Topic {
        &self.topic
    }

    /// Returns the Kafka partition.
    #[must_use]
    pub fn partition(&self) -> Partition {
        self.partition
    }

    /// Returns the consumer group ID.
    #[must_use]
    pub fn consumer_group(&self) -> &ConsumerGroup {
        &self.consumer_group
    }
}

/// Computes a deterministic segment ID from topic, partition, and consumer
/// group.
///
/// Uses `UUIDv5` with OID namespace to generate a reproducible identifier from
/// the string `"{topic}/{partition}:{consumer_group}"`.
///
/// # Arguments
///
/// * `topic` - Kafka topic
/// * `partition` - Kafka partition
/// * `consumer_group` - Consumer group ID
///
/// # Returns
///
/// A deterministic [`SegmentId`] (UUID) for this segment.
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
