//! Identity: the consumer group, the segment, and the two segment ids a
//! segment derives.

use crate::segment::{partition_segment_id, timer_segment_id, timer_segment_name};
use crate::timers::store::SegmentId as TimerStoreSegmentId;
use crate::{ConsumerGroup, Partition, SegmentId, Topic};
use std::fmt;
use uuid::Uuid;

/// A consumer group id. A group owns segments.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GroupId(ConsumerGroup);

impl GroupId {
    /// Names the group `id`.
    #[must_use]
    pub fn new(id: &str) -> Self {
        Self(ConsumerGroup::from(id))
    }

    /// The group id as it appears in the stores.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One Kafka partition that one consumer group owns. It is the unit of durable
/// state, and it derives both on-disk segment ids.
///
/// **Invariant (frozen on-disk contract):** the two ids address two different
/// families of tables and are never interchangeable. Each one is a distinct
/// newtype with no `Deref` and no conversion from a raw [`Uuid`], so a method
/// that takes a defer id cannot receive a timer id. The formulas live in
/// `crate::segment`, and the frozen-bytes tests there pin both.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Segment {
    /// The consumer group that owns the partition.
    pub group: GroupId,
    /// The Kafka topic.
    pub topic: Topic,
    /// The Kafka partition.
    pub partition: Partition,
}

impl Segment {
    /// Names the partition `partition` of `topic` under `group`.
    #[must_use]
    pub fn new(group: GroupId, topic: Topic, partition: Partition) -> Self {
        Self {
            group,
            topic,
            partition,
        }
    }

    /// The id of this segment's deferred message and timer rows.
    #[must_use]
    pub fn defer_id(&self) -> DeferSegmentId {
        DeferSegmentId(partition_segment_id(
            self.topic,
            self.partition,
            self.group.as_str(),
        ))
    }

    /// The id of this segment's timer rows.
    #[must_use]
    pub fn timer_id(&self) -> TimerSegmentId {
        let name = timer_segment_name(self.group.as_str(), self.topic, self.partition);
        TimerSegmentId(timer_segment_id(&name))
    }
}

impl fmt::Display for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}/{}", self.group, self.topic, self.partition)
    }
}

/// Addresses the deferred segment registry, the deferred message rows, and the
/// deferred timer rows. See [`Segment`] for the invariant it upholds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DeferSegmentId(SegmentId);

impl DeferSegmentId {
    /// The id the defer tables are keyed by.
    #[must_use]
    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Addresses the timer segment row, the timer slab index, and the timer key
/// index. See [`Segment`] for the invariant it upholds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimerSegmentId(TimerStoreSegmentId);

impl TimerSegmentId {
    /// The id the timer tables are keyed by.
    #[must_use]
    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}
