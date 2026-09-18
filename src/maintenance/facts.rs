//! Facts: what the stores said about a segment and its deferred keys at one
//! moment.

use super::identity::Segment;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentVersion;
use crate::{Key, Offset};

/// What the timer segment row said about one segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentFacts {
    /// The segment these facts describe.
    pub segment: Segment,
    /// The slab size the live scheduler runs with.
    pub slab_size: CompactDuration,
    /// The durable layout the segment row reported.
    pub version: SegmentVersion,
}

impl SegmentFacts {
    /// Whether the layout stores a timer type.
    ///
    /// A V1 layout cannot hold a typed retry timer.
    #[must_use]
    pub fn typed_layout(&self) -> bool {
        self.version != SegmentVersion::V1
    }
}

/// One deferred key and its retry timer, read together.
///
/// **Invariant:** a `DeferredKey` exists only for a queue that holds at least
/// one row. [`RetryTimer::Absent`] is the stranded case, so no field says
/// "stranded".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeferredKey {
    /// The segment the key belongs to.
    pub segment: Segment,
    /// The key.
    pub key: Key,
    /// The head of the key's deferred queue.
    pub queue: DeferredQueue,
    /// The retry timer of the queue's type in the key index.
    pub retry_timer: RetryTimer,
}

/// The head of a deferred queue that holds at least one row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeferredQueue {
    /// A queue of deferred message offsets.
    Messages {
        /// Attempts already made for the key.
        retry_count: u32,
        /// The offset the next attempt reloads.
        next_offset: Offset,
    },
    /// A queue of deferred timers.
    Timers {
        /// Attempts already made for the key.
        retry_count: u32,
        /// The original fire time the next attempt reloads.
        next: CompactDateTime,
    },
}

impl DeferredQueue {
    /// The retry timer type that must pair with this queue. This method is the
    /// one place that pairs a queue with its timer type.
    #[must_use]
    pub fn timer_type(&self) -> TimerType {
        match self {
            Self::Messages { .. } => TimerType::DeferredMessage,
            Self::Timers { .. } => TimerType::DeferredTimer,
        }
    }
}

/// The retry timer of a queue's type in the timer key index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetryTimer {
    /// No timer of that type. The queue is stranded.
    Absent,
    /// The earliest timer of that type.
    Scheduled {
        /// When the timer fires.
        time: CompactDateTime,
        /// The attempt tag the timer carries.
        tag: i32,
    },
}
