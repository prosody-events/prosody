//! Event identity, admission decisions, and store outcomes.
//!
//! [`EventRef`] is the durable reference to the upstream event that owns a
//! provisional cell. [`CommitDecision`] supplies the admission decision
//! for a staged event; [`StoreOutcome`] reports whether a mid-handler
//! `commit()`/`rollback()` call took effect.

use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use uuid::Uuid;

/// Durable reference to the upstream event that owns a provisional cell.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum EventRef {
    /// Kafka message event identified by its deduplication marker.
    Message {
        /// Identifier of the message deduplication row.
        dedup_id: Uuid,
    },

    /// Timer event identified by its durable timer row coordinates.
    Timer(TimerEventRef),
}

impl EventRef {
    /// Wire discriminator for the message variant in the Cassandra
    /// `event_ref` UDT `kind` column.
    pub(in crate::state) const MESSAGE_KIND: i8 = 0;
    /// Wire discriminator for the timer variant in the Cassandra
    /// `event_ref` UDT `kind` column.
    pub(in crate::state) const TIMER_KIND: i8 = 1;
}

/// Durable timer identity stored in a provisional cell.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct TimerEventRef {
    /// Timer namespace.
    pub timer_type: TimerType,

    /// Scheduled fire time.
    pub time: CompactDateTime,

    /// Timer row tag observed when the cell was staged.
    pub tag: i32,
}

impl TimerEventRef {
    /// Creates a durable timer event reference.
    #[must_use]
    pub fn new(timer_type: TimerType, time: CompactDateTime, tag: i32) -> Self {
        Self {
            timer_type,
            time,
            tag,
        }
    }
}

/// The explicit admission decision for one staged event.
/// Store operations apply this decision to the frozen stage.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CommitDecision {
    /// The event's provisional write committed: promote it to the committed
    /// value.
    Committed,

    /// The event did not commit: roll the cell back to its committed base.
    NotCommitted,
}

/// Did this call take effect.
///
/// Returned by the mid-handler transactional pair every collection handle
/// exposes, `commit()` and `rollback()` (their contract lives on the
/// [`collection`](super::collection) module). It is
/// [`StoreOutcome::Applied`] when buffered ops were drained: `commit()` writes
/// them to the committed value, `rollback()` discards them. It is
/// [`StoreOutcome::NoOp`] when nothing was buffered.
///
/// [`CommitDecision`] selects promotion or rollback during admission.
/// This value reports whether the call took effect.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum StoreOutcome {
    /// The call took effect.
    Applied,

    /// Nothing was buffered (idempotent no-op).
    NoOp,
}
