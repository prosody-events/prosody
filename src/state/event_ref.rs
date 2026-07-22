//! Event identity and store/oracle verdicts.
//!
//! [`EventRef`] is the durable reference to the upstream event that owns a
//! provisional cell. [`CommitDecision`] is the oracle's recovery-time verdict
//! on a provisional cell; [`StoreOutcome`] reports whether a mid-handler
//! `commit()`/`rollback()` call took effect.

use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use uuid::Uuid;

/// Durable reference to the upstream event that owns a provisional cell.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum EventRef {
    /// Kafka message event identified by its deduplication marker.
    Message {
        /// Deduplication row identifier written at the event commit point.
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

/// Oracle verdict on a provisional cell's event.
///
/// Returned by the commit oracle when it resolves a provisional cell's
/// [`EventRef`] against the upstream commit source (deduplication store for
/// messages, timer-row tag for timers — see
/// [`CommitOracle`](crate::state::oracle::CommitOracle)). Distinct from
/// [`StoreOutcome`], which reports whether a call took effect: the oracle
/// decides, the store — or the buffer drain — acts.
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
/// Returned by the mid-handler transactional pair
/// ([`commit`](super::session::CellWrite::commit) /
/// [`rollback`](super::session::CellWrite::rollback)):
/// [`StoreOutcome::Applied`] when buffered ops were drained — written to the
/// committed value by `commit()`, discarded by `rollback()` —
/// [`StoreOutcome::NoOp`] when nothing was buffered.
///
/// Distinct from [`CommitDecision`]: the oracle decides whether a provisional
/// cell should commit, this reports whether the call actually took effect.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum StoreOutcome {
    /// The call took effect.
    Applied,

    /// Nothing was buffered (idempotent no-op).
    NoOp,
}
