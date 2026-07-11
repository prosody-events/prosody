//! Event identity and store/oracle verdicts.
//!
//! [`EventRef`] is the durable reference to the upstream event that owns a
//! provisional cell. [`CommitDecision`] and [`StoreOutcome`] are the two
//! distinct verdicts threaded through recovery: the oracle decides, the
//! store acts and reports.

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
/// [`StoreOutcome`], which is the durable store's "did this call mutate
/// state" signal: the oracle decides, the store acts on the decision.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CommitDecision {
    /// The event's provisional write committed: promote it to the committed
    /// value.
    Committed,

    /// The event did not commit: roll the cell back to its committed base.
    NotCommitted,
}

/// Did this store call mutate authoritative state.
///
/// Returned by the mid-handler write-through path
/// ([`checkpoint`](super::session::CellSession::checkpoint)):
/// [`StoreOutcome::Applied`] when buffered ops were written to the committed
/// value, [`StoreOutcome::NoOp`] when nothing was buffered.
///
/// Distinct from [`CommitDecision`]: the oracle decides whether a provisional
/// cell should commit, the store reports whether it actually changed durable
/// state when called.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum StoreOutcome {
    /// The call mutated authoritative state.
    Applied,

    /// No durable state changed (idempotent no-op).
    NoOp,
}
