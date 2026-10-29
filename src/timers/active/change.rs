//! What an operation changes in timer memory around its row change, and how
//! the row change ended.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use smallvec::SmallVec;

/// How an insert changes memory.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Insert {
    /// Queues the timer. A waiting attempt keeps its tag.
    Schedule,
    /// As `Schedule`, and removes the other timers of the key and type.
    Clear,
    /// Queues the attempt of the key row with the tag of that row.
    Repair,
}

/// How the row change of an operation ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The row change never started.
    Unchanged,
    /// The row change failed or stopped, so the row may or may not have
    /// changed.
    Unknown,
    /// The row change succeeded.
    Done,
}

/// What an operation changes in memory around its row change. Only
/// [`Timers`](super::Timers) makes one, and [`Timers::end`](super::Timers::end)
/// uses it.
#[derive(Debug)]
pub(crate) struct Change {
    pub(super) key: Key,
    pub(super) timer_type: TimerType,
    /// The attempt that the operation writes.
    pub(super) insert: Option<Inserted>,
    /// The timers whose rows the operation deletes.
    pub(super) removed: SmallVec<[Removed; 1]>,
    /// For a change that deletes every row of the key and type, the times
    /// whose rows stay. A slab load during the change can add entries that
    /// `removed` does not name, and the end of a done change drops them too.
    pub(super) all_but: Option<SmallVec<[CompactDateTime; 1]>>,
}

#[derive(Debug)]
pub(super) struct Inserted {
    pub(super) trigger: Trigger,
    /// Whether the start made the entry.
    pub(super) created: bool,
}

#[derive(Debug)]
pub(super) struct Removed {
    pub(super) time: CompactDateTime,
    /// The attempt that the start took, to restore if the row stays.
    pub(super) restore: Option<Trigger>,
}

impl Change {
    /// A change that deletes the rows of `removed`.
    pub(super) fn removal(
        key: Key,
        timer_type: TimerType,
        removed: SmallVec<[Removed; 1]>,
    ) -> Self {
        Self {
            key,
            timer_type,
            insert: None,
            removed,
            all_but: None,
        }
    }
}
