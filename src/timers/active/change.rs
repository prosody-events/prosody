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
    pub(super) rows: Rows,
    /// The attempts that the start took, to put back if the rows stay.
    pub(super) restore: SmallVec<[Trigger; 1]>,
}

/// The rows of the key and type that an operation changes.
#[derive(Debug)]
pub(super) enum Rows {
    /// Writes the row of `trigger`. A clear also deletes every other row.
    Write {
        trigger: Trigger,
        insert: Insert,
        /// Whether the start made the entry.
        created: bool,
    },
    /// Deletes the row at this time.
    Remove(CompactDateTime),
    /// Deletes every row except the rows at these times. A slab load during
    /// the change can add entries for the deleted rows, so the end of a done
    /// change drops every entry at another time.
    RemoveAll(SmallVec<[CompactDateTime; 1]>),
}

impl Change {
    /// The key and timer type of the operation. The actor runs one operation
    /// at a time on them.
    pub(crate) fn lane(&self) -> (Key, TimerType) {
        (self.key.clone(), self.timer_type)
    }
}
