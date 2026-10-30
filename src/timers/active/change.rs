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
    /// The rows that the operation deletes.
    pub(super) deleted: Deleted,
    /// The attempts that the start took, to put back if the rows stay.
    pub(super) restore: SmallVec<[Trigger; 1]>,
}

#[derive(Debug)]
pub(super) struct Inserted {
    pub(super) trigger: Trigger,
    /// Whether the start made the entry.
    pub(super) created: bool,
}

/// The rows of the key and type that an operation deletes.
#[derive(Debug)]
pub(super) enum Deleted {
    Nothing,
    At(CompactDateTime),
    /// Every row except the rows at these times. A slab load during the
    /// change can add entries for the deleted rows, so the end of a done
    /// change drops every entry at another time.
    AllBut(SmallVec<[CompactDateTime; 1]>),
}

impl Change {
    /// The key and timer type of the operation. The actor runs one operation
    /// at a time on them.
    pub(crate) fn lane(&self) -> (Key, TimerType) {
        (self.key.clone(), self.timer_type)
    }
}
