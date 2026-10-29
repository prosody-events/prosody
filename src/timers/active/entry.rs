//! The entry that memory holds for one coordinate, and its place in the delay
//! queue.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use smallvec::SmallVec;
use std::time::Duration;
use tokio_util::time::{DelayQueue, delay_queue};

/// The entries of one key, by time and timer type. A key almost always has
/// one entry, so it stays inline.
pub(super) type Coordinates = SmallVec<[(CompactDateTime, TimerType, Entry); 1]>;

/// What memory holds for one coordinate.
///
/// An entry without an attempt and without a delivery fires nothing. It stands
/// for a row that may exist: an aborted delivery, or a row change in progress.
#[derive(Debug, Default)]
pub(super) struct Entry {
    /// The attempt that fires next.
    pub(super) next: Option<Next>,
    /// The delivery of this coordinate that a handler runs.
    pub(super) firing: Option<Delivery>,
}

/// The row of a coordinate while its delivery runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Delivery {
    /// The row may exist.
    Stored,
    /// A clear deleted the row, and no write followed. The delivery has no
    /// row to keep or to delete.
    Deleted,
}

/// An attempt that fires next.
#[derive(Debug)]
pub(super) struct Next {
    /// The attempt. It shares its trace with its queue item.
    pub(super) trigger: Trigger,
    /// The queue item, or `None` after the attempt left the queue. A `Some`
    /// key is always in the queue, because only [`Timers`](super::Timers)
    /// removes items.
    pub(super) queued: Option<delay_queue::Key>,
}

impl Next {
    /// Puts `trigger` in the queue until its time.
    pub(super) fn queue(queue: &mut DelayQueue<Trigger>, trigger: Trigger) -> Self {
        let delay = trigger.time.duration_from_now().unwrap_or(Duration::ZERO);
        let queued = Some(queue.insert(trigger.clone(), delay));
        Self { trigger, queued }
    }

    /// Takes the attempt out of the queue.
    pub(super) fn dequeue(self, queue: &mut DelayQueue<Trigger>) -> Trigger {
        if let Some(key) = self.queued {
            queue.try_remove(&key);
        }
        self.trigger
    }
}

pub(super) fn get_mut<'a>(
    entries: &'a mut HashMap<Key, Coordinates>,
    key: &Key,
    time: CompactDateTime,
    timer_type: TimerType,
) -> Option<&'a mut Entry> {
    entries
        .get_mut(key)?
        .iter_mut()
        .find(|(t, ty, _)| (*t, *ty) == (time, timer_type))
        .map(|(_, _, entry)| entry)
}

/// Returns the entry of a coordinate, and whether this call made it.
pub(super) fn get_or_insert<'a>(
    entries: &'a mut HashMap<Key, Coordinates>,
    key: &Key,
    time: CompactDateTime,
    timer_type: TimerType,
) -> (bool, &'a mut Entry) {
    let coordinates = entries.entry(key.clone()).or_default();
    let found = coordinates
        .iter()
        .position(|&(t, ty, _)| (t, ty) == (time, timer_type));
    let index = found.unwrap_or_else(|| {
        coordinates.push((time, timer_type, Entry::default()));
        coordinates.len() - 1
    });
    (found.is_none(), &mut coordinates[index].2)
}
