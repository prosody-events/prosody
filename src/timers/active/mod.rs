//! The timers that the scheduler holds in memory.
//!
//! [`Timers`] belongs to the scheduler actor alone. It keeps an entry for each
//! coordinate (key, time and timer type) that a load read or an operation
//! changes, and the delay queue of the attempts that wait to fire.
//!
//! # Operations
//!
//! An operation that changes a row starts in the actor, changes the row
//! outside it, and then ends in the actor. Its [`Change`] carries what the end
//! needs. Commands reach the actor in order, so the end of an operation comes
//! before the start of the next operation on the same key.
//!
//! # Durable row invariant
//!
//! Every row that may exist in an owned slab has an entry, so cleanup keeps the
//! slab and a restart loads the row. An operation keeps an entry for its
//! coordinate from its start to its end. When the result of a row change is
//! unknown, the end keeps memory for the row as if it exists.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use smallvec::SmallVec;
use std::collections::BTreeSet;
use std::collections::hash_map::Entry as MapEntry;
use std::future::poll_fn;
use std::task::{Context, Poll, ready};
use tokio_util::time::DelayQueue;

mod entry;

use entry::{Coordinates, Delivery, Entry, Next, get_mut};

/// The in-memory timers of one partition.
#[derive(Debug, Default)]
pub(crate) struct Timers {
    entries: HashMap<Key, Coordinates>,
    queue: DelayQueue<Trigger>,
}

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

/// What an operation changes in memory around its row change. Only [`Timers`]
/// makes one, and [`Timers::end`] uses it.
#[derive(Debug)]
pub(crate) struct Change {
    key: Key,
    timer_type: TimerType,
    /// The attempt that the operation writes.
    insert: Option<Inserted>,
    /// The timers whose rows the operation deletes.
    removed: SmallVec<[Removed; 1]>,
}

#[derive(Debug)]
struct Inserted {
    trigger: Trigger,
    /// Whether the start made the entry.
    created: bool,
}

#[derive(Debug)]
struct Removed {
    time: CompactDateTime,
    /// The attempt that the start took, to restore if the row stays.
    restore: Option<Trigger>,
}

/// How memory lists the time of a timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Listed {
    /// An attempt waits to fire, and no delivery runs.
    Waits,
    /// A delivery runs. `again` tells whether an attempt also waits.
    Firing { again: bool },
}

/// Point-in-time counts of active timers for metrics reporting.
#[derive(Clone, Copy, Debug, Default)]
pub struct TimerSnapshot {
    /// Timers in any state in the loaded scheduling window.
    pub active: u32,
    /// Timers whose delivery runs.
    pub in_flight: u32,
    /// Timers whose `fire_time <= now` (any state).
    pub overdue: u32,
    /// Age in seconds of the oldest overdue timer; 0 when none.
    pub oldest_overdue_secs: u32,
}

impl Timers {
    /// Starts an insert of `trigger`, and returns the attempt to write. The
    /// entry stays from now to the end, so cleanup keeps the slab.
    pub(crate) fn start_insert(
        &mut self,
        mut trigger: Trigger,
        insert: Insert,
    ) -> (Trigger, Change) {
        let removed = match insert {
            Insert::Clear => self.take_all(&trigger.key, trigger.timer_type, Some(trigger.time)),
            Insert::Schedule | Insert::Repair => SmallVec::new(),
        };
        let coordinates = self.entries.entry(trigger.key.clone()).or_default();
        let (created, entry) = match coordinates.entry((trigger.time, trigger.timer_type)) {
            MapEntry::Occupied(entry) => (false, entry.into_mut()),
            MapEntry::Vacant(entry) => (true, entry.insert(Entry::default())),
        };
        if let Some(next) = &entry.next
            && !matches!(insert, Insert::Repair)
        {
            trigger.tag = next.trigger.tag;
        }
        let change = Change {
            key: trigger.key.clone(),
            timer_type: trigger.timer_type,
            insert: Some(Inserted {
                trigger: trigger.clone(),
                created,
            }),
            removed,
        };
        (trigger, change)
    }

    /// Starts the removal of the timer at `time`, and returns the change that
    /// deletes its row. A delivery that runs keeps its row: only its waiting
    /// attempt goes, and no change returns.
    pub(crate) fn start_unschedule(
        &mut self,
        key: Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<Change> {
        let Self { entries, queue } = self;
        if let Some(entry) = get_mut(entries, &key, time, timer_type)
            && entry.firing.is_some()
        {
            if let Some(next) = entry.next.take() {
                next.dequeue(queue);
            }
            return None;
        }
        let restore = self.take(&key, time, timer_type);
        Some(Change {
            key,
            timer_type,
            insert: None,
            removed: SmallVec::from_buf([Removed { time, restore }]),
        })
    }

    /// Starts the removal of every timer of a key and type. Returns the change
    /// that deletes their rows, and the times whose delivery runs: those keep
    /// their rows, and only their waiting attempts go.
    pub(crate) fn start_unschedule_all(
        &mut self,
        key: Key,
        timer_type: TimerType,
    ) -> (Change, SmallVec<[CompactDateTime; 1]>) {
        let Self { entries, queue } = self;
        let mut removed = SmallVec::new();
        let mut firing = SmallVec::new();
        for (&(time, _), entry) in entries
            .get_mut(&key)
            .into_iter()
            .flatten()
            .filter(|&(&(_, ty), _)| ty == timer_type)
        {
            let restore = entry.next.take().map(|next| next.dequeue(queue));
            if entry.firing.is_some() {
                firing.push(time);
            } else {
                removed.push(Removed { time, restore });
            }
        }
        let change = Change {
            key,
            timer_type,
            insert: None,
            removed,
        };
        (change, firing)
    }

    /// Ends the delivery of a coordinate. Returns the change that deletes its
    /// row, or `None` when no row needs a delete: the timer was scheduled
    /// again during the delivery, or a clear deleted the row.
    pub(crate) fn start_complete(
        &mut self,
        key: Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<Change> {
        if let Some(entry) = self.get_mut(&key, time, timer_type)
            && let Some(delivery) = entry.firing.take()
        {
            if entry.next.is_some() {
                return None;
            }
            if delivery == Delivery::Deleted {
                self.forget(&key, time, timer_type);
                return None;
            }
        }
        let restore = self.take(&key, time, timer_type);
        Some(Change {
            key,
            timer_type,
            insert: None,
            removed: SmallVec::from_buf([Removed { time, restore }]),
        })
    }

    /// Aborts a delivery, or a delivered attempt that never fired. The row
    /// stays, and a restart loads it. A delivery whose row a clear deleted
    /// leaves nothing.
    pub(crate) fn abort(&mut self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        let Self { entries, queue } = self;
        let Some(entry) = get_mut(entries, key, time, timer_type) else {
            return;
        };
        match entry.firing.take() {
            Some(Delivery::Stored) => {}
            Some(Delivery::Deleted) => self.forget(key, time, timer_type),
            None => {
                if let Some(next) = entry.next.take() {
                    next.dequeue(queue);
                }
            }
        }
    }

    /// Ends an operation. `owned` tells whether the actor owns the slab that
    /// holds a time.
    pub(crate) fn end(
        &mut self,
        change: Change,
        outcome: Outcome,
        owned: impl Fn(CompactDateTime) -> bool,
    ) {
        let Change {
            key,
            timer_type,
            insert,
            removed,
        } = change;
        for Removed { time, restore } in removed {
            if outcome == Outcome::Done {
                self.drop_row(&key, time, timer_type);
            } else if let Some(trigger) = restore {
                self.restore(trigger);
            }
        }
        if let Some(Inserted { trigger, created }) = insert {
            if outcome == Outcome::Unchanged {
                if created {
                    self.forget(&key, trigger.time, timer_type);
                }
            } else {
                let owned = owned(trigger.time);
                self.queue(trigger, owned);
            }
        }
    }

    /// Starts the delivery of an attempt that left the queue, if it is still
    /// the attempt that fires next.
    pub(crate) fn fire(&mut self, trigger: &Trigger) -> bool {
        let Some(entry) = self.get_mut(&trigger.key, trigger.time, trigger.timer_type) else {
            return false;
        };
        let due = entry
            .next
            .as_ref()
            .is_some_and(|next| next.queued.is_none() && next.trigger.tag == trigger.tag);
        if due {
            entry.next = None;
            entry.firing = Some(Delivery::Stored);
        }
        due
    }

    /// Adds a trigger that a slab load read. An existing entry stays as it
    /// is, because the operations that made it know more than the load.
    pub(crate) fn load(&mut self, trigger: Trigger) {
        let Self { entries, queue } = self;
        let coordinates = entries.entry(trigger.key.clone()).or_default();
        if let MapEntry::Vacant(entry) = coordinates.entry((trigger.time, trigger.timer_type)) {
            entry.insert(Entry {
                next: Some(Next::queue(queue, trigger)),
                firing: None,
            });
        }
    }

    /// Waits for the next attempt whose time came, and takes it from the
    /// queue. Returns `None` when the queue is empty.
    pub(crate) async fn next(&mut self) -> Option<Trigger> {
        poll_fn(|cx| self.poll_expired(cx)).await
    }

    fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<Option<Trigger>> {
        let Some(expired) = ready!(self.queue.poll_expired(cx)) else {
            return Poll::Ready(None);
        };
        let trigger = expired.into_inner();
        if let Some(next) = self
            .get_mut(&trigger.key, trigger.time, trigger.timer_type)
            .and_then(|entry| entry.next.as_mut())
        {
            next.queued = None;
        }
        Poll::Ready(Some(trigger))
    }

    /// Lists the times of `key` that memory decides: the ones with a waiting
    /// attempt, and the ones that fire with no attempt waiting.
    pub(crate) fn list(
        &self,
        key: &Key,
        timer_type: TimerType,
    ) -> SmallVec<[(CompactDateTime, Listed); 2]> {
        self.entries
            .get(key)
            .into_iter()
            .flatten()
            .filter(|&(&(_, ty), _)| ty == timer_type)
            .filter_map(|(&(time, _), entry)| {
                match (entry.next.is_some(), entry.firing.is_some()) {
                    (true, false) => Some((time, Listed::Waits)),
                    (again, true) => Some((time, Listed::Firing { again })),
                    (false, false) => None,
                }
            })
            .collect()
    }

    /// The slabs that hold an entry. Cleanup keeps them.
    pub(crate) fn slabs(&self, slab_size: CompactDuration) -> BTreeSet<SlabId> {
        self.entries
            .values()
            .flat_map(HashMap::keys)
            .map(|&(time, _)| Slab::from_time(slab_size, time).id())
            .collect()
    }

    /// Computes a point-in-time [`TimerSnapshot`]. Saturating arithmetic
    /// prevents wrapping under pathological loads.
    pub(crate) fn snapshot(&self, now: CompactDateTime) -> TimerSnapshot {
        let mut s = TimerSnapshot::default();
        for (&(time, _), entry) in self.entries.values().flatten() {
            s.active = s.active.saturating_add(1);
            if entry.firing.is_some() {
                s.in_flight = s.in_flight.saturating_add(1);
            }
            if time <= now {
                s.overdue = s.overdue.saturating_add(1);
                let age = now.epoch_seconds().saturating_sub(time.epoch_seconds());
                s.oldest_overdue_secs = s.oldest_overdue_secs.max(age);
            }
        }
        s
    }

    /// Queues the attempt that an insert wrote. In an unowned slab, an entry
    /// that holds nothing else goes instead, and a load reads the row later.
    fn queue(&mut self, trigger: Trigger, owned: bool) {
        let Self { entries, queue } = self;
        let coordinates = entries.entry(trigger.key.clone()).or_default();
        let entry = coordinates
            .entry((trigger.time, trigger.timer_type))
            .or_default();
        if let Some(delivery) = &mut entry.firing {
            *delivery = Delivery::Stored;
        }
        match &entry.next {
            None if !owned && entry.firing.is_none() => {
                self.forget(&trigger.key, trigger.time, trigger.timer_type);
            }
            Some(next) if next.trigger.tag == trigger.tag => {
                next.trigger.adopt_trace_from(&trigger);
            }
            _ => {
                if let Some(old) = entry.next.replace(Next::queue(queue, trigger)) {
                    old.dequeue(queue);
                }
            }
        }
    }

    /// Puts back an attempt that a removal took, unless another one waits.
    fn restore(&mut self, trigger: Trigger) {
        let Self { entries, queue } = self;
        let coordinates = entries.entry(trigger.key.clone()).or_default();
        let entry = coordinates
            .entry((trigger.time, trigger.timer_type))
            .or_default();
        if entry.next.is_none() {
            entry.next = Some(Next::queue(queue, trigger));
        }
    }

    /// Forgets a deleted row: no attempt waits, and the entry goes unless a
    /// delivery runs.
    fn drop_row(&mut self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        let Self { entries, queue } = self;
        if let Some(entry) = get_mut(entries, key, time, timer_type) {
            if let Some(next) = entry.next.take() {
                next.dequeue(queue);
            }
            if let Some(delivery) = &mut entry.firing {
                *delivery = Delivery::Deleted;
            }
        }
        self.forget(key, time, timer_type);
    }

    /// Removes an entry that holds no attempt and no delivery.
    fn forget(&mut self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        let Some(coordinates) = self.entries.get_mut(key) else {
            return;
        };
        if let MapEntry::Occupied(entry) = coordinates.entry((time, timer_type))
            && entry.get().next.is_none()
            && entry.get().firing.is_none()
        {
            entry.remove();
            if coordinates.is_empty() {
                self.entries.remove(key);
            }
        }
    }

    /// Takes the waiting attempt of a coordinate out of the queue.
    fn take(&mut self, key: &Key, time: CompactDateTime, timer_type: TimerType) -> Option<Trigger> {
        let Self { entries, queue } = self;
        let next = get_mut(entries, key, time, timer_type)?.next.take()?;
        Some(next.dequeue(queue))
    }

    /// Takes the waiting attempts of a key and type, except at `except`.
    fn take_all(
        &mut self,
        key: &Key,
        timer_type: TimerType,
        except: Option<CompactDateTime>,
    ) -> SmallVec<[Removed; 1]> {
        let Self { entries, queue } = self;
        entries
            .get_mut(key)
            .into_iter()
            .flatten()
            .filter(|&(&(time, ty), _)| ty == timer_type && Some(time) != except)
            .map(|(&(time, _), entry)| Removed {
                time,
                restore: entry.next.take().map(|next| next.dequeue(queue)),
            })
            .collect()
    }

    fn get_mut(
        &mut self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<&mut Entry> {
        get_mut(&mut self.entries, key, time, timer_type)
    }
}

#[cfg(test)]
mod tests;
