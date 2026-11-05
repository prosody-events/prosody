//! Trace + model checks for [`Timers`].
//!
//! The model is the set of stored rows that operations leave: absent, present
//! with a tag, or unknown after a row change with an unknown result. After
//! every operation, memory must cover each row that may exist in an owned slab,
//! hold nothing for a row that cannot exist, and keep the delay queue and the
//! entries in step. A stored row must also wait, run, or stay after a delivery
//! that ended without deleting it. A random property checks long traces, and
//! a search checks every state that a small universe can reach.

use super::entry::Entry;
use super::*;
use color_eyre::eyre::{Result, ensure};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::array::from_fn;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tracing::Span;

mod search;

/// Times of the trace. The slab of the last one is not owned until a load
/// reads it.
const TIMES: [u32; 3] = [100, 200, 300];
const UNOWNED: usize = TIMES.len() - 1;
const TYPES: [TimerType; 2] = [TimerType::Application, TimerType::DeferredMessage];

/// One coordinate of the trace, as indexes into [`TIMES`] and [`TYPES`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct At(usize, usize);

/// An operation of the trace.
#[derive(Clone, Copy, Debug)]
enum Op {
    Insert(At, Insert, i32, Outcome, During),
    Unschedule(At, Outcome, During),
    UnscheduleAll(usize, Outcome, During),
    /// Commits the delivery of the attempt with this tag.
    Complete(At, i32, Outcome),
    /// Aborts with the tag of a delivered attempt.
    Abort(At, i32),
    /// The delay queue releases the waiting attempt.
    Pop(At),
    /// Fires the attempt with this tag.
    Fire(At, i32),
    /// A slab load reads the stored row. In the unowned slab, it reads every
    /// row, and the slab is owned from then on.
    Load(At),
}

/// What the actor does while the row change of an operation runs.
#[derive(Clone, Copy, Debug)]
enum During {
    Nothing,
    /// A load reads the unowned slab.
    Load,
    /// The delay queue releases every waiting attempt.
    Pop,
}

/// What the store holds at one coordinate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Stored {
    #[default]
    Absent,
    Present(i32),
    /// A row change with an unknown result touched the row.
    Unknown,
}

/// A value for each coordinate.
type Grid<T> = [[T; TYPES.len()]; TIMES.len()];

/// Memory and the model of a trace.
#[derive(Default)]
struct Trace {
    timers: Timers,
    rows: Grid<Stored>,
    /// Whether a load read the unowned slab.
    loaded: bool,
    /// The rows that stay without an attempt because a delivery ended without
    /// deleting them.
    kept: Grid<bool>,
    /// The tag of the attempt whose delivery runs.
    delivering: Grid<Option<i32>>,
}

#[test]
fn prop_timers_cover_every_row() {
    fn property(ops: Vec<Op>) -> Result<()> {
        replay(ops)
    }
    QuickCheck::new().quickcheck(property as fn(Vec<Op>) -> Result<()>);
}

/// Applies `ops` to empty memory, and checks after each one.
fn replay(ops: impl IntoIterator<Item = Op>) -> Result<()> {
    let runtime = runtime()?;
    let _runtime = runtime.enter();
    let mut trace = Trace::default();
    for (step, op) in ops.into_iter().enumerate() {
        trace
            .step(op)
            .map_err(|error| error.wrap_err(format!("step {step}: {op:?}")))?;
    }
    Ok(())
}

/// A runtime for the delay queue, whose timer needs one.
fn runtime() -> Result<Runtime> {
    Ok(Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()?)
}

impl Trace {
    /// Applies `op` and checks the invariants.
    fn step(&mut self, op: Op) -> Result<()> {
        self.apply(op)?;
        self.check()
    }

    /// Applies `op`, and checks the rules of the operation itself: a removal
    /// that is not done keeps the waiting attempt, an abort keeps every
    /// attempt that it did not deliver, and a schedule of the waiting attempt
    /// moves it onto the newest trace.
    fn apply(&mut self, op: Op) -> Result<()> {
        let key = key();
        let mut ended = None;
        match op {
            Op::Insert(at, insert, tag, outcome, during) => {
                let (trigger, change, _) = self.timers.start_insert(trigger(at, tag), insert);
                self.during(during);
                let cleared = matches!(insert, Insert::Clear).then_some(at.1);
                for (time, row) in self.rows.iter_mut().enumerate() {
                    if let Some(ty) = cleared
                        && time != at.0
                    {
                        delete(&mut row[ty], outcome);
                    }
                }
                write(&mut self.rows[at.0][at.1], trigger.tag, outcome);
                self.end(change, outcome);
                if let Some(next) = self.entry(at).and_then(|entry| entry.next.as_ref())
                    && outcome != Outcome::Unchanged
                    && next.trigger.tag == trigger.tag
                {
                    let trace = next.trigger.trace.load_full();
                    ensure!(
                        Arc::ptr_eq(&trace, &trigger.trace.load_full()),
                        "the waiting attempt kept an old trace"
                    );
                }
            }
            Op::Unschedule(at @ At(time, ty), outcome, during) => {
                let before = self.waiting(at);
                if let Some(change) = self.timers.start_unschedule(key, time_of(time), TYPES[ty]) {
                    self.during(during);
                    delete(&mut self.rows[time][ty], outcome);
                    self.end(change, outcome);
                    self.kept_attempt(at, before, outcome)?;
                }
            }
            Op::UnscheduleAll(ty, outcome, during) => {
                let before: [_; TIMES.len()] = from_fn(|time| self.waiting(At(time, ty)));
                let (change, firing) = self.timers.start_unschedule_all(key, TYPES[ty]);
                self.during(during);
                for (time, row) in self.rows.iter_mut().enumerate() {
                    if !firing.contains(&time_of(time)) {
                        delete(&mut row[ty], outcome);
                    }
                }
                self.end(change, outcome);
                for (time, before) in before.into_iter().enumerate() {
                    self.kept_attempt(At(time, ty), before, outcome)?;
                }
            }
            // A delivery ends when its commit needs no delete or the delete is
            // done. A commit without its delivery changes nothing.
            Op::Complete(at @ At(time, ty), tag, outcome) => {
                let before = self.waiting(at);
                let delivered = self.delivering[time][ty] == Some(tag);
                let id = TriggerId::new(&key, time_of(time), TYPES[ty]);
                let change = self.timers.start_complete(id, tag);
                ensure!(
                    delivered || (change.is_none() && self.waiting(at) == before),
                    "a commit without its delivery changed memory"
                );
                if let Some(change) = change {
                    delete(&mut self.rows[time][ty], outcome);
                    self.end(change, outcome);
                    self.kept_attempt(at, before, outcome)?;
                    ended = (outcome != Outcome::Done).then_some(at);
                }
                if delivered && ended.is_none() {
                    self.delivering[time][ty] = None;
                }
            }
            Op::Abort(at @ At(time, ty), tag) => {
                let next = self.entry(at).and_then(|entry| entry.next.as_ref());
                let undelivered = next
                    .filter(|next| !next.delivered(tag))
                    .map(|next| next.trigger.tag);
                self.timers
                    .abort(&TriggerId::new(&key, time_of(time), TYPES[ty]), tag);
                ensure!(
                    undelivered.is_none() || self.waiting(at) == undelivered,
                    "an abort took an attempt that it did not deliver"
                );
                ended = Some(at);
                let delivering = &mut self.delivering[time][ty];
                *delivering = delivering.filter(|&delivered| delivered != tag);
            }
            Op::Pop(at) => self.pop(at),
            Op::Fire(at, tag) => self.fire(at, tag)?,
            Op::Load(at) => {
                if at.0 == UNOWNED {
                    self.load_slab();
                } else if let Stored::Present(tag) = self.rows[at.0][at.1] {
                    self.timers.load(trigger(at, tag));
                }
            }
        }
        for (time, ty) in coordinates() {
            let at = At(time, ty);
            let idle = self.entry(at).is_some_and(idle);
            self.kept[time][ty] = idle && (self.kept[time][ty] || ended.is_some_and(|e| e == at));
        }
        Ok(())
    }

    /// Checks the invariants of memory against the stored rows.
    fn check(&self) -> Result<()> {
        let mut queued = 0_usize;
        for (time, ty) in coordinates() {
            let (row, at) = (self.rows[time][ty], At(time, ty));
            let entry = self.entry(at);
            let owned = owned(self.loaded, time_of(time));
            let covered = row == Stored::Absent || !owned || entry.is_some();
            ensure!(covered, "an owned row that may exist has no entry");
            ensure!(
                entry
                    .and_then(|entry| entry.firing)
                    .map(|delivery| delivery.tag)
                    == self.delivering[time][ty],
                "memory runs another delivery than the model"
            );
            let Some(entry) = entry else {
                continue;
            };
            ensure!(
                row != Stored::Absent || entry.firing.is_some(),
                "an entry stands for no row and no delivery"
            );
            ensure!(
                row == Stored::Absent
                    || entry
                        .firing
                        .is_none_or(|delivery| delivery.row != Row::Deleted),
                "a delivery marks its row deleted, but the row may exist"
            );
            if let Stored::Present(tag) = row {
                ensure!(
                    !owned || !idle(entry) || self.kept[time][ty],
                    "a stored row neither waits nor runs, and no delivery kept it"
                );
                ensure!(
                    entry
                        .next
                        .as_ref()
                        .is_none_or(|next| next.trigger.tag == tag),
                    "the waiting attempt is not the stored one"
                );
            }
            queued += usize::from(
                entry
                    .next
                    .as_ref()
                    .is_some_and(|next| next.queued.is_some()),
            );
        }
        ensure!(
            queued == self.timers.queue.len(),
            "the queue and the entries disagree"
        );
        Ok(())
    }

    /// Runs what the actor does while a row change runs.
    fn during(&mut self, during: During) {
        match during {
            During::Nothing => {}
            During::Load => self.load_slab(),
            During::Pop => coordinates().for_each(|(time, ty)| self.pop(At(time, ty))),
        }
    }

    /// Fires the attempt with `tag` at `at`, which starts a delivery only if
    /// no other delivery of the timer runs.
    fn fire(&mut self, at: At, tag: i32) -> Result<()> {
        let fired = self.timers.fire(&trigger(at, tag).id(), tag);
        let delivering = &mut self.delivering[at.0][at.1];
        ensure!(
            !fired || delivering.is_none(),
            "a second delivery of a timer started"
        );
        if fired {
            *delivering = Some(tag);
        }
        Ok(())
    }

    /// The delay queue releases the attempt that waits at `at`.
    fn pop(&mut self, At(time, ty): At) {
        let Timers { entries, queue } = &mut self.timers;
        if let Some(next) =
            get_mut(entries, &key(), time_of(time), TYPES[ty]).and_then(|entry| entry.next.as_mut())
            && let Some(item) = next.queued.take()
        {
            queue.try_remove(&item);
        }
    }

    /// A load reads every row of the unowned slab, which is owned from then
    /// on. The load finds no row where a row change had an unknown result.
    fn load_slab(&mut self) {
        if self.loaded {
            return;
        }
        self.loaded = true;
        for (ty, row) in self.rows[UNOWNED].iter_mut().enumerate() {
            match *row {
                Stored::Present(tag) => self.timers.load(trigger(At(UNOWNED, ty), tag)),
                Stored::Unknown => *row = Stored::Absent,
                Stored::Absent => {}
            }
        }
    }

    /// Ends an operation in memory.
    fn end(&mut self, change: Change, outcome: Outcome) {
        let loaded = self.loaded;
        self.timers.end(change, outcome, |time| owned(loaded, time));
    }

    /// Checks that a removal that is not done leaves the attempt that waited.
    fn kept_attempt(&self, at: At, before: Option<i32>, outcome: Outcome) -> Result<()> {
        ensure!(
            outcome == Outcome::Done || before.is_none() || self.waiting(at) == before,
            "a removal that is not done lost the waiting attempt"
        );
        Ok(())
    }

    /// The tag of the attempt that waits at `at`.
    fn waiting(&self, at: At) -> Option<i32> {
        let next = self.entry(at)?.next.as_ref();
        next.map(|next| next.trigger.tag)
    }

    fn entry(&self, At(time, ty): At) -> Option<&Entry> {
        let coordinate = (time_of(time), TYPES[ty]);
        self.timers
            .entries
            .get(&key())?
            .iter()
            .find(|&&(t, ty, _)| (t, ty) == coordinate)
            .map(|(_, _, entry)| entry)
    }
}

/// Whether an entry holds no attempt and no delivery.
fn idle(entry: &Entry) -> bool {
    entry.next.is_none() && entry.firing.is_none()
}

/// Every coordinate, in time order.
fn coordinates() -> impl Iterator<Item = (usize, usize)> {
    (0..TIMES.len()).flat_map(|time| (0..TYPES.len()).map(move |ty| (time, ty)))
}

/// Records a row write with `outcome`.
fn write(row: &mut Stored, tag: i32, outcome: Outcome) {
    *row = match (outcome, *row) {
        (Outcome::Unchanged, row) => row,
        (Outcome::Done, _) => Stored::Present(tag),
        (Outcome::Unknown, Stored::Present(stored)) if stored == tag => Stored::Present(tag),
        (Outcome::Unknown, _) => Stored::Unknown,
    };
}

/// Records a row delete with `outcome`.
fn delete(row: &mut Stored, outcome: Outcome) {
    *row = match (outcome, *row) {
        (Outcome::Unchanged, row) | (Outcome::Unknown, row @ Stored::Absent) => row,
        (Outcome::Done, _) => Stored::Absent,
        (Outcome::Unknown, _) => Stored::Unknown,
    };
}

fn owned(loaded: bool, time: CompactDateTime) -> bool {
    loaded || time != time_of(UNOWNED)
}

fn key() -> Key {
    Arc::from("key")
}

fn time_of(index: usize) -> CompactDateTime {
    CompactDateTime::from(TIMES[index])
}

fn trigger(At(time, ty): At, tag: i32) -> Trigger {
    Trigger::with_tag(key(), time_of(time), TYPES[ty], tag, Span::none())
}

impl Arbitrary for At {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(
            usize::from(u8::arbitrary(g)) % TIMES.len(),
            usize::from(u8::arbitrary(g)) % TYPES.len(),
        )
    }
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        let at = At::arbitrary(g);
        // A small tag pool makes stale and current tags collide.
        let tag = i32::from(u8::arbitrary(g) % 3);
        let outcome = *g
            .choose(&[
                Outcome::Unchanged,
                Outcome::Unknown,
                Outcome::Done,
                Outcome::Done,
            ])
            .unwrap_or(&Outcome::Done);
        let insert = *g
            .choose(&[Insert::Schedule, Insert::Clear, Insert::Repair])
            .unwrap_or(&Insert::Schedule);
        let during = *g
            .choose(&[During::Nothing, During::Nothing, During::Load, During::Pop])
            .unwrap_or(&During::Nothing);
        match u8::arbitrary(g) % 10 {
            0..=2 => Op::Insert(at, insert, tag, outcome, during),
            3 => Op::Unschedule(at, outcome, during),
            4 => Op::UnscheduleAll(at.1, outcome, during),
            5 => Op::Complete(at, tag, outcome),
            6 => Op::Abort(at, tag),
            7 => Op::Pop(at),
            8 => Op::Fire(at, tag),
            _ => Op::Load(at),
        }
    }
}
