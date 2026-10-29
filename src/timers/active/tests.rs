//! Trace + model property for [`Timers`].
//!
//! The model is the set of stored rows that operations leave: absent, present
//! with a tag, or unknown after a row change with an unknown result. After
//! every operation, memory must cover each row that may exist in an owned slab,
//! hold nothing for a row that cannot exist, and keep the delay queue and the
//! entries in step.

use super::*;
use color_eyre::eyre::{Result, ensure};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::array::from_fn;
use std::sync::Arc;
use tokio::runtime::Builder;
use tracing::Span;

/// Times of the trace. The slab of the last one is not owned.
const TIMES: [u32; 3] = [100, 200, 300];
const TYPES: [TimerType; 2] = [TimerType::Application, TimerType::DeferredMessage];

/// One coordinate of the trace, as indexes into [`TIMES`] and [`TYPES`].
#[derive(Clone, Copy, Debug)]
struct At(usize, usize);

#[derive(Clone, Copy, Debug)]
enum Op {
    Insert(At, Insert, i32, Outcome),
    Unschedule(At, Outcome),
    UnscheduleAll(usize, Outcome),
    Complete(At, Outcome),
    Abort(At),
    /// The delay queue releases the waiting attempt.
    Pop(At),
    /// Fires the attempt with this tag.
    Fire(At, i32),
    /// A slab load reads the stored row.
    Load(At),
}

/// What the store holds at one coordinate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Stored {
    #[default]
    Absent,
    Present(i32),
    /// A row change with an unknown result touched the row.
    Unknown,
}

/// The rows of the trace, by coordinate.
type Rows = [[Stored; TYPES.len()]; TIMES.len()];

#[test]
fn prop_timers_cover_every_row() {
    fn property(ops: Vec<Op>) -> Result<()> {
        // The delay queue needs the timer of a runtime.
        let runtime = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()?;
        let _runtime = runtime.enter();
        run(ops)
    }
    QuickCheck::new().quickcheck(property as fn(Vec<Op>) -> Result<()>);
}

fn run(ops: Vec<Op>) -> Result<()> {
    let mut timers = Timers::default();
    let mut rows = Rows::default();
    for (step, op) in ops.into_iter().enumerate() {
        apply(&mut timers, &mut rows, op)
            .and_then(|()| check(&timers, &rows))
            .map_err(|error| error.wrap_err(format!("step {step}: {op:?}")))?;
    }
    Ok(())
}

/// Applies `op`, and checks the rules of the operation itself: a removal that
/// is not done keeps the waiting attempt, and a schedule of the waiting
/// attempt moves it onto the newest trace.
fn apply(timers: &mut Timers, rows: &mut Rows, op: Op) -> Result<()> {
    let key = key();
    match op {
        Op::Insert(at, insert, tag, outcome) => {
            let (trigger, change) = timers.start_insert(trigger(at, tag), insert);
            let cleared = matches!(insert, Insert::Clear).then_some(at.1);
            for (time, row) in rows.iter_mut().enumerate() {
                if let Some(ty) = cleared
                    && time != at.0
                {
                    delete(&mut row[ty], outcome);
                }
            }
            write(&mut rows[at.0][at.1], trigger.tag, outcome);
            timers.end(change, outcome, owned);
            if let Some(next) = entry(timers, at).and_then(|entry| entry.next.as_ref())
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
        Op::Unschedule(at @ At(time, ty), outcome) => {
            let before = waiting(timers, at);
            if let Some(change) = timers.start_unschedule(key, time_of(time), TYPES[ty]) {
                delete(&mut rows[time][ty], outcome);
                timers.end(change, outcome, owned);
                kept(timers, at, before, outcome)?;
            }
        }
        Op::UnscheduleAll(ty, outcome) => {
            let before: [_; TIMES.len()] = from_fn(|time| waiting(timers, At(time, ty)));
            let (change, firing) = timers.start_unschedule_all(key, TYPES[ty]);
            for (time, row) in rows.iter_mut().enumerate() {
                if !firing.contains(&time_of(time)) {
                    delete(&mut row[ty], outcome);
                }
            }
            timers.end(change, outcome, owned);
            for (time, before) in before.into_iter().enumerate() {
                if !firing.contains(&time_of(time)) {
                    kept(timers, At(time, ty), before, outcome)?;
                }
            }
        }
        Op::Complete(at @ At(time, ty), outcome) => {
            let before = waiting(timers, at);
            if let Some(change) = timers.start_complete(key, time_of(time), TYPES[ty]) {
                delete(&mut rows[time][ty], outcome);
                timers.end(change, outcome, owned);
                kept(timers, at, before, outcome)?;
            }
        }
        Op::Abort(At(time, ty)) => timers.abort(&key, time_of(time), TYPES[ty]),
        Op::Pop(At(time, ty)) => {
            let Timers { entries, queue } = timers;
            if let Some(next) = get_mut(entries, &key, time_of(time), TYPES[ty])
                .and_then(|entry| entry.next.as_mut())
                && let Some(item) = next.queued.take()
            {
                queue.try_remove(&item);
            }
        }
        Op::Fire(at, tag) => {
            timers.fire(&trigger(at, tag));
        }
        Op::Load(at) => {
            if let Stored::Present(tag) = rows[at.0][at.1]
                && owned(time_of(at.0))
            {
                timers.load(trigger(at, tag));
            }
        }
    }
    Ok(())
}

/// Checks the invariants of memory against the stored rows.
fn check(timers: &Timers, rows: &Rows) -> Result<()> {
    let mut queued = 0_usize;
    for (time, row) in rows.iter().enumerate() {
        for (ty, &row) in row.iter().enumerate() {
            let entry = entry(timers, At(time, ty));
            let covered = row == Stored::Absent || !owned(time_of(time)) || entry.is_some();
            ensure!(covered, "an owned row that may exist has no entry");
            let Some(entry) = entry else {
                continue;
            };
            ensure!(
                row != Stored::Absent || entry.firing,
                "an entry stands for no row and no delivery"
            );
            if let (Some(next), Stored::Present(tag)) = (&entry.next, row) {
                ensure!(
                    next.trigger.tag == tag,
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
    }
    ensure!(
        queued == timers.queue.len(),
        "the queue and the entries disagree"
    );
    Ok(())
}

/// Checks that a removal that is not done leaves the attempt that waited.
fn kept(timers: &Timers, at: At, before: Option<i32>, outcome: Outcome) -> Result<()> {
    ensure!(
        outcome == Outcome::Done || waiting(timers, at) == before,
        "a removal that is not done lost the waiting attempt"
    );
    Ok(())
}

/// The tag of the attempt that waits at `at`.
fn waiting(timers: &Timers, at: At) -> Option<i32> {
    entry(timers, at)?
        .next
        .as_ref()
        .map(|next| next.trigger.tag)
}

fn entry(timers: &Timers, At(time, ty): At) -> Option<&Entry> {
    timers.entries.get(&key())?.get(&(time_of(time), TYPES[ty]))
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

fn owned(time: CompactDateTime) -> bool {
    time != time_of(TIMES.len() - 1)
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
        match u8::arbitrary(g) % 10 {
            0..=2 => Op::Insert(at, insert, tag, outcome),
            3 => Op::Unschedule(at, outcome),
            4 => Op::UnscheduleAll(at.1, outcome),
            5 => Op::Complete(at, outcome),
            6 => Op::Abort(at),
            7 => Op::Pop(at),
            8 => Op::Fire(at, tag),
            _ => Op::Load(at),
        }
    }
}
