//! Trace + model property for [`ActiveTriggers`].
//!
//! `ActiveTriggers` is a concurrent registry whose only invariants are pure
//! map/aggregation facts: a read returns the last write, `remove` deletes,
//! `scan_active_times` visits exactly the live `(time, type)` tuples, and
//! `snapshot` counts match a fold over the contents. The timer *state machine*
//! (schedule/fire/commit/abort) is not implemented here — it lives in the
//! scheduler and is proven by `prop_scheduler_invariants`. So this suite drives
//! the registry and a plain `HashMap` model through a random op sequence and
//! asserts equivalence after every op.

use super::*;
use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::collections::HashSet;
use strum::VariantArray;
use tracing::Span;

/// Cap on ops per trace — long enough for collisions and re-use, short enough
/// to shrink quickly.
const MAX_OPS: usize = 40;

/// Fire-time pool, chosen to straddle [`NOW`]: past (`1000`, `1100`), equal
/// (`1200`), and future (`1300`, `1400`) so every snapshot branch is reached.
const TIME_POOL: [u32; 5] = [1000, 1100, 1200, 1300, 1400];

/// The fixed `now` handed to every `snapshot` assertion; the midpoint of
/// [`TIME_POOL`].
const NOW: u32 = 1200;

/// The four lifecycle states `set_state` can store. `TimerState` has no
/// `Arbitrary` of its own, so ops index this pool directly.
const STATES: [TimerState; 4] = [
    TimerState::Scheduled,
    TimerState::Firing,
    TimerState::FiringRescheduled,
    TimerState::Aborted,
];

/// Identifies one registry entry across the real registry and the model.
type Triple = (Key, CompactDateTime, TimerType);

/// Deliberately-simple model: a flat map keyed by the full triple. Whole-entry
/// removal is implicit here, an equivalent flattening of the registry's
/// two-level `key → (time, type)` map.
type Model = HashMap<Triple, ActiveTriggerEntry>;

/// One registry operation, drawn from small fixed pools so collisions
/// (same key/time/type) and cross-key isolation happen naturally.
#[derive(Clone, Debug)]
enum Op {
    Insert {
        key: Key,
        time: CompactDateTime,
        ty: TimerType,
        tag: i32,
    },
    Remove {
        key: Key,
        time: CompactDateTime,
        ty: TimerType,
    },
    SetState {
        key: Key,
        time: CompactDateTime,
        ty: TimerType,
        state: TimerState,
    },
    SetTag {
        key: Key,
        time: CompactDateTime,
        ty: TimerType,
        tag: i32,
    },
}

impl Op {
    /// The registry entry this op targets — recorded so equivalence is checked
    /// for absent triples too, catching a spurious presence.
    fn triple(&self) -> Triple {
        match self {
            Op::Insert { key, time, ty, .. }
            | Op::Remove { key, time, ty }
            | Op::SetState { key, time, ty, .. }
            | Op::SetTag { key, time, ty, .. } => (key.clone(), *time, *ty),
        }
    }
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        let key = format!("key-{}", u8::arbitrary(g) % 3).into();
        let time =
            CompactDateTime::from(TIME_POOL[usize::from(u8::arbitrary(g)) % TIME_POOL.len()]);
        let ty = TimerType::VARIANTS[usize::from(u8::arbitrary(g)) % TimerType::VARIANTS.len()];
        match u8::arbitrary(g) % 4 {
            0 => Op::Insert {
                key,
                time,
                ty,
                tag: i32::arbitrary(g),
            },
            1 => Op::Remove { key, time, ty },
            2 => Op::SetState {
                key,
                time,
                ty,
                state: STATES[usize::from(u8::arbitrary(g)) % STATES.len()],
            },
            _ => Op::SetTag {
                key,
                time,
                ty,
                tag: i32::arbitrary(g),
            },
        }
    }
}

/// A shrinkable op sequence. Dropping ops is the high-value reduction, so
/// shrink delegates to the `Vec`.
#[derive(Clone, Debug)]
struct Trace {
    ops: Vec<Op>,
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: Vec::<Op>::arbitrary(g).into_iter().take(MAX_OPS).collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.ops.shrink().map(|ops| Self { ops }))
    }
}

/// Asserts every reader of the registry agrees with the model: per-triple
/// `get_state`/`get_tag`/`contains`/`is_scheduled`, the `scan_active_times`
/// multiset, and the `snapshot` fold — the last recomputed independently, not
/// by calling `snapshot` again.
async fn assert_equiv(active: &ActiveTriggers, model: &Model, seen: &[Triple]) {
    for (key, time, ty) in seen {
        let entry = model.get(&(key.clone(), *time, *ty));
        assert_eq!(
            active.get_state(key, *time, *ty).await,
            entry.map(|e| e.state)
        );
        assert_eq!(active.get_tag(key, *time, *ty).await, entry.map(|e| e.tag));
        assert_eq!(active.contains(key, *time, *ty).await, entry.is_some());
        assert_eq!(
            active.is_scheduled(key, *time, *ty).await,
            entry.is_some_and(|e| matches!(
                e.state,
                TimerState::Scheduled | TimerState::FiringRescheduled
            ))
        );
    }

    // scan is per-key: a (time, type) shared by two keys appears twice, so the
    // oracle is a sorted multiset — one entry per model key — not a set.
    let mut scanned: Vec<(CompactDateTime, TimerType)> = Vec::new();
    active
        .scan_active_times(|time, ty| scanned.push((time, ty)))
        .await;
    scanned.sort();
    let mut expected: Vec<(CompactDateTime, TimerType)> =
        model.keys().map(|(_, time, ty)| (*time, *ty)).collect();
    expected.sort();
    assert_eq!(scanned, expected, "scan_active_times multiset");

    let now = CompactDateTime::from(NOW);
    let snap = active.snapshot(now).await;
    let (mut count, mut in_flight, mut overdue, mut oldest) = (0_u32, 0_u32, 0_u32, 0_u32);
    for ((_, time, _), entry) in model {
        count = count.saturating_add(1);
        if matches!(
            entry.state,
            TimerState::Firing | TimerState::FiringRescheduled
        ) {
            in_flight = in_flight.saturating_add(1);
        }
        if *time <= now {
            overdue = overdue.saturating_add(1);
            oldest = oldest.max(now.epoch_seconds().saturating_sub(time.epoch_seconds()));
        }
    }
    assert_eq!(snap.active, count, "snapshot.active");
    assert_eq!(snap.in_flight, in_flight, "snapshot.in_flight");
    assert_eq!(snap.overdue, overdue, "snapshot.overdue");
    assert_eq!(
        snap.oldest_overdue_secs, oldest,
        "snapshot.oldest_overdue_secs"
    );
}

/// Applies each op to the registry and the model in lockstep, asserting
/// equivalence on the empty state and after every op. `set_state`/`set_tag`
/// return values are compared inline, where the model's pre-op membership is
/// known.
async fn run_trace(trace: Trace) {
    let active = ActiveTriggers::default();
    let mut model = Model::default();
    let mut seen: Vec<Triple> = Vec::new();
    let mut seen_set: HashSet<Triple> = HashSet::new();

    assert_equiv(&active, &model, &seen).await;

    for op in trace.ops {
        let triple = op.triple();
        if seen_set.insert(triple.clone()) {
            seen.push(triple.clone());
        }

        match op {
            Op::Insert { key, time, ty, tag } => {
                active
                    .insert(Trigger::with_tag(
                        key.clone(),
                        time,
                        ty,
                        tag,
                        Span::current(),
                    ))
                    .await;
                model.entry((key, time, ty)).or_insert(ActiveTriggerEntry {
                    state: TimerState::Scheduled,
                    tag,
                });
            }
            Op::Remove { key, time, ty } => {
                active.remove(&key, time, ty).await;
                model.remove(&(key, time, ty));
            }
            Op::SetState {
                key,
                time,
                ty,
                state,
            } => {
                let got = active.set_state(&key, time, ty, state).await;
                let want = match model.get_mut(&(key, time, ty)) {
                    Some(entry) => {
                        entry.state = state;
                        true
                    }
                    None => false,
                };
                assert_eq!(got, want, "set_state return");
            }
            Op::SetTag { key, time, ty, tag } => {
                let got = active.set_tag(&key, time, ty, tag).await;
                let want = match model.get_mut(&(key, time, ty)) {
                    Some(entry) => {
                        entry.tag = tag;
                        true
                    }
                    None => false,
                };
                assert_eq!(got, want, "set_tag return");
            }
        }

        assert_equiv(&active, &model, &seen).await;
    }
}

/// The registry tracks a plain `HashMap` model op-for-op across
/// Insert/Remove/SetState/SetTag, and every reader (`get_state`, `get_tag`,
/// `contains`, `is_scheduled`, `scan_active_times`, `snapshot`) agrees after
/// every op.
#[test]
fn prop_active_triggers_track_model() {
    fn property(trace: Trace) {
        executor::block_on(run_trace(trace));
    }
    QuickCheck::new().quickcheck(property as fn(Trace));
}

/// Receipt and retire preserve the split timer lifecycle for every prior state.
#[test]
fn receipt_and_retire_transition_table() {
    use TimerState::{Aborted, Firing, FiringRescheduled, Scheduled};

    let receipt_delete = [Some(Firing), Some(Scheduled), Some(Aborted), None];
    for prior in receipt_delete {
        let transition = transition(prior, TimerOp::Receipt);
        assert_eq!(transition.store(), StoreEffect::DeleteKeyRow);
        assert_eq!(transition.phases().0.queue, QueueEffect::None);
        assert!(transition.announce().is_none());
    }

    let rescheduled = transition(Some(FiringRescheduled), TimerOp::Receipt);
    assert_eq!(rescheduled.store(), StoreEffect::UpdateTag);
    assert!(rescheduled.phases().1.adopt_tag);

    for prior in [Some(Firing), Some(Aborted), None] {
        let transition = transition(prior, TimerOp::Retire);
        assert_eq!(transition.store(), StoreEffect::DeleteSlabRow);
        assert_eq!(transition.phases().0.queue, QueueEffect::Deactivate);
    }

    for prior in [Some(Scheduled), Some(FiringRescheduled)] {
        let transition = transition(prior, TimerOp::Retire);
        assert_eq!(transition.store(), StoreEffect::None);
        assert_eq!(transition.phases().0.queue, QueueEffect::None);
    }
}
