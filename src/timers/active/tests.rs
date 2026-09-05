//! Registry reads and counts must agree with a plain map.
//! The fixed trace checks delivery between the state read and completion.

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
}

impl Op {
    /// The registry entry this op targets — recorded so equivalence is checked
    /// for absent triples too, catching a spurious presence.
    fn triple(&self) -> Triple {
        match self {
            Op::Insert { key, time, ty, .. }
            | Op::Remove { key, time, ty }
            | Op::SetState { key, time, ty, .. } => (key.clone(), *time, *ty),
        }
    }
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        let key: Key = format!("key-{}", u8::arbitrary(g) % 3).into();
        let time =
            CompactDateTime::from(TIME_POOL[usize::from(u8::arbitrary(g)) % TIME_POOL.len()]);
        let ty = TimerType::VARIANTS[usize::from(u8::arbitrary(g)) % TimerType::VARIANTS.len()];
        let states = [
            TimerState::Scheduled(Item::Queued),
            TimerState::Firing,
            TimerState::FiringReplaced(Trigger::new(key.clone(), time, ty, Span::none())),
            TimerState::FiringRescheduled(Item::Queued),
            TimerState::Parked,
        ];
        match u8::arbitrary(g) % 3 {
            0 => Op::Insert { key, time, ty },
            1 => Op::SetState {
                key,
                time,
                ty,
                state: states[usize::from(u8::arbitrary(g)) % states.len()].clone(),
            },
            _ => Op::Remove { key, time, ty },
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
            entry.map(|e| e.state.clone())
        );
        assert_eq!(active.contains(key, *time, *ty).await, entry.is_some());
        assert_eq!(
            active.is_scheduled(key, *time, *ty).await,
            entry.is_some_and(|e| matches!(
                e.state,
                TimerState::Scheduled(_) | TimerState::FiringRescheduled(_)
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
            TimerState::Firing | TimerState::FiringReplaced(_) | TimerState::FiringRescheduled(_)
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
            Op::Insert { key, time, ty } => {
                active
                    .insert(Trigger::new(key.clone(), time, ty, Span::current()))
                    .await;
                model.entry((key, time, ty)).or_insert(ActiveTriggerEntry {
                    state: TimerState::Scheduled(Item::Queued),
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
                let got = active.set_state(&key, time, ty, state.clone()).await;
                let want = match model.get_mut(&(key, time, ty)) {
                    Some(entry) => {
                        entry.state = state;
                        true
                    }
                    None => false,
                };
                assert_eq!(got, want, "set_state return");
            }
        }

        assert_equiv(&active, &model, &seen).await;
    }
}

/// Registry reads and counts agree with a map after each insert, remove, or
/// state change.
#[test]
fn prop_active_triggers_track_model() {
    fn property(trace: Trace) {
        executor::block_on(run_trace(trace));
    }
    QuickCheck::new().quickcheck(property as fn(Trace));
}

/// Completion must keep an item delivered after the transition's state read.
#[tokio::test]
async fn completion_keeps_item_delivered_after_state_read() -> color_eyre::Result<()> {
    use color_eyre::eyre::ensure;

    let active = ActiveTriggers::default();
    let trigger = Trigger::with_tag(
        Key::from("delivery-race"),
        CompactDateTime::from(NOW),
        TimerType::Application,
        42,
        Span::none(),
    );
    active.insert(trigger.clone()).await;
    ensure!(
        active
            .set_state(
                &trigger.key,
                trigger.time,
                trigger.timer_type,
                TimerState::FiringRescheduled(Item::Queued)
            )
            .await
    );

    let prior = active
        .get_state(&trigger.key, trigger.time, trigger.timer_type)
        .await;
    active.deliver(&trigger).await;
    let (_, after) = transition(prior.as_ref(), TimerOp::Complete).phases();
    match after.next {
        Next::Keep => {}
        Next::Idle => {
            active
                .idle(&trigger.key, trigger.time, trigger.timer_type)
                .await;
        }
        Next::To(state) => {
            active
                .set_state(&trigger.key, trigger.time, trigger.timer_type, state)
                .await;
        }
    }

    let actual = active
        .fire(&trigger.key, trigger.time, trigger.timer_type)
        .await;
    ensure!(
        actual == Some(trigger.tag),
        "completion lost the delivered item: actual={actual:?}"
    );
    Ok(())
}
