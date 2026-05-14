//! Property test (`prop_scheduler_invariants`) for the scheduler actor.
//!
//! Drives random sequences of scheduler ops against the actor's handlers
//! and asserts the four load-bearing invariants after every op:
//!
//!   P1 (owned↔DB parity): every `(key, time, type)` in `loaded_slab_ids`'s
//!       slabs is in `ActiveTriggers` iff it's currently in the DB.
//!   P2 (pending = store-only): triggers for slabs outside the owned range
//!       never land in `ActiveTriggers`.
//!   P3 (watermark I1): every slab clustering row sits strictly above
//!       `last_persisted_watermark`.
//!   P4 (cleanup safety): a single `cleanup_step` invocation never deletes
//!       a slab that has active triggers or that contains `now`.
//!   P5 (restart preserves): the property holds across actor restart, so no
//!       timer rows can be lost when the in-memory state is discarded and
//!       rebuilt against the same store.

use super::actor::{
    ActorState, MIN_PRELOAD, calculate_preload, calculate_wait_time, cleanup_step,
    collect_active_slab_ids, handle_add, load_step, next_unloaded_slab_id,
};
use crate::Key;
use crate::timers::active::TimerState;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::queue::TriggerQueue;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::store::{Segment, SegmentVersion, TriggerStore};
use crate::timers::{TimerType, Trigger};
use ahash::HashSet;
use color_eyre::eyre::Result;
use futures::TryStreamExt;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::BTreeSet;
use std::result::Result as StdResult;
use std::time::Duration as StdDuration;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::time::{Instant, advance};
use tracing::Span;
use uuid::Uuid;

/// Tests use a 300s slab and a deterministic preload window so slab
/// math is predictable across runs.
const SLAB_SIZE_SECS: u32 = 300;
const PRELOAD_SECS: u32 = 120;

fn test_segment() -> Segment {
    Segment {
        id: Uuid::new_v4(),
        name: "scheduler-test".to_owned(),
        slab_size: CompactDuration::new(SLAB_SIZE_SECS),
        version: SegmentVersion::V3,
    }
}

type TestStore = TableAdapter<InMemoryTriggerStore>;

fn fresh_state(store: TestStore, segment: Segment) -> ActorState<TestStore> {
    let now = Instant::now();
    ActorState {
        store,
        segment,
        loaded_slab_ids: BTreeSet::new(),
        last_persisted_watermark: None,
        highest_loaded_slab_id: None,
        preload_window: CompactDuration::new(PRELOAD_SECS),
        next_load_at: now,
        next_cleanup_at: now,
    }
}

// ===================================================================
// Pure helper tests
// ===================================================================

#[tokio::test(start_paused = true)]
async fn test_calculate_wait_time_future() -> Result<()> {
    let now = CompactDateTime::now()?;
    let future = now.add_duration(CompactDuration::new(120))?;
    let preload = CompactDuration::new(30);
    // 120s out minus 30s preload = 90s wait.
    assert_eq!(
        calculate_wait_time(future, preload),
        CompactDuration::new(90)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn test_calculate_wait_time_within_preload_window() -> Result<()> {
    let now = CompactDateTime::now()?;
    let near = now.add_duration(CompactDuration::new(15))?;
    // 15s out minus 30s preload saturates at zero — load immediately.
    assert!(calculate_wait_time(near, CompactDuration::new(30)).is_zero());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn test_calculate_wait_time_past_time() -> Result<()> {
    let load_time = CompactDateTime::now()?;
    advance(StdDuration::from_mins(1)).await;
    // Load time is now in the past — return zero.
    assert!(calculate_wait_time(load_time, CompactDuration::new(30)).is_zero());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn test_calculate_wait_time_exact_preload_boundary() -> Result<()> {
    let now = CompactDateTime::now()?;
    let boundary = now.add_duration(CompactDuration::new(30))?;
    // Exactly at the preload boundary — load immediately.
    assert!(calculate_wait_time(boundary, CompactDuration::new(30)).is_zero());
    Ok(())
}

#[test]
fn test_calculate_preload_within_bounds() {
    let slab_size = CompactDuration::new(SLAB_SIZE_SECS);
    for _ in 0_i32..32_i32 {
        let window = calculate_preload(slab_size);
        assert!(
            window >= MIN_PRELOAD && window <= slab_size,
            "preload {window} outside [{MIN_PRELOAD}, {slab_size}]"
        );
    }
}

#[test]
fn test_next_unloaded_slab_id_from_fresh_state() {
    let segment = test_segment();
    let store = memory_store(segment.clone());
    let mut state = fresh_state(store, segment);
    // No watermark, no high-water: scan starts at 0.
    assert_eq!(next_unloaded_slab_id(&state), Some(0));

    state.last_persisted_watermark = Some(7);
    assert_eq!(next_unloaded_slab_id(&state), Some(8));

    state.highest_loaded_slab_id = Some(12);
    assert_eq!(next_unloaded_slab_id(&state), Some(13));

    state.highest_loaded_slab_id = Some(SlabId::MAX);
    // Slab-id space exhausted.
    assert_eq!(next_unloaded_slab_id(&state), None);
}

// ===================================================================
// Unified property test
// ===================================================================

/// Bounded domain so quickcheck explores meaningful interleavings
/// rather than spreading triggers across a unique slab per op.
const PROP_KEYS: u8 = 4;
/// Slab offset span (`-PROP_PAST_SLABS..=PROP_FUTURE_SLABS`) relative to
/// `now_slab_id`. Negative offsets eventually fall below the watermark
/// once cleanups have advanced it, exercising the past-time BATCH path.
const PROP_PAST_SLABS: i32 = 5;
const PROP_FUTURE_SLABS: i32 = 5;

/// One element of the random op sequence applied to the actor.
#[derive(Clone, Debug)]
enum Op {
    Schedule(TriggerSpec),
    Unschedule(TriggerSpec),
    Fire(TriggerSpec),
    LoadStep,
    CleanupStep,
    Restart,
}

/// Compact, hashable trigger identity used by the generator and the
/// expected-state model. Resolved to a concrete `(Key, time, type)` at
/// fixture time so all ops within one quickcheck iteration share a
/// consistent `now_slab_id` anchor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct TriggerSpec {
    key_idx: u8,
    slab_offset: i32,
    timer_type: TimerType,
}

impl TriggerSpec {
    fn resolve(self, now_slab: SlabId) -> (Key, CompactDateTime, TimerType) {
        let signed = i64::from(now_slab) + i64::from(self.slab_offset);
        let slab_id: SlabId = signed.max(0).try_into().unwrap_or(0);
        let time = CompactDateTime::from(slab_id.saturating_mul(SLAB_SIZE_SECS));
        (Key::from(format!("k{}", self.key_idx)), time, self.timer_type)
    }
}

fn timer_type_from_idx(idx: u8) -> TimerType {
    match idx % 3 {
        0 => TimerType::Application,
        1 => TimerType::DeferredMessage,
        _ => TimerType::DeferredTimer,
    }
}

impl Arbitrary for TriggerSpec {
    fn arbitrary(g: &mut Gen) -> Self {
        let slab_span: i32 = PROP_PAST_SLABS + PROP_FUTURE_SLABS + 1_i32;
        let raw = i32::from(u8::arbitrary(g)) % slab_span;
        let slab_offset = raw - PROP_PAST_SLABS;
        Self {
            key_idx: u8::arbitrary(g) % PROP_KEYS,
            slab_offset,
            timer_type: timer_type_from_idx(u8::arbitrary(g)),
        }
    }
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        // Bias toward Schedule/Load so the actor accumulates state to
        // exercise rather than thrashing through cleanups and restarts.
        match u8::arbitrary(g) % 11 {
            0..=3 => Self::Schedule(TriggerSpec::arbitrary(g)),
            4..=5 => Self::Unschedule(TriggerSpec::arbitrary(g)),
            6 => Self::Fire(TriggerSpec::arbitrary(g)),
            7..=8 => Self::LoadStep,
            9 => Self::CleanupStep,
            _ => Self::Restart,
        }
    }
}

/// Vec of ops with a small minimum length so degenerate empty cases
/// don't dominate the run.
#[derive(Clone, Debug)]
struct OpSequence(Vec<Op>);

impl Arbitrary for OpSequence {
    fn arbitrary(g: &mut Gen) -> Self {
        let n = (u8::arbitrary(g) % 40 + 10) as usize;
        Self((0..n).map(|_| Op::arbitrary(g)).collect())
    }
}

/// The set of triples the test has scheduled and not yet unscheduled.
/// This is the "what should be in the store and (when slab is owned) in
/// `ActiveTriggers`" model.
type TripleSet = HashSet<(Key, CompactDateTime, TimerType)>;

struct Fixture {
    store: TableAdapter<InMemoryTriggerStore>,
    segment: Segment,
    state: ActorState<TableAdapter<InMemoryTriggerStore>>,
    triggers: TriggerQueue,
    /// Authoritative "should-be-scheduled" model.
    expected: TripleSet,
    /// Anchored at construction so all ops within one iteration use a
    /// stable `now_slab_id` regardless of wall-clock drift.
    now_slab: SlabId,
    /// All `(key, time, type)` triples the generator can produce, used
    /// for the universe-iteration check after every op.
    universe: Vec<(Key, CompactDateTime, TimerType)>,
}

impl Fixture {
    async fn new() -> StdResult<Self, String> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        store
            .insert_segment()
            .await
            .map_err(|e| format!("insert_segment: {e:?}"))?;

        let now = CompactDateTime::now()
            .map_err(|e| format!("wall-clock now: {e:?}"))?;
        let now_slab = Slab::from_time(segment.slab_size, now).id();

        let universe = build_universe(now_slab);

        let state = fresh_state(store.clone(), segment.clone());
        let triggers = TriggerQueue::new();
        Ok(Self {
            store,
            segment,
            state,
            triggers,
            expected: TripleSet::default(),
            now_slab,
            universe,
        })
    }

    async fn apply(&mut self, op: Op) -> StdResult<(), String> {
        match op {
            Op::Schedule(spec) => {
                let (key, time, ty) = spec.resolve(self.now_slab);
                let trigger = Trigger::new(key.clone(), time, ty, Span::current());
                self.store
                    .add_trigger(trigger.clone())
                    .await
                    .map_err(|e| format!("add_trigger: {e:?}"))?;
                handle_add(&mut self.state, &mut self.triggers, trigger)
                    .await
                    .map_err(|e| format!("handle_add: {e:?}"))?;
                self.expected.insert((key, time, ty));
            }
            Op::Unschedule(spec) => {
                let (key, time, ty) = spec.resolve(self.now_slab);
                self.store
                    .remove_trigger(&key, time, ty)
                    .await
                    .map_err(|e| format!("remove_trigger: {e:?}"))?;
                let trigger =
                    Trigger::new(key.clone(), time, ty, Span::current());
                self.triggers.remove(&trigger).await;
                self.expected.remove(&(key, time, ty));
            }
            Op::Fire(spec) => {
                // Only a state transition — does not change membership.
                let (key, time, ty) = spec.resolve(self.now_slab);
                if matches!(
                    self.triggers
                        .active_triggers()
                        .get_state(&key, time, ty)
                        .await,
                    Some(TimerState::Scheduled)
                ) {
                    self.triggers
                        .active_triggers()
                        .set_state(&key, time, ty, TimerState::Firing)
                        .await;
                }
            }
            Op::LoadStep => {
                load_step(&mut self.state, &mut self.triggers).await;
            }
            Op::CleanupStep => {
                self.check_cleanup_safety().await?;
            }
            Op::Restart => {
                self.state = fresh_state(self.store.clone(), self.segment.clone());
                self.state.last_persisted_watermark = self
                    .store
                    .get_slab_watermark()
                    .await
                    .map_err(|e| format!("get_slab_watermark on restart: {e:?}"))?;
                self.triggers = TriggerQueue::new();
            }
        }
        Ok(())
    }

    /// Wraps `cleanup_step` so we can verify P4 (no active or now-slab
    /// deletion) by diffing snapshots.
    async fn check_cleanup_safety(&mut self) -> StdResult<(), String> {
        let before_loaded = self.state.loaded_slab_ids.clone();
        let before_active =
            collect_active_slab_ids(self.segment.slab_size, &self.triggers).await;

        cleanup_step(&mut self.state, &self.triggers).await;

        let after_loaded = &self.state.loaded_slab_ids;
        let deleted: BTreeSet<SlabId> =
            before_loaded.difference(after_loaded).copied().collect();

        let now_slab = match CompactDateTime::now() {
            Ok(t) => Slab::from_time(self.segment.slab_size, t).id(),
            Err(_) => self.now_slab,
        };

        for &slab_id in &deleted {
            if before_active.contains(&slab_id) {
                return Err(format!(
                    "P4: cleanup deleted slab {slab_id} that had active triggers"
                ));
            }
            if slab_id >= now_slab {
                return Err(format!(
                    "P4: cleanup deleted slab {slab_id} >= now_slab {now_slab}"
                ));
            }
        }
        Ok(())
    }

    async fn check_invariants(&self) -> StdResult<(), String> {
        // P3: every slab clustering row sits above the persisted watermark.
        let watermark = self.state.last_persisted_watermark;
        let store_slabs: BTreeSet<SlabId> = self
            .store
            .get_slab_range(0..=SlabId::MAX)
            .try_collect()
            .await
            .map_err(|e| format!("get_slab_range: {e:?}"))?;
        if let Some(w) = watermark
            && let Some(&min_slab) = store_slabs.iter().next()
            && min_slab <= w
        {
            return Err(format!(
                "P3 violated: watermark={w} but slab {min_slab} present"
            ));
        }
        let persisted_watermark = self
            .store
            .get_slab_watermark()
            .await
            .map_err(|e| format!("get_slab_watermark: {e:?}"))?;
        if persisted_watermark != watermark {
            return Err(format!(
                "watermark drift: actor={watermark:?} store={persisted_watermark:?}"
            ));
        }

        // P1 + P2: walk the entire generator universe so we verify both
        // directions (every triple's actual state matches the expected
        // state derived from `expected` + `loaded_slab_ids`).
        let loaded = &self.state.loaded_slab_ids;
        for (key, time, ty) in &self.universe {
            let in_expected = self.expected.contains(&(key.clone(), *time, *ty));
            let slab_id = Slab::from_time(self.segment.slab_size, *time).id();

            // DB membership mirrors `expected`: every Scheduled triple has a
            // store row; every Unscheduled triple has none.
            let store_times: BTreeSet<CompactDateTime> = self
                .store
                .get_key_times(*ty, key)
                .try_collect()
                .await
                .map_err(|e| format!("get_key_times: {e:?}"))?;
            let in_db = store_times.contains(time);
            if in_db != in_expected {
                return Err(format!(
                    "DB mismatch for ({key}, {time:?}, {ty:?}): in_db={in_db} expected={in_expected}"
                ));
            }

            // ActiveTriggers membership: only when the slab is owned AND
            // the triple is currently scheduled.
            let active = self
                .triggers
                .active_triggers()
                .contains(key, *time, *ty)
                .await;
            let owned = loaded.contains(&slab_id);
            let expected_active = in_expected && owned;
            if active != expected_active {
                return Err(format!(
                    "Active mismatch for ({key}, {time:?}, {ty:?}): \
                     active={active} expected={expected_active} \
                     (in_expected={in_expected}, owned={owned})"
                ));
            }
        }

        Ok(())
    }
}

/// Enumerates every `(key, time, type)` that the generator can produce
/// given `now_slab`. Used to drive an exhaustive per-triple check after
/// every op without needing to iterate `ActiveTriggers`' internal map.
fn build_universe(now_slab: SlabId) -> Vec<(Key, CompactDateTime, TimerType)> {
    let mut out = Vec::with_capacity(usize::from(PROP_KEYS) * 11 * 3);
    for key_idx in 0..PROP_KEYS {
        for offset in -PROP_PAST_SLABS..=PROP_FUTURE_SLABS {
            let signed = i64::from(now_slab) + i64::from(offset);
            let slab_id: SlabId = signed.max(0).try_into().unwrap_or(0);
            let time = CompactDateTime::from(slab_id.saturating_mul(SLAB_SIZE_SECS));
            for ty_idx in 0_u8..3 {
                out.push((
                    Key::from(format!("k{key_idx}")),
                    time,
                    timer_type_from_idx(ty_idx),
                ));
            }
        }
    }
    out
}

async fn run_property(ops: Vec<Op>) -> StdResult<(), String> {
    let mut fixture = Fixture::new().await?;
    let history = ops.clone();
    for (i, op) in ops.into_iter().enumerate() {
        fixture
            .apply(op.clone())
            .await
            .map_err(|e| format!("op #{i} {op:?}: {e}\nhistory: {history:#?}"))?;
        fixture
            .check_invariants()
            .await
            .map_err(|e| format!("after op #{i} {op:?}: {e}\nhistory: {history:#?}"))?;
    }
    Ok(())
}

#[test]
fn prop_scheduler_invariants() {
    fn property(seq: OpSequence) -> TestResult {
        let runtime = match RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(r) => r,
            Err(e) => return TestResult::error(format!("runtime build: {e:?}")),
        };
        match runtime.block_on(run_property(seq.0)) {
            Ok(()) => TestResult::passed(),
            Err(e) => TestResult::error(e),
        }
    }

    // Iteration count is read from the `QUICKCHECK_TESTS` env var, with
    // quickcheck's built-in default applying when unset. Never hardcoded.
    QuickCheck::new().quickcheck(property as fn(OpSequence) -> TestResult);
}
