//! Property test (`prop_scheduler_invariants`) for the scheduler actor.
//!
//! Drives random sequences of scheduler ops against the actor's handlers
//! and asserts the four load-bearing invariants after every op:
//!
//!   P1 (owned↔DB parity): every `(key, time, type)` in the loaded numeric
//!       range is in `ActiveTriggers` iff it's currently in the DB.
//!   P2 (pending = store-only): triggers for slabs outside the owned range
//!       never land in `ActiveTriggers`.
//!   P3 (watermark I1): every slab clustering row sits strictly above
//!       `last_persisted_watermark`.
//!   P4 (cleanup safety): cleanup never deletes
//!       a slab that has active triggers or that contains `now`.
//!   P5 (restart preserves): the property holds across actor restart, so no
//!       timer rows can be lost when the in-memory state is discarded and
//!       rebuilt against the same store.
//!   P6 (cleanup progress): a persisted slab at or below the load high-water
//!       that is neither active nor current-time is deleted by cleanup, even
//!       when it has no trigger rows.
//!   P7 (past-watermark schedule): scheduling an unregistered slab at or below
//!       the watermark lowers the watermark, preserves compact ownership, and
//!       activates the trigger.

use super::actor::{
    ActorState, MIN_PRELOAD, calculate_preload, calculate_wait_time, cleanup_step,
    collect_active_slab_ids, handle_add, load_step, next_unloaded_slab_id, owns_slab,
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
use ahash::HashMap;
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
        known_slab_ids: ahash::HashSet::default(),
        last_persisted_watermark: None,
        highest_loaded_slab_id: None,
        preload_window: CompactDuration::new(PRELOAD_SECS),
        next_load_at: now,
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
    Abort(TriggerSpec),
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
        (
            Key::from(format!("k{}", self.key_idx)),
            time,
            self.timer_type,
        )
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
        match u8::arbitrary(g) % 12 {
            0..=3 => Self::Schedule(TriggerSpec::arbitrary(g)),
            4..=5 => Self::Unschedule(TriggerSpec::arbitrary(g)),
            6 => Self::Fire(TriggerSpec::arbitrary(g)),
            7 => Self::Abort(TriggerSpec::arbitrary(g)),
            8..=9 => Self::LoadStep,
            10 => Self::CleanupStep,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ModelActiveState {
    Scheduled,
    Firing,
    Aborted,
}

impl ModelActiveState {
    fn from_timer_state(state: TimerState) -> StdResult<Self, String> {
        match state {
            TimerState::Scheduled => Ok(Self::Scheduled),
            TimerState::Firing => Ok(Self::Firing),
            TimerState::Aborted => Ok(Self::Aborted),
            TimerState::FiringRescheduled => {
                Err("property model does not create FiringRescheduled timers".to_owned())
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TriggerModel {
    in_store: bool,
    active_state: Option<ModelActiveState>,
}

/// Per-trigger model for persistent membership plus in-memory lifecycle state.
type TriggerModels = HashMap<(Key, CompactDateTime, TimerType), TriggerModel>;

struct Fixture {
    store: TableAdapter<InMemoryTriggerStore>,
    segment: Segment,
    state: ActorState<TableAdapter<InMemoryTriggerStore>>,
    triggers: TriggerQueue,
    /// Authoritative per-trigger model.
    expected: TriggerModels,
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

        let now = CompactDateTime::now().map_err(|e| format!("wall-clock now: {e:?}"))?;
        let now_slab = Slab::from_time(segment.slab_size, now).id();

        let universe = build_universe(now_slab);

        let state = fresh_state(store.clone(), segment.clone());
        let triggers = TriggerQueue::new();
        Ok(Self {
            store,
            segment,
            state,
            triggers,
            expected: TriggerModels::default(),
            now_slab,
            universe,
        })
    }

    async fn apply(&mut self, op: Op) -> StdResult<(), String> {
        match op {
            Op::Schedule(spec) => {
                let (key, time, ty) = spec.resolve(self.now_slab);
                let trigger = Trigger::new(key.clone(), time, ty, Span::current());
                let current_model = self.model_for(&key, time, ty);
                if current_model.active_state == Some(ModelActiveState::Aborted) {
                    self.triggers
                        .active_triggers()
                        .set_state(&key, time, ty, TimerState::Scheduled)
                        .await;
                    self.triggers.insert_queue_only(trigger);
                    self.set_model(
                        key,
                        time,
                        ty,
                        TriggerModel {
                            in_store: true,
                            active_state: Some(ModelActiveState::Scheduled),
                        },
                    );
                    return Ok(());
                }

                let slab_id = Slab::from_time(self.segment.slab_size, time).id();
                let past_unregistered = self
                    .state
                    .last_persisted_watermark
                    .is_some_and(|w| slab_id <= w)
                    && !self.state.known_slab_ids.contains(&slab_id);
                let old_watermark = self.state.last_persisted_watermark;
                self.store
                    .add_trigger(trigger.clone())
                    .await
                    .map_err(|e| format!("add_trigger: {e:?}"))?;
                handle_add(&mut self.state, &mut self.triggers, trigger)
                    .await
                    .map_err(|e| format!("handle_add: {e:?}"))?;
                if past_unregistered {
                    let expected_watermark = slab_id.checked_sub(1);
                    if self.state.last_persisted_watermark != expected_watermark {
                        return Err(format!(
                            "past-watermark schedule: expected watermark {expected_watermark:?}, got {:?}",
                            self.state.last_persisted_watermark
                        ));
                    }
                    if let Some(old_watermark) = old_watermark
                        && self
                            .state
                            .highest_loaded_slab_id
                            .is_none_or(|h| h < old_watermark)
                    {
                        return Err(format!(
                            "past-watermark schedule: highest_loaded_slab_id {:?} did not preserve ownership through old watermark {old_watermark}",
                            self.state.highest_loaded_slab_id
                        ));
                    }
                    if !self
                        .triggers
                        .active_triggers()
                        .contains(&key, time, ty)
                        .await
                    {
                        return Err(format!(
                            "past-watermark schedule: trigger ({key}, {time:?}, {ty:?}) was not active"
                        ));
                    }
                }
                let mut model = current_model;
                model.in_store = true;
                if owns_slab(&self.state, slab_id) && model.active_state.is_none() {
                    model.active_state = Some(ModelActiveState::Scheduled);
                }
                self.set_model(key, time, ty, model);
            }
            Op::Unschedule(spec) => {
                let (key, time, ty) = spec.resolve(self.now_slab);
                self.store
                    .remove_trigger(&key, time, ty)
                    .await
                    .map_err(|e| format!("remove_trigger: {e:?}"))?;
                let trigger = Trigger::new(key.clone(), time, ty, Span::current());
                self.triggers.remove(&trigger).await;
                self.set_model(
                    key,
                    time,
                    ty,
                    TriggerModel {
                        in_store: false,
                        active_state: None,
                    },
                );
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
                    let trigger = Trigger::new(key.clone(), time, ty, Span::current());
                    self.triggers.remove_queue_only(&trigger);
                    self.triggers
                        .active_triggers()
                        .set_state(&key, time, ty, TimerState::Firing)
                        .await;
                    let mut model = self.model_for(&key, time, ty);
                    model.active_state = Some(ModelActiveState::Firing);
                    self.set_model(key, time, ty, model);
                }
            }
            Op::Abort(spec) => {
                let (key, time, ty) = spec.resolve(self.now_slab);
                let current_state = self
                    .triggers
                    .active_triggers()
                    .get_state(&key, time, ty)
                    .await;
                match current_state {
                    Some(TimerState::Scheduled) => {
                        self.triggers
                            .active_triggers()
                            .set_state(&key, time, ty, TimerState::Aborted)
                            .await;
                        let trigger = Trigger::new(key.clone(), time, ty, Span::current());
                        self.triggers.remove_queue_only(&trigger);
                        let mut model = self.model_for(&key, time, ty);
                        model.active_state = Some(ModelActiveState::Aborted);
                        self.set_model(key, time, ty, model);
                    }
                    Some(TimerState::Firing) => {
                        self.triggers
                            .active_triggers()
                            .set_state(&key, time, ty, TimerState::Aborted)
                            .await;
                        let mut model = self.model_for(&key, time, ty);
                        model.active_state = Some(ModelActiveState::Aborted);
                        self.set_model(key, time, ty, model);
                    }
                    Some(TimerState::Aborted) | Some(TimerState::FiringRescheduled) | None => {}
                }
            }
            Op::LoadStep => {
                load_step(&mut self.state, &mut self.triggers).await;
                self.reconcile_loaded_models();
            }
            Op::CleanupStep => {
                self.check_cleanup_effects().await?;
            }
            Op::Restart => {
                self.state = fresh_state(self.store.clone(), self.segment.clone());
                self.state.last_persisted_watermark = self
                    .store
                    .get_slab_watermark()
                    .await
                    .map_err(|e| format!("get_slab_watermark on restart: {e:?}"))?;
                self.triggers = TriggerQueue::new();
                for model in self.expected.values_mut() {
                    model.active_state = None;
                }
            }
        }
        Ok(())
    }

    fn model_for(&self, key: &Key, time: CompactDateTime, ty: TimerType) -> TriggerModel {
        self.expected
            .get(&(key.clone(), time, ty))
            .copied()
            .unwrap_or_default()
    }

    fn set_model(&mut self, key: Key, time: CompactDateTime, ty: TimerType, model: TriggerModel) {
        let triple = (key, time, ty);
        if model.in_store || model.active_state.is_some() {
            self.expected.insert(triple, model);
        } else {
            self.expected.remove(&triple);
        }
    }

    fn reconcile_loaded_models(&mut self) {
        for (key, time, ty) in &self.universe {
            let slab_id = Slab::from_time(self.segment.slab_size, *time).id();
            if !owns_slab(&self.state, slab_id) {
                continue;
            }
            let triple = (key.clone(), *time, *ty);
            if let Some(model) = self.expected.get_mut(&triple)
                && model.in_store
                && model.active_state.is_none()
            {
                model.active_state = Some(ModelActiveState::Scheduled);
            }
        }
    }

    /// Wraps `cleanup_step` so we can verify P4 (safety) plus the progress
    /// properties that keep slab metadata and the watermark from getting
    /// pinned behind empty rows.
    async fn check_cleanup_effects(&mut self) -> StdResult<(), String> {
        let before_active = collect_active_slab_ids(self.segment.slab_size, &self.triggers).await;
        let before_store_slabs: BTreeSet<SlabId> = self
            .store
            .get_slab_range(0..=SlabId::MAX)
            .try_collect()
            .await
            .map_err(|e| format!("get_slab_range before cleanup: {e:?}"))?;
        let before_watermark = self.state.last_persisted_watermark;

        let now_slab = match CompactDateTime::now() {
            Ok(t) => Slab::from_time(self.segment.slab_size, t).id(),
            Err(_) => self.now_slab,
        };

        let expected_deletes = cleanup_candidates_for_test(
            before_watermark,
            self.state.highest_loaded_slab_id,
            now_slab,
            &before_active,
            &before_store_slabs,
        );

        cleanup_step(&mut self.state, &self.triggers).await;

        let after_store_slabs: BTreeSet<SlabId> = self
            .store
            .get_slab_range(0..=SlabId::MAX)
            .try_collect()
            .await
            .map_err(|e| format!("get_slab_range after cleanup: {e:?}"))?;
        let deleted: BTreeSet<SlabId> = before_store_slabs
            .difference(&after_store_slabs)
            .copied()
            .collect();

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

        for slab_id in expected_deletes {
            if after_store_slabs.contains(&slab_id) {
                return Err(format!(
                    "cleanup progress: eligible slab {slab_id} was not deleted"
                ));
            }
        }

        for ((key, time, ty), model) in &self.expected {
            if model.in_store && model.active_state.is_some() {
                let slab_id = Slab::from_time(self.segment.slab_size, *time).id();
                if !after_store_slabs.contains(&slab_id) {
                    return Err(format!(
                        "cleanup reloadability: active stored timer ({key}, {time:?}, {ty:?}) \
                         lost slab metadata for slab {slab_id}"
                    ));
                }
            }
        }

        let candidate = match after_store_slabs.iter().next().copied() {
            Some(first_slab) => first_slab.checked_sub(1),
            None => now_slab.checked_sub(1),
        };
        let should_advance = match (before_watermark, candidate) {
            (_, None) => false,
            (Some(current), Some(candidate)) => candidate > current,
            (None, Some(_)) => true,
        };
        if should_advance && self.state.last_persisted_watermark != candidate {
            return Err(format!(
                "watermark progress: expected {candidate:?}, got {:?}",
                self.state.last_persisted_watermark
            ));
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
        // state derived from `expected` + loaded numeric range).

        for (key, time, ty) in &self.universe {
            let expected = self.model_for(key, *time, *ty);
            let slab_id = Slab::from_time(self.segment.slab_size, *time).id();

            // DB membership mirrors `expected`: every stored triple has a
            // store row; every Unscheduled triple has none.
            let store_times: BTreeSet<CompactDateTime> = self
                .store
                .get_key_times(*ty, key)
                .try_collect()
                .await
                .map_err(|e| format!("get_key_times: {e:?}"))?;
            let in_db = store_times.contains(time);
            if in_db != expected.in_store {
                return Err(format!(
                    "DB mismatch for ({key}, {time:?}, {ty:?}): in_db={in_db} expected={}",
                    expected.in_store
                ));
            }

            // ActiveTriggers state: owned stored timers are active unless they
            // have not yet been loaded after a restart.
            let actual_state = self
                .triggers
                .active_triggers()
                .get_state(key, *time, *ty)
                .await;
            let owned = owns_slab(&self.state, slab_id);
            let actual_model_state = actual_state
                .map(ModelActiveState::from_timer_state)
                .transpose()?;
            if actual_model_state != expected.active_state {
                return Err(format!(
                    "Active mismatch for ({key}, {time:?}, {ty:?}): \
                     active={actual_model_state:?} expected={:?} \
                     (in_store={}, owned={owned})",
                    expected.active_state, expected.in_store
                ));
            }
            if expected.in_store && owned && expected.active_state.is_none() {
                return Err(format!(
                    "Model mismatch for ({key}, {time:?}, {ty:?}): owned stored timer is not active"
                ));
            }
        }

        Ok(())
    }
}

fn cleanup_candidates_for_test(
    watermark: Option<SlabId>,
    highest_loaded: Option<SlabId>,
    now_slab: SlabId,
    active: &ahash::HashSet<SlabId>,
    store_slabs: &BTreeSet<SlabId>,
) -> Vec<SlabId> {
    let Some(highest_loaded) = highest_loaded else {
        return Vec::new();
    };
    let Some(end) = now_slab.checked_sub(1).map(|s| s.min(highest_loaded)) else {
        return Vec::new();
    };
    let start = watermark.map_or(0, |w| w.saturating_add(1));
    if start > end {
        return Vec::new();
    }

    store_slabs
        .range(start..=end)
        .copied()
        .filter(|slab_id| !active.contains(slab_id))
        .collect()
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

#[tokio::test]
async fn test_cleanup_preserves_aborted_timer_slab_and_reload_schedules_it() -> Result<()> {
    let segment = test_segment();
    let store = memory_store(segment.clone());
    store.insert_segment().await?;

    let now = CompactDateTime::now()?;
    let now_slab = Slab::from_time(segment.slab_size, now).id();
    let slab_id = now_slab
        .checked_sub(1)
        .ok_or_else(|| color_eyre::eyre::eyre!("current slab has no predecessor"))?;
    let time = CompactDateTime::from(slab_id.saturating_mul(SLAB_SIZE_SECS));
    let slab = Slab::from_time(segment.slab_size, time);
    let key = Key::from("aborted-cleanup-key");
    let ty = TimerType::Application;
    let trigger = Trigger::new(key.clone(), time, ty, Span::current());

    store.insert_slab(slab).await?;
    store.add_trigger(trigger.clone()).await?;

    let mut state = fresh_state(store.clone(), segment.clone());
    state.known_slab_ids.insert(slab_id);
    state.highest_loaded_slab_id = Some(slab_id);

    let mut triggers = TriggerQueue::new();
    triggers.insert(trigger.clone()).await;
    triggers
        .active_triggers()
        .set_state(&key, time, ty, TimerState::Firing)
        .await;
    triggers
        .active_triggers()
        .set_state(&key, time, ty, TimerState::Aborted)
        .await;
    triggers.remove_queue_only(&trigger);

    cleanup_step(&mut state, &triggers).await;

    let slabs_after_cleanup: BTreeSet<SlabId> =
        store.get_slab_range(0..=SlabId::MAX).try_collect().await?;
    assert!(
        slabs_after_cleanup.contains(&slab_id),
        "cleanup must keep slab metadata while an Aborted timer is active"
    );
    let times_after_cleanup: BTreeSet<CompactDateTime> =
        store.get_key_times(ty, &key).try_collect().await?;
    assert!(
        times_after_cleanup.contains(&time),
        "cleanup must not delete the persisted timer row"
    );

    let mut reloaded_state = fresh_state(store.clone(), segment.clone());
    reloaded_state.last_persisted_watermark = store.get_slab_watermark().await?;
    let mut reloaded_triggers = TriggerQueue::new();
    load_step(&mut reloaded_state, &mut reloaded_triggers).await;

    assert_eq!(
        reloaded_triggers
            .active_triggers()
            .get_state(&key, time, ty)
            .await,
        Some(TimerState::Scheduled),
        "restart/load should reconstruct persisted aborted timers as Scheduled"
    );

    Ok(())
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
        let runtime = match RuntimeBuilder::new_current_thread().enable_all().build() {
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
