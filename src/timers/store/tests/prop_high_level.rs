//! Property-based tests for V2 high-level dual-index operations.
//!
//! Tests the high-level operations that coordinate updates across both slab
//! and key indices, verifying dual-index consistency.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, SegmentId, SegmentVersion, TriggerStore};
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use futures::{StreamExt, TryStreamExt};
use quickcheck::{Arbitrary, Gen};
use std::collections::{BTreeSet, HashSet};
use std::fmt::Debug;
use std::ops::RangeInclusive;
use tracing::Span;
use uuid::Uuid;

/// Type alias for trigger tuple (key, time, `timer_type`).
type TriggerTuple = (Key, CompactDateTime, TimerType);

/// High-level operations that coordinate across dual indices.
/// Only uses the 9 public `TriggerStore` methods.
#[derive(Clone, Debug)]
pub enum HighLevelOperation {
    /// Add a trigger to both slab and key indices (`add_trigger`).
    AddTrigger {
        /// The segment.
        segment: Segment,
        /// The slab.
        slab: Slab,
        /// The trigger to add.
        trigger: Trigger,
    },
    /// Remove a trigger from both slab and key indices (`remove_trigger`).
    RemoveTrigger {
        /// The segment.
        segment: Segment,
        /// The slab.
        slab: Slab,
        /// The key.
        key: Key,
        /// The time.
        time: CompactDateTime,
        /// The timer type.
        timer_type: TimerType,
    },
    /// Delete a slab from storage (`delete_slab`).
    DeleteSlab {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab ID.
        slab_id: SlabId,
    },
    /// Query slab triggers (all types) to verify state
    /// (`get_slab_triggers_all_types`).
    GetSlabTriggersAllTypes {
        /// The slab.
        slab: Slab,
    },
    /// Query key times to verify state (`get_key_times`).
    GetKeyTimes {
        /// The segment ID.
        segment_id: SegmentId,
        /// The timer type.
        timer_type: TimerType,
        /// The key.
        key: Key,
    },
    /// Query key triggers to verify state (`get_key_triggers`).
    GetKeyTriggers {
        /// The segment ID.
        segment_id: SegmentId,
        /// The timer type.
        timer_type: TimerType,
        /// The key.
        key: Key,
    },
    /// Query slab range to verify state (`get_slab_range`).
    GetSlabRange {
        /// The segment ID.
        segment_id: SegmentId,
        /// The range of slab IDs.
        range: RangeInclusive<SlabId>,
    },
    /// Atomically clear all triggers for a key/type and schedule a new one
    /// (`clear_and_schedule`).
    ClearAndSchedule {
        /// The segment.
        segment: Segment,
        /// The new slab for the replacement trigger.
        new_slab: Slab,
        /// The replacement trigger.
        new_trigger: Trigger,
        /// Slabs that contained old triggers to scan and clean.
        old_slabs: Vec<Slab>,
    },
}

/// Test input containing isolated segment IDs and high-level operations.
#[derive(Clone, Debug)]
pub struct HighLevelTestInput {
    /// Pool of segments used by operations in this trial.
    pub segments: Vec<Segment>,
    /// Sequence of operations to apply.
    pub operations: Vec<HighLevelOperation>,
    /// Slab size used for all segments in this test.
    pub slab_size: CompactDuration,
}

/// Helper type for tracking existing triggers during operation generation.
type TriggerKey = (SegmentId, SlabId, Key, CompactDateTime, TimerType);
type ExistingTriggers = HashSet<TriggerKey>;

/// Tracks which slabs contain triggers for each `(SegmentId, Key, TimerType)`.
///
/// Unlike [`ExistingTriggers`], this map is NOT modified by `delete_slab`
/// because `delete_slab` only removes slab metadata—trigger data persists in
/// the slab table. This ensures `clear_and_schedule` can build a complete
/// `old_slabs` list even after `delete_slab` operations.
type KeyTriggerSlabs = HashMap<(SegmentId, Key, TimerType), HashSet<SlabId>>;

/// Tracks trigger times per `(SegmentId, Key, TimerType)`.
///
/// Like [`KeyTriggerSlabs`], this is NOT modified by `delete_slab` because
/// trigger data persists in the store after slab metadata deletion. Used by
/// `generate_clear_and_schedule` to avoid generating new times that collide
/// with existing entries — the adapter's Step 3 would self-delete the new
/// trigger if (slab, time) matches an old entry.
type KeyTriggerTimes = HashMap<(SegmentId, Key, TimerType), HashSet<CompactDateTime>>;

/// Generates a random test key (key-0 through key-4).
fn random_key(g: &mut Gen) -> Key {
    format!("key-{}", u8::arbitrary(g) % 5).into()
}

/// Generates a random timer type.
fn random_timer_type(g: &mut Gen) -> TimerType {
    match u8::arbitrary(g) % 3 {
        0 => TimerType::Application,
        1 => TimerType::DeferredMessage,
        _ => TimerType::DeferredTimer,
    }
}

/// Generates an `AddTrigger` operation.
fn generate_add_trigger(
    g: &mut Gen,
    segment: &Segment,
    existing_triggers: &mut ExistingTriggers,
) -> HighLevelOperation {
    let key = random_key(g);
    let time = CompactDateTime::arbitrary(g);
    let timer_type = random_timer_type(g);
    let slab = Slab::from_time(segment.id, segment.slab_size, time);

    existing_triggers.insert((segment.id, slab.id(), key.clone(), time, timer_type));

    HighLevelOperation::AddTrigger {
        segment: segment.clone(),
        slab: slab.clone(),
        trigger: Trigger::new(key, time, timer_type, Span::current()),
    }
}

/// Generates a `RemoveTrigger` operation (random or from existing triggers).
fn generate_remove_trigger(
    g: &mut Gen,
    segment: &Segment,
    segments: &[Segment],
    existing_triggers: &mut ExistingTriggers,
) -> HighLevelOperation {
    if existing_triggers.is_empty() || bool::arbitrary(g) {
        // Generate random remove (might not exist)
        let key = random_key(g);
        let time = CompactDateTime::arbitrary(g);
        let timer_type = random_timer_type(g);
        let slab = Slab::from_time(segment.id, segment.slab_size, time);

        HighLevelOperation::RemoveTrigger {
            segment: segment.clone(),
            slab,
            key,
            time,
            timer_type,
        }
    } else {
        // Remove an existing trigger
        let keys: Vec<_> = existing_triggers.iter().cloned().collect();
        let (seg_id, slab_id, key, time, timer_type) = &keys[usize::arbitrary(g) % keys.len()];

        existing_triggers.remove(&(*seg_id, *slab_id, key.clone(), *time, *timer_type));

        // Find the matching segment
        let segment = segments
            .iter()
            .find(|s| s.id == *seg_id)
            .unwrap_or(&segments[0])
            .clone();
        let slab = Slab::from_time(*seg_id, segment.slab_size, *time);

        HighLevelOperation::RemoveTrigger {
            segment,
            slab,
            key: key.clone(),
            time: *time,
            timer_type: *timer_type,
        }
    }
}

/// Generates a `DeleteSlab` operation.
fn generate_delete_slab(
    g: &mut Gen,
    segment: &Segment,
    existing_triggers: &mut ExistingTriggers,
) -> HighLevelOperation {
    let time = CompactDateTime::arbitrary(g);
    let slab = Slab::from_time(segment.id, segment.slab_size, time);
    let slab_id = slab.id();

    // Remove all triggers in this slab from existing_triggers
    existing_triggers.retain(|&(seg_id, sid, ..)| !(seg_id == segment.id && sid == slab_id));

    HighLevelOperation::DeleteSlab {
        segment_id: segment.id,
        slab_id,
    }
}

/// Generates a `ClearAndSchedule` operation.
///
/// Uses [`KeyTriggerSlabs`] (not [`ExistingTriggers`]) to build the complete
/// `old_slabs` list, ensuring slabs are included even after `delete_slab`
/// removed them from `ExistingTriggers`.
fn generate_clear_and_schedule(
    g: &mut Gen,
    segment: &Segment,
    existing_triggers: &mut ExistingTriggers,
    key_trigger_slabs: &mut KeyTriggerSlabs,
    key_trigger_times: &mut KeyTriggerTimes,
) -> HighLevelOperation {
    let key = random_key(g);
    let timer_type = random_timer_type(g);

    // Use the durable time tracker (survives delete_slab) to check for
    // collisions. The adapter's Step 3 deletes old entries by exact
    // (slab, type, key, time). If new_time == old_time and new_slab ==
    // old_slab, Step 3 would remove the just-written new entry.
    let map_key = (segment.id, key.clone(), timer_type);
    let existing_times = key_trigger_times.get(&map_key).cloned().unwrap_or_default();

    // Generate a new_time that doesn't collide with any existing trigger.
    let mut new_time = CompactDateTime::arbitrary(g);
    while existing_times.contains(&new_time) {
        new_time = CompactDateTime::arbitrary(g);
    }

    let new_slab = Slab::from_time(segment.id, segment.slab_size, new_time);

    // Build old_slabs from the complete slab tracker (survives delete_slab)
    let old_slabs: Vec<Slab> = key_trigger_slabs
        .get(&map_key)
        .map(|slab_ids| {
            slab_ids
                .iter()
                .map(|&sid| Slab::new(segment.id, sid, segment.slab_size))
                .collect()
        })
        .unwrap_or_default();

    // Remove old triggers from existing_triggers
    existing_triggers
        .retain(|(seg_id, _, k, _, tt)| !(*seg_id == segment.id && *k == key && *tt == timer_type));

    // Track the new trigger in existing_triggers
    existing_triggers.insert((segment.id, new_slab.id(), key.clone(), new_time, timer_type));

    // Reset slab tracker to only the new slab
    let slab_set = key_trigger_slabs.entry(map_key.clone()).or_default();
    slab_set.clear();
    slab_set.insert(new_slab.id());

    // Reset time tracker to only the new time
    let time_set = key_trigger_times.entry(map_key).or_default();
    time_set.clear();
    time_set.insert(new_time);

    HighLevelOperation::ClearAndSchedule {
        segment: segment.clone(),
        new_slab,
        new_trigger: Trigger::new(key, new_time, timer_type, Span::current()),
        old_slabs,
    }
}

/// Generates a `GetSlabTriggersAllTypes` query operation.
fn generate_get_slab_triggers_all_types(g: &mut Gen, segment: &Segment) -> HighLevelOperation {
    let time = CompactDateTime::arbitrary(g);
    let slab = Slab::from_time(segment.id, segment.slab_size, time);
    HighLevelOperation::GetSlabTriggersAllTypes { slab }
}

/// Generates a `GetKeyTimes` query operation.
fn generate_get_key_times(g: &mut Gen, segment: &Segment) -> HighLevelOperation {
    let key = random_key(g);
    let timer_type = random_timer_type(g);
    HighLevelOperation::GetKeyTimes {
        segment_id: segment.id,
        timer_type,
        key,
    }
}

/// Generates a `GetKeyTriggers` query operation.
fn generate_get_key_triggers(g: &mut Gen, segment: &Segment) -> HighLevelOperation {
    let key = random_key(g);
    let timer_type = random_timer_type(g);
    HighLevelOperation::GetKeyTriggers {
        segment_id: segment.id,
        timer_type,
        key,
    }
}

/// Generates a `GetSlabRange` query operation.
fn generate_get_slab_range(g: &mut Gen, segment: &Segment) -> HighLevelOperation {
    let start = u32::arbitrary(g) % 10;
    let end = start + (u32::arbitrary(g) % 5);
    HighLevelOperation::GetSlabRange {
        segment_id: segment.id,
        range: start..=end,
    }
}

impl Arbitrary for HighLevelTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate a slab size for this test (60-660 seconds for faster tests)
        let slab_size = CompactDuration::new((u32::arbitrary(g) % 600) + 60);

        // Generate 2 segments with random UUIDs, all using the same slab_size
        let segments: Vec<Segment> = (0_u32..2_u32)
            .map(|i| Segment {
                id: Uuid::new_v4(),
                name: format!("segment-{i}"),
                slab_size,
                version: SegmentVersion::V2,
            })
            .collect();

        // Generate 10-50 operations
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        // Track which triggers exist to generate valid removes
        let mut existing_triggers = ExistingTriggers::default();
        // Track slab locations per (segment, key, type) — survives delete_slab
        let mut key_trigger_slabs = KeyTriggerSlabs::default();
        // Track trigger times per (segment, key, type) — survives delete_slab
        let mut key_trigger_times = KeyTriggerTimes::default();

        for _ in 0..op_count {
            let segment_idx = usize::from(u8::arbitrary(g)) % segments.len();
            let segment = &segments[segment_idx];

            let op = match u8::arbitrary(g) % 8 {
                0 => {
                    let op = generate_add_trigger(g, segment, &mut existing_triggers);
                    // Mirror add into slab + time trackers
                    if let HighLevelOperation::AddTrigger {
                        ref trigger,
                        ref slab,
                        ..
                    } = op
                    {
                        let map_key = (segment.id, trigger.key.clone(), trigger.timer_type);
                        key_trigger_slabs
                            .entry(map_key.clone())
                            .or_default()
                            .insert(slab.id());
                        key_trigger_times
                            .entry(map_key)
                            .or_default()
                            .insert(trigger.time);
                    }
                    op
                }
                1 => {
                    let op = generate_remove_trigger(g, segment, &segments, &mut existing_triggers);
                    // Mirror remove: update slab + time trackers
                    if let HighLevelOperation::RemoveTrigger {
                        ref segment,
                        ref key,
                        timer_type,
                        time,
                        ..
                    } = op
                    {
                        let slab = Slab::from_time(segment.id, segment.slab_size, time);
                        let map_key = (segment.id, key.clone(), timer_type);
                        let has_more = existing_triggers.iter().any(|(seg, sid, k, _, tt)| {
                            *seg == segment.id && *sid == slab.id() && k == key && *tt == timer_type
                        });
                        if !has_more && let Some(slab_set) = key_trigger_slabs.get_mut(&map_key) {
                            slab_set.remove(&slab.id());
                        }
                        // Remove time if no more triggers at this time for key/type
                        let has_time = existing_triggers.iter().any(|(seg, _, k, t, tt)| {
                            *seg == segment.id && k == key && *tt == timer_type && *t == time
                        });
                        if !has_time && let Some(time_set) = key_trigger_times.get_mut(&map_key) {
                            time_set.remove(&time);
                        }
                    }
                    op
                }
                2 => generate_delete_slab(g, segment, &mut existing_triggers),
                3 => generate_clear_and_schedule(
                    g,
                    segment,
                    &mut existing_triggers,
                    &mut key_trigger_slabs,
                    &mut key_trigger_times,
                ),
                4 => generate_get_slab_triggers_all_types(g, segment),
                5 => generate_get_key_times(g, segment),
                6 => generate_get_key_triggers(g, segment),
                _ => generate_get_slab_range(g, segment),
            };

            operations.push(op);
        }

        Self {
            segments,
            operations,
            slab_size,
        }
    }
}

/// Reference model tracking dual indices.
#[derive(Clone, Debug)]
pub struct HighLevelModel {
    /// Triggers indexed by (`segment_id`, `slab_id`, `timer_type`).
    slab_index: HashMap<(SegmentId, SlabId, TimerType), BTreeSet<TriggerTuple>>,
    /// Triggers indexed by (`segment_id`, key, `timer_type`).
    key_index: HashMap<(SegmentId, Key, TimerType), BTreeSet<TriggerTuple>>,
}

impl Default for HighLevelModel {
    fn default() -> Self {
        Self::new()
    }
}

impl HighLevelModel {
    /// Creates a new empty model.
    #[must_use]
    pub fn new() -> Self {
        Self {
            slab_index: HashMap::default(),
            key_index: HashMap::default(),
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &HighLevelOperation) {
        match op {
            HighLevelOperation::AddTrigger {
                segment,
                slab,
                trigger,
            } => {
                let tuple = (trigger.key.clone(), trigger.time, trigger.timer_type);

                // Add to slab index
                self.slab_index
                    .entry((segment.id, slab.id(), trigger.timer_type))
                    .or_default()
                    .insert(tuple.clone());

                // Add to key index
                self.key_index
                    .entry((segment.id, trigger.key.clone(), trigger.timer_type))
                    .or_default()
                    .insert(tuple);
            }
            HighLevelOperation::RemoveTrigger {
                segment,
                slab,
                key,
                time,
                timer_type,
            } => {
                let tuple = (key.clone(), *time, *timer_type);

                // Remove from slab index
                if let Some(triggers) =
                    self.slab_index
                        .get_mut(&(segment.id, slab.id(), *timer_type))
                {
                    triggers.remove(&tuple);
                }

                // Remove from key index
                if let Some(triggers) =
                    self.key_index
                        .get_mut(&(segment.id, key.clone(), *timer_type))
                {
                    triggers.remove(&tuple);
                }
            }
            HighLevelOperation::ClearAndSchedule {
                segment,
                new_slab,
                new_trigger,
                old_slabs,
            } => {
                let key = &new_trigger.key;
                let timer_type = new_trigger.timer_type;

                // Step 1: Clear key index for this (segment, key, timer_type)
                if let Some(triggers) =
                    self.key_index
                        .get_mut(&(segment.id, key.clone(), timer_type))
                {
                    triggers.clear();
                }

                // Step 2: Remove matching triggers from old slabs in slab index
                for old_slab in old_slabs {
                    if let Some(triggers) =
                        self.slab_index
                            .get_mut(&(segment.id, old_slab.id(), timer_type))
                    {
                        triggers.retain(|(k, ..)| k != key);
                    }
                }

                // Step 3: Add new trigger to both indices
                let tuple = (key.clone(), new_trigger.time, timer_type);

                self.slab_index
                    .entry((segment.id, new_slab.id(), timer_type))
                    .or_default()
                    .insert(tuple.clone());

                self.key_index
                    .entry((segment.id, key.clone(), timer_type))
                    .or_default()
                    .insert(tuple);
            }
            HighLevelOperation::DeleteSlab { .. }
            | HighLevelOperation::GetSlabTriggersAllTypes { .. }
            | HighLevelOperation::GetKeyTimes { .. }
            | HighLevelOperation::GetKeyTriggers { .. }
            | HighLevelOperation::GetSlabRange { .. } => {
                // delete_slab only removes slab metadata, NOT triggers
                // Triggers remain in both slab_triggers and key_triggers tables
                // until explicitly removed with remove_trigger
                // The model tracks trigger data, not metadata, so this is a
                // no-op
                //
                // Query operations also don't modify state
            }
        }
    }

    /// Gets triggers from slab index.
    #[must_use]
    pub fn get_slab_triggers(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
        timer_type: TimerType,
    ) -> Vec<TriggerTuple> {
        self.slab_index
            .get(&(*segment_id, slab_id, timer_type))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Gets triggers from key index.
    #[must_use]
    pub fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Vec<TriggerTuple> {
        self.key_index
            .get(&(*segment_id, key.clone(), timer_type))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }
}

/// Verifies `get_slab_triggers_all_types` query against model.
async fn verify_slab_triggers_all_types<S>(
    store: &S,
    model: &HighLevelModel,
    slab: &Slab,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Get expected from model - collect all triggers for this slab across ALL timer
    // types
    let mut expected = BTreeSet::new();
    let target_seg_id = slab.segment_id();
    let target_slab_id = slab.id();
    for ((seg_id, slab_id, _timer_type), trigger_set) in &model.slab_index {
        if seg_id == target_seg_id && *slab_id == target_slab_id {
            expected.extend(trigger_set.iter().cloned());
        }
    }

    let actual: Vec<Trigger> = store
        .get_slab_triggers_all_types(slab)
        .try_collect()
        .await
        .map_err(|e| {
            color_eyre::eyre::eyre!("Op #{op_idx} GetSlabTriggersAllTypes failed: {e:?}")
        })?;

    let actual_tuples: BTreeSet<TriggerTuple> = actual
        .iter()
        .map(|t| (t.key.clone(), t.time, t.timer_type))
        .collect();

    if expected != actual_tuples {
        return Err(color_eyre::eyre::eyre!(
            "Op #{op_idx} GetSlabTriggersAllTypes mismatch: expected {expected:?}, got \
             {actual_tuples:?}"
        ));
    }
    Ok(())
}

/// Verifies `get_key_times` query against model.
async fn verify_key_times<S>(
    store: &S,
    model: &HighLevelModel,
    segment_id: &SegmentId,
    timer_type: TimerType,
    key: &Key,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let expected: BTreeSet<CompactDateTime> = model
        .get_key_triggers(segment_id, key, timer_type)
        .iter()
        .map(|(_, time, _)| *time)
        .collect();

    let actual: Vec<CompactDateTime> = store
        .get_key_times(segment_id, timer_type, key)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetKeyTimes failed: {e:?}"))?;

    let actual_set: BTreeSet<CompactDateTime> = actual.into_iter().collect();

    if expected != actual_set {
        return Err(color_eyre::eyre::eyre!(
            "Op #{op_idx} GetKeyTimes mismatch: expected {expected:?}, got {actual_set:?}"
        ));
    }
    Ok(())
}

/// Verifies `get_key_triggers` query against model.
async fn verify_key_triggers<S>(
    store: &S,
    model: &HighLevelModel,
    segment_id: &SegmentId,
    timer_type: TimerType,
    key: &Key,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let expected: BTreeSet<TriggerTuple> = model
        .get_key_triggers(segment_id, key, timer_type)
        .into_iter()
        .collect();

    let actual: Vec<Trigger> = store
        .get_key_triggers(segment_id, timer_type, key)
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetKeyTriggers failed: {e:?}"))?;

    let actual_tuples: BTreeSet<TriggerTuple> = actual
        .iter()
        .map(|t| (t.key.clone(), t.time, t.timer_type))
        .collect();

    if expected != actual_tuples {
        return Err(color_eyre::eyre::eyre!(
            "Op #{op_idx} GetKeyTriggers mismatch: expected {expected:?}, got {actual_tuples:?}"
        ));
    }
    Ok(())
}

/// Verifies `get_slab_range` query against model.
async fn verify_slab_range<S>(
    store: &S,
    model: &HighLevelModel,
    segment_id: &SegmentId,
    range: &RangeInclusive<SlabId>,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let expected: BTreeSet<SlabId> = model
        .slab_index
        .keys()
        .filter(|(seg_id, slab_id, _)| *seg_id == *segment_id && range.contains(slab_id))
        .map(|(_, slab_id, _)| *slab_id)
        .collect();

    let actual: Vec<SlabId> = store
        .get_slab_range(segment_id, range.clone())
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetSlabRange failed: {e:?}"))?;

    let actual_set: BTreeSet<SlabId> = actual.into_iter().collect();

    if expected != actual_set {
        return Err(color_eyre::eyre::eyre!(
            "Op #{op_idx} GetSlabRange mismatch: expected {expected:?}, got {actual_set:?}"
        ));
    }
    Ok(())
}

/// Applies high-level operations with inline verification.
async fn apply_high_level_operations<S>(
    store: &S,
    model: &mut HighLevelModel,
    operations: &[HighLevelOperation],
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    for (op_idx, op) in operations.iter().enumerate() {
        match op {
            HighLevelOperation::AddTrigger {
                segment,
                slab,
                trigger,
            } => {
                model.apply(op);
                store
                    .add_trigger(segment, slab.clone(), trigger.clone())
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} AddTrigger failed: {e:?}")
                    })?;
            }
            HighLevelOperation::RemoveTrigger {
                segment,
                slab,
                key,
                time,
                timer_type,
            } => {
                model.apply(op);
                store
                    .remove_trigger(segment, slab, key, *time, *timer_type)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} RemoveTrigger failed: {e:?}")
                    })?;
            }
            HighLevelOperation::DeleteSlab {
                segment_id,
                slab_id,
            } => {
                model.apply(op);
                store.delete_slab(segment_id, *slab_id).await.map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} DeleteSlab failed: {e:?}")
                })?;
            }
            HighLevelOperation::ClearAndSchedule {
                segment,
                new_slab,
                new_trigger,
                old_slabs,
            } => {
                model.apply(op);
                store
                    .clear_and_schedule(
                        segment,
                        new_slab.clone(),
                        new_trigger.clone(),
                        old_slabs.clone(),
                    )
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} ClearAndSchedule failed: {e:?}")
                    })?;
            }
            HighLevelOperation::GetSlabTriggersAllTypes { slab } => {
                verify_slab_triggers_all_types(store, model, slab, op_idx).await?;
            }
            HighLevelOperation::GetKeyTimes {
                segment_id,
                timer_type,
                key,
            } => {
                verify_key_times(store, model, segment_id, *timer_type, key, op_idx).await?;
            }
            HighLevelOperation::GetKeyTriggers {
                segment_id,
                timer_type,
                key,
            } => {
                verify_key_triggers(store, model, segment_id, *timer_type, key, op_idx).await?;
            }
            HighLevelOperation::GetSlabRange { segment_id, range } => {
                verify_slab_range(store, model, segment_id, range, op_idx).await?;
            }
        }
    }
    Ok(())
}

/// Cleans up all test data using only public `TriggerStore` API.
async fn cleanup_test_data<S>(
    store: &S,
    model: &HighLevelModel,
    segments: &[Segment],
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Remove all triggers from the model
    for ((segment_id, _slab_id, _timer_type), trigger_set) in &model.slab_index {
        let segment = segments
            .iter()
            .find(|s| s.id == *segment_id)
            .ok_or_else(|| color_eyre::eyre::eyre!("Segment not found during cleanup"))?;

        for (key, time, timer_type) in trigger_set {
            let slab = Slab::from_time(*segment_id, segment.slab_size, *time);
            // Ignore errors during cleanup - triggers may have been removed by test
            // operations
            let _ = store
                .remove_trigger(segment, &slab, key, *time, *timer_type)
                .await;
        }
    }

    // Delete all slabs that were created
    let mut deleted_slabs = HashSet::new();
    for (segment_id, slab_id, _timer_type) in model.slab_index.keys() {
        if deleted_slabs.insert((*segment_id, *slab_id)) {
            // Ignore errors - slabs may have been deleted by test operations
            let _ = store.delete_slab(segment_id, *slab_id).await;
        }
    }

    Ok(())
}

/// Verifies dual-index consistency by comparing all triggers in slab and key
/// indices.
///
/// This is the critical verification that high-level operations maintain
/// consistency across both indices.
async fn verify_dual_index_consistency<S>(
    store: &S,
    model: &HighLevelModel,
    segments: &[Segment],
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    use std::collections::HashSet;

    // Collect all triggers from slab indices using get_slab_triggers_all_types
    let mut slab_triggers = HashSet::new();
    let mut processed_slabs = HashSet::new();

    for (segment_id, slab_id, _timer_type) in model.slab_index.keys() {
        // Only process each slab once (since get_slab_triggers_all_types returns all
        // timer types)
        if !processed_slabs.insert((*segment_id, *slab_id)) {
            continue;
        }

        let segment = segments
            .iter()
            .find(|s| s.id == *segment_id)
            .unwrap_or(&segments[0]);
        let slab = Slab::new(*segment_id, *slab_id, segment.slab_size);

        let triggers: Vec<Trigger> = store
            .get_slab_triggers_all_types(&slab)
            .try_collect()
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!("Failed to get slab triggers for consistency check: {e:?}")
            })?;

        for trigger in triggers {
            slab_triggers.insert((
                trigger.key.clone(),
                trigger.time,
                trigger.timer_type,
                *segment_id,
            ));
        }
    }

    // Collect all triggers from key indices
    let mut key_triggers = HashSet::new();
    for (segment_id, key, timer_type) in model.key_index.keys() {
        let triggers: Vec<Trigger> = store
            .get_key_triggers(segment_id, *timer_type, key)
            .try_collect()
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!("Failed to get key triggers for consistency check: {e:?}")
            })?;

        for trigger in triggers {
            key_triggers.insert((
                trigger.key.clone(),
                trigger.time,
                trigger.timer_type,
                *segment_id,
            ));
        }
    }

    // Verify both indices contain exactly the same triggers
    if slab_triggers != key_triggers {
        let slab_only: Vec<_> = slab_triggers.difference(&key_triggers).collect();
        let key_only: Vec<_> = key_triggers.difference(&slab_triggers).collect();

        return Err(color_eyre::eyre::eyre!(
            "Dual-index consistency violation!\nTriggers in slab index but not key index: \
             {slab_only:?}\nTriggers in key index but not slab index: {key_only:?}"
        ));
    }

    // Verify store matches model - collect all triggers from model
    let mut model_triggers = HashSet::new();
    for ((segment_id, _slab_id, _timer_type), trigger_set) in &model.slab_index {
        for (key, time, timer_type) in trigger_set {
            model_triggers.insert((key.clone(), *time, *timer_type, *segment_id));
        }
    }

    // Compare store vs model
    if slab_triggers != model_triggers {
        let store_only: Vec<_> = slab_triggers.difference(&model_triggers).collect();
        let model_only: Vec<_> = model_triggers.difference(&slab_triggers).collect();

        return Err(color_eyre::eyre::eyre!(
            "Store does not match model!\nTriggers in store but not model: \
             {store_only:?}\nTriggers in model but not store: {model_only:?}"
        ));
    }

    Ok(())
}

/// Verifies that high-level operations maintain dual-index consistency.
///
/// # Errors
///
/// Returns an error if dual-index consistency is violated or store operations
/// fail.
pub async fn prop_high_level_dual_index_consistency<S>(
    store: &S,
    input: HighLevelTestInput,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Insert all segments first
    for segment in &input.segments {
        store
            .insert_segment(segment.clone())
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to insert segment: {e:?}"))?;
    }

    let mut model = HighLevelModel::new();

    // Apply operations and verify, ensuring cleanup happens even if test fails
    let result = async {
        apply_high_level_operations(store, &mut model, &input.operations).await?;
        // CRITICAL: Verify dual-index consistency BEFORE cleanup
        verify_dual_index_consistency(store, &model, &input.segments).await?;
        Ok(())
    }
    .await;

    // Always cleanup, even if test failed
    cleanup_test_data(store, &model, &input.segments).await?;

    // Return the verification result
    result
}
