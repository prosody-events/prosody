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
type StoreTriggerTuple = (Key, CompactDateTime, TimerType, SegmentId);

/// High-level operations that coordinate across dual indices.
/// Only uses the 9 public `TriggerStore` methods.
#[derive(Clone, Debug)]
pub enum HighLevelOperation {
    /// Add a trigger to both slab and key indices (`add_trigger`).
    AddTrigger {
        /// The segment.
        segment: Segment,
        /// The trigger to add.
        trigger: Trigger,
    },
    /// Remove a trigger from both slab and key indices (`remove_trigger`).
    RemoveTrigger {
        /// The segment.
        segment: Segment,
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
        /// The slab ID.
        slab_id: SlabId,
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
        /// The replacement trigger.
        new_trigger: Trigger,
    },
    /// Update the tag on a specific trigger (`update_tag`).
    UpdateTag {
        /// The segment.
        segment: Segment,
        /// The key of the trigger to update.
        key: Key,
        /// The scheduled time.
        time: CompactDateTime,
        /// The timer type.
        timer_type: TimerType,
        /// The new tag value to write.
        new_tag: i32,
    },
    /// Query the current tag for a trigger (`current_tag`).
    CurrentTag {
        /// The segment.
        segment: Segment,
        /// The key.
        key: Key,
        /// The scheduled time.
        time: CompactDateTime,
        /// The timer type.
        timer_type: TimerType,
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

/// Tracks trigger times per `(SegmentId, Key, TimerType)`.
///
/// NOT modified by `delete_slab` because trigger data persists in the store
/// after slab metadata deletion. Used by `generate_clear_and_schedule` to
/// avoid generating new times that collide with existing entries — the
/// adapter's Step 3 would self-delete the new trigger if the new time matches
/// an old entry still in the slab index.
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

fn derive_tag(key: &Key, time: CompactDateTime, timer_type: TimerType) -> i32 {
    let mut hash = 0x811c_9dc5_u32;
    for byte in key.as_ref().as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash ^= time.epoch_seconds();
    hash = hash.wrapping_mul(0x0100_0193);
    hash ^= timer_type as u32;
    i32::from_le_bytes(hash.to_le_bytes())
}

fn model_tag_for_trigger(
    model: &HighLevelModel,
    segment_id: SegmentId,
    trigger: &Trigger,
) -> Option<i32> {
    model.get_tag(segment_id, &trigger.key, trigger.timer_type, trigger.time)
}

/// Generates an `AddTrigger` operation.
fn generate_add_trigger(
    g: &mut Gen,
    segment: &Segment,
    existing_triggers: &mut ExistingTriggers,
) -> HighLevelOperation {
    let key = random_key(g);
    let timer_type = random_timer_type(g);
    let mut time = CompactDateTime::arbitrary(g);
    let mut slab_id = Slab::from_time(segment.slab_size, time).id();
    while existing_triggers.contains(&(segment.id, slab_id, key.clone(), time, timer_type)) {
        time = CompactDateTime::arbitrary(g);
        slab_id = Slab::from_time(segment.slab_size, time).id();
    }

    existing_triggers.insert((segment.id, slab_id, key.clone(), time, timer_type));

    let tag = derive_tag(&key, time, timer_type);
    HighLevelOperation::AddTrigger {
        segment: segment.clone(),
        trigger: Trigger::with_tag(key, time, timer_type, tag, Span::current()),
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

        HighLevelOperation::RemoveTrigger {
            segment: segment.clone(),
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

        HighLevelOperation::RemoveTrigger {
            segment,
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
    let slab = Slab::from_time(segment.slab_size, time);
    let slab_id = slab.id();

    // Remove all triggers in this slab from existing_triggers
    existing_triggers.retain(|&(seg_id, sid, ..)| !(seg_id == segment.id && sid == slab_id));

    HighLevelOperation::DeleteSlab {
        segment_id: segment.id,
        slab_id,
    }
}

/// Generates a `ClearAndSchedule` operation.
fn generate_clear_and_schedule(
    g: &mut Gen,
    segment: &Segment,
    existing_triggers: &mut ExistingTriggers,
    key_trigger_times: &mut KeyTriggerTimes,
) -> HighLevelOperation {
    let key = random_key(g);
    let timer_type = random_timer_type(g);

    // Use the durable time tracker (survives delete_slab) to check for
    // collisions. The adapter's Step 3 deletes old entries by exact
    // (key, time). If new_time == old_time, Step 3 would remove the
    // just-written new entry.
    let map_key = (segment.id, key.clone(), timer_type);
    let existing_times = key_trigger_times.get(&map_key).cloned().unwrap_or_default();

    // Generate a new_time that doesn't collide with any existing trigger.
    let mut new_time = CompactDateTime::arbitrary(g);
    while existing_times.contains(&new_time) {
        new_time = CompactDateTime::arbitrary(g);
    }

    let new_slab_id = Slab::from_time(segment.slab_size, new_time).id();

    // Remove old triggers from existing_triggers
    existing_triggers
        .retain(|(seg_id, _, k, _, tt)| !(*seg_id == segment.id && *k == key && *tt == timer_type));

    // Track the new trigger in existing_triggers
    existing_triggers.insert((segment.id, new_slab_id, key.clone(), new_time, timer_type));

    // Reset time tracker to only the new time
    let time_set = key_trigger_times.entry(map_key).or_default();
    time_set.clear();
    time_set.insert(new_time);

    let tag = derive_tag(&key, new_time, timer_type);
    HighLevelOperation::ClearAndSchedule {
        segment: segment.clone(),
        new_trigger: Trigger::with_tag(key, new_time, timer_type, tag, Span::current()),
    }
}

/// Generates a `GetSlabTriggersAllTypes` query operation.
fn generate_get_slab_triggers_all_types(g: &mut Gen, segment: &Segment) -> HighLevelOperation {
    let time = CompactDateTime::arbitrary(g);
    let slab_id = Slab::from_time(segment.slab_size, time).id();
    HighLevelOperation::GetSlabTriggersAllTypes { slab_id }
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

/// Generates an `UpdateTag` operation targeting an existing trigger.
///
/// `update_tag` requires the caller to have observed the target as
/// currently scheduled — the Cassandra store would write a partial row
/// otherwise. The generator encodes this precondition by only picking from
/// `existing_triggers`; if none exist, no `UpdateTag` op is generated.
fn generate_update_tag(
    g: &mut Gen,
    segment: &Segment,
    existing_triggers: &ExistingTriggers,
) -> Option<HighLevelOperation> {
    if existing_triggers.is_empty() {
        return None;
    }
    let new_tag = i32::arbitrary(g);
    let keys: Vec<_> = existing_triggers.iter().cloned().collect();
    let (_, _, key, time, timer_type) = &keys[usize::arbitrary(g) % keys.len()];
    Some(HighLevelOperation::UpdateTag {
        segment: segment.clone(),
        key: key.clone(),
        time: *time,
        timer_type: *timer_type,
        new_tag,
    })
}

/// Generates a `CurrentTag` query operation.
fn generate_current_tag(
    g: &mut Gen,
    segment: &Segment,
    existing_triggers: &ExistingTriggers,
) -> HighLevelOperation {
    if !existing_triggers.is_empty() && bool::arbitrary(g) {
        let keys: Vec<_> = existing_triggers.iter().cloned().collect();
        let (_, _, key, time, timer_type) = &keys[usize::arbitrary(g) % keys.len()];
        return HighLevelOperation::CurrentTag {
            segment: segment.clone(),
            key: key.clone(),
            time: *time,
            timer_type: *timer_type,
        };
    }
    HighLevelOperation::CurrentTag {
        segment: segment.clone(),
        key: random_key(g),
        time: CompactDateTime::arbitrary(g),
        timer_type: random_timer_type(g),
    }
}

impl Arbitrary for HighLevelTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate a slab size for this test (60-660 seconds for faster tests)
        let slab_size = CompactDuration::new((u32::arbitrary(g) % 600) + 60);

        // Single segment per trial — matches the production invariant that each
        // store instance is scoped to one partition (one segment). Using multiple
        // segments would break the per-partition state cache.
        let segments: Vec<Segment> = vec![Segment {
            id: Uuid::new_v4(),
            name: "segment-0".to_owned(),
            slab_size,
            version: SegmentVersion::V3,
        }];

        // Generate 10-50 operations
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        // Track which triggers exist to generate valid removes
        let mut existing_triggers = ExistingTriggers::default();
        // Track trigger times per (segment, key, type) — survives delete_slab
        let mut key_trigger_times = KeyTriggerTimes::default();

        for _ in 0..op_count {
            let segment_idx = usize::from(u8::arbitrary(g)) % segments.len();
            let segment = &segments[segment_idx];

            let op = match u8::arbitrary(g) % 10 {
                0 => {
                    let op = generate_add_trigger(g, segment, &mut existing_triggers);
                    if let HighLevelOperation::AddTrigger { ref trigger, .. } = op {
                        let map_key = (segment.id, trigger.key.clone(), trigger.timer_type);
                        key_trigger_times
                            .entry(map_key)
                            .or_default()
                            .insert(trigger.time);
                    }
                    op
                }
                1 => {
                    let op = generate_remove_trigger(g, segment, &segments, &mut existing_triggers);
                    if let HighLevelOperation::RemoveTrigger {
                        ref segment,
                        ref key,
                        timer_type,
                        time,
                        ..
                    } = op
                    {
                        let map_key = (segment.id, key.clone(), timer_type);
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
                    &mut key_trigger_times,
                ),
                4 => generate_get_slab_triggers_all_types(g, segment),
                5 => generate_get_key_times(g, segment),
                6 => generate_get_key_triggers(g, segment),
                7 => generate_get_slab_range(g, segment),
                8 => match generate_update_tag(g, segment, &existing_triggers) {
                    Some(op) => op,
                    // No existing triggers yet — fall back to a query op so this
                    // iteration still produces something useful.
                    None => generate_current_tag(g, segment, &existing_triggers),
                },
                _ => generate_current_tag(g, segment, &existing_triggers),
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
    /// Slab metadata registry — tracks which slab IDs have been registered
    /// via `insert_slab`. `delete_slab` removes entries here, mirroring the
    /// real store where slab metadata is separate from trigger data.
    slab_registry: HashMap<SegmentId, BTreeSet<SlabId>>,
    /// Tag for each key-table row: `(segment_id, key, timer_type, time)` → tag.
    /// Only present for triggers currently in the key index.
    tag_index: HashMap<(SegmentId, Key, TimerType, CompactDateTime), i32>,
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
            slab_registry: HashMap::default(),
            tag_index: HashMap::default(),
        }
    }

    /// Returns the model's current tag for a row, or `None` if absent.
    #[must_use]
    pub fn get_tag(
        &self,
        segment_id: SegmentId,
        key: &Key,
        timer_type: TimerType,
        time: CompactDateTime,
    ) -> Option<i32> {
        self.tag_index
            .get(&(segment_id, key.clone(), timer_type, time))
            .copied()
    }

    fn set_tag(
        &mut self,
        segment_id: SegmentId,
        key: Key,
        timer_type: TimerType,
        time: CompactDateTime,
        tag: i32,
    ) {
        self.tag_index
            .insert((segment_id, key, timer_type, time), tag);
    }

    fn remove_tag(
        &mut self,
        segment_id: SegmentId,
        key: &Key,
        timer_type: TimerType,
        time: CompactDateTime,
    ) {
        self.tag_index
            .remove(&(segment_id, key.clone(), timer_type, time));
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &HighLevelOperation) {
        match op {
            HighLevelOperation::AddTrigger { segment, trigger } => {
                let slab_id = Slab::from_time(segment.slab_size, trigger.time).id();
                let tuple = (trigger.key.clone(), trigger.time, trigger.timer_type);
                self.slab_index
                    .entry((segment.id, slab_id, trigger.timer_type))
                    .or_default()
                    .insert(tuple.clone());
                self.key_index
                    .entry((segment.id, trigger.key.clone(), trigger.timer_type))
                    .or_default()
                    .insert(tuple);
                self.slab_registry
                    .entry(segment.id)
                    .or_default()
                    .insert(slab_id);
                self.set_tag(
                    segment.id,
                    trigger.key.clone(),
                    trigger.timer_type,
                    trigger.time,
                    trigger.tag,
                );
            }
            HighLevelOperation::RemoveTrigger {
                segment,
                key,
                time,
                timer_type,
            } => {
                let slab_id = Slab::from_time(segment.slab_size, *time).id();
                let tuple = (key.clone(), *time, *timer_type);
                if let Some(t) = self.slab_index.get_mut(&(segment.id, slab_id, *timer_type)) {
                    t.remove(&tuple);
                }
                if let Some(t) = self
                    .key_index
                    .get_mut(&(segment.id, key.clone(), *timer_type))
                {
                    t.remove(&tuple);
                }
                self.remove_tag(segment.id, key, *timer_type, *time);
            }
            HighLevelOperation::ClearAndSchedule {
                segment,
                new_trigger,
            } => {
                self.apply_clear_and_schedule(segment, new_trigger);
            }
            HighLevelOperation::UpdateTag {
                segment,
                key,
                time,
                timer_type,
                new_tag,
            } => {
                if self.get_tag(segment.id, key, *timer_type, *time).is_some() {
                    self.set_tag(segment.id, key.clone(), *timer_type, *time, *new_tag);
                }
            }
            HighLevelOperation::DeleteSlab {
                segment_id,
                slab_id,
            } => {
                if let Some(slabs) = self.slab_registry.get_mut(segment_id) {
                    slabs.remove(slab_id);
                }
            }
            HighLevelOperation::GetSlabTriggersAllTypes { .. }
            | HighLevelOperation::GetKeyTimes { .. }
            | HighLevelOperation::GetKeyTriggers { .. }
            | HighLevelOperation::GetSlabRange { .. }
            | HighLevelOperation::CurrentTag { .. } => {}
        }
    }

    fn apply_clear_and_schedule(&mut self, segment: &Segment, new_trigger: &Trigger) {
        let key = &new_trigger.key;
        let timer_type = new_trigger.timer_type;
        let new_slab_id = Slab::from_time(segment.slab_size, new_trigger.time).id();
        let index_key = (segment.id, key.clone(), timer_type);
        let old_entry = self.key_index.remove(&index_key).unwrap_or_default();

        let tuple = (key.clone(), new_trigger.time, timer_type);
        self.slab_index
            .entry((segment.id, new_slab_id, timer_type))
            .or_default()
            .insert(tuple.clone());
        self.key_index.insert(index_key, BTreeSet::from([tuple]));
        self.slab_registry
            .entry(segment.id)
            .or_default()
            .insert(new_slab_id);

        for (_, old_time, _) in &old_entry {
            self.remove_tag(segment.id, key, timer_type, *old_time);
            let old_slab_id = Slab::from_time(segment.slab_size, *old_time).id();
            if let Some(s) = self
                .slab_index
                .get_mut(&(segment.id, old_slab_id, timer_type))
            {
                s.retain(|(k, t, _)| k != key || *t != *old_time);
            }
        }
        self.set_tag(
            segment.id,
            key.clone(),
            timer_type,
            new_trigger.time,
            new_trigger.tag,
        );
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
    slab_id: SlabId,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Get expected from model - collect all triggers for this slab across ALL timer
    // types. The store is single-segment, so segment filtering is not needed here.
    let mut expected = BTreeSet::new();
    for ((_seg_id, sid, _timer_type), trigger_set) in &model.slab_index {
        if *sid == slab_id {
            expected.extend(trigger_set.iter().cloned());
        }
    }

    let actual: Vec<Trigger> = store
        .get_slab_triggers_all_types(slab_id)
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

    for trigger in &actual {
        let key_tag = store
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!(
                    "Op #{op_idx} GetSlabTriggersAllTypes current_tag failed: {e:?}"
                )
            })?;
        let Some(key_tag) = key_tag else {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} GetSlabTriggersAllTypes returned slab trigger without key tag: \
                 key={:?} time={:?} type={:?}",
                trigger.key,
                trigger.time,
                trigger.timer_type
            ));
        };
        if trigger.tag != key_tag {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} GetSlabTriggersAllTypes tag mismatch: key={:?} time={:?} \
                 type={:?} expected key tag {key_tag:?}, got {:?}",
                trigger.key,
                trigger.time,
                trigger.timer_type,
                trigger.tag
            ));
        }

        if let Some(expected_tag) = model_tag_for_trigger(model, store.segment_id(), trigger)
            && trigger.tag != expected_tag
        {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} GetSlabTriggersAllTypes model tag mismatch: key={:?} time={:?} \
                 type={:?} expected {expected_tag:?}, got {:?}",
                trigger.key,
                trigger.time,
                trigger.timer_type,
                trigger.tag
            ));
        }
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
        .get_key_times(timer_type, key)
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
        .get_key_triggers(timer_type, key)
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

    for trigger in &actual {
        let Some(expected_tag) = model_tag_for_trigger(model, *segment_id, trigger) else {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} GetKeyTriggers returned unmodeled trigger: key={:?} time={:?} \
                 type={:?}",
                trigger.key,
                trigger.time,
                trigger.timer_type
            ));
        };
        if trigger.tag != expected_tag {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} GetKeyTriggers tag mismatch: key={:?} time={:?} type={:?} \
                 expected {expected_tag:?}, got {:?}",
                trigger.key,
                trigger.time,
                trigger.timer_type,
                trigger.tag
            ));
        }
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
        .slab_registry
        .get(segment_id)
        .into_iter()
        .flatten()
        .filter(|slab_id| range.contains(slab_id))
        .copied()
        .collect();

    let actual: Vec<SlabId> = store
        .get_slab_range(range.clone())
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

/// Verifies `current_tag` against the model's tag index.
async fn verify_current_tag<S>(
    store: &S,
    model: &HighLevelModel,
    segment: &Segment,
    key: &Key,
    time: CompactDateTime,
    timer_type: TimerType,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let expected = model.get_tag(segment.id, key, timer_type, time);
    let actual = store
        .current_tag(key, time, timer_type)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} CurrentTag failed: {e:?}"))?;

    if expected != actual {
        return Err(color_eyre::eyre::eyre!(
            "Op #{op_idx} CurrentTag mismatch key={key} time={time:?} type={timer_type:?}: \
             model={expected:?}, store={actual:?}"
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
        let mutating = matches!(
            op,
            HighLevelOperation::AddTrigger { .. }
                | HighLevelOperation::RemoveTrigger { .. }
                | HighLevelOperation::DeleteSlab { .. }
                | HighLevelOperation::ClearAndSchedule { .. }
                | HighLevelOperation::UpdateTag { .. }
        );

        match op {
            HighLevelOperation::AddTrigger { trigger, .. } => {
                model.apply(op);
                store.add_trigger(trigger.clone()).await.map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} AddTrigger failed: {e:?}")
                })?;
            }
            HighLevelOperation::RemoveTrigger {
                key,
                time,
                timer_type,
                ..
            } => {
                model.apply(op);
                store
                    .remove_trigger(key, *time, *timer_type)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} RemoveTrigger failed: {e:?}")
                    })?;
            }
            HighLevelOperation::DeleteSlab { slab_id, .. } => {
                model.apply(op);
                store.delete_slab(*slab_id).await.map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} DeleteSlab failed: {e:?}")
                })?;
            }
            HighLevelOperation::ClearAndSchedule { new_trigger, .. } => {
                model.apply(op);
                store
                    .clear_and_schedule(new_trigger.clone())
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} ClearAndSchedule failed: {e:?}")
                    })?;
            }
            HighLevelOperation::GetSlabTriggersAllTypes { slab_id } => {
                verify_slab_triggers_all_types(store, model, *slab_id, op_idx).await?;
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
            HighLevelOperation::UpdateTag {
                key,
                time,
                timer_type,
                new_tag,
                ..
            } => {
                model.apply(op);
                store
                    .update_tag(key, *time, *timer_type, *new_tag)
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} UpdateTag failed: {e:?}"))?;
            }
            HighLevelOperation::CurrentTag {
                segment,
                key,
                time,
                timer_type,
            } => {
                verify_current_tag(store, model, segment, key, *time, *timer_type, op_idx).await?;
            }
        }

        if mutating {
            verify_dual_index_consistency(store, model)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} consistency check: {e}"))?;
        }
    }
    Ok(())
}

/// Cleans up all test data using only public `TriggerStore` API.
async fn cleanup_test_data<S>(store: &S, model: &HighLevelModel) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Remove all triggers from the model
    for ((_segment_id, _slab_id, _timer_type), trigger_set) in &model.slab_index {
        for (key, time, timer_type) in trigger_set {
            // Ignore errors during cleanup - triggers may have been removed by test
            // operations
            let _ = store.remove_trigger(key, *time, *timer_type).await;
        }
    }

    // Delete all slabs that were created
    let mut deleted_slabs = HashSet::new();
    for (_segment_id, slab_id, _timer_type) in model.slab_index.keys() {
        if deleted_slabs.insert(*slab_id) {
            // Ignore errors - slabs may have been deleted by test operations
            let _ = store.delete_slab(*slab_id).await;
        }
    }

    Ok(())
}

/// Verifies dual-index consistency by comparing all triggers in slab and key
/// indices.
///
/// This is the critical verification that high-level operations maintain
/// consistency across both indices.
async fn collect_slab_trigger_set<S>(
    store: &S,
    model: &HighLevelModel,
) -> color_eyre::Result<HashSet<StoreTriggerTuple>>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let mut slab_triggers = HashSet::new();
    let mut processed_slabs = HashSet::new();

    for (segment_id, slab_id, _timer_type) in model.slab_index.keys() {
        if !processed_slabs.insert((*segment_id, *slab_id)) {
            continue;
        }

        let triggers: Vec<Trigger> = store
            .get_slab_triggers_all_types(*slab_id)
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

    Ok(slab_triggers)
}

async fn collect_key_trigger_set<S>(
    store: &S,
    model: &HighLevelModel,
) -> color_eyre::Result<HashSet<StoreTriggerTuple>>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let mut key_triggers = HashSet::new();
    for (segment_id, key, timer_type) in model.key_index.keys() {
        let triggers: Vec<Trigger> = store
            .get_key_triggers(*timer_type, key)
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

    Ok(key_triggers)
}

fn collect_model_trigger_set(model: &HighLevelModel) -> HashSet<StoreTriggerTuple> {
    let mut model_triggers = HashSet::new();
    for ((segment_id, _slab_id, _timer_type), trigger_set) in &model.slab_index {
        for (key, time, timer_type) in trigger_set {
            model_triggers.insert((key.clone(), *time, *timer_type, *segment_id));
        }
    }
    model_triggers
}

fn verify_trigger_sets(
    slab_triggers: &HashSet<StoreTriggerTuple>,
    key_triggers: &HashSet<StoreTriggerTuple>,
    model_triggers: &HashSet<StoreTriggerTuple>,
) -> color_eyre::Result<()> {
    if slab_triggers != key_triggers {
        let slab_only: Vec<_> = slab_triggers.difference(key_triggers).collect();
        let key_only: Vec<_> = key_triggers.difference(slab_triggers).collect();

        return Err(color_eyre::eyre::eyre!(
            "Dual-index consistency violation!\nTriggers in slab index but not key index: \
             {slab_only:?}\nTriggers in key index but not slab index: {key_only:?}"
        ));
    }

    if slab_triggers != model_triggers {
        let store_only: Vec<_> = slab_triggers.difference(model_triggers).collect();
        let model_only: Vec<_> = model_triggers.difference(slab_triggers).collect();

        return Err(color_eyre::eyre::eyre!(
            "Store does not match model!\nTriggers in store but not model: \
             {store_only:?}\nTriggers in model but not store: {model_only:?}"
        ));
    }

    Ok(())
}

async fn verify_tag_consistency<S>(store: &S, model: &HighLevelModel) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    for ((segment_id, key, timer_type, time), expected_tag) in &model.tag_index {
        let current_tag = store
            .current_tag(key, *time, *timer_type)
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!(
                    "Failed to read current_tag during consistency check: {e:?}"
                )
            })?;

        if current_tag != Some(*expected_tag) {
            return Err(color_eyre::eyre::eyre!(
                "Key-index current_tag mismatch: key={key:?} time={time:?} type={timer_type:?} \
                 expected Some({expected_tag}), got {current_tag:?}"
            ));
        }

        let key_triggers: Vec<Trigger> = store
            .get_key_triggers(*timer_type, key)
            .try_collect()
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!(
                    "Failed to read key triggers during tag consistency check: {e:?}"
                )
            })?;
        let key_tag = key_triggers
            .iter()
            .find(|t| t.time == *time && t.timer_type == *timer_type)
            .map(|t| t.tag);

        if key_tag != Some(*expected_tag) {
            return Err(color_eyre::eyre::eyre!(
                "Key-index trigger tag mismatch: segment={segment_id} key={key:?} time={time:?} \
                 type={timer_type:?} expected Some({expected_tag}), got {key_tag:?}"
            ));
        }

        let slab_id = Slab::from_time(store.slab_size(), *time).id();
        let slab_tags: Vec<Trigger> = store
            .get_slab_triggers_all_types(slab_id)
            .try_collect()
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!(
                    "Failed to read slab triggers during tag consistency check: {e:?}"
                )
            })?;
        let slab_tag = slab_tags
            .iter()
            .find(|t| t.key == *key && t.time == *time && t.timer_type == *timer_type)
            .map(|t| t.tag);

        if slab_tag != Some(*expected_tag) {
            return Err(color_eyre::eyre::eyre!(
                "Slab-index trigger tag mismatch: segment={segment_id} slab={slab_id} key={key:?} \
                 time={time:?} type={timer_type:?} expected Some({expected_tag}), got {slab_tag:?}"
            ));
        }
    }

    Ok(())
}

async fn verify_dual_index_consistency<S>(
    store: &S,
    model: &HighLevelModel,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let slab_triggers = collect_slab_trigger_set(store, model).await?;
    let key_triggers = collect_key_trigger_set(store, model).await?;
    let model_triggers = collect_model_trigger_set(model);

    verify_trigger_sets(&slab_triggers, &key_triggers, &model_triggers)?;
    verify_tag_consistency(store, model).await
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
    for _segment in &input.segments {
        store
            .insert_segment()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to insert segment: {e:?}"))?;
    }

    let mut model = HighLevelModel::new();

    // Apply operations and verify, ensuring cleanup happens even if test fails
    let result = async {
        apply_high_level_operations(store, &mut model, &input.operations).await?;
        // CRITICAL: Verify dual-index consistency BEFORE cleanup
        verify_dual_index_consistency(store, &model).await?;
        Ok(())
    }
    .await;

    // Always cleanup, even if test failed
    cleanup_test_data(store, &model).await?;

    // Return the verification result
    result
}
