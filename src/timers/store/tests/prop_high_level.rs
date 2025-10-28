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
use futures::TryStreamExt;
use quickcheck::{Arbitrary, Gen, TestResult};
use std::collections::BTreeSet;
use std::fmt::Debug;
use tracing::Span;
use uuid::Uuid;

/// Type alias for trigger tuple (key, time, timer_type).
type TriggerTuple = (Key, CompactDateTime, TimerType);

/// High-level operations that coordinate across dual indices.
#[derive(Clone, Debug)]
pub enum HighLevelOperation {
    /// Add a trigger to both slab and key indices.
    AddTrigger {
        /// The segment.
        segment: Segment,
        /// The slab.
        slab: Slab,
        /// The trigger to add.
        trigger: Trigger,
    },
    /// Remove a trigger from both slab and key indices.
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
    /// Clear all triggers for a key+type from both indices.
    ClearTriggersForKey {
        /// The segment ID.
        segment_id: SegmentId,
        /// The timer type.
        timer_type: TimerType,
        /// The key.
        key: Key,
        /// The slab size.
        slab_size: CompactDuration,
    },
    /// Clear all triggers for a key (all types) from both indices.
    ClearAllTriggersForKey {
        /// The segment ID.
        segment_id: SegmentId,
        /// The key.
        key: Key,
        /// The slab size.
        slab_size: CompactDuration,
    },
    /// Query slab triggers to verify state.
    GetSlabTriggers {
        /// The slab.
        slab: Slab,
        /// The timer type.
        timer_type: TimerType,
    },
    /// Query key triggers to verify state.
    GetKeyTriggers {
        /// The segment ID.
        segment_id: SegmentId,
        /// The timer type.
        timer_type: TimerType,
        /// The key.
        key: Key,
    },
}

/// Test input containing isolated segment IDs and high-level operations.
#[derive(Clone, Debug)]
pub struct HighLevelTestInput {
    /// Pool of segments used by operations in this trial.
    pub segments: Vec<Segment>,
    /// Sequence of operations to apply.
    pub operations: Vec<HighLevelOperation>,
}

impl Arbitrary for HighLevelTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 2 segments with random UUIDs
        let segments: Vec<Segment> = (0..2)
            .map(|i| Segment {
                id: Uuid::new_v4(),
                name: format!("segment-{i}"),
                slab_size: CompactDuration::new((u32::arbitrary(g) % 600) + 60), // 60-660 seconds
                version: SegmentVersion::V2,
            })
            .collect();

        // Generate 10-50 operations
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        // Track which triggers exist to generate valid removes
        let mut existing_triggers: HashMap<(SegmentId, SlabId, Key, CompactDateTime, TimerType), ()> =
            HashMap::default();

        for _ in 0..op_count {
            let segment_idx = usize::from(u8::arbitrary(g)) % segments.len();
            let segment = &segments[segment_idx];

            let op = match u8::arbitrary(g) % 6 {
                0 => {
                    // AddTrigger
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 5).into();
                    let time = CompactDateTime::arbitrary(g);
                    let timer_type = if bool::arbitrary(g) {
                        TimerType::Application
                    } else {
                        TimerType::DeferRetry
                    };
                    let slab = Slab::from_time(segment.id, segment.slab_size, time);

                    existing_triggers.insert((segment.id, slab.id(), key.clone(), time, timer_type), ());

                    HighLevelOperation::AddTrigger {
                        segment: segment.clone(),
                        slab: slab.clone(),
                        trigger: Trigger::new(key.clone(), time, timer_type, Span::current()),
                    }
                }
                1 => {
                    // RemoveTrigger - pick from existing triggers if any
                    if existing_triggers.is_empty() || bool::arbitrary(g) {
                        // Generate random remove (might not exist)
                        let key: Key = format!("key-{}", u8::arbitrary(g) % 5).into();
                        let time = CompactDateTime::arbitrary(g);
                        let timer_type = if bool::arbitrary(g) {
                            TimerType::Application
                        } else {
                            TimerType::DeferRetry
                        };
                        let slab = Slab::from_time(segment.id, segment.slab_size, time);

                        HighLevelOperation::RemoveTrigger {
                            segment: segment.clone(),
                            slab: slab.clone(),
                            key,
                            time,
                            timer_type,
                        }
                    } else {
                        // Remove an existing trigger
                        let keys: Vec<_> = existing_triggers.keys().cloned().collect();
                        let (seg_id, slab_id, key, time, timer_type) = &keys[usize::arbitrary(g) % keys.len()];

                        existing_triggers.remove(&(*seg_id, *slab_id, key.clone(), *time, *timer_type));

                        // Find the matching segment - use first segment as fallback (should never happen)
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
                2 => {
                    // ClearTriggersForKey
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 5).into();
                    let timer_type = if bool::arbitrary(g) {
                        TimerType::Application
                    } else {
                        TimerType::DeferRetry
                    };

                    // Remove all triggers for this key+type
                    existing_triggers.retain(|&(seg_id, _, ref k, _, tt), _| {
                        !(seg_id == segment.id && k == &key && tt == timer_type)
                    });

                    HighLevelOperation::ClearTriggersForKey {
                        segment_id: segment.id,
                        timer_type,
                        key,
                        slab_size: segment.slab_size,
                    }
                }
                3 => {
                    // ClearAllTriggersForKey
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 5).into();

                    // Remove all triggers for this key (all types)
                    existing_triggers.retain(|&(seg_id, _, ref k, _, _), _| {
                        !(seg_id == segment.id && k == &key)
                    });

                    HighLevelOperation::ClearAllTriggersForKey {
                        segment_id: segment.id,
                        key,
                        slab_size: segment.slab_size,
                    }
                }
                4 => {
                    // GetSlabTriggers query
                    let time = CompactDateTime::arbitrary(g);
                    let slab = Slab::from_time(segment.id, segment.slab_size, time);
                    let timer_type = if bool::arbitrary(g) {
                        TimerType::Application
                    } else {
                        TimerType::DeferRetry
                    };

                    HighLevelOperation::GetSlabTriggers { slab, timer_type }
                }
                _ => {
                    // GetKeyTriggers query
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 5).into();
                    let timer_type = if bool::arbitrary(g) {
                        TimerType::Application
                    } else {
                        TimerType::DeferRetry
                    };

                    HighLevelOperation::GetKeyTriggers {
                        segment_id: segment.id,
                        timer_type,
                        key,
                    }
                }
            };

            operations.push(op);
        }

        Self {
            segments,
            operations,
        }
    }
}

/// Reference model tracking dual indices.
#[derive(Clone, Debug)]
pub struct HighLevelModel {
    /// Triggers indexed by (segment_id, slab_id, timer_type).
    slab_index: HashMap<(SegmentId, SlabId, TimerType), BTreeSet<TriggerTuple>>,
    /// Triggers indexed by (segment_id, key, timer_type).
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
                if let Some(triggers) = self.slab_index.get_mut(&(segment.id, slab.id(), *timer_type)) {
                    triggers.remove(&tuple);
                }

                // Remove from key index
                if let Some(triggers) = self.key_index.get_mut(&(segment.id, key.clone(), *timer_type)) {
                    triggers.remove(&tuple);
                }
            }
            HighLevelOperation::ClearTriggersForKey {
                segment_id,
                timer_type,
                key,
                slab_size,
            } => {
                // Get all triggers for this key+type from key index
                let triggers_to_remove: Vec<TriggerTuple> = self.key_index
                    .get(&(*segment_id, key.clone(), *timer_type))
                    .map(|set| set.iter().cloned().collect())
                    .unwrap_or_default();

                // Remove from slab index for each trigger
                for (k, time, tt) in &triggers_to_remove {
                    let slab = Slab::from_time(*segment_id, *slab_size, *time);
                    if let Some(triggers) = self.slab_index.get_mut(&(*segment_id, slab.id(), *tt)) {
                        triggers.remove(&(k.clone(), *time, *tt));
                    }
                }

                // Clear from key index
                self.key_index.remove(&(*segment_id, key.clone(), *timer_type));
            }
            HighLevelOperation::ClearAllTriggersForKey {
                segment_id,
                key,
                slab_size,
            } => {
                // Collect all triggers for this key across all timer types
                let mut triggers_to_remove = Vec::new();
                for timer_type in [TimerType::Application, TimerType::DeferRetry] {
                    if let Some(triggers) = self.key_index.get(&(*segment_id, key.clone(), timer_type)) {
                        triggers_to_remove.extend(triggers.iter().cloned());
                    }
                }

                // Remove from slab index for each trigger
                for (k, time, tt) in &triggers_to_remove {
                    let slab = Slab::from_time(*segment_id, *slab_size, *time);
                    if let Some(triggers) = self.slab_index.get_mut(&(*segment_id, slab.id(), *tt)) {
                        triggers.remove(&(k.clone(), *time, *tt));
                    }
                }

                // Clear from key index for all timer types
                for timer_type in [TimerType::Application, TimerType::DeferRetry] {
                    self.key_index.remove(&(*segment_id, key.clone(), timer_type));
                }
            }
            HighLevelOperation::GetSlabTriggers { .. } | HighLevelOperation::GetKeyTriggers { .. } => {
                // Queries don't modify state
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
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} AddTrigger failed: {e:?}"))?;
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
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} RemoveTrigger failed: {e:?}"))?;
            }
            HighLevelOperation::ClearTriggersForKey {
                segment_id,
                timer_type,
                key,
                slab_size,
            } => {
                model.apply(op);
                store
                    .clear_triggers_for_key(segment_id, *timer_type, key, *slab_size)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} ClearTriggersForKey failed: {e:?}")
                    })?;
            }
            HighLevelOperation::ClearAllTriggersForKey {
                segment_id,
                key,
                slab_size,
            } => {
                model.apply(op);
                store
                    .clear_all_triggers_for_key(segment_id, key, *slab_size)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} ClearAllTriggersForKey failed: {e:?}")
                    })?;
            }
            HighLevelOperation::GetSlabTriggers { slab, timer_type } => {
                let expected = model.get_slab_triggers(&slab.segment_id(), slab.id(), *timer_type);
                let actual: Vec<Trigger> = store
                    .get_slab_triggers(slab, *timer_type)
                    .try_collect()
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} GetSlabTriggers failed: {e:?}")
                    })?;

                let actual_tuples: Vec<TriggerTuple> = actual
                    .iter()
                    .map(|t| (t.key.clone(), t.time, t.timer_type))
                    .collect();

                if expected != actual_tuples {
                    return Err(color_eyre::eyre::eyre!(
                        "Op #{op_idx} GetSlabTriggers mismatch: expected {expected:?}, got \
                         {actual_tuples:?}"
                    ));
                }
            }
            HighLevelOperation::GetKeyTriggers {
                segment_id,
                timer_type,
                key,
            } => {
                let expected = model.get_key_triggers(segment_id, key, *timer_type);
                let actual: Vec<Trigger> = store
                    .get_key_triggers(segment_id, *timer_type, key)
                    .try_collect()
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} GetKeyTriggers failed: {e:?}")
                    })?;

                let actual_tuples: Vec<TriggerTuple> = actual
                    .iter()
                    .map(|t| (t.key.clone(), t.time, t.timer_type))
                    .collect();

                if expected != actual_tuples {
                    return Err(color_eyre::eyre::eyre!(
                        "Op #{op_idx} GetKeyTriggers mismatch: expected {expected:?}, got \
                         {actual_tuples:?}"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Verifies dual-index consistency by comparing all triggers in slab and key indices.
///
/// This is the critical verification that high-level operations maintain consistency
/// across both indices.
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

    // Collect all triggers from slab indices
    let mut slab_triggers = HashSet::new();
    for ((segment_id, slab_id, timer_type), _) in &model.slab_index {
        let segment = segments.iter().find(|s| s.id == *segment_id).unwrap_or(&segments[0]);
        let slab = Slab::new(*segment_id, *slab_id, segment.slab_size);

        let triggers: Vec<Trigger> = store
            .get_slab_triggers(&slab, *timer_type)
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
    for ((segment_id, key, timer_type), _) in &model.key_index {
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
            "Dual-index consistency violation!\n\
             Triggers in slab index but not key index: {slab_only:?}\n\
             Triggers in key index but not slab index: {key_only:?}"
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
            "Store does not match model!\n\
             Triggers in store but not model: {store_only:?}\n\
             Triggers in model but not store: {model_only:?}"
        ));
    }

    Ok(())
}

/// Verifies that high-level operations maintain dual-index consistency.
pub async fn prop_high_level_dual_index_consistency<S>(
    store: &S,
    input: HighLevelTestInput,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Clean up test data
    for segment in &input.segments {
        store
            .delete_segment(&segment.id)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to clean up segment: {e:?}"))?;
    }

    let mut model = HighLevelModel::new();
    apply_high_level_operations(store, &mut model, &input.operations).await?;

    // CRITICAL: Verify dual-index consistency after all operations
    verify_dual_index_consistency(store, &model, &input.segments).await
}

/// [`QuickCheck`] wrapper for high-level dual-index consistency property.
pub fn test_prop_high_level_dual_index_consistency<S>(
    store: &S,
    input: HighLevelTestInput,
) -> TestResult
where
    S: TriggerStore + Send + Sync + 'static,
    S::Error: Debug,
{
    use tokio::runtime::Runtime;

    let Ok(rt) = Runtime::new() else {
        return TestResult::error("Failed to create tokio runtime");
    };

    let result = rt.block_on(async { prop_high_level_dual_index_consistency(store, input).await });

    match result {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(format!("{e:?}")),
    }
}
