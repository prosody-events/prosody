//! Property-based tests for V1 high-level dual-index operations.
//!
//! Tests the high-level operations that coordinate updates across both V1 slab
//! and key indices, verifying dual-index consistency for V1 schema (without
//! `timer_type` field).

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{SegmentId, TriggerStore, TriggerV1};
use ahash::HashMap;
use futures::TryStreamExt;
use quickcheck::{Arbitrary, Gen, TestResult};
use std::collections::BTreeSet;
use std::fmt::Debug;
use tracing::Span;
use uuid::Uuid;

/// Type alias for V1 trigger tuple (key, time) - no timer_type in V1.
type TriggerV1Tuple = (Key, CompactDateTime);

/// V1 high-level operations that coordinate across dual indices.
#[derive(Clone, Debug)]
pub enum V1HighLevelOperation {
    /// Add a V1 trigger to both slab and key indices.
    AddTrigger {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab ID.
        slab_id: SlabId,
        /// The trigger to add.
        trigger: TriggerV1,
    },
    /// Remove a V1 trigger from both slab and key indices.
    RemoveTrigger {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab ID.
        slab_id: SlabId,
        /// The key.
        key: Key,
        /// The time.
        time: CompactDateTime,
    },
    /// Clear all V1 triggers for a key from both indices.
    ClearTriggersForKey {
        /// The segment ID.
        segment_id: SegmentId,
        /// The key.
        key: Key,
        /// The slab size.
        slab_size: CompactDuration,
    },
    /// Delete a V1 slab (metadata + triggers).
    DeleteSlab {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab ID.
        slab_id: SlabId,
    },
    /// Query V1 slab triggers to verify state.
    GetSlabTriggers {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab ID.
        slab_id: SlabId,
    },
    /// Query V1 key triggers to verify state.
    GetKeyTriggers {
        /// The segment ID.
        segment_id: SegmentId,
        /// The key.
        key: Key,
    },
}

/// Test input containing isolated segment IDs and V1 high-level operations.
#[derive(Clone, Debug)]
pub struct V1HighLevelTestInput {
    /// Pool of segment IDs used by operations in this trial.
    pub segment_ids: Vec<SegmentId>,
    /// Slab size for this trial.
    pub slab_size: CompactDuration,
    /// Sequence of operations to apply.
    pub operations: Vec<V1HighLevelOperation>,
}

impl Arbitrary for V1HighLevelTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 2 random segment UUIDs
        let segment_ids = vec![Uuid::new_v4(), Uuid::new_v4()];

        // Generate consistent slab size for this trial
        let slab_size = CompactDuration::new((u32::arbitrary(g) % 600) + 60); // 60-660 seconds

        // Generate 10-50 operations
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        // Track which triggers exist to generate valid removes
        let mut existing_triggers: HashMap<(SegmentId, SlabId, Key, CompactDateTime), ()> =
            HashMap::default();

        for _ in 0..op_count {
            let seg_idx = usize::from(u8::arbitrary(g)) % segment_ids.len();
            let segment_id = segment_ids[seg_idx];

            let op = match u8::arbitrary(g) % 6 {
                0 => {
                    // AddTrigger
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 5).into();
                    let time = CompactDateTime::arbitrary(g);
                    let slab = Slab::from_time(segment_id, slab_size, time);
                    let slab_id = slab.id();

                    existing_triggers.insert((segment_id, slab_id, key.clone(), time), ());

                    V1HighLevelOperation::AddTrigger {
                        segment_id,
                        slab_id,
                        trigger: TriggerV1 {
                            key,
                            time,
                            span: Span::current(),
                        },
                    }
                }
                1 => {
                    // RemoveTrigger - pick from existing triggers if any
                    if existing_triggers.is_empty() || bool::arbitrary(g) {
                        // Generate random remove (might not exist)
                        let key: Key = format!("key-{}", u8::arbitrary(g) % 5).into();
                        let time = CompactDateTime::arbitrary(g);
                        let slab = Slab::from_time(segment_id, slab_size, time);
                        let slab_id = slab.id();

                        V1HighLevelOperation::RemoveTrigger {
                            segment_id,
                            slab_id,
                            key,
                            time,
                        }
                    } else {
                        // Remove an existing trigger
                        let keys: Vec<_> = existing_triggers.keys().cloned().collect();
                        let (seg_id, slab_id, key, time) = &keys[usize::arbitrary(g) % keys.len()];

                        existing_triggers.remove(&(*seg_id, *slab_id, key.clone(), *time));

                        V1HighLevelOperation::RemoveTrigger {
                            segment_id: *seg_id,
                            slab_id: *slab_id,
                            key: key.clone(),
                            time: *time,
                        }
                    }
                }
                2 => {
                    // ClearTriggersForKey
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 5).into();

                    // Remove all triggers for this key
                    existing_triggers.retain(|&(seg_id, _, ref k, _), _| {
                        !(seg_id == segment_id && k == &key)
                    });

                    V1HighLevelOperation::ClearTriggersForKey {
                        segment_id,
                        key,
                        slab_size,
                    }
                }
                3 => {
                    // DeleteSlab
                    let time = CompactDateTime::arbitrary(g);
                    let slab = Slab::from_time(segment_id, slab_size, time);
                    let slab_id = slab.id();

                    // Remove all triggers in this slab
                    existing_triggers.retain(|&(seg_id, sid, _, _), _| {
                        !(seg_id == segment_id && sid == slab_id)
                    });

                    V1HighLevelOperation::DeleteSlab {
                        segment_id,
                        slab_id,
                    }
                }
                4 => {
                    // GetSlabTriggers query
                    let time = CompactDateTime::arbitrary(g);
                    let slab = Slab::from_time(segment_id, slab_size, time);
                    let slab_id = slab.id();

                    V1HighLevelOperation::GetSlabTriggers {
                        segment_id,
                        slab_id,
                    }
                }
                _ => {
                    // GetKeyTriggers query
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 5).into();

                    V1HighLevelOperation::GetKeyTriggers { segment_id, key }
                }
            };

            operations.push(op);
        }

        Self {
            segment_ids,
            slab_size,
            operations,
        }
    }
}

/// Reference model tracking V1 dual indices.
#[derive(Clone, Debug)]
pub struct V1HighLevelModel {
    /// Triggers indexed by (segment_id, slab_id).
    slab_index: HashMap<(SegmentId, SlabId), BTreeSet<TriggerV1Tuple>>,
    /// Triggers indexed by (segment_id, key).
    key_index: HashMap<(SegmentId, Key), BTreeSet<TriggerV1Tuple>>,
}

impl Default for V1HighLevelModel {
    fn default() -> Self {
        Self::new()
    }
}

impl V1HighLevelModel {
    /// Creates a new empty model.
    #[must_use]
    pub fn new() -> Self {
        Self {
            slab_index: HashMap::default(),
            key_index: HashMap::default(),
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &V1HighLevelOperation) {
        match op {
            V1HighLevelOperation::AddTrigger {
                segment_id,
                slab_id,
                trigger,
            } => {
                let tuple = (trigger.key.clone(), trigger.time);

                // Add to slab index
                self.slab_index
                    .entry((*segment_id, *slab_id))
                    .or_default()
                    .insert(tuple.clone());

                // Add to key index
                self.key_index
                    .entry((*segment_id, trigger.key.clone()))
                    .or_default()
                    .insert(tuple);
            }
            V1HighLevelOperation::RemoveTrigger {
                segment_id,
                slab_id,
                key,
                time,
            } => {
                let tuple = (key.clone(), *time);

                // Remove from slab index
                if let Some(triggers) = self.slab_index.get_mut(&(*segment_id, *slab_id)) {
                    triggers.remove(&tuple);
                }

                // Remove from key index
                if let Some(triggers) = self.key_index.get_mut(&(*segment_id, key.clone())) {
                    triggers.remove(&tuple);
                }
            }
            V1HighLevelOperation::ClearTriggersForKey {
                segment_id,
                key,
                slab_size,
            } => {
                // Get all triggers for this key from key index
                let triggers_to_remove: Vec<TriggerV1Tuple> = self
                    .key_index
                    .get(&(*segment_id, key.clone()))
                    .map(|set| set.iter().cloned().collect())
                    .unwrap_or_default();

                // Remove from slab index for each trigger
                for (k, time) in &triggers_to_remove {
                    let slab = Slab::from_time(*segment_id, *slab_size, *time);
                    if let Some(triggers) = self.slab_index.get_mut(&(*segment_id, slab.id())) {
                        triggers.remove(&(k.clone(), *time));
                    }
                }

                // Clear from key index
                self.key_index.remove(&(*segment_id, key.clone()));
            }
            V1HighLevelOperation::DeleteSlab {
                segment_id,
                slab_id,
            } => {
                // Get all triggers in this slab
                let triggers_to_remove: Vec<TriggerV1Tuple> = self
                    .slab_index
                    .get(&(*segment_id, *slab_id))
                    .map(|set| set.iter().cloned().collect())
                    .unwrap_or_default();

                // Remove from key index for each trigger
                for (key, time) in &triggers_to_remove {
                    if let Some(triggers) = self.key_index.get_mut(&(*segment_id, key.clone())) {
                        triggers.remove(&(key.clone(), *time));
                    }
                }

                // Clear slab index
                self.slab_index.remove(&(*segment_id, *slab_id));
            }
            V1HighLevelOperation::GetSlabTriggers { .. }
            | V1HighLevelOperation::GetKeyTriggers { .. } => {
                // Queries don't modify state
            }
        }
    }

    /// Gets triggers from V1 slab index.
    #[must_use]
    pub fn get_slab_triggers(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Vec<TriggerV1Tuple> {
        self.slab_index
            .get(&(*segment_id, slab_id))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Gets triggers from V1 key index.
    #[must_use]
    pub fn get_key_triggers(&self, segment_id: &SegmentId, key: &Key) -> Vec<TriggerV1Tuple> {
        self.key_index
            .get(&(*segment_id, key.clone()))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }
}

/// Applies V1 high-level operations with inline verification.
async fn apply_v1_high_level_operations<S>(
    store: &S,
    model: &mut V1HighLevelModel,
    operations: &[V1HighLevelOperation],
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    for (op_idx, op) in operations.iter().enumerate() {
        match op {
            V1HighLevelOperation::AddTrigger {
                segment_id,
                slab_id,
                trigger,
            } => {
                model.apply(op);
                store
                    .add_trigger_v1(segment_id, *slab_id, trigger.clone())
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} AddTrigger v1 failed: {e:?}"))?;
            }
            V1HighLevelOperation::RemoveTrigger {
                segment_id,
                slab_id,
                key,
                time,
            } => {
                model.apply(op);
                store
                    .remove_trigger_v1(segment_id, *slab_id, key, *time)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} RemoveTrigger v1 failed: {e:?}")
                    })?;
            }
            V1HighLevelOperation::ClearTriggersForKey {
                segment_id,
                key,
                slab_size,
            } => {
                model.apply(op);
                store
                    .clear_triggers_for_key_v1(segment_id, key, *slab_size)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} ClearTriggersForKey v1 failed: {e:?}")
                    })?;
            }
            V1HighLevelOperation::DeleteSlab {
                segment_id,
                slab_id,
            } => {
                model.apply(op);
                store
                    .delete_slab_v1(segment_id, *slab_id)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} DeleteSlab v1 failed: {e:?}")
                    })?;
            }
            V1HighLevelOperation::GetSlabTriggers {
                segment_id,
                slab_id,
            } => {
                let expected = model.get_slab_triggers(segment_id, *slab_id);
                let actual: Vec<TriggerV1> = store
                    .get_slab_triggers_v1(segment_id, *slab_id)
                    .try_collect()
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} GetSlabTriggers v1 failed: {e:?}")
                    })?;

                let actual_tuples: Vec<TriggerV1Tuple> =
                    actual.iter().map(|t| (t.key.clone(), t.time)).collect();

                if expected != actual_tuples {
                    return Err(color_eyre::eyre::eyre!(
                        "Op #{op_idx} GetSlabTriggers v1 mismatch: expected {expected:?}, got \
                         {actual_tuples:?}"
                    ));
                }
            }
            V1HighLevelOperation::GetKeyTriggers { segment_id, key } => {
                let expected = model.get_key_triggers(segment_id, key);
                let actual: Vec<TriggerV1> = store
                    .get_key_triggers_v1(segment_id, key)
                    .try_collect()
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} GetKeyTriggers v1 failed: {e:?}")
                    })?;

                let actual_tuples: Vec<TriggerV1Tuple> =
                    actual.iter().map(|t| (t.key.clone(), t.time)).collect();

                if expected != actual_tuples {
                    return Err(color_eyre::eyre::eyre!(
                        "Op #{op_idx} GetKeyTriggers v1 mismatch: expected {expected:?}, got \
                         {actual_tuples:?}"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Verifies V1 dual-index consistency by comparing all triggers in slab and key indices.
///
/// This is the critical verification that V1 high-level operations maintain consistency
/// across both indices. V1 schema has no `timer_type` field.
async fn verify_v1_dual_index_consistency<S>(
    store: &S,
    model: &V1HighLevelModel,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    use std::collections::HashSet;

    // Collect all triggers from V1 slab indices
    let mut slab_triggers = HashSet::new();
    for ((segment_id, slab_id), _) in &model.slab_index {
        let triggers: Vec<TriggerV1> = store
            .get_slab_triggers_v1(segment_id, *slab_id)
            .try_collect()
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!("Failed to get V1 slab triggers for consistency check: {e:?}")
            })?;

        for trigger in triggers {
            slab_triggers.insert((trigger.key.clone(), trigger.time, *segment_id));
        }
    }

    // Collect all triggers from V1 key indices
    let mut key_triggers = HashSet::new();
    for ((segment_id, key), _) in &model.key_index {
        let triggers: Vec<TriggerV1> = store
            .get_key_triggers_v1(segment_id, key)
            .try_collect()
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!("Failed to get V1 key triggers for consistency check: {e:?}")
            })?;

        for trigger in triggers {
            key_triggers.insert((trigger.key.clone(), trigger.time, *segment_id));
        }
    }

    // Verify both indices contain exactly the same triggers
    if slab_triggers != key_triggers {
        let slab_only: Vec<_> = slab_triggers.difference(&key_triggers).collect();
        let key_only: Vec<_> = key_triggers.difference(&slab_triggers).collect();

        return Err(color_eyre::eyre::eyre!(
            "V1 dual-index consistency violation!\n\
             Triggers in slab index but not key index: {slab_only:?}\n\
             Triggers in key index but not slab index: {key_only:?}"
        ));
    }

    // Verify store matches model - collect all triggers from model
    let mut model_triggers = HashSet::new();
    for ((segment_id, _slab_id), trigger_set) in &model.slab_index {
        for (key, time) in trigger_set {
            model_triggers.insert((key.clone(), *time, *segment_id));
        }
    }

    // Compare store vs model
    if slab_triggers != model_triggers {
        let store_only: Vec<_> = slab_triggers.difference(&model_triggers).collect();
        let model_only: Vec<_> = model_triggers.difference(&slab_triggers).collect();

        return Err(color_eyre::eyre::eyre!(
            "V1 store does not match model!\n\
             Triggers in store but not model: {store_only:?}\n\
             Triggers in model but not store: {model_only:?}"
        ));
    }

    Ok(())
}

/// Verifies that V1 high-level operations maintain dual-index consistency.
pub async fn prop_v1_high_level_dual_index_consistency<S>(
    store: &S,
    input: V1HighLevelTestInput,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Clean up test data - clear all potential V1 keys and slabs
    for segment_id in &input.segment_ids {
        for key_idx in 0_i32..5_i32 {
            let key: Key = format!("key-{key_idx}").into();
            store
                .clear_key_triggers_v1(segment_id, &key)
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Failed to clear v1 key triggers during cleanup: {e:?}")
                })?;
        }

        // Clean up potential slabs - delete a range of slab IDs
        for slab_id in 0_u32..100_u32 {
            let _ = store.delete_slab_v1(segment_id, slab_id).await;
        }
    }

    let mut model = V1HighLevelModel::new();
    apply_v1_high_level_operations(store, &mut model, &input.operations).await?;

    // CRITICAL: Verify V1 dual-index consistency after all operations
    verify_v1_dual_index_consistency(store, &model).await
}

/// [`QuickCheck`] wrapper for V1 high-level dual-index consistency property.
pub fn test_prop_v1_high_level_dual_index_consistency<S>(
    store: &S,
    input: V1HighLevelTestInput,
) -> TestResult
where
    S: TriggerStore + Send + Sync + 'static,
    S::Error: Debug,
{
    use tokio::runtime::Runtime;

    let Ok(rt) = Runtime::new() else {
        return TestResult::error("Failed to create tokio runtime");
    };

    let result = rt.block_on(async { prop_v1_high_level_dual_index_consistency(store, input).await });

    match result {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(format!("{e:?}")),
    }
}
