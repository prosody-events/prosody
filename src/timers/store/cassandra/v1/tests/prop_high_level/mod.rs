//! Property-based tests for V1 high-level dual-index operations.
//!
//! Tests the high-level operations that coordinate updates across both V1 slab
//! and key indices, verifying dual-index consistency for V1 schema (without
//! `timer_type` field).

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::cassandra::v1::{V1Operations, coordinated};
use crate::timers::store::{SegmentId, TriggerV1};
use ahash::HashMap;
use futures::TryStreamExt;
use quickcheck::{Arbitrary, Gen};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

mod model;
use model::V1HighLevelModel;

/// Type alias for V1 trigger tuple (key, time) - no `timer_type` in V1.
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
                    let slab = Slab::from_time(slab_size, time);
                    let slab_id = slab.id();

                    existing_triggers.insert((segment_id, slab_id, key.clone(), time), ());

                    V1HighLevelOperation::AddTrigger {
                        segment_id,
                        slab_id,
                        trigger: TriggerV1 {
                            key,
                            time,
                            context: Span::current().context(),
                        },
                    }
                }
                1 => {
                    // RemoveTrigger - pick from existing triggers if any
                    if existing_triggers.is_empty() || bool::arbitrary(g) {
                        // Generate random remove (might not exist)
                        let key: Key = format!("key-{}", u8::arbitrary(g) % 5).into();
                        let time = CompactDateTime::arbitrary(g);
                        let slab = Slab::from_time(slab_size, time);
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
                    existing_triggers
                        .retain(|&(seg_id, _, ref k, _), ()| !(seg_id == segment_id && k == &key));

                    V1HighLevelOperation::ClearTriggersForKey {
                        segment_id,
                        key,
                        slab_size,
                    }
                }
                3 => {
                    // DeleteSlab
                    let time = CompactDateTime::arbitrary(g);
                    let slab = Slab::from_time(slab_size, time);
                    let slab_id = slab.id();

                    // Remove all triggers in this slab
                    existing_triggers
                        .retain(|&(seg_id, sid, ..), ()| !(seg_id == segment_id && sid == slab_id));

                    V1HighLevelOperation::DeleteSlab {
                        segment_id,
                        slab_id,
                    }
                }
                4 => {
                    // GetSlabTriggers query
                    let time = CompactDateTime::arbitrary(g);
                    let slab = Slab::from_time(slab_size, time);
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

/// Applies V1 high-level operations with inline verification.
async fn apply_v1_high_level_operations(
    store: &V1Operations,
    model: &mut V1HighLevelModel,
    operations: &[V1HighLevelOperation],
) -> color_eyre::Result<()> {
    for (op_idx, op) in operations.iter().enumerate() {
        match op {
            V1HighLevelOperation::AddTrigger {
                segment_id,
                slab_id,
                trigger,
            } => {
                model.apply(op);
                coordinated::add_trigger(store, segment_id, *slab_id, trigger.clone())
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} AddTrigger v1 failed: {e:?}")
                    })?;
            }
            V1HighLevelOperation::RemoveTrigger {
                segment_id,
                slab_id,
                key,
                time,
            } => {
                model.apply(op);
                coordinated::remove_trigger(store, segment_id, *slab_id, key, *time)
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
                coordinated::clear_triggers_for_key(store, segment_id, key, *slab_size)
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
                coordinated::delete_slab(store, segment_id, *slab_id)
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
                    .get_slab_triggers(segment_id, *slab_id)
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
                    .get_key_triggers(segment_id, key)
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

/// Verifies V1 dual-index consistency by comparing all triggers in slab and key
/// indices.
///
/// This is the critical verification that V1 high-level operations maintain
/// consistency across both indices. V1 schema has no `timer_type` field.
async fn verify_v1_dual_index_consistency(
    store: &V1Operations,
    model: &V1HighLevelModel,
) -> color_eyre::Result<()> {
    use std::collections::HashSet;

    // Collect all triggers from V1 slab indices
    let mut slab_triggers = HashSet::new();
    for (segment_id, slab_id) in model.slab_index.keys() {
        let triggers: Vec<TriggerV1> = store
            .get_slab_triggers(segment_id, *slab_id)
            .try_collect()
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!(
                    "Failed to get V1 slab triggers for consistency check: {e:?}"
                )
            })?;

        for trigger in triggers {
            slab_triggers.insert((trigger.key.clone(), trigger.time, *segment_id));
        }
    }

    // Collect all triggers from V1 key indices
    let mut key_triggers = HashSet::new();
    for (segment_id, key) in model.key_index.keys() {
        let triggers: Vec<TriggerV1> = store
            .get_key_triggers(segment_id, key)
            .try_collect()
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!(
                    "Failed to get V1 key triggers for consistency check: {e:?}"
                )
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
            "V1 dual-index consistency violation!\nTriggers in slab index but not key index: \
             {slab_only:?}\nTriggers in key index but not slab index: {key_only:?}"
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
            "V1 store does not match model!\nTriggers in store but not model: \
             {store_only:?}\nTriggers in model but not store: {model_only:?}"
        ));
    }

    Ok(())
}

/// Verifies that V1 high-level operations maintain dual-index consistency.
pub async fn prop_v1_high_level_dual_index_consistency(
    operations: &V1Operations,
    input: V1HighLevelTestInput,
) -> color_eyre::Result<()> {
    // Clean up test data - clear all potential V1 keys and slabs
    for segment_id in &input.segment_ids {
        for key_idx in 0_i32..5_i32 {
            let key: Key = format!("key-{key_idx}").into();
            operations
                .clear_key_triggers(segment_id, &key)
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Failed to clear v1 key triggers during cleanup: {e:?}")
                })?;
        }

        // Clean up potential slabs - delete a range of slab IDs
        for slab_id in 0_u32..100_u32 {
            let _ = operations.delete_slab_metadata(segment_id, slab_id).await;
        }
    }

    let mut model = V1HighLevelModel::new();
    apply_v1_high_level_operations(operations, &mut model, &input.operations).await?;

    // CRITICAL: Verify V1 dual-index consistency after all operations
    verify_v1_dual_index_consistency(operations, &model).await
}
