//! Property-based tests for v1 slab trigger table operations.
//!
//! Tests the low-level v1 slab trigger CRUD operations in isolation using a
//! simple reference model to verify correctness. V1 triggers do not have
//! [`timer_type`] field.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::slab::SlabId;
use crate::timers::store::cassandra::v1::V1Operations;
use crate::timers::store::{SegmentId, TriggerV1};
use ahash::HashMap;
use futures::TryStreamExt;
use quickcheck::{Arbitrary, Gen, TestResult};
use std::collections::BTreeSet;
use tracing::Span;
use uuid::Uuid;

/// Type alias for v1 trigger tuple without `timer_type`.
type TriggerV1Tuple = (Key, CompactDateTime);

/// Operations that can be performed on the v1 slab trigger table.
#[derive(Clone, Debug)]
pub enum V1SlabTriggerOperation {
    /// Insert a trigger for a slab.
    InsertTrigger {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab ID.
        slab_id: SlabId,
        /// The v1 trigger to insert.
        trigger: TriggerV1,
    },
    /// Retrieve all triggers in a slab.
    GetTriggers {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab ID.
        slab_id: SlabId,
    },
    /// Delete a specific trigger.
    Delete {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab ID.
        slab_id: SlabId,
        /// The trigger key.
        key: Key,
        /// The trigger time.
        time: CompactDateTime,
    },
    /// Delete all triggers in a slab.
    Clear {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab ID.
        slab_id: SlabId,
    },
}

/// Test input containing isolated segment IDs and operations.
///
/// Each test trial uses randomly generated segment IDs, ensuring complete
/// isolation between trials and allowing parallel test execution.
#[derive(Clone, Debug)]
pub struct V1SlabTriggerTestInput {
    /// Pool of segment IDs used by operations in this trial.
    pub segment_ids: Vec<SegmentId>,
    /// Sequence of operations to apply.
    pub operations: Vec<V1SlabTriggerOperation>,
}

impl Arbitrary for V1SlabTriggerTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 3 random v4 UUIDs unique to this trial
        let segment_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

        // Generate 10-50 operations using these segments
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        for _ in 0..op_count {
            let idx = usize::from(u8::arbitrary(g)) % segment_ids.len();
            let segment_id = segment_ids[idx];

            // Use small slab IDs for better collision testing
            let slab_id = SlabId::from(u8::arbitrary(g) % 5);

            let op = match u8::arbitrary(g) % 4 {
                0 => {
                    // Insert operation
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 10).into();
                    let time = CompactDateTime::arbitrary(g);
                    let trigger = TriggerV1 {
                        key: key.clone(),
                        time,
                        span: Span::current(),
                    };
                    V1SlabTriggerOperation::InsertTrigger {
                        segment_id,
                        slab_id,
                        trigger,
                    }
                }
                1 => V1SlabTriggerOperation::GetTriggers {
                    segment_id,
                    slab_id,
                },
                2 => {
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 10).into();
                    let time = CompactDateTime::arbitrary(g);
                    V1SlabTriggerOperation::Delete {
                        segment_id,
                        slab_id,
                        key,
                        time,
                    }
                }
                _ => V1SlabTriggerOperation::Clear {
                    segment_id,
                    slab_id,
                },
            };
            operations.push(op);
        }

        Self {
            segment_ids,
            operations,
        }
    }
}

/// Reference model for v1 slab trigger table behavior.
///
/// Uses a nested [`HashMap`] and [`BTreeSet`] to track triggers by slab.
/// V1 triggers are ordered by (key, time) without `timer_type`.
#[derive(Clone, Debug)]
pub struct V1SlabTriggerModel {
    triggers: HashMap<(SegmentId, SlabId), BTreeSet<TriggerV1Tuple>>,
}

impl Default for V1SlabTriggerModel {
    fn default() -> Self {
        Self::new()
    }
}

impl V1SlabTriggerModel {
    /// Creates a new empty v1 slab trigger model.
    #[must_use]
    pub fn new() -> Self {
        Self {
            triggers: HashMap::default(),
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &V1SlabTriggerOperation) {
        match op {
            V1SlabTriggerOperation::InsertTrigger {
                segment_id,
                slab_id,
                trigger,
            } => {
                self.triggers
                    .entry((*segment_id, *slab_id))
                    .or_default()
                    .insert((trigger.key.clone(), trigger.time));
            }
            V1SlabTriggerOperation::GetTriggers { .. } => {
                // Queries don't modify state
            }
            V1SlabTriggerOperation::Delete {
                segment_id,
                slab_id,
                key,
                time,
            } => {
                if let Some(set) = self.triggers.get_mut(&(*segment_id, *slab_id)) {
                    set.remove(&(key.clone(), *time));
                }
            }
            V1SlabTriggerOperation::Clear {
                segment_id,
                slab_id,
            } => {
                self.triggers.remove(&(*segment_id, *slab_id));
            }
        }
    }

    /// Gets all triggers for a slab in ascending order.
    #[must_use]
    pub fn get_triggers(&self, segment_id: &SegmentId, slab_id: SlabId) -> Vec<TriggerV1Tuple> {
        self.triggers
            .get(&(*segment_id, slab_id))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns all slabs in the model.
    #[must_use]
    pub fn all_slabs(&self) -> Vec<(SegmentId, SlabId)> {
        self.triggers.keys().copied().collect()
    }
}

/// Applies v1 slab trigger operations to both store and model with inline
/// verification.
///
/// # Errors
///
/// Returns an error if operations fail or query results don't match model.
async fn apply_v1_slab_trigger_operations(
    v1_ops: &V1Operations,
    model: &mut V1SlabTriggerModel,
    operations: &[V1SlabTriggerOperation],
) -> color_eyre::Result<()> {
    for (op_idx, op) in operations.iter().enumerate() {
        match op {
            V1SlabTriggerOperation::InsertTrigger {
                segment_id,
                slab_id,
                trigger,
            } => {
                model.apply(op);
                v1_ops
                    .insert_slab_trigger_v1(segment_id, *slab_id, trigger.clone())
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Insert v1 trigger failed: {e:?}")
                    })?;
            }
            V1SlabTriggerOperation::GetTriggers {
                segment_id,
                slab_id,
            } => {
                let expected = model.get_triggers(segment_id, *slab_id);
                let actual: Vec<TriggerV1> = v1_ops
                    .get_slab_triggers_v1(segment_id, *slab_id)
                    .try_collect()
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} GetTriggers v1 failed: {e:?}")
                    })?;

                let actual_tuples: Vec<TriggerV1Tuple> =
                    actual.iter().map(|t| (t.key.clone(), t.time)).collect();

                if expected != actual_tuples {
                    return Err(color_eyre::eyre::eyre!(
                        "Op #{op_idx} GetTriggers v1 query mismatch for slab ({segment_id}, \
                         {slab_id}): expected {expected:?}, got {actual_tuples:?}"
                    ));
                }

                for window in actual_tuples.windows(2) {
                    if window[0] >= window[1] {
                        return Err(color_eyre::eyre::eyre!(
                            "Op #{op_idx} GetTriggers v1 ordering violation for slab \
                             ({segment_id}, {slab_id}): {window:?}"
                        ));
                    }
                }
            }
            V1SlabTriggerOperation::Delete {
                segment_id,
                slab_id,
                key,
                time,
            } => {
                model.apply(op);
                v1_ops
                    .delete_slab_trigger_v1(segment_id, *slab_id, key, *time)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Delete v1 slab trigger failed: {e:?}")
                    })?;
            }
            V1SlabTriggerOperation::Clear {
                segment_id,
                slab_id,
            } => {
                model.apply(op);
                v1_ops
                    .clear_slab_triggers_v1(segment_id, *slab_id)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Clear v1 slab triggers failed: {e:?}")
                    })?;
            }
        }
    }
    Ok(())
}

/// Verifies that v1 slab trigger operations match the reference model behavior.
///
/// # Test Strategy
///
/// 1. Start with empty store and model
/// 2. Apply sequence of random operations to both
/// 3. Verify that for every slab:
///    - `operations.get_slab_triggers_v1(seg, slab)` matches model
///    - Ordering is correct (ascending by key, time)
///
/// # Errors
///
/// Returns an error if:
/// - V1 operations fail (insert, delete, get, clear)
/// - Store state doesn't match model state
/// - Ordering invariants are violated
pub async fn prop_v1_slab_trigger_model_equivalence(
    operations: &V1Operations,
    input: V1SlabTriggerTestInput,
) -> color_eyre::Result<()> {
    for segment_id in &input.segment_ids {
        for slab_id in 0..5 {
            operations
                .clear_slab_triggers_v1(segment_id, slab_id)
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!(
                        "Failed to clear v1 slab triggers during cleanup: {e:?}"
                    )
                })?;
        }
    }

    let mut model = V1SlabTriggerModel::new();
    apply_v1_slab_trigger_operations(operations, &mut model, &input.operations).await?;

    let all_slabs = model.all_slabs();
    for (segment_id, slab_id) in &all_slabs {
        let model_triggers = model.get_triggers(segment_id, *slab_id);
        let operations_triggers: Vec<TriggerV1> = operations
            .get_slab_triggers_v1(segment_id, *slab_id)
            .try_collect()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Get v1 slab triggers failed: {e:?}"))?;

        let operations_tuples: Vec<TriggerV1Tuple> = operations_triggers
            .iter()
            .map(|t| (t.key.clone(), t.time))
            .collect();

        if model_triggers != operations_tuples {
            return Err(color_eyre::eyre::eyre!(
                "V1 trigger mismatch for slab ({}, {}): expected {:?}, got {:?}",
                segment_id,
                slab_id,
                model_triggers,
                operations_tuples
            ));
        }

        for window in operations_tuples.windows(2) {
            if window[0] >= window[1] {
                return Err(color_eyre::eyre::eyre!(
                    "V1 trigger ordering violation for slab ({}, {}): {:?} >= {:?}",
                    segment_id,
                    slab_id,
                    window[0],
                    window[1]
                ));
            }
        }
    }

    Ok(())
}

/// [`QuickCheck`] wrapper for v1 slab trigger model equivalence property.
pub fn test_prop_v1_slab_trigger_model_equivalence(
    operations: &V1Operations,
    input: V1SlabTriggerTestInput,
) -> TestResult {
    use tokio::runtime::Runtime;

    let Ok(rt) = Runtime::new() else {
        return TestResult::error("Failed to create tokio runtime");
    };

    let result =
        rt.block_on(async { prop_v1_slab_trigger_model_equivalence(operations, input).await });

    match result {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(format!("{e:?}")),
    }
}
