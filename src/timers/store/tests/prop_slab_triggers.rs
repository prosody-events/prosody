//! Property-based tests for slab trigger table operations.
//!
//! Tests the low-level slab trigger CRUD operations in isolation using a
//! simple reference model to verify correctness.
//!
//! Each store instance is scoped to exactly one segment. All slab trigger
//! operations are routed through that segment's partition — the `segment_id`
//! embedded in a slab is ignored by the store, which always uses its own
//! `self.segment.id`. This test constructs all slabs using the store's own
//! segment ID to reflect that invariant.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use futures::TryStreamExt;
use quickcheck::{Arbitrary, Gen};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::Debug;
use strum::VariantArray;
use tracing::Span;

/// Operations that can be performed on the slab trigger table.
#[derive(Clone, Debug)]
pub enum SlabTriggerOperation {
    /// Insert a trigger into a slab.
    Insert {
        /// Slab ID (the store's segment ID is used; this identifies the slab).
        slab_id: SlabId,
        /// The trigger to insert.
        trigger: Trigger,
    },
    /// Retrieve triggers of a specific type from a slab.
    GetByType {
        /// Slab ID to query.
        slab_id: SlabId,
        /// The timer type to filter by.
        timer_type: TimerType,
    },
    /// Retrieve all triggers from a slab across all types.
    GetAllTypes(SlabId),
    /// Delete a specific trigger from a slab.
    Delete {
        /// Slab ID to delete from.
        slab_id: SlabId,
        /// The timer type.
        timer_type: TimerType,
        /// The trigger key.
        key: Key,
        /// The trigger time.
        time: CompactDateTime,
    },
    /// Clear all triggers from a slab.
    Clear(SlabId),
}

/// Test input for single-segment slab trigger operations.
///
/// Each test trial uses a fresh store with its own segment ID. All slabs are
/// constructed using the store's segment, matching the store's own partition.
#[derive(Clone, Debug)]
pub struct SlabTriggerTestInput {
    /// Sequence of operations to apply.
    pub operations: Vec<SlabTriggerOperation>,
    /// Slab size used for all slabs in this test.
    pub slab_size: CompactDuration,
}

impl Arbitrary for SlabTriggerTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate a slab size for this test (1 second to 7 days to avoid TTL overflow)
        let slab_size = CompactDuration::new(u32::arbitrary(g).clamp(1, 604_800));

        // Use small pools for keys to increase collision probability
        let key_pool = ["key-a", "key-b", "key-c"];

        // Generate 10-50 operations
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        for _ in 0..op_count {
            let slab_id = SlabId::from(u8::arbitrary(g) % 5); // 0-4

            let key_idx = usize::from(u8::arbitrary(g)) % key_pool.len();
            let key = Key::from(key_pool[key_idx]);

            let timer_type = match u8::arbitrary(g) % 3 {
                0 => TimerType::Application,
                1 => TimerType::DeferredMessage,
                _ => TimerType::DeferredTimer,
            };

            let time = CompactDateTime::arbitrary(g);

            let op = match u8::arbitrary(g) % 5 {
                0 => {
                    let trigger = Trigger::new(key, time, timer_type, Span::current());
                    SlabTriggerOperation::Insert { slab_id, trigger }
                }
                1 => SlabTriggerOperation::GetByType {
                    slab_id,
                    timer_type,
                },
                2 => SlabTriggerOperation::GetAllTypes(slab_id),
                3 => SlabTriggerOperation::Delete {
                    slab_id,
                    timer_type,
                    key,
                    time,
                },
                _ => SlabTriggerOperation::Clear(slab_id),
            };
            operations.push(op);
        }

        Self {
            operations,
            slab_size,
        }
    }
}

/// Trigger tuple containing type, key, and time.
type TriggerTuple = (TimerType, Key, CompactDateTime);

/// Reference model for slab trigger table behavior.
///
/// Uses `HashMap<SlabId, BTreeSet<TriggerTuple>>` to track triggers for each
/// slab (all within the single store segment). [`BTreeSet`] provides natural
/// ordering and set semantics.
#[derive(Clone, Debug, Default)]
pub struct SlabTriggerModel {
    triggers: HashMap<SlabId, BTreeSet<TriggerTuple>>,
}

impl SlabTriggerModel {
    /// Creates a new empty slab trigger model.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &SlabTriggerOperation) {
        match op {
            SlabTriggerOperation::Insert { slab_id, trigger } => {
                self.triggers.entry(*slab_id).or_default().insert((
                    trigger.timer_type,
                    trigger.key.clone(),
                    trigger.time,
                ));
            }
            SlabTriggerOperation::GetByType { .. } | SlabTriggerOperation::GetAllTypes(_) => {}
            SlabTriggerOperation::Delete {
                slab_id,
                timer_type,
                key,
                time,
            } => {
                if let Some(set) = self.triggers.get_mut(slab_id) {
                    set.remove(&(*timer_type, key.clone(), *time));
                }
            }
            SlabTriggerOperation::Clear(slab_id) => {
                self.triggers.remove(slab_id);
            }
        }
    }

    /// Gets triggers of a specific type from a slab in ascending order.
    #[must_use]
    pub fn get_by_type(&self, slab_id: SlabId, timer_type: TimerType) -> Vec<TriggerTuple> {
        self.triggers
            .get(&slab_id)
            .map(|set| {
                set.iter()
                    .filter(|(tt, ..)| *tt == timer_type)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Gets all triggers from a slab in ascending order.
    #[must_use]
    pub fn get_all_types(&self, slab_id: SlabId) -> Vec<TriggerTuple> {
        self.triggers
            .get(&slab_id)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns all slab IDs present in the model.
    #[must_use]
    pub fn all_slab_ids(&self) -> Vec<SlabId> {
        self.triggers.keys().copied().collect()
    }
}

/// Verifies slab triggers for a specific timer type.
async fn verify_slab_triggers_by_type<T>(
    operations: &T,
    model: &SlabTriggerModel,
    slab: &Slab,
    timer_type: TimerType,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let slab_id = slab.id();

    let expected = model.get_by_type(slab_id, timer_type);
    let store_triggers: Vec<Trigger> = operations
        .get_slab_triggers(slab, timer_type)
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetByType failed: {e:?}"))?;

    let actual: Vec<TriggerTuple> = store_triggers
        .iter()
        .map(|t| (t.timer_type, t.key.clone(), t.time))
        .collect();

    if expected != actual {
        return Err(color_eyre::eyre::eyre!(
            "Op #{op_idx} Trigger mismatch for slab ({}, {slab_id}) type {timer_type:?}: expected \
             {expected:?}, got {actual:?}",
            operations.segment().id
        ));
    }

    // Verify type filtering
    for trigger in &store_triggers {
        if trigger.timer_type != timer_type {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} Type filtering failed for slab {slab_id}: expected {timer_type:?}, \
                 got {:?}",
                trigger.timer_type
            ));
        }
    }

    // Verify ordering
    for window in actual.windows(2) {
        if window[0] >= window[1] {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} Ordering violation in slab {slab_id} type {timer_type:?}: {window:?}"
            ));
        }
    }

    Ok(())
}

/// Verifies all-types query against the model.
async fn verify_slab_triggers_all_types<T>(
    operations: &T,
    model: &SlabTriggerModel,
    slab: &Slab,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let slab_id = slab.id();
    let expected = model.get_all_types(slab_id);
    let actual: Vec<Trigger> = operations
        .get_slab_triggers_all_types(slab)
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetAllTypes failed: {e:?}"))?;

    let actual_tuples: Vec<TriggerTuple> = actual
        .iter()
        .map(|t| (t.timer_type, t.key.clone(), t.time))
        .collect();

    if expected != actual_tuples {
        return Err(color_eyre::eyre::eyre!(
            "Op #{op_idx} GetAllTypes mismatch for slab {slab_id}: expected {expected:?}, got \
             {actual_tuples:?}"
        ));
    }

    // Verify ordering
    for window in actual_tuples.windows(2) {
        if window[0] >= window[1] {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} Ordering violation in all-types for slab {slab_id}: {window:?}"
            ));
        }
    }

    Ok(())
}

/// Verifies final state of all slabs in the model.
async fn verify_final_slab_state<T>(
    operations: &T,
    model: &SlabTriggerModel,
    slab_size: CompactDuration,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let segment_id = operations.segment().id;

    for slab_id in model.all_slab_ids() {
        let slab = Slab::new(segment_id, slab_id, slab_size);

        // Verify get_slab_triggers for each timer type
        for &timer_type in TimerType::VARIANTS {
            verify_slab_triggers_by_type(operations, model, &slab, timer_type, usize::MAX).await?;
        }

        // Verify get_slab_triggers_all_types
        let expected_all = model.get_all_types(slab_id);
        let store_all: Vec<Trigger> = operations
            .get_slab_triggers_all_types(&slab)
            .try_collect()
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!("Final GetAllTypes failed for slab {slab_id}: {e:?}")
            })?;

        let store_all_tuples: Vec<TriggerTuple> = store_all
            .iter()
            .map(|t| (t.timer_type, t.key.clone(), t.time))
            .collect();

        if expected_all != store_all_tuples {
            return Err(color_eyre::eyre::eyre!(
                "Final all-types mismatch for slab {slab_id}: expected {expected_all:?}, got \
                 {store_all_tuples:?}"
            ));
        }

        // Verify ordering
        for window in store_all_tuples.windows(2) {
            if window[0] >= window[1] {
                return Err(color_eyre::eyre::eyre!(
                    "Final ordering violation for slab {slab_id}: {:?} >= {:?}",
                    window[0],
                    window[1]
                ));
            }
        }
    }

    Ok(())
}

/// Verifies that slab trigger operations match the reference model behavior.
///
/// # Test Strategy
///
/// 1. Start with an empty store (no triggers)
/// 2. Apply a sequence of random operations to both the store and the model
/// 3. For every `Get` operation, verify the store's response matches the model
/// 4. After all operations, verify final state matches
///
/// All slabs are constructed using `operations.segment().id` — the store's own
/// segment — since the store always queries its own partition regardless of
/// what `segment_id` was embedded in the slab.
///
/// # Errors
///
/// Returns an error if:
/// - Store operations fail
/// - Store state doesn't match model state
/// - Ordering invariants are violated
/// - Type filtering is incorrect
pub async fn prop_slab_trigger_model_equivalence<T>(
    operations: &T,
    input: SlabTriggerTestInput,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let segment_id = operations.segment().id;

    // Clean up the slabs from any previous trial
    for slab_id in 0u32..5 {
        let slab = Slab::new(segment_id, slab_id, input.slab_size);
        operations
            .clear_slab_triggers(&slab)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to clear slab {slab_id}: {e:?}"))?;
    }

    let mut model = SlabTriggerModel::new();

    for (op_idx, op) in input.operations.iter().enumerate() {
        match op {
            SlabTriggerOperation::Insert { slab_id, trigger } => {
                model.apply(op);
                let slab = Slab::new(segment_id, *slab_id, input.slab_size);
                operations
                    .insert_slab_trigger(slab, trigger.clone())
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Insert trigger failed: {e:?}")
                    })?;
            }
            SlabTriggerOperation::GetByType {
                slab_id,
                timer_type,
            } => {
                let slab = Slab::new(segment_id, *slab_id, input.slab_size);
                verify_slab_triggers_by_type(operations, &model, &slab, *timer_type, op_idx)
                    .await?;
            }
            SlabTriggerOperation::GetAllTypes(slab_id) => {
                let slab = Slab::new(segment_id, *slab_id, input.slab_size);
                verify_slab_triggers_all_types(operations, &model, &slab, op_idx).await?;
            }
            SlabTriggerOperation::Delete {
                slab_id,
                timer_type,
                key,
                time,
            } => {
                model.apply(op);
                let slab = Slab::new(segment_id, *slab_id, input.slab_size);
                operations
                    .delete_slab_trigger(&slab, *timer_type, key, *time)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Delete trigger failed: {e:?}")
                    })?;
            }
            SlabTriggerOperation::Clear(slab_id) => {
                model.apply(op);
                let slab = Slab::new(segment_id, *slab_id, input.slab_size);
                operations.clear_slab_triggers(&slab).await.map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} Clear slab failed: {e:?}")
                })?;
            }
        }
    }

    verify_final_slab_state(operations, &model, input.slab_size).await
}
