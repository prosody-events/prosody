//! Property-based tests for slab trigger table operations.
//!
//! Tests the low-level slab trigger CRUD operations in isolation using a
//! simple reference model to verify correctness.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::SegmentId;
use crate::timers::store::operations::TriggerOperations;
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use futures::TryStreamExt;
use quickcheck::{Arbitrary, Gen};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::Debug;
use tracing::Span;
use uuid::Uuid;

/// Test input containing isolated segment IDs and operations.
///
/// Each test trial uses randomly generated segment IDs, ensuring complete
/// isolation between trials and allowing parallel test execution.
#[derive(Clone, Debug)]
pub struct SlabTriggerTestInput {
    /// Pool of segment IDs used by operations in this trial.
    pub segment_ids: Vec<SegmentId>,
    /// Sequence of operations to apply.
    pub operations: Vec<SlabTriggerOperation>,
    /// Slab size used for all slabs in this test.
    pub slab_size: CompactDuration,
}

/// Operations that can be performed on the slab trigger table.
#[derive(Clone, Debug)]
pub enum SlabTriggerOperation {
    /// Insert a trigger into a slab.
    Insert {
        /// The slab to insert into.
        slab: Slab,
        /// The trigger to insert.
        trigger: Trigger,
    },
    /// Retrieve triggers of a specific type from a slab.
    GetByType {
        /// The slab to query.
        slab: Slab,
        /// The timer type to filter by.
        timer_type: TimerType,
    },
    /// Retrieve all triggers from a slab across all types.
    GetAllTypes(Slab),
    /// Delete a specific trigger from a slab.
    Delete {
        /// The slab to delete from.
        slab: Slab,
        /// The timer type.
        timer_type: TimerType,
        /// The trigger key.
        key: Key,
        /// The trigger time.
        time: CompactDateTime,
    },
    /// Clear all triggers from a slab.
    Clear(Slab),
}

impl Arbitrary for SlabTriggerTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 3 random v4 UUIDs unique to this trial
        // v4 UUIDs have a structure that prevents QuickCheck from shrinking them
        // into colliding values
        let segment_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

        // Generate a slab size for this test (1 second to 7 days to avoid TTL overflow)
        let slab_size = CompactDuration::new(u32::arbitrary(g).clamp(1, 604_800));

        // Use small pools for keys to increase collision probability
        let key_pool = ["key-a", "key-b", "key-c"];

        // Generate 10-50 operations using these segments
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        for _ in 0..op_count {
            let idx = usize::from(u8::arbitrary(g)) % segment_ids.len();
            let segment_id = segment_ids[idx];

            let slab_id = SlabId::from(u8::arbitrary(g) % 5); // 0-4
            let slab = Slab::new(segment_id, slab_id, slab_size);

            let key_idx = usize::from(u8::arbitrary(g)) % key_pool.len();
            let key = Key::from(key_pool[key_idx]);

            let timer_type = if bool::arbitrary(g) {
                TimerType::Application
            } else {
                TimerType::DeferRetry
            };

            let time = CompactDateTime::arbitrary(g);

            let op = match u8::arbitrary(g) % 5 {
                0 => {
                    // Insert operation
                    let trigger = Trigger::new(key, time, timer_type, Span::none());
                    SlabTriggerOperation::Insert { slab, trigger }
                }
                1 => SlabTriggerOperation::GetByType { slab, timer_type },
                2 => SlabTriggerOperation::GetAllTypes(slab),
                3 => SlabTriggerOperation::Delete {
                    slab,
                    timer_type,
                    key,
                    time,
                },
                _ => SlabTriggerOperation::Clear(slab),
            };
            operations.push(op);
        }

        Self {
            segment_ids,
            operations,
            slab_size,
        }
    }
}

/// Slab key combining segment ID, slab size, and slab ID for trigger tracking.
type SlabKey = (SegmentId, u32, SlabId);

/// Trigger tuple containing type, key, and time.
type TriggerTuple = (TimerType, Key, CompactDateTime);

/// Reference model for slab trigger table behavior.
///
/// Uses `HashMap<SlabKey, BTreeSet<TriggerTuple>>`
/// to track triggers for each slab. The key includes `slab_size` (as seconds)
/// because Cassandra's partition key includes it. [`BTreeSet`] provides natural
/// ordering and set semantics.
#[derive(Clone, Debug)]
pub struct SlabTriggerModel {
    triggers: HashMap<SlabKey, BTreeSet<TriggerTuple>>,
}

impl Default for SlabTriggerModel {
    fn default() -> Self {
        Self::new()
    }
}

impl SlabTriggerModel {
    /// Creates a new empty slab trigger model.
    #[must_use]
    pub fn new() -> Self {
        Self {
            triggers: HashMap::default(),
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &SlabTriggerOperation) {
        match op {
            SlabTriggerOperation::Insert { slab, trigger } => {
                self.triggers
                    .entry((*slab.segment_id(), slab.size().seconds(), slab.id()))
                    .or_default()
                    .insert((trigger.timer_type, trigger.key.clone(), trigger.time));
            }
            SlabTriggerOperation::GetByType { .. } | SlabTriggerOperation::GetAllTypes(_) => {
                // Queries don't modify state
            }
            SlabTriggerOperation::Delete {
                slab,
                timer_type,
                key,
                time,
            } => {
                if let Some(set) =
                    self.triggers
                        .get_mut(&(*slab.segment_id(), slab.size().seconds(), slab.id()))
                {
                    set.remove(&(*timer_type, key.clone(), *time));
                }
            }
            SlabTriggerOperation::Clear(slab) => {
                self.triggers
                    .remove(&(*slab.segment_id(), slab.size().seconds(), slab.id()));
            }
        }
    }

    /// Gets triggers of a specific type from a slab in ascending order.
    #[must_use]
    pub fn get_by_type(
        &self,
        slab: &Slab,
        timer_type: TimerType,
    ) -> Vec<(TimerType, Key, CompactDateTime)> {
        self.triggers
            .get(&(*slab.segment_id(), slab.size().seconds(), slab.id()))
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
    pub fn get_all_types(&self, slab: &Slab) -> Vec<(TimerType, Key, CompactDateTime)> {
        self.triggers
            .get(&(*slab.segment_id(), slab.size().seconds(), slab.id()))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns all slab keys in the model.
    #[must_use]
    pub fn all_slabs(&self) -> Vec<(SegmentId, u32, SlabId)> {
        self.triggers.keys().copied().collect()
    }
}

/// Verifies slab triggers for a specific timer type.
///
/// # Errors
///
/// Returns an error if triggers don't match or ordering is wrong.
async fn verify_slab_triggers_by_type<T>(
    operations: &T,
    model: &SlabTriggerModel,
    slab: &Slab,
    timer_type: TimerType,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let segment_id = slab.segment_id();
    let slab_id = slab.id();

    let model_triggers = model.get_by_type(slab, timer_type);
    let store_triggers: Vec<Trigger> = operations
        .get_slab_triggers(slab, timer_type)
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Get slab triggers failed: {e:?}"))?;

    let store_tuples: Vec<TriggerTuple> = store_triggers
        .iter()
        .map(|t| (t.timer_type, t.key.clone(), t.time))
        .collect();

    if model_triggers != store_tuples {
        return Err(color_eyre::eyre::eyre!(
            "Trigger mismatch for slab ({segment_id}, {slab_id}) type {timer_type:?}: expected \
             {model_triggers:?}, got {store_tuples:?}"
        ));
    }

    // Verify ordering within type
    for window in store_tuples.windows(2) {
        if window[0] >= window[1] {
            return Err(color_eyre::eyre::eyre!(
                "Ordering violation in slab ({segment_id}, {slab_id}) type {timer_type:?}: \
                 {window:?}"
            ));
        }
    }

    // Verify all returned triggers match the requested type
    for trigger in &store_triggers {
        if trigger.timer_type != timer_type {
            return Err(color_eyre::eyre::eyre!(
                "Type filtering failed: expected {timer_type:?}, got {:?} in slab ({segment_id}, \
                 {slab_id})",
                trigger.timer_type
            ));
        }
    }

    Ok(())
}

/// Verifies all-types query against the model.
///
/// # Errors
///
/// Returns an error if:
/// - Query fails
/// - Returned triggers don't match model
/// - Ordering is incorrect
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
    let expected = model.get_all_types(slab);
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
            "Op #{op_idx} GetAllTypes query mismatch for slab ({}, {}): expected {expected:?}, \
             got {actual_tuples:?}",
            slab.segment_id(),
            slab.id()
        ));
    }

    // Verify ordering
    for window in actual_tuples.windows(2) {
        if window[0] >= window[1] {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} GetAllTypes ordering violation for slab ({}, {}): {window:?}",
                slab.segment_id(),
                slab.id()
            ));
        }
    }

    Ok(())
}

/// Cleans up slab triggers for the test trial.
///
/// # Errors
///
/// Returns an error if cleanup fails.
async fn cleanup_slab_triggers<T>(
    operations: &T,
    segment_ids: &[SegmentId],
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let slab_size_pool = [3600, 7200, 1800]; // Match the pool in Arbitrary

    for segment_id in segment_ids {
        for slab_id in 0..5 {
            // Match the slab_id range in Arbitrary
            for &slab_size_seconds in &slab_size_pool {
                let slab_size = CompactDuration::new(slab_size_seconds);
                let slab = Slab::new(*segment_id, slab_id, slab_size);
                operations
                    .clear_slab_triggers(&slab)
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Failed to clear slab triggers: {e:?}"))?;
            }
        }
    }

    Ok(())
}

/// Verifies final state of all slabs in the model.
///
/// # Errors
///
/// Returns an error if:
/// - Any query fails
/// - Returned triggers don't match model
/// - Ordering is incorrect
async fn verify_final_slab_state<T>(
    operations: &T,
    model: &SlabTriggerModel,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let all_slabs = model.all_slabs();

    for (segment_id, slab_size_seconds, slab_id) in &all_slabs {
        // Reconstruct slab with the correct size
        let slab_size = CompactDuration::new(*slab_size_seconds);
        let slab = Slab::new(*segment_id, *slab_id, slab_size);

        // Verify get_slab_triggers for each timer type
        for timer_type in [TimerType::Application, TimerType::DeferRetry] {
            verify_slab_triggers_by_type(operations, model, &slab, timer_type).await?;
        }

        // Verify get_slab_triggers_all_types
        let model_all = model.get_all_types(&slab);
        let store_all: Vec<Trigger> = operations
            .get_slab_triggers_all_types(&slab)
            .try_collect()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Get slab triggers all types failed: {e:?}"))?;

        let store_all_tuples: Vec<(TimerType, Key, CompactDateTime)> = store_all
            .iter()
            .map(|t| (t.timer_type, t.key.clone(), t.time))
            .collect();

        if model_all != store_all_tuples {
            return Err(color_eyre::eyre::eyre!(
                "All-types trigger mismatch for slab ({}, {}): expected {:?}, got {:?}",
                segment_id,
                slab_id,
                model_all,
                store_all_tuples
            ));
        }

        // Verify ordering in all-types query
        for window in store_all_tuples.windows(2) {
            if window[0] >= window[1] {
                return Err(color_eyre::eyre::eyre!(
                    "Ordering violation in all-types for slab ({}, {}): {:?} >= {:?}",
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

/// Verifies that slab trigger operations match the reference model behavior.
///
/// # Test Strategy
///
/// 1. Start with empty store and model
/// 2. Apply sequence of random operations to both
/// 3. Verify that for every slab:
///    - `operations.get_slab_triggers(slab, type)` matches
///      `model.get_by_type(slab, type)`
///    - `operations.get_slab_triggers_all_types(slab)` matches
///      `model.get_all_types(slab)`
///    - Ordering is correct (ascending by `timer_type`, key, time)
///    - Type filtering works correctly
///
/// # Errors
///
/// Returns an error if:
/// - Store operations fail (insert, delete, get, clear)
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
    // Clean up the slabs from this trial to ensure isolation
    // Even with unique v4 UUIDs, cleanup prevents test pollution if trials fail and
    // rerun
    cleanup_slab_triggers(operations, &input.segment_ids).await?;

    let mut model = SlabTriggerModel::new();

    // Apply all operations to both store and model, verifying queries inline
    for (op_idx, op) in input.operations.iter().enumerate() {
        match op {
            SlabTriggerOperation::Insert { slab, trigger } => {
                model.apply(op);
                operations
                    .insert_slab_trigger(slab.clone(), trigger.clone())
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Insert trigger failed: {e:?}")
                    })?;
            }
            SlabTriggerOperation::GetByType { slab, timer_type } => {
                // Verify query immediately against model
                verify_slab_triggers_by_type(operations, &model, slab, *timer_type)
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetByType: {e}"))?;
            }
            SlabTriggerOperation::GetAllTypes(slab) => {
                // Verify query immediately against model
                verify_slab_triggers_all_types(operations, &model, slab, op_idx).await?;
            }
            SlabTriggerOperation::Delete {
                slab,
                timer_type,
                key,
                time,
            } => {
                model.apply(op);
                operations
                    .delete_slab_trigger(slab, *timer_type, key, *time)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Delete trigger failed: {e:?}")
                    })?;
            }
            SlabTriggerOperation::Clear(slab) => {
                model.apply(op);
                operations.clear_slab_triggers(slab).await.map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} Clear triggers failed: {e:?}")
                })?;
            }
        }
    }

    // Final sanity check: verify model-store equivalence for all slabs
    // (queries were already verified inline, this catches any missed state)
    verify_final_slab_state(operations, &model).await?;

    Ok(())
}
