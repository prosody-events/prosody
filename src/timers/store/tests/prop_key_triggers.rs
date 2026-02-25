//! Property-based tests for key trigger table operations.
//!
//! Tests the low-level key trigger CRUD operations in isolation using a
//! simple reference model to verify correctness.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
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
use uuid::Uuid;

/// Test input containing isolated segment IDs and operations.
///
/// Each test trial uses randomly generated segment IDs, ensuring complete
/// isolation between trials and allowing parallel test execution.
#[derive(Clone, Debug)]
pub struct KeyTriggerTestInput {
    /// Pool of segment IDs used by operations in this trial.
    pub segment_ids: Vec<SegmentId>,
    /// Sequence of operations to apply.
    pub operations: Vec<KeyTriggerOperation>,
    /// Slab size used for all triggers in this test.
    pub slab_size: CompactDuration,
}

/// Operations that can be performed on the key trigger table.
#[derive(Clone, Debug)]
pub enum KeyTriggerOperation {
    /// Insert a trigger for a key.
    Insert {
        /// The segment ID.
        segment_id: SegmentId,
        /// The trigger to insert.
        trigger: Trigger,
    },
    /// Retrieve scheduled times for a key and timer type.
    GetTimes {
        /// The segment ID.
        segment_id: SegmentId,
        /// The timer type.
        timer_type: TimerType,
        /// The key.
        key: Key,
    },
    /// Retrieve full triggers for a key and timer type.
    GetTriggers {
        /// The segment ID.
        segment_id: SegmentId,
        /// The timer type.
        timer_type: TimerType,
        /// The key.
        key: Key,
    },
    /// Retrieve all triggers for a key across all types.
    GetAllTypes {
        /// The segment ID.
        segment_id: SegmentId,
        /// The key.
        key: Key,
    },
    /// Delete a specific trigger for a key.
    Delete {
        /// The segment ID.
        segment_id: SegmentId,
        /// The timer type.
        timer_type: TimerType,
        /// The key.
        key: Key,
        /// The time.
        time: CompactDateTime,
    },
    /// Clear all triggers for a key and timer type.
    ClearByType {
        /// The segment ID.
        segment_id: SegmentId,
        /// The timer type.
        timer_type: TimerType,
        /// The key.
        key: Key,
    },
    /// Clear all triggers for a key across all types.
    ClearAllTypes {
        /// The segment ID.
        segment_id: SegmentId,
        /// The key.
        key: Key,
    },
    /// Atomically clear all triggers for a key/type and schedule a new one
    /// (`clear_and_schedule_key`).
    ClearAndSchedule {
        /// The segment ID.
        segment_id: SegmentId,
        /// The replacement trigger.
        trigger: Trigger,
    },
}

impl Arbitrary for KeyTriggerTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Single segment per trial — matches the production invariant that each
        // store instance is scoped to one partition (one segment). Using multiple
        // segments would break the per-partition state cache.
        let segment_ids = vec![Uuid::new_v4()];

        // Generate a slab size for this test (1 second to 7 days to avoid TTL overflow)
        let slab_size = CompactDuration::new(u32::arbitrary(g).clamp(1, 604_800));

        // Use small key pool to increase collision probability
        let key_pool = ["key-a", "key-b", "key-c"];

        // Generate 10-50 operations using these segments
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        for _ in 0..op_count {
            let idx = usize::from(u8::arbitrary(g)) % segment_ids.len();
            let segment_id = segment_ids[idx];

            let key_idx = usize::from(u8::arbitrary(g)) % key_pool.len();
            let key = Key::from(key_pool[key_idx]);

            let timer_type = match u8::arbitrary(g) % 3 {
                0 => TimerType::Application,
                1 => TimerType::DeferredMessage,
                _ => TimerType::DeferredTimer,
            };

            let time = CompactDateTime::arbitrary(g);

            let op = match u8::arbitrary(g) % 8 {
                0 => {
                    // Insert operation
                    let trigger = Trigger::new(key, time, timer_type, Span::current());
                    KeyTriggerOperation::Insert {
                        segment_id,
                        trigger,
                    }
                }
                1 => KeyTriggerOperation::GetTimes {
                    segment_id,
                    timer_type,
                    key,
                },
                2 => KeyTriggerOperation::GetTriggers {
                    segment_id,
                    timer_type,
                    key,
                },
                3 => KeyTriggerOperation::GetAllTypes { segment_id, key },
                4 => KeyTriggerOperation::Delete {
                    segment_id,
                    timer_type,
                    key,
                    time,
                },
                5 => KeyTriggerOperation::ClearByType {
                    segment_id,
                    timer_type,
                    key,
                },
                6 => {
                    let trigger = Trigger::new(key, time, timer_type, Span::current());
                    KeyTriggerOperation::ClearAndSchedule {
                        segment_id,
                        trigger,
                    }
                }
                _ => KeyTriggerOperation::ClearAllTypes { segment_id, key },
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

/// Reference model for key trigger table behavior.
///
/// Uses `HashMap<(SegmentId, Key), BTreeSet<(TimerType, CompactDateTime)>>`
/// to track triggers for each key. [`BTreeSet`] provides natural ordering
/// and set semantics.
#[derive(Clone, Debug)]
pub struct KeyTriggerModel {
    triggers: HashMap<(SegmentId, Key), BTreeSet<(TimerType, CompactDateTime)>>,
}

impl Default for KeyTriggerModel {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyTriggerModel {
    /// Creates a new empty key trigger model.
    #[must_use]
    pub fn new() -> Self {
        Self {
            triggers: HashMap::default(),
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &KeyTriggerOperation) {
        match op {
            KeyTriggerOperation::Insert {
                segment_id,
                trigger,
            } => {
                self.triggers
                    .entry((*segment_id, trigger.key.clone()))
                    .or_default()
                    .insert((trigger.timer_type, trigger.time));
            }
            KeyTriggerOperation::GetTimes { .. }
            | KeyTriggerOperation::GetTriggers { .. }
            | KeyTriggerOperation::GetAllTypes { .. } => {
                // Queries don't modify state
            }
            KeyTriggerOperation::Delete {
                segment_id,
                timer_type,
                key,
                time,
            } => {
                if let Some(set) = self.triggers.get_mut(&(*segment_id, key.clone())) {
                    set.remove(&(*timer_type, *time));
                }
            }
            KeyTriggerOperation::ClearByType {
                segment_id,
                timer_type,
                key,
            } => {
                if let Some(set) = self.triggers.get_mut(&(*segment_id, key.clone())) {
                    set.retain(|(tt, _)| *tt != *timer_type);
                }
            }
            KeyTriggerOperation::ClearAllTypes { segment_id, key } => {
                self.triggers.remove(&(*segment_id, key.clone()));
            }
            KeyTriggerOperation::ClearAndSchedule {
                segment_id,
                trigger,
            } => {
                // Clear all triggers for this key/type, then insert the new one
                let entry = self
                    .triggers
                    .entry((*segment_id, trigger.key.clone()))
                    .or_default();
                entry.retain(|(tt, _)| *tt != trigger.timer_type);
                entry.insert((trigger.timer_type, trigger.time));
            }
        }
    }

    /// Gets scheduled times for a key and timer type in ascending order.
    #[must_use]
    pub fn get_times(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> Vec<CompactDateTime> {
        self.triggers
            .get(&(*segment_id, key.clone()))
            .map(|set| {
                set.iter()
                    .filter(|(tt, _)| *tt == timer_type)
                    .map(|(_, time)| *time)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Gets full triggers for a key and timer type in ascending order.
    #[must_use]
    pub fn get_triggers(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> Vec<(TimerType, CompactDateTime)> {
        self.triggers
            .get(&(*segment_id, key.clone()))
            .map(|set| {
                set.iter()
                    .filter(|(tt, _)| *tt == timer_type)
                    .copied()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Gets all triggers for a key across all types in ascending order.
    #[must_use]
    pub fn get_all_types(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> Vec<(TimerType, CompactDateTime)> {
        self.triggers
            .get(&(*segment_id, key.clone()))
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Returns all keys in the model.
    #[must_use]
    pub fn all_keys(&self) -> Vec<(SegmentId, Key)> {
        self.triggers.keys().cloned().collect()
    }
}

/// Verifies key times for a specific segment, timer type, and key.
///
/// # Errors
///
/// Returns an error if times don't match or ordering is wrong.
async fn verify_key_times<T>(
    operations: &T,
    model: &KeyTriggerModel,
    segment_id: &SegmentId,
    timer_type: TimerType,
    key: &Key,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let model_times = model.get_times(segment_id, timer_type, key);
    let store_times: Vec<CompactDateTime> = operations
        .get_key_times(timer_type, key)
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Get key times failed: {e:?}"))?;

    if model_times != store_times {
        return Err(color_eyre::eyre::eyre!(
            "Time mismatch for key ({segment_id}, {key}) type {timer_type:?}: expected \
             {model_times:?}, got {store_times:?}"
        ));
    }

    // Verify ordering of times
    for window in store_times.windows(2) {
        if window[0] >= window[1] {
            return Err(color_eyre::eyre::eyre!(
                "Time ordering violation for key ({segment_id}, {key}) type {timer_type:?}: \
                 {window:?}"
            ));
        }
    }

    Ok(())
}

/// Verifies key triggers for a specific segment, timer type, and key.
///
/// # Errors
///
/// Returns an error if triggers don't match, ordering is wrong, or type
/// filtering fails.
async fn verify_key_triggers<T>(
    operations: &T,
    model: &KeyTriggerModel,
    segment_id: &SegmentId,
    timer_type: TimerType,
    key: &Key,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let model_triggers = model.get_triggers(segment_id, timer_type, key);
    let store_triggers: Vec<Trigger> = operations
        .get_key_triggers(timer_type, key)
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Get key triggers failed: {e:?}"))?;

    let store_tuples: Vec<(TimerType, CompactDateTime)> = store_triggers
        .iter()
        .map(|t| (t.timer_type, t.time))
        .collect();

    if model_triggers != store_tuples {
        return Err(color_eyre::eyre::eyre!(
            "Trigger mismatch for key ({segment_id}, {key}) type {timer_type:?}: expected \
             {model_triggers:?}, got {store_tuples:?}"
        ));
    }

    // Verify all returned triggers match the requested type and key
    for trigger in &store_triggers {
        if trigger.timer_type != timer_type {
            return Err(color_eyre::eyre::eyre!(
                "Type filtering failed: expected {timer_type:?}, got {:?} for key ({segment_id}, \
                 {key})",
                trigger.timer_type
            ));
        }
        if &trigger.key != key {
            return Err(color_eyre::eyre::eyre!(
                "Key mismatch: expected {key:?}, got {:?}",
                trigger.key
            ));
        }
    }

    // Verify times match trigger times
    let store_times: Vec<CompactDateTime> = operations
        .get_key_times(timer_type, key)
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Get key times failed: {e:?}"))?;

    let trigger_times: Vec<CompactDateTime> = store_triggers.iter().map(|t| t.time).collect();
    if store_times != trigger_times {
        return Err(color_eyre::eyre::eyre!(
            "Times vs triggers mismatch for key ({segment_id}, {key}) type {timer_type:?}: times \
             {store_times:?}, trigger_times {trigger_times:?}"
        ));
    }

    Ok(())
}

/// Verifies all trigger types for a specific segment and key.
///
/// # Errors
///
/// Returns an error if triggers don't match, ordering is wrong, or keys don't
/// match.
async fn verify_all_types<T>(
    operations: &T,
    model: &KeyTriggerModel,
    segment_id: &SegmentId,
    key: &Key,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let model_all = model.get_all_types(segment_id, key);
    let store_all: Vec<Trigger> = operations
        .get_key_triggers_all_types(key)
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Get key triggers all types failed: {e:?}"))?;

    let store_all_tuples: Vec<(TimerType, CompactDateTime)> =
        store_all.iter().map(|t| (t.timer_type, t.time)).collect();

    if model_all != store_all_tuples {
        return Err(color_eyre::eyre::eyre!(
            "All-types trigger mismatch for key ({segment_id}, {key}): expected {model_all:?}, \
             got {store_all_tuples:?}"
        ));
    }

    // Verify ordering in all-types query
    for window in store_all_tuples.windows(2) {
        if window[0] >= window[1] {
            return Err(color_eyre::eyre::eyre!(
                "Ordering violation in all-types for key ({segment_id}, {key}): {window:?}"
            ));
        }
    }

    // Verify all triggers have the correct key
    for trigger in &store_all {
        if &trigger.key != key {
            return Err(color_eyre::eyre::eyre!(
                "Key mismatch in all-types: expected {key:?}, got {:?}",
                trigger.key
            ));
        }
    }

    Ok(())
}

/// Applies a single operation to both the store and the reference model.
async fn apply_operation<T>(
    operations: &T,
    model: &mut KeyTriggerModel,
    op: &KeyTriggerOperation,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    match op {
        KeyTriggerOperation::Insert { trigger, .. } => {
            model.apply(op);
            operations
                .insert_key_trigger(trigger.clone())
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} Insert trigger failed: {e:?}")
                })?;
        }
        KeyTriggerOperation::GetTimes {
            segment_id,
            timer_type,
            key,
        } => {
            verify_key_times(operations, model, segment_id, *timer_type, key)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetTimes: {e}"))?;
        }
        KeyTriggerOperation::GetTriggers {
            segment_id,
            timer_type,
            key,
        } => {
            verify_key_triggers(operations, model, segment_id, *timer_type, key)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetTriggers: {e}"))?;
        }
        KeyTriggerOperation::GetAllTypes { segment_id, key } => {
            verify_all_types(operations, model, segment_id, key)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetAllTypes: {e}"))?;
        }
        KeyTriggerOperation::Delete {
            timer_type,
            key,
            time,
            ..
        } => {
            model.apply(op);
            operations
                .delete_key_trigger(*timer_type, key, *time)
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} Delete trigger failed: {e:?}")
                })?;
        }
        KeyTriggerOperation::ClearByType {
            timer_type, key, ..
        } => {
            model.apply(op);
            operations
                .clear_key_triggers(*timer_type, key)
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} Clear triggers by type failed: {e:?}")
                })?;
        }
        KeyTriggerOperation::ClearAllTypes { key, .. } => {
            model.apply(op);
            operations
                .clear_key_triggers_all_types(key)
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} Clear all triggers for key failed: {e:?}")
                })?;
        }
        KeyTriggerOperation::ClearAndSchedule { trigger, .. } => {
            model.apply(op);
            operations
                .clear_and_schedule_key(trigger.clone())
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} ClearAndSchedule key failed: {e:?}")
                })?;
        }
    }
    Ok(())
}

/// Verifies that key trigger operations match the reference model behavior.
///
/// # Test Strategy
///
/// 1. Start with empty store and model
/// 2. Apply sequence of random operations to both
/// 3. Verify that for every key:
///    - `operations.get_key_times(seg, type, key)` matches
///      `model.get_times(seg, type, key)`
///    - `operations.get_key_triggers(seg, type, key)` matches
///      `model.get_triggers(seg, type, key)`
///    - `operations.get_key_triggers_all_types(seg, key)` matches
///      `model.get_all_types(seg, key)`
///    - Ordering is correct (ascending by `timer_type`, time)
///    - Type filtering works correctly
///
/// # Errors
///
/// Returns an error if:
/// - Store operations fail (insert, delete, get, clear)
/// - Store state doesn't match model state
/// - Ordering invariants are violated
/// - Type filtering is incorrect
pub async fn prop_key_trigger_model_equivalence<T>(
    operations: &T,
    input: KeyTriggerTestInput,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    // Clean up the keys from this trial to ensure isolation
    // Even with unique v4 UUIDs, cleanup prevents test pollution if trials fail and
    // rerun
    let key_pool = ["key-a", "key-b", "key-c"]; // Match the pool in Arbitrary

    for _segment_id in &input.segment_ids {
        for key_str in &key_pool {
            let key = Key::from(*key_str);
            operations
                .clear_key_triggers_all_types(&key)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Failed to clear key triggers: {e:?}"))?;
        }
    }

    let mut model = KeyTriggerModel::new();

    for (op_idx, op) in input.operations.iter().enumerate() {
        apply_operation(operations, &mut model, op, op_idx).await?;
    }

    // Final sanity check: verify model-store equivalence for all keys
    // (queries were already verified inline, this catches any missed state)
    for (segment_id, key) in &model.all_keys() {
        for &timer_type in TimerType::VARIANTS {
            verify_key_times(operations, &model, segment_id, timer_type, key).await?;
            verify_key_triggers(operations, &model, segment_id, timer_type, key).await?;
        }
        verify_all_types(operations, &model, segment_id, key).await?;
    }

    Ok(())
}
