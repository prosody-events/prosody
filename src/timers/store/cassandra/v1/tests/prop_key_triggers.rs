//! Property-based tests for v1 key trigger operations.
//!
//! Tests the low-level v1 key trigger CRUD operations in isolation using a
//! simple reference model to verify correctness. V1 key triggers are stored
//! in the `timer_keys` table without `timer_type` field.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
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

/// Operations that can be performed on v1 key triggers.
#[derive(Clone, Debug)]
pub enum V1KeyTriggerOperation {
    /// Insert a trigger into the `timer_keys` table.
    InsertTrigger {
        /// The segment ID.
        segment_id: SegmentId,
        /// The v1 trigger to insert.
        trigger: TriggerV1,
    },
    /// Retrieve all triggers for a key.
    GetTriggers {
        /// The segment ID.
        segment_id: SegmentId,
        /// The key to query.
        key: Key,
    },
    /// Delete a specific trigger for a key.
    Delete {
        /// The segment ID.
        segment_id: SegmentId,
        /// The key.
        key: Key,
        /// The time to delete.
        time: CompactDateTime,
    },
    /// Clear all triggers for a key from v1 `timer_keys` table.
    ClearKey {
        /// The segment ID.
        segment_id: SegmentId,
        /// The key to clear.
        key: Key,
    },
}

/// Test input containing isolated segment IDs and operations.
///
/// Each test trial uses randomly generated segment IDs, ensuring complete
/// isolation between trials and allowing parallel test execution.
#[derive(Clone, Debug)]
pub struct V1KeyTriggerTestInput {
    /// Pool of segment IDs used by operations in this trial.
    pub segment_ids: Vec<SegmentId>,
    /// Sequence of operations to apply.
    pub operations: Vec<V1KeyTriggerOperation>,
}

impl Arbitrary for V1KeyTriggerTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 3 random v4 UUIDs unique to this trial
        let segment_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

        // Generate 10-50 operations using these segments
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        for _ in 0..op_count {
            let idx = usize::from(u8::arbitrary(g)) % segment_ids.len();
            let segment_id = segment_ids[idx];

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
                    V1KeyTriggerOperation::InsertTrigger {
                        segment_id,
                        trigger,
                    }
                }
                1 => {
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 10).into();
                    V1KeyTriggerOperation::GetTriggers { segment_id, key }
                }
                2 => {
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 10).into();
                    let time = CompactDateTime::arbitrary(g);
                    V1KeyTriggerOperation::Delete {
                        segment_id,
                        key,
                        time,
                    }
                }
                _ => {
                    let key: Key = format!("key-{}", u8::arbitrary(g) % 10).into();
                    V1KeyTriggerOperation::ClearKey { segment_id, key }
                }
            };
            operations.push(op);
        }

        Self {
            segment_ids,
            operations,
        }
    }
}

/// Reference model for v1 key trigger operations.
///
/// Tracks actual trigger data per (`segment_id`, `key`) to verify correctness.
#[derive(Clone, Debug)]
pub struct V1KeyTriggerModel {
    /// Track triggers per (`segment_id`, `key`).
    triggers: HashMap<(SegmentId, Key), BTreeSet<TriggerV1Tuple>>,
}

impl Default for V1KeyTriggerModel {
    fn default() -> Self {
        Self::new()
    }
}

impl V1KeyTriggerModel {
    /// Creates a new empty v1 key trigger model.
    #[must_use]
    pub fn new() -> Self {
        Self {
            triggers: HashMap::default(),
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &V1KeyTriggerOperation) {
        match op {
            V1KeyTriggerOperation::InsertTrigger {
                segment_id,
                trigger,
            } => {
                self.triggers
                    .entry((*segment_id, trigger.key.clone()))
                    .or_default()
                    .insert((trigger.key.clone(), trigger.time));
            }
            V1KeyTriggerOperation::GetTriggers { .. } => {
                // Queries don't modify state
            }
            V1KeyTriggerOperation::Delete {
                segment_id,
                key,
                time,
            } => {
                if let Some(triggers) = self.triggers.get_mut(&(*segment_id, key.clone())) {
                    triggers.remove(&(key.clone(), *time));
                }
            }
            V1KeyTriggerOperation::ClearKey { segment_id, key } => {
                self.triggers.remove(&(*segment_id, key.clone()));
            }
        }
    }

    /// Gets all triggers for a key in ascending order.
    #[must_use]
    pub fn get_triggers(&self, segment_id: &SegmentId, key: &Key) -> Vec<TriggerV1Tuple> {
        self.triggers
            .get(&(*segment_id, key.clone()))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns all (`segment_id`, `key`) pairs in the model.
    #[must_use]
    pub fn all_keys(&self) -> Vec<(SegmentId, Key)> {
        self.triggers.keys().cloned().collect()
    }
}

/// Applies v1 key trigger operations to both store and model with inline
/// verification.
///
/// # Errors
///
/// Returns an error if operations fail or query results don't match model.
async fn apply_v1_key_trigger_operations(
    v1_ops: &V1Operations,
    model: &mut V1KeyTriggerModel,
    operations: &[V1KeyTriggerOperation],
) -> color_eyre::Result<()> {
    for (op_idx, op) in operations.iter().enumerate() {
        match op {
            V1KeyTriggerOperation::InsertTrigger {
                segment_id,
                trigger,
            } => {
                model.apply(op);
                v1_ops
                    .insert_key_trigger(segment_id, trigger.clone())
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Insert v1 key trigger failed: {e:?}")
                    })?;
            }
            V1KeyTriggerOperation::GetTriggers { segment_id, key } => {
                let expected = model.get_triggers(segment_id, key);
                let actual: Vec<TriggerV1> = v1_ops
                    .get_key_triggers(segment_id, key)
                    .try_collect()
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} GetTriggers v1 key failed: {e:?}")
                    })?;

                let actual_tuples: Vec<TriggerV1Tuple> =
                    actual.iter().map(|t| (t.key.clone(), t.time)).collect();

                if expected != actual_tuples {
                    return Err(color_eyre::eyre::eyre!(
                        "Op #{op_idx} GetTriggers v1 key query mismatch for ({segment_id}, \
                         {key}): expected {expected:?}, got {actual_tuples:?}"
                    ));
                }

                for window in actual_tuples.windows(2) {
                    if window[0] >= window[1] {
                        return Err(color_eyre::eyre::eyre!(
                            "Op #{op_idx} GetTriggers v1 key ordering violation for \
                             ({segment_id}, {key}): {window:?}"
                        ));
                    }
                }
            }
            V1KeyTriggerOperation::Delete {
                segment_id,
                key,
                time,
            } => {
                model.apply(op);
                v1_ops
                    .delete_key_trigger(segment_id, key, *time)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Delete v1 key trigger failed: {e:?}")
                    })?;
            }
            V1KeyTriggerOperation::ClearKey { segment_id, key } => {
                model.apply(op);
                v1_ops
                    .clear_key_triggers(segment_id, key)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Clear v1 key triggers failed: {e:?}")
                    })?;
            }
        }
    }
    Ok(())
}

/// Verifies that v1 key trigger operations match the reference model behavior.
///
/// # Test Strategy
///
/// 1. Start with empty store and model
/// 2. Apply sequence of random operations to both
/// 3. Verify that for every (`segment_id`, `key`):
///    - `operations.get_key_triggers(seg, key)` matches model
///    - Ordering is correct (ascending by key, time)
///
/// # Errors
///
/// Returns an error if:
/// - V1 operations fail (insert, delete, get, clear)
/// - Store state doesn't match model state
/// - Ordering invariants are violated
pub async fn prop_v1_key_trigger_model_equivalence(
    operations: &V1Operations,
    input: V1KeyTriggerTestInput,
) -> color_eyre::Result<()> {
    for segment_id in &input.segment_ids {
        for key_idx in 0_i32..10_i32 {
            let key: Key = format!("key-{key_idx}").into();
            operations
                .clear_key_triggers(segment_id, &key)
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Failed to clear v1 key triggers during cleanup: {e:?}")
                })?;
        }
    }

    let mut model = V1KeyTriggerModel::new();
    apply_v1_key_trigger_operations(operations, &mut model, &input.operations).await?;

    let all_keys = model.all_keys();
    for (segment_id, key) in &all_keys {
        let model_triggers = model.get_triggers(segment_id, key);
        let operations_triggers: Vec<TriggerV1> = operations
            .get_key_triggers(segment_id, key)
            .try_collect()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Get v1 key triggers failed: {e:?}"))?;

        let operations_tuples: Vec<TriggerV1Tuple> = operations_triggers
            .iter()
            .map(|t| (t.key.clone(), t.time))
            .collect();

        if model_triggers != operations_tuples {
            return Err(color_eyre::eyre::eyre!(
                "V1 key trigger mismatch for ({}, {}): expected {:?}, got {:?}",
                segment_id,
                key,
                model_triggers,
                operations_tuples
            ));
        }

        for window in operations_tuples.windows(2) {
            if window[0] >= window[1] {
                return Err(color_eyre::eyre::eyre!(
                    "V1 key trigger ordering violation for ({}, {}): {:?} >= {:?}",
                    segment_id,
                    key,
                    window[0],
                    window[1]
                ));
            }
        }
    }

    Ok(())
}

/// [`QuickCheck`] wrapper for v1 key trigger model equivalence property.
pub fn test_prop_v1_key_trigger_model_equivalence(
    operations: &V1Operations,
    input: V1KeyTriggerTestInput,
) -> TestResult {
    use tokio::runtime::Runtime;

    // Initialize tracing subscriber to create valid spans in tests
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::ERROR)
        .try_init();

    let Ok(rt) = Runtime::new() else {
        return TestResult::error("Failed to create tokio runtime");
    };

    let result =
        rt.block_on(async { prop_v1_key_trigger_model_equivalence(operations, input).await });

    match result {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(format!("{e:?}")),
    }
}
