//! Property-based tests for defer store operations.
//!
//! Tests the `DeferStore` trait using a simple reference model to verify
//! correctness across both memory and Cassandra implementations.

use crate::Offset;
use crate::consumer::middleware::defer::store::{DeferStore, RetryCompletionResult};
use crate::timers::datetime::CompactDateTime;
use ahash::HashMap;
use quickcheck::{Arbitrary, Gen};
use std::collections::{BTreeSet, HashSet};
use std::error::Error;
use uuid::Uuid;

/// Operations that can be performed on the defer store.
#[derive(Clone, Debug)]
pub enum DeferOperation {
    /// Get the next deferred message for a key.
    GetNext(Uuid),
    /// Check if a key is deferred.
    IsDeferred(Uuid),
    /// Defer first message for a key (compound operation).
    DeferFirst {
        /// Key ID.
        key_id: Uuid,
        /// Offset to defer.
        offset: Offset,
        /// Expected retry time (for TTL).
        expected_retry_time: CompactDateTime,
    },
    /// Defer additional message for an already-deferred key (compound
    /// operation).
    DeferAdditional {
        /// Key ID.
        key_id: Uuid,
        /// Offset to defer.
        offset: Offset,
        /// Expected retry time (for TTL).
        expected_retry_time: CompactDateTime,
    },
    /// Complete successful retry (compound operation).
    CompleteRetrySuccess {
        /// Key ID.
        key_id: Uuid,
        /// Offset that was successfully retried.
        offset: Offset,
    },
    /// Increment retry count (compound operation).
    IncrementRetryCount {
        /// Key ID.
        key_id: Uuid,
        /// Current retry count.
        current_retry_count: u32,
    },
    /// Append a new offset for a key (primitive operation).
    Append {
        /// Key ID.
        key_id: Uuid,
        /// Offset to append.
        offset: Offset,
        /// Expected retry time (for TTL).
        expected_retry_time: CompactDateTime,
    },
    /// Remove an offset for a key (primitive operation).
    Remove {
        /// Key ID.
        key_id: Uuid,
        /// Offset to remove.
        offset: Offset,
    },
    /// Set retry count for a key (hidden primitive operation).
    SetRetryCount {
        /// Key ID.
        key_id: Uuid,
        /// New retry count.
        retry_count: u32,
    },
    /// Delete all data for a key (hidden primitive operation).
    DeleteKey(Uuid),
}

/// Test input containing isolated key IDs and operations.
#[derive(Clone, Debug)]
pub struct DeferTestInput {
    /// Pool of key IDs used by operations in this trial.
    pub key_ids: Vec<Uuid>,
    /// Sequence of operations to apply.
    pub operations: Vec<DeferOperation>,
}

impl Arbitrary for DeferTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 3-5 random UUIDs unique to this trial
        let key_count = (usize::arbitrary(g) % 3) + 3;
        let key_ids: Vec<Uuid> = (0..key_count).map(|_| Uuid::new_v4()).collect();

        // Generate 20-50 operations using these keys
        let op_count = (usize::arbitrary(g) % 30) + 20;
        let mut operations = Vec::with_capacity(op_count);

        // Track which keys have had DeferFirst called to avoid calling it multiple
        // times
        let mut deferred_keys = HashSet::new();

        for _ in 0..op_count {
            let idx = usize::arbitrary(g) % key_ids.len();
            let key_id = key_ids[idx];

            // Helper to generate offset and retry time
            let offset = Offset::from(i64::from(u32::arbitrary(g)));
            let now_timestamp = chrono::Utc::now().timestamp() as u32;
            let offset_seconds = (u32::arbitrary(g) % 3600) + 60; // 1-60 mins
            let expected_retry_time = CompactDateTime::from(now_timestamp + offset_seconds);

            let op = match u8::arbitrary(g) % 100 {
                // Compound operations (60%)
                0..=14 => {
                    // DeferFirst (15%) - only if not already deferred
                    if deferred_keys.contains(&key_id) {
                        // Use DeferAdditional instead
                        DeferOperation::DeferAdditional {
                            key_id,
                            offset,
                            expected_retry_time,
                        }
                    } else {
                        deferred_keys.insert(key_id);
                        DeferOperation::DeferFirst {
                            key_id,
                            offset,
                            expected_retry_time,
                        }
                    }
                }
                15..=29 => {
                    // DeferAdditional (15%)
                    DeferOperation::DeferAdditional {
                        key_id,
                        offset,
                        expected_retry_time,
                    }
                }
                30..=44 => {
                    // CompleteRetrySuccess (15%)
                    DeferOperation::CompleteRetrySuccess { key_id, offset }
                }
                45..=59 => {
                    // IncrementRetryCount (15%)
                    let current_retry_count = u32::arbitrary(g) % 10;
                    DeferOperation::IncrementRetryCount {
                        key_id,
                        current_retry_count,
                    }
                }

                // Queries (20%)
                60..=69 => {
                    // GetNext (10%)
                    DeferOperation::GetNext(key_id)
                }
                70..=79 => {
                    // IsDeferred (10%)
                    DeferOperation::IsDeferred(key_id)
                }

                // Primitive operations (20%)
                80..=84 => {
                    // Append (5%)
                    DeferOperation::Append {
                        key_id,
                        offset,
                        expected_retry_time,
                    }
                }
                85..=89 => {
                    // Remove (5%)
                    DeferOperation::Remove { key_id, offset }
                }
                90..=94 => {
                    // SetRetryCount (5%)
                    let retry_count = u32::arbitrary(g) % 20;
                    DeferOperation::SetRetryCount {
                        key_id,
                        retry_count,
                    }
                }
                _ => {
                    // DeleteKey (5%) - clear from deferred_keys
                    deferred_keys.remove(&key_id);
                    DeferOperation::DeleteKey(key_id)
                }
            };
            operations.push(op);
        }

        Self {
            key_ids,
            operations,
        }
    }
}

/// Reference model for defer store behavior.
///
/// Uses simple data structures to track expected state:
/// - `BTreeSet<Offset>` for offsets (maintains FIFO ordering)
/// - `Option<u32>` for `retry_count` (represents Cassandra static column NULL
///   state)
///
/// A key can exist in the model with an empty offset set, representing
/// a Cassandra partition with only the static `retry_count` column set
/// (created via `set_retry_count` on a non-existent key).
#[derive(Clone, Debug)]
pub struct DeferModel {
    /// Map from `key_id` to (offsets, `retry_count`).
    ///
    /// States:
    /// - Key not present: No partition in Cassandra
    /// - `retry_count = None`: Static column is NULL (reads as 0)
    /// - `retry_count = Some(n)`: Static column explicitly set to `n`
    /// - Empty offsets: Partition exists but has no clustering rows
    keys: HashMap<Uuid, (BTreeSet<Offset>, Option<u32>)>,
}

impl Default for DeferModel {
    fn default() -> Self {
        Self::new()
    }
}

impl DeferModel {
    /// Creates a new empty defer model.
    #[must_use]
    pub fn new() -> Self {
        Self {
            keys: HashMap::default(),
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &DeferOperation) {
        match op {
            // Queries don't modify state
            DeferOperation::GetNext(_) | DeferOperation::IsDeferred(_) => {}

            // Compound operations
            DeferOperation::DeferFirst { key_id, offset, .. } => {
                // Set retry_count=0 and append offset
                // Note: Does NOT delete existing offsets (Cassandra INSERT behavior)
                let entry = self
                    .keys
                    .entry(*key_id)
                    .or_insert_with(|| (BTreeSet::new(), None));
                entry.0.insert(*offset);
                entry.1 = Some(0);
            }
            DeferOperation::DeferAdditional { key_id, offset, .. } => {
                // Append offset without modifying retry_count
                let entry = self.keys.entry(*key_id).or_insert_with(|| {
                    // Shouldn't happen (should use DeferFirst first), but handle gracefully
                    (BTreeSet::new(), Some(0))
                });
                entry.0.insert(*offset);
            }
            DeferOperation::CompleteRetrySuccess { key_id, offset } => {
                if let Some(entry) = self.keys.get_mut(key_id) {
                    // Remove the offset
                    entry.0.remove(offset);

                    // Check if more messages exist
                    if entry.0.is_empty() {
                        // No more messages - delete entire key
                        self.keys.remove(key_id);
                    } else {
                        // More messages exist - reset retry_count to 0
                        entry.1 = Some(0);
                    }
                }
            }
            DeferOperation::IncrementRetryCount {
                key_id,
                current_retry_count,
            } => {
                // Increment retry count (saturating_add)
                // Note: Creates partition if doesn't exist (via set_retry_count)
                let new_count = current_retry_count.saturating_add(1);
                let entry = self
                    .keys
                    .entry(*key_id)
                    .or_insert_with(|| (BTreeSet::new(), None));
                entry.1 = Some(new_count);
            }

            // Primitive operations
            DeferOperation::Append { key_id, offset, .. } => {
                let entry = self.keys.entry(*key_id).or_insert_with(|| {
                    // When creating a new key, retry_count defaults to None (NULL in Cassandra)
                    (BTreeSet::new(), None)
                });
                entry.0.insert(*offset);
            }
            DeferOperation::Remove { key_id, offset } => {
                if let Some(entry) = self.keys.get_mut(key_id) {
                    entry.0.remove(offset);
                }
            }
            DeferOperation::SetRetryCount {
                key_id,
                retry_count,
            } => {
                // Match Cassandra: UPDATE creates partition with static column even if no
                // offsets exist
                let entry = self
                    .keys
                    .entry(*key_id)
                    .or_insert_with(|| (BTreeSet::new(), None));
                entry.1 = Some(*retry_count);
            }
            DeferOperation::DeleteKey(key_id) => {
                self.keys.remove(key_id);
            }
        }
    }

    /// Gets the next deferred message for a key.
    ///
    /// Returns `None` if the key has no offsets, even if `retry_count` is set.
    /// This matches Cassandra behavior where partitions with only static
    /// columns return empty result sets (zero rows).
    ///
    /// When returning a value, converts NULL `retry_count` (None) to 0,
    /// matching the Cassandra read behavior in `get_next_deferred_message`.
    #[must_use]
    pub fn get_next(&self, key_id: &Uuid) -> Option<(Offset, u32)> {
        self.keys.get(key_id).and_then(|(offsets, retry_count)| {
            // Only return a result if there are actual offsets (BTreeSet iteration is
            // ordered) Convert None (NULL) to 0 to match Cassandra read
            // behavior
            offsets
                .iter()
                .next()
                .map(|&offset| (offset, retry_count.unwrap_or(0)))
        })
    }

    /// Checks if a key is deferred.
    ///
    /// Returns `retry_count` if the key has offsets, `None` otherwise.
    #[must_use]
    pub fn is_deferred(&self, key_id: &Uuid) -> Option<u32> {
        self.get_next(key_id).map(|(_, retry_count)| retry_count)
    }

    /// Returns all key IDs in the model.
    #[must_use]
    pub fn all_key_ids(&self) -> Vec<Uuid> {
        self.keys.keys().copied().collect()
    }
}

/// Verifies that model and store are equivalent for all key IDs.
async fn verify_final_state<S>(
    store: &S,
    model: &DeferModel,
    input: &DeferTestInput,
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    // Check ALL keys used in operations, not just those in model
    // This catches Cassandra partitions with only static columns set (from
    // SetRetryCount)
    for key_id in &input.key_ids {
        let model_result = model.get_next(key_id);
        let store_result = store
            .get_next_deferred_message(key_id)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Get next failed: {e:?}"))?;

        match (model_result, store_result) {
            (Some((exp_offset, exp_retry)), Some((act_offset, act_retry))) => {
                if exp_offset != act_offset {
                    return Err(color_eyre::eyre::eyre!(
                        "Offset mismatch for {key_id}: expected {exp_offset}, got {act_offset}"
                    ));
                }
                if exp_retry != act_retry {
                    return Err(color_eyre::eyre::eyre!(
                        "Retry count mismatch for {key_id}: expected {exp_retry}, got {act_retry}"
                    ));
                }
            }
            (None, None) => {
                // Both agree key has no messages - correct
            }
            (Some((exp_offset, exp_retry)), None) => {
                return Err(color_eyre::eyre::eyre!(
                    "Key {key_id} exists in model (offset={exp_offset}, retry={exp_retry}) but \
                     not in store"
                ));
            }
            (None, Some((act_offset, act_retry))) => {
                return Err(color_eyre::eyre::eyre!(
                    "Key {key_id} exists in store (offset={act_offset}, retry={act_retry}) but \
                     not in model"
                ));
            }
        }
    }

    Ok(())
}

/// Verifies result of `complete_retry_success` matches model expectations.
fn verify_completion_result(
    expected_next: Option<(Offset, u32)>,
    result: RetryCompletionResult,
    key_id: &Uuid,
    op_idx: usize,
) -> color_eyre::Result<()> {
    match (expected_next, result) {
        (Some((expected_offset, _)), RetryCompletionResult::MoreMessages { next_offset }) => {
            if expected_offset != next_offset {
                return Err(color_eyre::eyre::eyre!(
                    "Op #{op_idx} CompleteRetrySuccess offset mismatch for {key_id}: expected \
                     next_offset={expected_offset}, got {next_offset}"
                ));
            }
        }
        (None, RetryCompletionResult::Completed) => {
            // Both agree no more messages - correct
        }
        (Some((expected_offset, _)), RetryCompletionResult::Completed) => {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} CompleteRetrySuccess mismatch for {key_id}: expected MoreMessages \
                 with offset={expected_offset}, got Completed"
            ));
        }
        (None, RetryCompletionResult::MoreMessages { next_offset }) => {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} CompleteRetrySuccess mismatch for {key_id}: expected Completed, got \
                 MoreMessages with offset={next_offset}"
            ));
        }
    }
    Ok(())
}

/// Handles query operations (`GetNext`, `IsDeferred`) by verifying against
/// model.
async fn apply_query_operation<S>(
    store: &S,
    model: &DeferModel,
    op: &DeferOperation,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    match op {
        DeferOperation::GetNext(key_id) => {
            let expected = model.get_next(key_id);
            let actual = store
                .get_next_deferred_message(key_id)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetNext failed: {e:?}"))?;

            if expected != actual {
                return Err(color_eyre::eyre::eyre!(
                    "Op #{op_idx} GetNext mismatch for {key_id}: expected {expected:?}, got \
                     {actual:?}"
                ));
            }
            Ok(())
        }
        DeferOperation::IsDeferred(key_id) => {
            let expected = model.is_deferred(key_id);
            let actual = store
                .is_deferred(key_id)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} IsDeferred failed: {e:?}"))?;

            if expected != actual {
                return Err(color_eyre::eyre::eyre!(
                    "Op #{op_idx} IsDeferred mismatch for {key_id}: expected {expected:?}, got \
                     {actual:?}"
                ));
            }
            Ok(())
        }
        _ => Err(color_eyre::eyre::eyre!(
            "Internal error: query operation expected"
        )),
    }
}

/// Applies `complete_retry_success` with result verification.
async fn apply_complete_retry<S>(
    store: &S,
    model: &mut DeferModel,
    key_id: &Uuid,
    offset: Offset,
    op_idx: usize,
    op: &DeferOperation,
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    model.apply(op);

    let result = store
        .complete_retry_success(key_id, offset)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} CompleteRetrySuccess failed: {e:?}"))?;

    let expected_next = model.get_next(key_id);
    verify_completion_result(expected_next, result, key_id, op_idx)
}

/// Applies `increment_retry_count` with return value verification.
async fn apply_increment_retry<S>(
    store: &S,
    model: &mut DeferModel,
    key_id: &Uuid,
    current_retry_count: u32,
    op_idx: usize,
    op: &DeferOperation,
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    model.apply(op);
    let new_count = store
        .increment_retry_count(key_id, current_retry_count)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} IncrementRetryCount failed: {e:?}"))?;

    let expected_count = current_retry_count.saturating_add(1);
    if new_count != expected_count {
        return Err(color_eyre::eyre::eyre!(
            "Op #{op_idx} IncrementRetryCount returned {new_count}, expected {expected_count}"
        ));
    }
    Ok(())
}

/// Handles mutation operations by applying to model and executing on store.
async fn apply_mutation_operation<S>(
    store: &S,
    model: &mut DeferModel,
    op: &DeferOperation,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    match op {
        DeferOperation::DeferFirst {
            key_id,
            offset,
            expected_retry_time,
        } => {
            model.apply(op);
            store
                .defer_first_message(key_id, *offset, *expected_retry_time)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} DeferFirst failed: {e:?}"))?;
            Ok(())
        }
        DeferOperation::DeferAdditional {
            key_id,
            offset,
            expected_retry_time,
        } => {
            model.apply(op);
            store
                .defer_additional_message(key_id, *offset, *expected_retry_time)
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} DeferAdditional failed: {e:?}")
                })?;
            Ok(())
        }
        DeferOperation::CompleteRetrySuccess { key_id, offset } => {
            apply_complete_retry(store, model, key_id, *offset, op_idx, op).await
        }
        DeferOperation::IncrementRetryCount {
            key_id,
            current_retry_count,
        } => apply_increment_retry(store, model, key_id, *current_retry_count, op_idx, op).await,
        DeferOperation::Append {
            key_id,
            offset,
            expected_retry_time,
        } => {
            model.apply(op);
            store
                .append_deferred_message(key_id, *offset, *expected_retry_time)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} Append failed: {e:?}"))?;
            Ok(())
        }
        DeferOperation::Remove { key_id, offset } => {
            model.apply(op);
            store
                .remove_deferred_message(key_id, *offset)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} Remove failed: {e:?}"))?;
            Ok(())
        }
        DeferOperation::SetRetryCount {
            key_id,
            retry_count,
        } => {
            model.apply(op);
            store
                .set_retry_count(key_id, *retry_count)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} SetRetryCount failed: {e:?}"))?;
            Ok(())
        }
        DeferOperation::DeleteKey(key_id) => {
            model.apply(op);
            store
                .delete_key(key_id)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} DeleteKey failed: {e:?}"))?;
            Ok(())
        }
        _ => Err(color_eyre::eyre::eyre!(
            "Internal error: mutation operation expected"
        )),
    }
}

/// Applies a single operation to both store and model, verifying results.
async fn apply_operation<S>(
    store: &S,
    model: &mut DeferModel,
    op: &DeferOperation,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    match op {
        DeferOperation::GetNext(_) | DeferOperation::IsDeferred(_) => {
            apply_query_operation(store, model, op, op_idx).await
        }
        _ => apply_mutation_operation(store, model, op, op_idx).await,
    }
}

/// Verifies that defer store operations match the reference model behavior.
///
/// # Test Strategy
///
/// 1. Start with empty store and model
/// 2. Apply sequence of random operations to both
/// 3. Verify that for every key ID:
///    - `store.get_next(key_id)` matches `model.get_next(key_id)`
///    - Both offset and `retry_count` are identical
///
/// # Errors
///
/// Returns an error if:
/// - Store operations fail
/// - Store state doesn't match model state
pub async fn prop_defer_store_model_equivalence<S>(
    store: &S,
    input: DeferTestInput,
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    // Clean up keys from this trial to ensure isolation
    for key_id in &input.key_ids {
        store
            .delete_key(key_id)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to clean up key: {e:?}"))?;
    }

    let mut model = DeferModel::new();

    // Apply all operations to both store and model, verifying queries inline
    for (op_idx, op) in input.operations.iter().enumerate() {
        apply_operation(store, &mut model, op, op_idx).await?;
    }

    // Verify final state matches
    verify_final_state(store, &model, &input).await?;

    Ok(())
}
