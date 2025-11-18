//! Property-based tests for defer store operations.
//!
//! Tests the `DeferStore` trait using a simple reference model to verify
//! correctness across both memory and Cassandra implementations.

use crate::Offset;
use crate::consumer::middleware::defer::store::DeferStore;
use crate::timers::datetime::CompactDateTime;
use ahash::HashMap;
use quickcheck::{Arbitrary, Gen};
use std::collections::BTreeSet;
use std::error::Error;
use uuid::Uuid;

/// Operations that can be performed on the defer store.
#[derive(Clone, Debug)]
pub enum DeferOperation {
    /// Get the next deferred message for a key.
    GetNext(Uuid),
    /// Append a new offset for a key (with optional retry_count).
    Append {
        /// Key ID.
        key_id: Uuid,
        /// Offset to append.
        offset: Offset,
        /// Expected retry time (for TTL).
        expected_retry_time: CompactDateTime,
        /// Optional retry count (for first failure).
        retry_count: Option<u32>,
    },
    /// Remove an offset for a key.
    Remove {
        /// Key ID.
        key_id: Uuid,
        /// Offset to remove.
        offset: Offset,
    },
    /// Set retry count for a key.
    SetRetryCount {
        /// Key ID.
        key_id: Uuid,
        /// New retry count.
        retry_count: u32,
    },
    /// Delete all data for a key.
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

        for _ in 0..op_count {
            let idx = usize::arbitrary(g) % key_ids.len();
            let key_id = key_ids[idx];

            let op = match u8::arbitrary(g) % 10 {
                0..=3 => {
                    // Append operation (40% - most common)
                    // Use unsigned to avoid overflow issues with i64::MIN.abs()
                    let offset = Offset::from(i64::from(u32::arbitrary(g)));
                    // Use a fixed future time for test operations
                    let now_timestamp = chrono::Utc::now().timestamp() as u32;
                    let offset_seconds = (u32::arbitrary(g) % 3600) + 60; // 1-60 mins
                    let expected_retry_time = CompactDateTime::from(now_timestamp + offset_seconds);

                    // Sometimes include retry_count (30% of appends)
                    let retry_count = if u8::arbitrary(g) % 10 < 3 {
                        Some(u32::arbitrary(g) % 10)
                    } else {
                        None
                    };

                    DeferOperation::Append {
                        key_id,
                        offset,
                        expected_retry_time,
                        retry_count,
                    }
                }
                4..=6 => {
                    // GetNext operation (30%)
                    DeferOperation::GetNext(key_id)
                }
                7 => {
                    // Remove operation (10%)
                    let offset = Offset::from(i64::from(u32::arbitrary(g)));
                    DeferOperation::Remove { key_id, offset }
                }
                8 => {
                    // SetRetryCount operation (10%)
                    let retry_count = u32::arbitrary(g) % 20;
                    DeferOperation::SetRetryCount {
                        key_id,
                        retry_count,
                    }
                }
                _ => {
                    // DeleteKey operation (10%)
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
/// - `Option<u32>` for retry_count (represents Cassandra static column NULL
///   state)
///
/// A key can exist in the model with an empty offset set, representing
/// a Cassandra partition with only the static retry_count column set
/// (created via `set_retry_count` on a non-existent key).
#[derive(Clone, Debug)]
pub struct DeferModel {
    /// Map from key_id to (offsets, retry_count).
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
            DeferOperation::GetNext(_) => {
                // Queries don't modify state
            }
            DeferOperation::Append {
                key_id,
                offset,
                retry_count,
                ..
            } => {
                let entry = self.keys.entry(*key_id).or_insert_with(|| {
                    // When creating a new key, retry_count defaults to None (NULL in Cassandra)
                    (BTreeSet::new(), None)
                });
                entry.0.insert(*offset);
                // Only update retry_count if explicitly provided (Some value in operation)
                if retry_count.is_some() {
                    entry.1 = *retry_count;
                }
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
    /// When returning a value, converts NULL retry_count (None) to 0, matching
    /// the Cassandra read behavior in `get_next_deferred_message`.
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
        match op {
            DeferOperation::GetNext(key_id) => {
                // Verify query immediately against model
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
            }
            DeferOperation::Append {
                key_id,
                offset,
                expected_retry_time,
                retry_count,
            } => {
                model.apply(op);
                store
                    .append_deferred_message(key_id, *offset, *expected_retry_time, *retry_count)
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} Append failed: {e:?}"))?;
            }
            DeferOperation::Remove { key_id, offset } => {
                model.apply(op);
                store
                    .remove_deferred_message(key_id, *offset)
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} Remove failed: {e:?}"))?;
            }
            DeferOperation::SetRetryCount {
                key_id,
                retry_count,
            } => {
                model.apply(op);
                store
                    .set_retry_count(key_id, *retry_count)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} SetRetryCount failed: {e:?}")
                    })?;
            }
            DeferOperation::DeleteKey(key_id) => {
                model.apply(op);
                store
                    .delete_key(key_id)
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} DeleteKey failed: {e:?}"))?;
            }
        }
    }

    // Verify final state matches
    verify_final_state(store, &model, &input).await?;

    Ok(())
}
