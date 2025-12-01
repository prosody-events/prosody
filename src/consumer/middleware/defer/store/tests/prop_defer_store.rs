//! Property-based tests for defer store operations.
//!
//! Tests the `DeferStore` trait using a simple reference model to verify
//! correctness across both memory and Cassandra implementations.

use crate::consumer::middleware::defer::store::{DeferStore, RetryCompletionResult};
use crate::timers::datetime::CompactDateTime;
use crate::{Key, Offset};
use ahash::HashMap;
use color_eyre::eyre::Report;
use quickcheck::{Arbitrary, Gen};
use std::collections::{BTreeSet, HashSet};
use std::error::Error;
use std::sync::Arc;

/// Test helper holding a message key.
///
/// Since stores are now constructed per-segment via the provider pattern,
/// tests only need to track the message key. The model uses the key directly
/// as the identifier.
#[derive(Clone, Debug)]
pub struct TestKeyComponents {
    /// Message key (business key for ordering).
    pub key: Key,
}

impl TestKeyComponents {
    /// Creates test key components with the given index for uniqueness.
    fn new(index: usize) -> Self {
        Self {
            key: Arc::from(format!("test-key-{index}")),
        }
    }
}

/// Operations that can be performed on the defer store.
///
/// Operations reference keys by index into `DeferTestInput.key_components`.
#[derive(Clone, Debug)]
pub enum DeferOperation {
    /// Get the next deferred message for a key.
    GetNext(usize),
    /// Check if a key is deferred.
    IsDeferred(usize),
    /// Defer first message for a key (compound operation).
    DeferFirst {
        /// Index into `key_components`.
        key_index: usize,
        /// Offset to defer.
        offset: Offset,
        /// Expected retry time (for TTL).
        expected_retry_time: CompactDateTime,
    },
    /// Defer additional message for an already-deferred key (compound
    /// operation).
    DeferAdditional {
        /// Index into `key_components`.
        key_index: usize,
        /// Offset to defer.
        offset: Offset,
        /// Expected retry time (for TTL).
        expected_retry_time: CompactDateTime,
    },
    /// Complete successful retry (compound operation).
    CompleteRetrySuccess {
        /// Index into `key_components`.
        key_index: usize,
        /// Offset that was successfully retried.
        offset: Offset,
    },
    /// Increment retry count (compound operation).
    IncrementRetryCount {
        /// Index into `key_components`.
        key_index: usize,
        /// Current retry count.
        current_retry_count: u32,
    },
    /// Append a new offset for a key (primitive operation).
    Append {
        /// Index into `key_components`.
        key_index: usize,
        /// Offset to append.
        offset: Offset,
        /// Expected retry time (for TTL).
        expected_retry_time: CompactDateTime,
    },
    /// Remove an offset for a key (primitive operation).
    Remove {
        /// Index into `key_components`.
        key_index: usize,
        /// Offset to remove.
        offset: Offset,
    },
    /// Set retry count for a key (hidden primitive operation).
    SetRetryCount {
        /// Index into `key_components`.
        key_index: usize,
        /// New retry count.
        retry_count: u32,
    },
    /// Delete all data for a key (hidden primitive operation).
    DeleteKey(usize),
}

/// Test input containing isolated key components and operations.
#[derive(Clone, Debug)]
pub struct DeferTestInput {
    /// Pool of key components used by operations in this trial.
    /// Operations reference these by index.
    pub key_components: Vec<TestKeyComponents>,
    /// Sequence of operations to apply.
    pub operations: Vec<DeferOperation>,
}

impl Arbitrary for DeferTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 3-5 unique key components for this trial
        let key_count = (usize::arbitrary(g) % 3) + 3;
        let key_components: Vec<TestKeyComponents> =
            (0..key_count).map(TestKeyComponents::new).collect();

        // Generate 20-50 operations using these keys
        let op_count = (usize::arbitrary(g) % 30) + 20;
        let mut operations = Vec::with_capacity(op_count);

        // Track which key indices have had DeferFirst called to avoid calling it
        // multiple times
        let mut deferred_indices = HashSet::new();

        for _ in 0..op_count {
            let key_index = usize::arbitrary(g) % key_components.len();

            // Helper to generate offset and retry time
            let offset = Offset::from(i64::from(u32::arbitrary(g)));
            let now_timestamp = chrono::Utc::now().timestamp() as u32;
            let offset_seconds = (u32::arbitrary(g) % 3600) + 60; // 1-60 mins
            let expected_retry_time = CompactDateTime::from(now_timestamp + offset_seconds);

            let op = match u8::arbitrary(g) % 100 {
                // Compound operations (60%)
                0..=14 => {
                    // DeferFirst (15%) - only if not already deferred
                    if deferred_indices.contains(&key_index) {
                        // Use DeferAdditional instead
                        DeferOperation::DeferAdditional {
                            key_index,
                            offset,
                            expected_retry_time,
                        }
                    } else {
                        deferred_indices.insert(key_index);
                        DeferOperation::DeferFirst {
                            key_index,
                            offset,
                            expected_retry_time,
                        }
                    }
                }
                15..=29 => {
                    // DeferAdditional (15%)
                    DeferOperation::DeferAdditional {
                        key_index,
                        offset,
                        expected_retry_time,
                    }
                }
                30..=44 => {
                    // CompleteRetrySuccess (15%)
                    DeferOperation::CompleteRetrySuccess { key_index, offset }
                }
                45..=59 => {
                    // IncrementRetryCount (15%)
                    let current_retry_count = u32::arbitrary(g) % 10;
                    DeferOperation::IncrementRetryCount {
                        key_index,
                        current_retry_count,
                    }
                }

                // Queries (20%)
                60..=69 => {
                    // GetNext (10%)
                    DeferOperation::GetNext(key_index)
                }
                70..=79 => {
                    // IsDeferred (10%)
                    DeferOperation::IsDeferred(key_index)
                }

                // Primitive operations (20%)
                80..=84 => {
                    // Append (5%)
                    DeferOperation::Append {
                        key_index,
                        offset,
                        expected_retry_time,
                    }
                }
                85..=89 => {
                    // Remove (5%)
                    DeferOperation::Remove { key_index, offset }
                }
                90..=94 => {
                    // SetRetryCount (5%)
                    let retry_count = u32::arbitrary(g) % 20;
                    DeferOperation::SetRetryCount {
                        key_index,
                        retry_count,
                    }
                }
                _ => {
                    // DeleteKey (5%) - clear from deferred_indices
                    deferred_indices.remove(&key_index);
                    DeferOperation::DeleteKey(key_index)
                }
            };
            operations.push(op);
        }

        Self {
            key_components,
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
    /// Map from message key to (offsets, `retry_count`).
    ///
    /// States:
    /// - Key not present: Key has no deferred messages
    /// - `retry_count = None`: Retry count not explicitly set (reads as 0)
    /// - `retry_count = Some(n)`: Retry count explicitly set to `n`
    /// - Empty offsets: Key exists but has no deferred offsets
    keys: HashMap<Key, (BTreeSet<Offset>, Option<u32>)>,
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
    ///
    /// Requires `key_components` to resolve key indices to keys.
    pub fn apply(&mut self, op: &DeferOperation, key_components: &[TestKeyComponents]) {
        match op {
            // Queries don't modify state
            DeferOperation::GetNext(_) | DeferOperation::IsDeferred(_) => {}

            // Compound operations
            DeferOperation::DeferFirst {
                key_index, offset, ..
            } => {
                let key = Arc::clone(&key_components[*key_index].key);
                // Set retry_count=0 and append offset
                // Note: Does NOT delete existing offsets (INSERT behavior)
                let entry = self
                    .keys
                    .entry(key)
                    .or_insert_with(|| (BTreeSet::new(), None));
                entry.0.insert(*offset);
                entry.1 = Some(0);
            }
            DeferOperation::DeferAdditional {
                key_index, offset, ..
            } => {
                let key = Arc::clone(&key_components[*key_index].key);
                // Append offset without modifying retry_count
                let entry = self.keys.entry(key).or_insert_with(|| {
                    // Shouldn't happen (should use DeferFirst first), but handle gracefully
                    (BTreeSet::new(), Some(0))
                });
                entry.0.insert(*offset);
            }
            DeferOperation::CompleteRetrySuccess { key_index, offset } => {
                let key = &key_components[*key_index].key;
                if let Some(entry) = self.keys.get_mut(key.as_ref()) {
                    // Remove the offset
                    entry.0.remove(offset);

                    // Check if more messages exist
                    if entry.0.is_empty() {
                        // No more messages - delete entire key
                        self.keys.remove(key.as_ref());
                    } else {
                        // More messages exist - reset retry_count to 0
                        entry.1 = Some(0);
                    }
                }
            }
            DeferOperation::IncrementRetryCount {
                key_index,
                current_retry_count,
            } => {
                let key = Arc::clone(&key_components[*key_index].key);
                // Increment retry count (saturating_add)
                // Note: Creates entry if doesn't exist (via set_retry_count)
                let new_count = current_retry_count.saturating_add(1);
                let entry = self
                    .keys
                    .entry(key)
                    .or_insert_with(|| (BTreeSet::new(), None));
                entry.1 = Some(new_count);
            }

            // Primitive operations
            DeferOperation::Append {
                key_index, offset, ..
            } => {
                let key = Arc::clone(&key_components[*key_index].key);
                let entry = self.keys.entry(key).or_insert_with(|| {
                    // When creating a new key, retry_count defaults to None
                    (BTreeSet::new(), None)
                });
                entry.0.insert(*offset);
            }
            DeferOperation::Remove { key_index, offset } => {
                let key = &key_components[*key_index].key;
                if let Some(entry) = self.keys.get_mut(key.as_ref()) {
                    entry.0.remove(offset);
                }
            }
            DeferOperation::SetRetryCount {
                key_index,
                retry_count,
            } => {
                let key = Arc::clone(&key_components[*key_index].key);
                // Match store behavior: set_retry_count creates entry even if no
                // offsets exist
                let entry = self
                    .keys
                    .entry(key)
                    .or_insert_with(|| (BTreeSet::new(), None));
                entry.1 = Some(*retry_count);
            }
            DeferOperation::DeleteKey(key_index) => {
                let key = &key_components[*key_index].key;
                self.keys.remove(key.as_ref());
            }
        }
    }

    /// Gets the next deferred message for a key.
    ///
    /// Returns `None` if the key has no offsets, even if `retry_count` is set.
    ///
    /// When returning a value, converts NULL `retry_count` (None) to 0,
    /// matching the store behavior in `get_next_deferred_message`.
    #[must_use]
    pub fn get_next(&self, key: &Key) -> Option<(Offset, u32)> {
        self.keys
            .get(key.as_ref())
            .and_then(|(offsets, retry_count)| {
                // Only return a result if there are actual offsets (BTreeSet iteration is
                // ordered) Convert None (NULL) to 0 to match store read behavior
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
    pub fn is_deferred(&self, key: &Key) -> Option<u32> {
        self.get_next(key).map(|(_, retry_count)| retry_count)
    }

    /// Returns all keys in the model.
    #[must_use]
    pub fn all_defer_keys(&self) -> Vec<Key> {
        self.keys.keys().map(Arc::clone).collect()
    }
}

/// Verifies that model and store are equivalent for all key components.
async fn verify_final_state<S>(
    store: &S,
    model: &DeferModel,
    input: &DeferTestInput,
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    // Check ALL keys used in operations
    for key_comp in &input.key_components {
        let key = &key_comp.key;

        let model_result = model.get_next(key);
        let store_result = store
            .get_next_deferred_message(key)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Get next failed: {e:?}"))?;

        match (model_result, store_result) {
            (Some((exp_offset, exp_retry)), Some((act_offset, act_retry))) => {
                if exp_offset != act_offset {
                    return Err(color_eyre::eyre::eyre!(
                        "Offset mismatch for key={key}: expected {exp_offset}, got {act_offset}"
                    ));
                }
                if exp_retry != act_retry {
                    return Err(color_eyre::eyre::eyre!(
                        "Retry count mismatch for key={key}: expected {exp_retry}, got {act_retry}"
                    ));
                }
            }
            (None, None) => {
                // Both agree key has no messages - correct
            }
            (Some((exp_offset, exp_retry)), None) => {
                return Err(color_eyre::eyre::eyre!(
                    "Key={key} exists in model (offset={exp_offset}, retry={exp_retry}) but not \
                     in store"
                ));
            }
            (None, Some((act_offset, act_retry))) => {
                return Err(color_eyre::eyre::eyre!(
                    "Key={key} exists in store (offset={act_offset}, retry={act_retry}) but not \
                     in model"
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
    key: &Key,
    op_idx: usize,
) -> color_eyre::Result<()> {
    match (expected_next, result) {
        (Some((expected_offset, _)), RetryCompletionResult::MoreMessages { next_offset }) => {
            if expected_offset != next_offset {
                return Err(color_eyre::eyre::eyre!(
                    "Op #{op_idx} CompleteRetrySuccess offset mismatch for key={key}: expected \
                     next_offset={expected_offset}, got {next_offset}"
                ));
            }
        }
        (None, RetryCompletionResult::Completed) => {
            // Both agree no more messages - correct
        }
        (Some((expected_offset, _)), RetryCompletionResult::Completed) => {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} CompleteRetrySuccess mismatch for key={key}: expected MoreMessages \
                 with offset={expected_offset}, got Completed"
            ));
        }
        (None, RetryCompletionResult::MoreMessages { next_offset }) => {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} CompleteRetrySuccess mismatch for key={key}: expected Completed, \
                 got MoreMessages with offset={next_offset}"
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
    key_components: &[TestKeyComponents],
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    match op {
        DeferOperation::GetNext(key_index) => {
            let key = &key_components[*key_index].key;

            let expected = model.get_next(key);
            let actual = store
                .get_next_deferred_message(key)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetNext failed: {e:?}"))?;

            if expected != actual {
                return Err(color_eyre::eyre::eyre!(
                    "Op #{op_idx} GetNext mismatch for key={key}: expected {expected:?}, got \
                     {actual:?}"
                ));
            }
            Ok(())
        }
        DeferOperation::IsDeferred(key_index) => {
            let key = &key_components[*key_index].key;

            let expected = model.is_deferred(key);
            let actual = store
                .is_deferred(key)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} IsDeferred failed: {e:?}"))?;

            if expected != actual {
                return Err(color_eyre::eyre::eyre!(
                    "Op #{op_idx} IsDeferred mismatch for key={key}: expected {expected:?}, got \
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
    key_index: usize,
    offset: Offset,
    op_idx: usize,
    op: &DeferOperation,
    key_components: &[TestKeyComponents],
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    let key = &key_components[key_index].key;

    model.apply(op, key_components);

    let result = store
        .complete_retry_success(key, offset)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} CompleteRetrySuccess failed: {e:?}"))?;

    let expected_next = model.get_next(key);
    verify_completion_result(expected_next, result, key, op_idx)
}

/// Applies `increment_retry_count` with return value verification.
async fn apply_increment_retry<S>(
    store: &S,
    model: &mut DeferModel,
    key_index: usize,
    current_retry_count: u32,
    op_idx: usize,
    op: &DeferOperation,
    key_components: &[TestKeyComponents],
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    let key = &key_components[key_index].key;

    model.apply(op, key_components);
    let new_count = store
        .increment_retry_count(key, current_retry_count)
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

/// Context for mutation operation execution.
struct MutationContext<'a, S> {
    store: &'a S,
    op_idx: usize,
    key_components: &'a [TestKeyComponents],
}

impl<S> MutationContext<'_, S>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    fn key(&self, key_index: usize) -> &Key {
        &self.key_components[key_index].key
    }

    fn err(&self, op_name: &str, e: S::Error) -> Report {
        color_eyre::eyre::eyre!("Op #{} {} failed: {e:?}", self.op_idx, op_name)
    }

    async fn defer_first(
        &self,
        model: &mut DeferModel,
        op: &DeferOperation,
        key_index: usize,
        offset: Offset,
        time: CompactDateTime,
    ) -> color_eyre::Result<()> {
        let key = self.key(key_index);
        model.apply(op, self.key_components);
        self.store
            .defer_first_message(key, offset, time)
            .await
            .map_err(|e| self.err("DeferFirst", e))
    }

    async fn defer_additional(
        &self,
        model: &mut DeferModel,
        op: &DeferOperation,
        key_index: usize,
        offset: Offset,
        time: CompactDateTime,
    ) -> color_eyre::Result<()> {
        let key = self.key(key_index);
        model.apply(op, self.key_components);
        self.store
            .defer_additional_message(key, offset, time)
            .await
            .map_err(|e| self.err("DeferAdditional", e))
    }

    async fn append(
        &self,
        model: &mut DeferModel,
        op: &DeferOperation,
        key_index: usize,
        offset: Offset,
        time: CompactDateTime,
    ) -> color_eyre::Result<()> {
        let key = self.key(key_index);
        model.apply(op, self.key_components);
        self.store
            .append_deferred_message(key, offset, time)
            .await
            .map_err(|e| self.err("Append", e))
    }

    async fn remove(
        &self,
        model: &mut DeferModel,
        op: &DeferOperation,
        key_index: usize,
        offset: Offset,
    ) -> color_eyre::Result<()> {
        let key = self.key(key_index);
        model.apply(op, self.key_components);
        self.store
            .remove_deferred_message(key, offset)
            .await
            .map_err(|e| self.err("Remove", e))
    }

    async fn set_retry_count(
        &self,
        model: &mut DeferModel,
        op: &DeferOperation,
        key_index: usize,
        retry_count: u32,
    ) -> color_eyre::Result<()> {
        let key = self.key(key_index);
        model.apply(op, self.key_components);
        self.store
            .set_retry_count(key, retry_count)
            .await
            .map_err(|e| self.err("SetRetryCount", e))
    }

    async fn delete_key(
        &self,
        model: &mut DeferModel,
        op: &DeferOperation,
        key_index: usize,
    ) -> color_eyre::Result<()> {
        let key = self.key(key_index);
        model.apply(op, self.key_components);
        self.store
            .delete_key(key)
            .await
            .map_err(|e| self.err("DeleteKey", e))
    }
}

/// Handles mutation operations by applying to model and executing on store.
async fn apply_mutation_operation<S>(
    store: &S,
    model: &mut DeferModel,
    op: &DeferOperation,
    op_idx: usize,
    key_components: &[TestKeyComponents],
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    let ctx = MutationContext {
        store,
        op_idx,
        key_components,
    };

    match op {
        DeferOperation::DeferFirst {
            key_index,
            offset,
            expected_retry_time,
        } => {
            ctx.defer_first(model, op, *key_index, *offset, *expected_retry_time)
                .await
        }
        DeferOperation::DeferAdditional {
            key_index,
            offset,
            expected_retry_time,
        } => {
            ctx.defer_additional(model, op, *key_index, *offset, *expected_retry_time)
                .await
        }
        DeferOperation::CompleteRetrySuccess { key_index, offset } => {
            apply_complete_retry(
                store,
                model,
                *key_index,
                *offset,
                op_idx,
                op,
                key_components,
            )
            .await
        }
        DeferOperation::IncrementRetryCount {
            key_index,
            current_retry_count,
        } => {
            apply_increment_retry(
                store,
                model,
                *key_index,
                *current_retry_count,
                op_idx,
                op,
                key_components,
            )
            .await
        }
        DeferOperation::Append {
            key_index,
            offset,
            expected_retry_time,
        } => {
            ctx.append(model, op, *key_index, *offset, *expected_retry_time)
                .await
        }
        DeferOperation::Remove { key_index, offset } => {
            ctx.remove(model, op, *key_index, *offset).await
        }
        DeferOperation::SetRetryCount {
            key_index,
            retry_count,
        } => {
            ctx.set_retry_count(model, op, *key_index, *retry_count)
                .await
        }
        DeferOperation::DeleteKey(key_index) => ctx.delete_key(model, op, *key_index).await,
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
    key_components: &[TestKeyComponents],
) -> color_eyre::Result<()>
where
    S: DeferStore,
    S::Error: Error + Send + Sync + 'static,
{
    match op {
        DeferOperation::GetNext(_) | DeferOperation::IsDeferred(_) => {
            apply_query_operation(store, model, op, op_idx, key_components).await
        }
        _ => apply_mutation_operation(store, model, op, op_idx, key_components).await,
    }
}

/// Verifies that defer store operations match the reference model behavior.
///
/// # Test Strategy
///
/// 1. Start with empty store and model
/// 2. Apply sequence of random operations to both
/// 3. Verify that for every key:
///    - `store.get_next(&key)` matches `model.get_next(&key)`
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
    for key_comp in &input.key_components {
        store
            .delete_key(&key_comp.key)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to clean up key: {e:?}"))?;
    }

    let mut model = DeferModel::new();

    // Apply all operations to both store and model, verifying queries inline
    for (op_idx, op) in input.operations.iter().enumerate() {
        apply_operation(store, &mut model, op, op_idx, &input.key_components).await?;
    }

    // Verify final state matches
    verify_final_state(store, &model, &input).await?;

    Ok(())
}
