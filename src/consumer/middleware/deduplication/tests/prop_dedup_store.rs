//! Property-based tests for deduplication store operations.
//!
//! Tests the [`DeduplicationStore`] trait using a simple reference model to
//! verify correctness across both memory and Cassandra implementations.

use crate::consumer::middleware::deduplication::store::DeduplicationStore;
use ahash::HashSet;
use color_eyre::eyre::eyre;
use quickcheck::{Arbitrary, Gen};
use std::error::Error;
use uuid::Uuid;

/// Operations that can be performed on the deduplication store.
#[derive(Clone, Debug)]
pub enum DeduplicationOperation {
    /// Check if an ID exists.
    Exists(usize),
    /// Insert an ID.
    Insert(usize),
}

/// Test input containing a pool of UUIDs and a sequence of operations.
#[derive(Clone, Debug)]
pub struct DeduplicationTestInput {
    /// Pool of UUIDs used by operations (referenced by index).
    pub ids: Vec<Uuid>,
    /// Sequence of operations to apply.
    pub operations: Vec<DeduplicationOperation>,
}

impl Arbitrary for DeduplicationTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 3-8 unique UUIDs for this trial
        let id_count = (usize::arbitrary(g) % 6_usize) + 3_usize;
        let ids: Vec<Uuid> = (0..id_count).map(|_| Uuid::new_v4()).collect();

        // Generate 20-50 operations
        let op_count = (usize::arbitrary(g) % 30_usize) + 20_usize;
        let mut operations = Vec::with_capacity(op_count);

        for _ in 0..op_count {
            let id_index = usize::arbitrary(g) % ids.len();
            let op = if bool::arbitrary(g) {
                DeduplicationOperation::Insert(id_index)
            } else {
                DeduplicationOperation::Exists(id_index)
            };
            operations.push(op);
        }

        Self { ids, operations }
    }
}

/// Reference model for deduplication store behavior.
///
/// Tracks which UUIDs have been inserted using a simple hash set.
#[derive(Clone, Debug, Default)]
pub struct DeduplicationModel {
    inserted: HashSet<Uuid>,
}

impl DeduplicationModel {
    fn exists(&self, id: Uuid) -> bool {
        self.inserted.contains(&id)
    }

    fn insert(&mut self, id: Uuid) {
        self.inserted.insert(id);
    }
}

/// Verifies that deduplication store operations match the reference model.
///
/// # Test Strategy
///
/// 1. Start with empty store and model
/// 2. Apply sequence of random operations to both
/// 3. After each `Exists` query, verify the store result matches the model
/// 4. After all operations, verify final state for all IDs
///
/// # Errors
///
/// Returns an error if store operations fail or state diverges from the model.
pub async fn prop_dedup_store_model_equivalence<S>(
    store: &S,
    input: DeduplicationTestInput,
) -> color_eyre::Result<()>
where
    S: DeduplicationStore,
    S::Error: Error + Send + Sync + 'static,
{
    let mut model = DeduplicationModel::default();

    for (op_idx, op) in input.operations.iter().enumerate() {
        match op {
            DeduplicationOperation::Exists(id_index) => {
                let id = input.ids[*id_index];
                let expected = model.exists(id);
                let actual = store
                    .exists(id)
                    .await
                    .map_err(|e| eyre!("Op #{op_idx} Exists failed: {e:?}"))?;

                if expected != actual {
                    return Err(eyre!(
                        "Op #{op_idx} Exists mismatch for id={id}: expected {expected}, got \
                         {actual}"
                    ));
                }
            }
            DeduplicationOperation::Insert(id_index) => {
                let id = input.ids[*id_index];
                model.insert(id);
                store
                    .insert(id)
                    .await
                    .map_err(|e| eyre!("Op #{op_idx} Insert failed: {e:?}"))?;
            }
        }
    }

    // Verify final state for all IDs
    for (i, &id) in input.ids.iter().enumerate() {
        let expected = model.exists(id);
        let actual = store
            .exists(id)
            .await
            .map_err(|e| eyre!("Final Exists check failed for id[{i}]={id}: {e:?}"))?;

        if expected != actual {
            return Err(eyre!(
                "Final state mismatch for id[{i}]={id}: expected {expected}, got {actual}"
            ));
        }
    }

    Ok(())
}
