//! Property-based tests for deduplication store operations.
//!
//! Tests the [`DeduplicationStore`] trait against
//! [`MemoryDeduplicationStore`] as the reference implementation. Any store
//! under test (notably [`CassandraDeduplicationStore`] with its in-process
//! write-through cache) must produce identical `exists` answers to a fresh
//! `MemoryDeduplicationStore` driven by the same operation sequence.
//!
//! [`CassandraDeduplicationStore`]: super::super::cassandra::CassandraDeduplicationStore

use crate::consumer::middleware::deduplication::memory::MemoryDeduplicationStore;
use crate::consumer::middleware::deduplication::store::DeduplicationStore;
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

/// Verifies that the store under test matches a fresh
/// [`MemoryDeduplicationStore`] reference driven by the same operations.
///
/// # Test Strategy
///
/// 1. Start with an empty subject store and a fresh `MemoryDeduplicationStore`
///    reference.
/// 2. Apply the operation sequence to both.
/// 3. After every `Exists` query, assert the subject's answer equals the
///    reference's answer.
/// 4. After all operations, re-check every ID in the pool against both stores.
///
/// # Errors
///
/// Returns an error if store operations fail or any answer diverges from the
/// reference.
pub async fn prop_dedup_store_model_equivalence<S>(
    store: &S,
    input: DeduplicationTestInput,
) -> color_eyre::Result<()>
where
    S: DeduplicationStore,
    S::Error: Error + Send + Sync + 'static,
{
    let reference = MemoryDeduplicationStore::new();

    for (op_idx, op) in input.operations.iter().enumerate() {
        match op {
            DeduplicationOperation::Exists(id_index) => {
                let id = input.ids[*id_index];
                let expected = reference
                    .exists(id)
                    .await
                    .map_err(|e| eyre!("Op #{op_idx} reference Exists failed: {e:?}"))?;
                let actual = store
                    .exists(id)
                    .await
                    .map_err(|e| eyre!("Op #{op_idx} Exists failed: {e:?}"))?;

                if expected != actual {
                    return Err(eyre!(
                        "Op #{op_idx} Exists mismatch for id={id}: reference={expected}, \
                         store={actual}"
                    ));
                }
            }
            DeduplicationOperation::Insert(id_index) => {
                let id = input.ids[*id_index];
                reference
                    .insert(id)
                    .await
                    .map_err(|e| eyre!("Op #{op_idx} reference Insert failed: {e:?}"))?;
                store
                    .insert(id)
                    .await
                    .map_err(|e| eyre!("Op #{op_idx} Insert failed: {e:?}"))?;
            }
        }
    }

    for (i, &id) in input.ids.iter().enumerate() {
        let expected = reference
            .exists(id)
            .await
            .map_err(|e| eyre!("Final reference Exists for id[{i}]={id}: {e:?}"))?;
        let actual = store
            .exists(id)
            .await
            .map_err(|e| eyre!("Final Exists check failed for id[{i}]={id}: {e:?}"))?;

        if expected != actual {
            return Err(eyre!(
                "Final state mismatch for id[{i}]={id}: reference={expected}, store={actual}"
            ));
        }
    }

    Ok(())
}
