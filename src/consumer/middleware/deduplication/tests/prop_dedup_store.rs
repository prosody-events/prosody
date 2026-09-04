//! Store parity holds for marker presence. The source can differ between
//! stores.

use crate::consumer::middleware::deduplication::memory::MemoryDeduplicationStore;
use crate::consumer::middleware::deduplication::store::{DeduplicationStore, Presence};
use color_eyre::eyre::{ensure, eyre};
use quickcheck::{Arbitrary, Gen};
use std::error::Error;
use uuid::Uuid;

/// Operations that can be performed on the deduplication store.
#[derive(Clone, Debug)]
pub enum DeduplicationOperation {
    /// Check if an ID lookup.
    Lookup(usize),
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
                DeduplicationOperation::Lookup(id_index)
            };
            operations.push(op);
        }

        Self { ids, operations }
    }
}

/// Compare marker presence after each read and at the end of the trace.
///
/// # Errors
///
/// Return an error if a store operation fails or the results differ.
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
            DeduplicationOperation::Lookup(id_index) => {
                let id = input.ids[*id_index];
                let expected = reference
                    .lookup(id)
                    .await
                    .map_err(|e| eyre!("Op #{op_idx} reference Lookup failed: {e:?}"))?;
                let actual = store
                    .lookup(id)
                    .await
                    .map_err(|e| eyre!("Op #{op_idx} Lookup failed: {e:?}"))?;

                ensure!(expected != Presence::Durable, "memory has no durable store");
                if expected.is_present() != actual.is_present() {
                    return Err(eyre!(
                        "Op #{op_idx} Lookup mismatch for id={id}: reference={expected:?}, \
                         store={actual:?}"
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
            .lookup(id)
            .await
            .map_err(|e| eyre!("Final reference Lookup for id[{i}]={id}: {e:?}"))?;
        let actual = store
            .lookup(id)
            .await
            .map_err(|e| eyre!("Final Lookup check failed for id[{i}]={id}: {e:?}"))?;

        ensure!(expected != Presence::Durable, "memory has no durable store");
        if expected.is_present() != actual.is_present() {
            return Err(eyre!(
                "Final state mismatch for id[{i}]={id}: reference={expected:?}, store={actual:?}"
            ));
        }
    }

    Ok(())
}
