//! Each store reports the same marker presence as the assignment model.

use crate::Topic;
use crate::consumer::middleware::deduplication::store::{
    DeduplicationStore, DeduplicationStoreProvider, Presence,
};
use color_eyre::eyre::{ensure, eyre};
use quickcheck::{Arbitrary, Gen};
use std::error::Error;
use uuid::Uuid;

/// Operations that can be performed on the deduplication store.
#[derive(Clone, Debug)]
pub enum DeduplicationOperation {
    /// Read a marker.
    Lookup(usize),
    /// Read a marker through the commit oracle.
    OracleRead(usize),
    /// Insert an ID.
    Insert(usize),
    /// Start another assignment over the same records.
    Reacquire,
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
            let op = match u8::arbitrary(g) % 4 {
                0 => DeduplicationOperation::Insert(id_index),
                1 => DeduplicationOperation::Lookup(id_index),
                2 => DeduplicationOperation::OracleRead(id_index),
                _ => DeduplicationOperation::Reacquire,
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
pub async fn prop_dedup_store_model_equivalence<P>(
    provider: &P,
    input: DeduplicationTestInput,
) -> color_eyre::Result<()>
where
    P: DeduplicationStoreProvider,
    <P::Store as DeduplicationStore>::Error: Error + Send + Sync + 'static,
{
    let mut store = provider.create_store(Topic::from("test"), 0, "test");
    let mut generation = 0;
    let mut stamps = vec![None; input.ids.len()];
    for (index, op) in input.operations.iter().enumerate() {
        match *op {
            DeduplicationOperation::Lookup(id_index) => {
                check_presence(
                    &store,
                    input.ids[id_index],
                    &mut stamps[id_index],
                    generation,
                )
                .await
                .map_err(|error| eyre!("Op #{index}: {error}"))?;
            }
            DeduplicationOperation::OracleRead(id_index) => {
                ensure!(
                    store.recorded(input.ids[id_index]).await? == stamps[id_index].is_some(),
                    "oracle presence differs at operation {index}"
                );
            }
            DeduplicationOperation::Insert(id_index) => {
                store.insert(input.ids[id_index]).await?;
                stamps[id_index] = Some(generation);
            }
            DeduplicationOperation::Reacquire => {
                generation += 1;
                store = provider.create_store(Topic::from("test"), 0, "test");
            }
        }
    }
    for (&id, stamp) in input.ids.iter().zip(&mut stamps) {
        check_presence(&store, id, stamp, generation).await?;
    }
    Ok(())
}

async fn check_presence<S>(
    store: &S,
    id: Uuid,
    stamp: &mut Option<usize>,
    generation: usize,
) -> color_eyre::Result<()>
where
    S: DeduplicationStore,
    S::Error: Error + Send + Sync + 'static,
{
    let expected = match *stamp {
        None => Presence::Absent,
        Some(previous) if previous == generation => Presence::Settled,
        Some(_) => Presence::Inherited,
    };
    let actual = store.lookup(id).await?;
    ensure!(
        actual == expected,
        "presence differs: expected={expected:?}, actual={actual:?}"
    );
    if stamp.is_some() {
        *stamp = Some(generation);
    }
    Ok(())
}
