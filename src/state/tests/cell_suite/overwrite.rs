//! Implicit overwrite: a new stage resolves its predecessor's provisional
//! cells.

use super::*;

/// One overwrite-trace step: a multi-cell write to a pooled collection (all
/// cells staged in one `write_provisional` call), and whether the event
/// commits. An empty cell set exercises the empty-batch no-op boundary.
#[derive(Clone, Debug)]
struct OverwriteOp {
    coll: u8,
    cells: Vec<(u8, Mutation)>,
    commit: bool,
}

impl Arbitrary for OverwriteOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let cells = capped_vec::<(u8, Mutation)>(g, CRASH_CELLS as usize)
            .into_iter()
            .map(|(cell, m)| (cell % CRASH_CELLS, m))
            .collect();
        Self {
            coll: u8::arbitrary(g) % POOL,
            cells,
            commit: bool::arbitrary(g),
        }
    }
}

/// A shrinkable overwrite trace.
#[derive(Clone, Debug)]
pub(crate) struct OverwriteTrace {
    ops: Vec<OverwriteOp>,
}

impl Arbitrary for OverwriteTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.ops.shrink().map(|ops| Self { ops }))
    }
}

/// Admission resolves the previous stage before each overwrite.
/// A cold store reads the committed base for every new stage.
/// The final admission must produce the model's values in every collection.
pub(crate) async fn run_overwrite_trace<S, F>(
    make_store: F,
    dedup: MemoryDeduplicationStore,
    trace: OverwriteTrace,
) -> Result<bool>
where
    S: CellStore,
    F: Fn() -> Result<S>,
{
    let (ids, refs) = pooled_collections()?;
    let mut model: Vec<BTreeMap<u8, Option<Bytes>>> = vec![BTreeMap::new(); POOL as usize];

    for (index, op) in trace.ops.into_iter().enumerate() {
        let slot = op.coll as usize;
        let dedup_id = Uuid::from_u128(index as u128);
        let event = EventRef::Message { dedup_id };
        // A fresh cold store: reads never hit a warm in-process cache, so every
        // overwrite resolves its predecessor's provisional cell durably.
        let store = make_store()?;
        admit_collection(&store, &dedup, &refs[slot]).await?;

        // The whole cell set stages in one `write_provisional`; each cell's
        // staged `prev` must equal its committed base.
        let cells = collapse_cells(op.cells);
        let mut cell_writes: Vec<(CellKey, ProvisionalWrite)> = Vec::with_capacity(cells.len());
        for &(coord, mutation) in &cells {
            let key = cell_at(coord);
            let prev = CellRead::<Values>::read(&store, &ids[slot], key.as_ref())
                .await?
                .0;
            if prev.get().cloned() != model[slot].get(&coord).cloned().flatten() {
                return Ok(false);
            }
            cell_writes.push((key, ProvisionalWrite::new(mutation.value(), prev, event)));
        }
        let marker = EventMarker::frozen(event, &cell_writes, &[], &evidence([].into(), None));
        if !cell_writes.is_empty() {
            store
                .write_provisional(&refs[slot], listed(&marker, &cell_writes)?)
                .await?;
        }
        if op.commit {
            if !cell_writes.is_empty() {
                seed_commit_evidence(&store, &refs[slot]).await?;
            }
            for &(coord, mutation) in &cells {
                model[slot].insert(coord, mutation.value());
            }
        }
    }

    // Every cell still provisional (a collection never re-staged, so no stage
    // boundary resolved it), resolved by this final read, converges.
    let store = make_store()?;
    for (i, id) in ids.iter().enumerate() {
        admit_collection(&store, &dedup, &refs[i]).await?;
        for (&coord, value) in &model[i] {
            if CellRead::<Values>::read(&store, id, cell_at(coord).as_ref())
                .await?
                .0
                .into_inner()
                != *value
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}
