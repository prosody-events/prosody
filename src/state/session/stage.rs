//! Staging, abort, and resolution of a session's collections.

use super::sealed::StagedCollection;
use crate::state::CommitDecision;
use crate::state::access::StateAccessError;
use crate::state::cell::{ProvisionalWrite, Values};
use crate::state::cell_key::{CellKey, Section};
use crate::state::dirty::{CellSnapshot, ClearedSections, DirtyVal, ResolvedCells};
use crate::state::identity::{CollectionId, CollectionRef};
use crate::state::marker::{EventEvidence, FrozenStage, SectionClear};
use crate::state::registry::CollectionDefRegistry;
use crate::state::resolve::resolve_event_marker;
use crate::state::retry::{StepOutcome, retry_step};
use crate::state::store::{
    CELL_BATCH, CellBuffer, CellRead, CellStore, CoordinateBatch, ensure_aligned,
};
use crate::state::{
    CommitMode, EventRef, SHARD_FANOUT_CONCURRENCY, STATE_FANOUT_CONCURRENCY, StateKey, StateName,
    StateType,
};
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::iter::from_fn;
use tokio::task::coop::cooperative;

/// One batch-read unit of a `ReadCommitted` stage: a section's contiguous
/// `≤CELL_BATCH` survivor run, its coordinate batch, and the dirty records —
/// in the same order — each batched committed base pairs with. `batch` aligns
/// 1:1 with `records` by construction (each is built from the same run).
struct StageChunk {
    section: Section,
    batch: CoordinateBatch,
    records: CellBuffer<(CellKey, Option<Bytes>)>,
}

/// Splits order-preserved survivors (sorted by `(section, coordinate)`) into
/// per-section `≤CELL_BATCH` [`StageChunk`]s. Concatenating the chunks' records
/// reproduces the input; each chunk's `batch` is built from its own records, so
/// it aligns 1:1 with them. Splitting per section (not purely by count) keeps
/// every `get_many` call single-section, as its `section` argument requires.
fn stage_chunks<I: Iterator<Item = (CellKey, Option<Bytes>)>>(
    survivors: I,
) -> impl Iterator<Item = StageChunk> + use<I> {
    let mut it = survivors.peekable();
    from_fn(move || {
        // `Section` is `Copy`, so the peek borrow ends here, before `next_if`.
        let section = it.peek()?.0.section;
        let mut records: CellBuffer<(CellKey, Option<Bytes>)> = CellBuffer::new();
        while let Some(record) =
            it.next_if(|(cell, _)| cell.section == section && records.len() < CELL_BATCH.get())
        {
            records.push(record);
        }
        // `records` is non-empty (peek showed a same-section head) and
        // `≤CELL_BATCH`, so `chunks` yields exactly one batch. Bind it in its
        // own statement so the borrow of `records` ends before it is moved.
        let batch =
            CoordinateBatch::chunks(records.iter().map(|(cell, _)| cell.coordinate.clone())).next();
        batch.map(|batch| StageChunk {
            section,
            batch,
            records,
        })
    })
}

/// Stages one collection's touched cells and returns the frozen
/// [`StagedCollection`] record that the receipt promotes or rolls back.
/// Returns `None` when there is nothing to stage, and for a `ReadUncommitted`
/// collection, which writes resolved values at stage time.
/// A `Cleared` cell in a cleared section is dropped on both arms. The clear's
/// gap erase covers it, so the write stays row-disjoint. This is a free
/// function so no `self` borrow crosses the concurrent fan-out.
pub(super) async fn stage_collection<S>(
    lower: &S,
    registry: &CollectionDefRegistry,
    event: EventRef,
    id: CollectionId,
    cleared: ClearedSections,
    cells: CellSnapshot,
    evidence: &EventEvidence,
) -> Result<Option<StagedCollection>, StateAccessError>
where
    S: CellStore,
{
    let collection_ref =
        CollectionRef::new(id.clone(), registry.ttl_for(id.state_type(), id.name()));
    let cleared = &cleared;
    let subsumed = |cell: &CellKey, value: &DirtyVal| {
        *value == DirtyVal::Cleared && cleared.contains(&cell.section)
    };
    match registry.commit_mode_for(id.state_type(), id.name()) {
        CommitMode::ReadCommitted => {
            let id = &id;
            // Read each surviving cell's committed base in per-section batches.
            // `lower` is the pre-overlay store.
            // The committed projection of a cell this event staged is its `prev`.
            // Thus, a retry re-stages over the same base (idempotent).
            // A `Set` cell in a cleared section keeps its committed pre-clear `prev` this
            // way.

            // `cooperative` adds a yield point per batch. `buffered` preserves order and
            // bounds concurrency; order is inert here because marker and clear
            // freezing sort internally and settle is row-disjoint. Drop cells that a
            // section clear subsumes first, so the batch stays row-disjoint.
            // Size the buffer once from the pre-filter snapshot; the filter can only
            // shrink it.
            let capacity = cells.len();
            let survivors = cells
                .into_iter()
                .filter(|(cell, value)| !subsumed(cell, value))
                .map(|(cell, value)| (cell, value.into_data()));
            let writes: Vec<(CellKey, ProvisionalWrite)> = stream::iter(stage_chunks(survivors))
                .map(|chunk| {
                    cooperative(async move {
                        let StageChunk {
                            section,
                            batch,
                            records,
                        } = chunk;
                        let bases =
                            CellRead::<Values>::read_many(lower, id, section, &batch.as_ref())
                                .await
                                .map_err(|e| StateAccessError::store(&e))?;
                        // Pair this chunk's bases with exactly its records
                        // before the fold flattens the chunks.
                        ensure_aligned(bases.len(), records.len())?;
                        let chunk_writes: CellBuffer<(CellKey, ProvisionalWrite)> = records
                            .into_iter()
                            .zip(bases.into_iter().map(|(committed, _)| committed))
                            .map(|((cell, data), prev)| {
                                (cell, ProvisionalWrite::new(data, prev, event))
                            })
                            .collect();
                        Ok::<_, StateAccessError>(chunk_writes)
                    })
                })
                .buffered(SHARD_FANOUT_CONCURRENCY)
                .try_fold(
                    Vec::with_capacity(capacity),
                    |mut acc, chunk_writes| async move {
                        acc.extend(chunk_writes);
                        Ok(acc)
                    },
                )
                .await?;
            if writes.is_empty() && cleared.is_empty() {
                return Ok(None);
            }
            // The marker lists exactly this stage's writes and frozen clears. A
            // clears-only collection stages no writes, but its Staged row still
            // lands so that promote applies the clear.
            let stage = FrozenStage::new(event, writes, cleared, evidence);
            lower
                .write_provisional(&collection_ref, stage.request())
                .await
                .map_err(|e| StateAccessError::store(&e))?;
            Ok(Some(StagedCollection {
                collection: collection_ref,
                stage,
            }))
        }
        CommitMode::ReadUncommitted => {
            let resolved: ResolvedCells = cells
                .into_iter()
                .map(|(cell, value)| (cell, value.into_data()))
                .collect();
            write_direct(lower, &collection_ref, cleared, resolved).await?;
            Ok(None)
        }
    }
}

/// Writes resolved cells and their frozen section clears in one direct write.
/// A `Cleared` cell in a cleared section is dropped, because the clear's gap
/// erase covers it and the write must stay row-disjoint. Returns `false` when
/// there is nothing to write.
pub(super) async fn write_direct<S: CellStore>(
    lower: &S,
    collection: &CollectionRef,
    cleared: &ClearedSections,
    mut cells: ResolvedCells,
) -> Result<bool, StateAccessError> {
    cells.retain(|(cell, data)| data.is_some() || !cleared.contains(&cell.section));
    if cells.is_empty() && cleared.is_empty() {
        return Ok(false);
    }
    let clears: Vec<SectionClear> = cleared
        .iter()
        .map(|&section| SectionClear::frozen_resolved(section, &cells))
        .collect();
    lower
        .write_resolved(collection, &cells, &clears)
        .await
        .map_err(|e| StateAccessError::store(&e))?;
    Ok(true)
}

/// Restores stage residue after every concurrent stage call has returned.
pub(super) async fn abort_stages<S: CellStore>(
    store: &S,
    registry: &CollectionDefRegistry,
    key: &StateKey,
    touched: &[(StateType, StateName)],
    shutdown: impl Fn() -> bool + Sync,
) {
    stream::iter(0..touched.len())
        .map(|index| {
            let (kind, name) = &touched[index];
            let shutdown = &shutdown;
            cooperative(async move {
                let collection = CollectionRef::new(
                    CollectionId::new(key.clone(), *kind, name.clone()),
                    registry.ttl_for(*kind, name),
                );
                let _ = retry_step(shutdown, "rejected stage rollback", || async {
                    if let Some(marker) = store.marker_state(collection.id()).await?.staged {
                        resolve_event_marker(
                            store,
                            &collection,
                            &marker,
                            CommitDecision::NotCommitted,
                        )
                        .await?;
                    }
                    Ok::<_, S::Error>(())
                })
                .await;
            })
        })
        .buffer_unordered(STATE_FANOUT_CONCURRENCY)
        .collect::<()>()
        .await;
}

/// Returns the collections whose promote did not complete.
pub(super) async fn resolve_collections<S: CellStore>(
    store: &S,
    collections: Vec<StagedCollection>,
    shutdown: &(impl Fn() -> bool + Sync),
) -> Vec<StagedCollection> {
    stream::iter(collections)
        .map(|staged| {
            cooperative(async move {
                let outcome = retry_step(shutdown, "keyed-state promote", || {
                    store.commit_provisional(
                        &staged.collection,
                        staged.stage.marker(),
                        staged.stage.writes(),
                    )
                })
                .await;
                match outcome {
                    StepOutcome::Done(()) => None,
                    StepOutcome::Skip | StepOutcome::Abandon => Some(staged),
                }
            })
        })
        .buffer_unordered(STATE_FANOUT_CONCURRENCY)
        .fold(Vec::new(), |mut rejected, staged| async move {
            rejected.extend(staged);
            rejected
        })
        .await
}

/// Rolls back every staged collection concurrently. Returns `false` when
/// shutdown abandoned a rollback.
pub(super) async fn abort_collections<S: CellStore>(
    store: &S,
    collections: &[StagedCollection],
    shutdown: &(impl Fn() -> bool + Sync),
) -> bool {
    // Indices keep the closure free of a higher-ranked borrow.
    stream::iter(0..collections.len())
        .map(|index| {
            let staged = &collections[index];
            cooperative(async move {
                !matches!(
                    retry_step(shutdown, "keyed-state rollback", || {
                        store.abort_provisional(&staged.collection, staged.stage.writes())
                    })
                    .await,
                    StepOutcome::Abandon
                )
            })
        })
        .buffer_unordered(STATE_FANOUT_CONCURRENCY)
        .fold(true, |all, done| async move { all && done })
        .await
}
