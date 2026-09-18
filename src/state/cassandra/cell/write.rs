use super::{
    BatchUnit, CassandraCellStoreError, CassandraStore, CellAddr, CellBatchRow, CellKey,
    CellStoreError, CollectionRef, EventMarker, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS,
    MarkerWriteRow, PER_STATEMENT_OVERHEAD, Pk, ProvisionalWrite, ResolveCellError, RowShape,
    StageRow, bind_ttl, blob_weight, encode_cell_blobs, smallvec,
};
use crate::state::SHARD_FANOUT_CONCURRENCY;
use crate::state::marker::MarkerRow;
use futures::{StreamExt, TryStreamExt, stream};
use smallvec::SmallVec;
use std::ops::Range;

pub(super) async fn write_provisional(
    store: &CassandraStore,
    collection: &CollectionRef,
    writes: &[(CellKey, ProvisionalWrite)],
    marker: Option<&EventMarker>,
) -> Result<(), CellStoreError> {
    // `None` ⇒ the explicit empty-stage no-op: no marker, no boundary
    // check (nothing to strand). A clears-only stage passes a marker with
    // empty `staged()` and runs the boundary like any stage.
    debug_assert!(
        marker.is_some() || writes.is_empty(),
        "a markerless stage must write nothing"
    );
    let Some(marker) = marker else {
        return Ok(());
    };
    debug_assert!(
        writes
            .iter()
            .all(|(cell, _)| marker.staged().binary_search(cell).is_ok()),
        "every staged write must be listed by the event marker"
    );
    let pk = Pk::of(collection.id());
    let marker_blob = super::store::stage_marker(marker)?;

    // Encode every cell's blobs up front (this Vec owns the `Bytes`); the
    // bound rows borrow into it and into each input cell's coordinate slice,
    // so the whole batch is one `blobs` allocation with no per-cell copy.
    // Stays a `Vec` (not a `CellBuffer`) — see the `run_batches` ruling.
    let mut blobs = Vec::with_capacity(writes.len());
    for (_, write) in writes {
        blobs.push(encode_cell_blobs(write.data(), write.prev()).map_err(ResolveCellError::Store)?);
    }

    // Cells and the Staged row bind the collection TTL.
    let ttl = bind_ttl(collection.ttl());
    // The marker unit joins every chunk; each cell unit is one row. `units`
    // stays a `Vec` (not a `CellBuffer`) — see the `run_batches` ruling.
    let marker = BatchUnit::new(
        marker_blob.payload.as_ref().len() as u64 + PER_STATEMENT_OVERHEAD,
        smallvec![CellBatchRow {
            statement: &store.queries.cells.marker_write,
            row: RowShape::MarkerWrite(MarkerWriteRow {
                ttl,
                payload: marker_blob.payload.as_ref(),
                encoding: marker_blob.payload.encoding(),
                event: marker_blob.event,
                addr: CellAddr::marker(pk, MarkerRow::Staged),
            }),
        }],
    );
    let mut units: Vec<BatchUnit<CellBatchRow>> = Vec::with_capacity(writes.len());
    units.extend(blobs.iter().zip(writes).map(|(blob, (cell, write))| {
        let addr = CellAddr::new(pk, cell);
        BatchUnit::new(
            blob_weight(blob),
            smallvec![CellBatchRow {
                statement: &store.queries.cells.write_provisional,
                row: RowShape::Stage(StageRow {
                    ttl,
                    data: blob.data(),
                    prev_data: blob.prev_data(),
                    encoding: blob.encoding(),
                    version: blob.version(),
                    event: write.event(),
                    addr,
                }),
            }],
        )
    }));

    let chunks: SmallVec<[Range<usize>; 1]> =
        super::batch::stage_batches(&marker, &units, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS)
            .collect();
    stream::iter(chunks)
        .map(|range| {
            let rows = super::batch::stage_chunk(&marker, &units, range);
            store.session.execute_unlogged_batch(rows)
        })
        .buffer_unordered(SHARD_FANOUT_CONCURRENCY)
        .try_collect::<()>()
        .await
        .map_err(CassandraCellStoreError::from)
        .map_err(ResolveCellError::Store)?;
    Ok(())
}
