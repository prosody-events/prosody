use super::{
    BatchUnit, CassandraStore, CellAddr, CellBatchRow, CellKey, CellStoreError, CollectionRef,
    CommitOracle, EventMarker, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS, MarkerWriteRow,
    PER_STATEMENT_OVERHEAD, Pk, ProvisionalWrite, ResolveCellError, RowShape, StageRow, bind_ttl,
    blob_weight, encode_cell_blobs, smallvec,
};
use crate::state::marker::MarkerRow;

pub(super) async fn write_provisional<O>(
    store: &CassandraStore<O>,
    collection: &CollectionRef,
    writes: &[(CellKey, ProvisionalWrite)],
    marker: Option<&EventMarker>,
) -> Result<(), CellStoreError<O::Error>>
where
    O: CommitOracle,
{
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
    let marker_blob = store.stage_marker(collection, marker).await?;

    // Encode every cell's blobs up front (this Vec owns the `Bytes`); the
    // bound rows borrow into it and into each input cell's coordinate slice,
    // so the whole batch is one `blobs` allocation with no per-cell copy.
    // Stays a `Vec` (not a `CellBuffer`) — see the `run_batches` ruling.
    let mut blobs = Vec::with_capacity(writes.len());
    for (_, write) in writes {
        blobs.push(encode_cell_blobs(write.data(), write.prev()).map_err(ResolveCellError::Store)?);
    }

    // Cells bind the collection TTL; the Staged row binds the evidence TTL.
    let ttl = bind_ttl(collection.ttl());
    // The marker unit leads; each cell unit is one row. `units` stays a
    // `Vec` (not a `CellBuffer`) — see the `run_batches` ruling.
    let mut units: Vec<BatchUnit<CellBatchRow>> = Vec::with_capacity(writes.len() + 1);
    units.push(BatchUnit::new(
        marker_blob.payload.as_ref().len() as u64 + PER_STATEMENT_OVERHEAD,
        smallvec![CellBatchRow {
            statement: &store.queries.marker_write,
            row: RowShape::MarkerWrite(MarkerWriteRow {
                ttl: bind_ttl(marker.evidence_ttl()),
                payload: marker_blob.payload.as_ref(),
                encoding: marker_blob.payload.encoding(),
                event: marker_blob.event,
                addr: CellAddr::marker(pk, MarkerRow::Staged),
            }),
        }],
    ));
    units.extend(blobs.iter().zip(writes).map(|(blob, (cell, write))| {
        let addr = CellAddr::new(pk, cell);
        BatchUnit::new(
            blob_weight(blob),
            smallvec![CellBatchRow {
                statement: &store.queries.write_provisional,
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

    // The first staged row makes a partial stage recoverable. Re-stamp it
    // after every cell batch completes, so evidence outlives all listed cells.
    // Await the phases in order because run_batches executes its chunks unordered.
    for phase in super::batch::stage_batches(&units, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS) {
        store
            .run_batches(&units[phase])
            .await
            .map_err(ResolveCellError::Store)?;
    }
    Ok(())
}
