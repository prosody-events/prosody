use super::{
    BatchUnit, CassandraStore, CellAddr, CellBatchRow, CellKey, CellStoreError, CollectionRef,
    CommitOracle, EventMarker, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS, MarkerBlob, MarkerWriteRow,
    PER_STATEMENT_OVERHEAD, Pk, ProvisionalWrite, ResolveCellError, RowShape, StageRow,
    blob_weight, encode_cell_blobs, fits_one_batch, smallvec, ttl_bind,
};

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
    debug_assert!(
        marker.is_none_or(|marker| writes
            .iter()
            .all(|(cell, _)| marker.staged().binary_search(cell).is_ok())),
        "every staged write must be listed by the event marker"
    );
    let pk = Pk::of(collection.id());
    let marker_blob: Option<MarkerBlob> = match marker {
        None => None,
        Some(marker) => Some(store.stage_marker(collection, marker).await?),
    };

    // Encode every cell's blobs up front (this Vec owns the `Bytes`); the
    // bound rows borrow into it and into each input cell's coordinate slice,
    // so the whole batch is one `blobs` allocation with no per-cell copy.
    // Stays a `Vec` (not a `CellBuffer`) — see the `run_batches` ruling.
    let mut blobs = Vec::with_capacity(writes.len());
    for (_, write) in writes {
        blobs.push(encode_cell_blobs(write.data(), write.prev()).map_err(ResolveCellError::Store)?);
    }

    // The marker and cells share the collection TTL.
    let ttl = ttl_bind(collection.ttl());
    // The marker unit leads; each cell unit is one row. `units` stays a
    // `Vec` (not a `CellBuffer`) — see the `run_batches` ruling.
    let mut units: Vec<BatchUnit<CellBatchRow>> = Vec::with_capacity(writes.len() + 1);
    if let Some(blob) = &marker_blob {
        units.push(BatchUnit::new(
            blob.payload.as_ref().len() as u64 + PER_STATEMENT_OVERHEAD,
            smallvec![CellBatchRow {
                statement: &store.queries.marker_write,
                row: RowShape::MarkerWrite(MarkerWriteRow {
                    ttl,
                    payload: blob.payload.as_ref(),
                    encoding: blob.payload.encoding(),
                    event: blob.event,
                    addr: CellAddr::marker(pk),
                }),
            }],
        ));
    }
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

    // Marker-first ordering. Within one batch the marker rides atomically;
    // an over-budget stage MUST await the marker batch to completion
    // before issuing the cell batches, because `execute_unlogged_batches`
    // runs its chunks `buffer_unordered` (chunk order is NOT guaranteed).
    // Marker-without-cells is the over-report-safe crash shape;
    // cells-without-marker would strand them from recovery.
    if marker_blob.is_none()
        || fits_one_batch(
            units.iter().map(BatchUnit::weight),
            MAX_BATCH_BYTES,
            MAX_BATCH_STATEMENTS,
        )
    {
        store
            .run_batches(&units)
            .await
            .map_err(ResolveCellError::Store)
    } else {
        store
            .run_batches(&units[..1])
            .await
            .map_err(ResolveCellError::Store)?;
        store
            .run_batches(&units[1..])
            .await
            .map_err(ResolveCellError::Store)
    }
}
