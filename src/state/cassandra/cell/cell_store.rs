#[cfg(test)]
use super::Ordering;
use super::batch::marker_delete_unit;
use super::decode::decode_body;
use super::projection::CassandraProjection;
use super::read::{fetch_batch, fetch_marker_state, fetch_point, split_point};
use super::{
    BatchUnit, Bytes, CacheBatch, CassandraStore, Cell, CellAddr, CellBatchRow, CellBuffer,
    CellKey, CellKind, CellStore, CellStoreError, CollectionId, CollectionRef, Committed,
    CommittedBatch, CompactDuration, Coordinate, CoordinateBatch, EventMarker, KeyRow,
    PER_STATEMENT_OVERHEAD, Pk, PresenceBatch, ProvisionalCell, ProvisionalWrite, ResolveCellError,
    RowShape, Scan, Section, SectionClear, Stream, StreamExt, bind_ttl, decode_provisional_batch,
    encode_cell_blobs, extend_gap_units, gap_count, smallvec, sorted_unique_coordinates,
    write_provisional,
};
use super::{CassandraCellStoreError, MarkerWriteRow, encode};
use crate::state::cell::{Presence, Values};
use crate::state::marker::{MarkerRow, MarkerState, encode_committed_payload};

impl CellStore for CassandraStore {
    type Error = CellStoreError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Committed, Self::Error> {
        // The committed value is exactly the cache-fill read minus its co-expiry
        // TTL; production only ever calls this via `Cached` (which uses
        // `get_for_cache`), so `get` is a thin convenience for direct callers.
        Ok(self.get_for_cache(collection, cell).await?.0)
    }

    async fn get_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<(Committed, Option<CompactDuration>), Self::Error> {
        self.read::<Values>(collection, cell).await
    }

    async fn get_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> Result<CommittedBatch, Self::Error> {
        // Mirrors `get` → `get_for_cache`: the committed value is the batch
        // cache-fill read minus its co-expiry TTLs.
        Ok(self
            .get_many_for_cache(collection, section, batch)
            .await?
            .into_iter()
            .map(|(committed, _)| committed)
            .collect())
    }

    async fn get_many_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> Result<CacheBatch, Self::Error> {
        self.read_many::<Values>(collection, section, batch).await
    }

    async fn contains_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> Result<PresenceBatch, Self::Error> {
        Ok(self
            .read_many::<Presence>(collection, section, batch)
            .await?
            .into_iter()
            .map(|(cell, _)| cell.get().is_some())
            .collect())
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        self.scan_inner::<Values>(collection, scan)
    }

    fn scan_keys<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<CellKey, Self::Error>> + Send + 'a {
        self.scan_inner::<Presence>(collection, scan)
            .map(|item| item.map(|(key, ())| key))
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        // Point-read the `kind=Cell` row and keep only a genuinely provisional
        // shape. An absent or resolved coordinate returns `None`.
        #[cfg(test)]
        self.counters
            .cell_point_reads
            .fetch_add(1, Ordering::Relaxed);
        let Some(row) = fetch_point::<Values>(
            &self.session,
            Values::statements(&self.queries),
            collection,
            cell,
        )
        .await
        .map_err(ResolveCellError::Store)?
        else {
            return Ok(None);
        };
        let raw =
            decode_body::<Values>(split_point::<Values>(row).0).map_err(ResolveCellError::Store)?;
        match raw {
            Cell::Provisional(provisional) => Ok(Some(provisional)),
            Cell::Resolved(_) => Ok(None),
        }
    }

    async fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error> {
        #[cfg(test)]
        self.counters
            .provisional_in_queries
            .fetch_add(1, Ordering::Relaxed);
        // This survivor-only output needs no expansion to the input order.
        let unique_coordinates = sorted_unique_coordinates(batch);
        // One IN query, reusing the TTL-bearing batch read; TTL is discarded in
        // the decoder. This read neither resolves cells nor writes state.
        // It leaves marker state unchanged, as `provisional_cell_at` does.
        let rows = fetch_batch::<Values>(
            &self.session,
            Values::statements(&self.queries),
            collection,
            section,
            &unique_coordinates,
        )
        .await
        .map_err(ResolveCellError::Store)?;
        decode_provisional_batch(rows, &unique_coordinates).map_err(ResolveCellError::Store)
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        write_provisional(self, collection, writes, marker).await
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // Survivors are the present-data `cells`, excluded from the gaps
        // positionally, so every batch row stays disjoint and may be packed
        // independently.
        let pk = Pk::of(collection.id());
        // Encode each cell's committed `data` up front (owns the `Bytes`); no
        // `prev`, so the blobs carry only `data` + its encoding/version. Both
        // `blobs` and `units` stay a `Vec` — see the `run_batches` ruling.
        let mut blobs = Vec::with_capacity(cells.len());
        for (_, data) in cells {
            blobs.push(encode_cell_blobs(data.as_ref(), None).map_err(ResolveCellError::Store)?);
        }
        let ttl = bind_ttl(collection.ttl());
        let mut units = Vec::with_capacity(cells.len() + gap_count(clears));
        extend_gap_units(&mut units, &self.queries, pk, clears);
        units.extend(self.resolved_units(pk, ttl, &blobs, cells));
        self.run_batches(&units)
            .await
            .map_err(ResolveCellError::Store)
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        let pk = Pk::of(collection.id());
        // A marker-free single-row primitive. Promotes carry no blob — only
        // key columns — so every unit weighs the fixed overhead and the count
        // budget alone splits an enormous promote set. `units` stays a `Vec`
        // (not a `CellBuffer`) — see the `run_batches` ruling.
        let units: Vec<BatchUnit<CellBatchRow>> = cells
            .iter()
            .map(|cell| {
                let addr = CellAddr::new(pk, cell);
                BatchUnit::new(
                    PER_STATEMENT_OVERHEAD,
                    smallvec![CellBatchRow {
                        statement: &self.queries.cells.mark_resolved,
                        row: RowShape::Key(KeyRow {
                            kind: CellKind::Cell,
                            addr,
                        }),
                    }],
                )
            })
            .collect();
        self.run_batches(&units)
            .await
            .map_err(ResolveCellError::Store)
    }

    async fn marker_state<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<MarkerState, Self::Error> {
        #[cfg(test)]
        self.counters
            .marker_point_reads
            .fetch_add(1, Ordering::Relaxed);
        fetch_marker_state(
            &self.session,
            &self.queries,
            collection,
            self.registry
                .ttl_for(collection.state_type(), collection.name()),
        )
        .await
        .map_err(ResolveCellError::Store)
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        marker: &'a EventMarker,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        let payload = encode_committed_payload(marker)
            .map_err(CassandraCellStoreError::from)
            .map_err(ResolveCellError::Store)?;
        let payload = encode(&payload)
            .map_err(CassandraCellStoreError::from)
            .map_err(ResolveCellError::Store)?;
        let clears = marker.clears();
        // Commit applies natively — present data promotes in place, a staged
        // clear deletes its row (the row-absence invariant).
        // Cell and gap rows are disjoint and idempotent: gaps exclude survivors
        // and one another positionally, while a cell delete inside a gap is a
        // harmless delete/delete tie.
        let pk = Pk::of(collection.id());
        // `units` stays a `Vec` (not a `CellBuffer`) — see the `run_batches` ruling.
        let mut units: Vec<BatchUnit<CellBatchRow>> =
            Vec::with_capacity(writes.len() + gap_count(clears) + 2);
        units.extend(writes.iter().map(|(cell, write)| {
            let addr = CellAddr::new(pk, cell);
            let statement = if write.data().is_some() {
                &self.queries.cells.mark_resolved
            } else {
                &self.queries.cells.cell_delete
            };
            BatchUnit::new(
                PER_STATEMENT_OVERHEAD,
                smallvec![CellBatchRow {
                    statement,
                    row: RowShape::Key(KeyRow {
                        kind: CellKind::Cell,
                        addr,
                    }),
                }],
            )
        }));
        extend_gap_units(&mut units, &self.queries, pk, clears);
        let evidence = BatchUnit::new(
            payload.as_ref().len() as u64 + PER_STATEMENT_OVERHEAD,
            smallvec![CellBatchRow {
                statement: &self.queries.cells.committed_write,
                row: RowShape::MarkerWrite(MarkerWriteRow {
                    ttl: bind_ttl(Some(marker.evidence_ttl())),
                    payload: payload.as_ref(),
                    encoding: payload.encoding(),
                    event: marker.event(),
                    addr: CellAddr::marker(pk, MarkerRow::Committed),
                }),
            }],
        );
        self.issue_markers(Some(evidence), units, marker_delete_unit(pk, &self.queries))
            .await?;
        Ok(())
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        // Rollback: write each staged cell's committed base `prev` back as
        // resolved natively (`prev = None` restores exact row absence), then
        // delete the marker.
        let pk = Pk::of(collection.id());
        // Synthesized (cell, prev) pairs for the shared `resolved_units` helper;
        // kept a `Vec` (the clones are O(1) `Arc`/`Bytes` refcount bumps) —
        // deleting it would force `resolved_units` onto a less-clear iterator
        // signature on the common `write_resolved` path, for the rare rollback.
        let cells: Vec<(CellKey, Option<Bytes>)> = writes
            .iter()
            .map(|(cell, write)| (cell.clone(), write.prev().cloned()))
            .collect();
        // `blobs` and `units` stay a `Vec` — see the `run_batches` ruling.
        let mut blobs = Vec::with_capacity(cells.len());
        for (_, data) in &cells {
            blobs.push(encode_cell_blobs(data.as_ref(), None).map_err(ResolveCellError::Store)?);
        }
        let ttl = bind_ttl(collection.ttl());
        let mut units = Vec::with_capacity(cells.len() + 1);
        units.extend(self.resolved_units(pk, ttl, &blobs, &cells));
        self.issue_markers(None, units, marker_delete_unit(pk, &self.queries))
            .await?;
        Ok(())
    }
}
