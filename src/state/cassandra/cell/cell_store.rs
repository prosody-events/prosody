#[cfg(test)]
use super::Ordering;
use super::{
    BatchUnit, Bytes, CacheBatch, CassandraStore, CassandraStoreError, Cell, CellAddr,
    CellBatchRow, CellBuffer, CellKey, CellKind, CellStore, CellStoreError, CollectionId,
    CollectionRef, CommitOracle, Committed, CommittedBatch, CompactDuration, Coordinate,
    CoordinateBatch, EventMarker, EventRef, KeyRow, PER_STATEMENT_OVERHEAD, Pk, PriorEventClear,
    ProvisionalCell, ProvisionalWrite, ReadPreparation, ResolveCellError, RowShape, Scan, Section,
    SectionClear, SmallVec, Stream, UnsettledClear, decode, decode_batch_rows,
    decode_cell_ttl_result, decode_provisional_batch, dedupe, encode_cell_blobs,
    expand_to_input_order, extend_gap_units, flatten_resolve, gap_count, into_store_err,
    match_batch_rows_to_coordinates, resolve_prior_clear_before_read, resolve_read,
    resolve_unsettled_clear_before_write, section_batches, smallvec, sorted_unique_coordinates,
    try_stream, ttl_seconds_to_duration, ttl_to_i32, write_provisional,
};

impl<O> CellStore for CassandraStore<O>
where
    O: CommitOracle,
{
    type Error = CellStoreError<O::Error>;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<Committed, Self::Error> {
        // The committed value is exactly the cache-fill read minus its co-expiry
        // TTL; production only ever calls this via `Cached` (which uses
        // `get_for_cache`), so `get` is a thin convenience for direct callers.
        Ok(self.get_for_cache(collection, cell, own).await?.0)
    }

    async fn get_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<(Committed, Option<CompactDuration>), Self::Error> {
        let collection_ref = self.resolver.collection_ref(collection);
        // The committed-unapplied read window (`resolve_prior_clear_before_read`):
        // consult the standing marker — memo-backed, so no durable read after
        // the one seed read —
        // CONCURRENTLY with the cell point read, keeping the ~always
        // marker-free fast path free of serial latency. If the marker was
        // resolved (foreign + clears), the pre-help row may hold a now-erased
        // value, so the point read is re-issued.
        let (row, marker) = futures::join!(
            self.point_read_cell_result(&self.queries.read_cell_ttl, collection, cell),
            self.unsettled_marker(collection),
        );
        let mut row = row.map_err(ResolveCellError::Store)?;
        let marker = marker?;
        let clear = marker
            .as_ref()
            .and_then(|marker| PriorEventClear::new(marker, own));
        if resolve_prior_clear_before_read(self, self.resolver.oracle(), &collection_ref, clear)
            .await
            .map_err(flatten_resolve)?
            == ReadPreparation::DurableStateChanged
        {
            row = self
                .point_read_cell_result(&self.queries.read_cell_ttl, collection, cell)
                .await
                .map_err(ResolveCellError::Store)?;
        }
        let (raw, ttl) = match decode_cell_ttl_result(&row).map_err(ResolveCellError::Store)? {
            Some(decoded) => decoded,
            None => (Cell::Resolved(Committed::new(None)), None),
        };
        let committed = resolve_read(
            self,
            self.resolver.oracle(),
            &collection_ref,
            cell,
            own,
            raw,
        )
        .await
        .map_err(flatten_resolve)?;
        Ok((committed, ttl_seconds_to_duration(ttl)))
    }

    async fn get_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> Result<CommittedBatch, Self::Error> {
        // Mirrors `get` → `get_for_cache`: the committed value is the batch
        // cache-fill read minus its co-expiry TTLs.
        Ok(self
            .get_many_for_cache(collection, section, batch, own)
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
        own: EventRef,
    ) -> Result<CacheBatch, Self::Error> {
        let collection_ref = self.resolver.collection_ref(collection);
        let (unique_coordinates, input_indices) = dedupe(batch);
        // The committed-unapplied read window, exactly as the point read
        // (`get_for_cache`): consult the standing marker — memo-backed, so no
        // durable read after the one seed read — CONCURRENTLY with the batch
        // query. If the marker was resolved (foreign + clears), the pre-help
        // rows may hold now-erased values, so the whole `IN` query is re-issued
        // once.
        let (rows, marker) = futures::join!(
            self.batch_read_result(collection, section, &unique_coordinates),
            self.unsettled_marker(collection),
        );
        let mut rows = rows.map_err(ResolveCellError::Store)?;
        let marker = marker?;
        let clear = marker
            .as_ref()
            .and_then(|marker| PriorEventClear::new(marker, own));
        if resolve_prior_clear_before_read(self, self.resolver.oracle(), &collection_ref, clear)
            .await
            .map_err(flatten_resolve)?
            == ReadPreparation::DurableStateChanged
        {
            rows = self
                .batch_read_result(collection, section, &unique_coordinates)
                .await
                .map_err(ResolveCellError::Store)?;
        }
        let rows =
            decode_batch_rows(&rows, &unique_coordinates).map_err(ResolveCellError::Store)?;
        let mut unique_answers: CacheBatch = SmallVec::with_capacity(unique_coordinates.len());
        for (&coordinate, row) in unique_coordinates.iter().zip(rows) {
            let cell = CellKey {
                section,
                coordinate: Coordinate::clone(coordinate),
            };
            let (raw, ttl) = match row {
                Some((cell, ttl)) => (cell, ttl),
                None => (Cell::Resolved(Committed::new(None)), None),
            };
            let committed = resolve_read(
                self,
                self.resolver.oracle(),
                &collection_ref,
                &cell,
                own,
                raw,
            )
            .await
            .map_err(flatten_resolve)?;
            unique_answers.push((committed, ttl_seconds_to_duration(ttl)));
        }
        Ok(expand_to_input_order(&input_indices, &unique_answers))
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        self.scan_inner(collection, scan, own)
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        try_stream! {
            // The **cold seed** is the standing event marker (memoized — the
            // one durable marker point read per collection per assignment
            // happens in `unsettled_marker`, wherever it fires first): its
            // frozen payload lists the staged coordinates, so recovery cost is
            // ∝ #provisional, never #cells, with no range read anywhere. The
            // warm short-circuit that skips this on a quiescent sweep lives
            // one layer up on `Cached` (which owns the fjall warm index).
            let Some(marker) = self.unsettled_marker(collection).await? else {
                return;
            };

            // Rebuild each listed coordinate's `ProvisionalCell` through one
            // raw `IN` query per per-section `<=CELL_BATCH` chunk (the section
            // is reattached to each survivor, since coordinates repeat across
            // sections). A listed coordinate whose row is absent (cell and
            // marker share one TTL) or already resolved (first-touch or a
            // concurrent resolve) is dropped by `provisional_many` — the
            // marker's over-report is safe. Sub-batches run sequentially: real
            // `IN`-query I/O leaves drive the coop budget.
            for (section, batch) in section_batches(marker.staged()) {
                // `Box::pin` keeps the large per-chunk batch-read future off
                // this generator's state so it stays small across the yield
                // (bounded per-chunk alloc on a cold recovery path).
                let survivors =
                    Box::pin(self.provisional_many(collection, section, &batch)).await?;
                for (coordinate, provisional) in survivors {
                    yield (CellKey { section, coordinate }, provisional);
                }
            }
        }
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        // Point-read the `kind=Cell` row and keep only a genuinely provisional
        // shape; an absent or resolved coordinate reads `None` (over-report-safe
        // — a coordinate the warm index over-reports is dropped here).
        #[cfg(test)]
        self.counters
            .cell_point_reads
            .fetch_add(1, Ordering::Relaxed);
        let Some(raw) = self
            .point_read_cell(&self.queries.read_cell, collection, cell)
            .await
            .map_err(ResolveCellError::Store)?
        else {
            return Ok(None);
        };
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
        // the decoder. Never consults the oracle, never resolves, never writes —
        // no read-window marker resolve, exactly as `provisional_cell_at`.
        let result = self
            .batch_read_result(collection, section, &unique_coordinates)
            .await
            .map_err(ResolveCellError::Store)?;
        let rows = match_batch_rows_to_coordinates(&result, &unique_coordinates)
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
        // The write-side committed-unapplied boundary
        // (`resolve_unsettled_clear_before_write`): resolve any standing
        // clears-bearing event marker before this blind write lands, ordering
        // the write after that resolution so a stale clear's positional replay
        // cannot erase it (modulo the concurrent-resolver residual documented
        // on `resolve_unsettled_clear_before_write`). Memo-backed
        // (`unsettled_marker`): one RAM/presence check steady-state,
        // and the first marker consumer per collection per assignment pays the
        // one durable seed read. The verb still never *writes* the marker slice
        // (the marker lifecycle belongs to the staged verbs — see
        // `CellStore::write_provisional`); the resolution runs through this
        // store's own `commit_provisional`/`abort_provisional`, keeping the
        // memo coherent.
        let marker = self.unsettled_marker(collection.id()).await?;
        let clear = marker.as_ref().and_then(UnsettledClear::new);
        resolve_unsettled_clear_before_write(self, self.resolver.oracle(), collection, clear)
            .await
            .map_err(flatten_resolve)?;
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
        let ttl = collection.ttl().map(ttl_to_i32);
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
                        statement: &self.queries.mark_resolved,
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

    async fn unsettled_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        // Memo hit: the presence latch says the durable marker was consulted
        // this assignment, so the RAM standing map is at least durable truth —
        // zero durable reads. A presence fjall error reads as unchecked (a
        // redundant durable re-check, never an under-report).
        if self.memo.checked.contains(collection).await {
            return Ok(self
                .memo
                .unsettled
                .read_async(collection, |_, marker| marker.clone())
                .await);
        }
        // Memo miss: the one durable point read at the fixed marker address,
        // seeding the memo for the rest of the assignment.
        #[cfg(test)]
        self.counters
            .marker_point_reads
            .fetch_add(1, Ordering::Relaxed);
        let pk = Pk::of(collection);
        let addr = CellAddr::marker(pk);
        let result = self
            .cql()
            .execute_unpaged(
                &self.queries.marker_read,
                (
                    pk.segment_id,
                    pk.key,
                    pk.state_type,
                    pk.name,
                    CellKind::Marker,
                    addr.section,
                    addr.coordinate,
                ),
            )
            .await
            .map_err(CassandraStoreError::from)
            .map_err(into_store_err::<O::Error>)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)
            .map_err(into_store_err::<O::Error>)?;
        let row = result
            .maybe_first_row::<decode::BorrowedMarkerRow<'_>>()
            .map_err(CassandraStoreError::from)
            .map_err(into_store_err::<O::Error>)?;
        let marker = row
            .map(decode::try_decode_marker)
            .transpose()
            .map_err(ResolveCellError::Store)?;
        if let Some(marker) = &marker {
            self.memo
                .unsettled
                .upsert_async(collection.clone(), marker.clone())
                .await;
        }
        self.memo.checked.set(collection).await;
        Ok(marker)
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // Commit applies natively — present data promotes in place, a staged
        // clear deletes its row (the row-absence invariant).
        // Cell and gap rows are disjoint and idempotent: gaps exclude survivors
        // and one another positionally, while a cell delete inside a gap is a
        // harmless delete/delete tie.
        let pk = Pk::of(collection.id());
        // `units` stays a `Vec` (not a `CellBuffer`) — see the `run_batches` ruling.
        let mut units: Vec<BatchUnit<CellBatchRow>> =
            Vec::with_capacity(writes.len() + gap_count(clears) + 1);
        units.extend(writes.iter().map(|(cell, write)| {
            let addr = CellAddr::new(pk, cell);
            let statement = if write.data().is_some() {
                &self.queries.mark_resolved
            } else {
                &self.queries.cell_delete
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
        self.issue_marker_last(pk, units).await?;
        self.record_marker_settled(collection.id()).await;
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
        let ttl = collection.ttl().map(ttl_to_i32);
        let mut units = Vec::with_capacity(cells.len() + 1);
        units.extend(self.resolved_units(pk, ttl, &blobs, &cells));
        self.issue_marker_last(pk, units).await?;
        self.record_marker_settled(collection.id()).await;
        Ok(())
    }
}
