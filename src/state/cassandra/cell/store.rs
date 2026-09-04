#[cfg(test)]
use super::RecoveryReadCounts;
use super::{
    Arc, BatchUnit, Bytes, CassandraCellStoreError, CassandraSession, CassandraStore, Cell,
    CellAddr, CellBatchRow, CellBlobs, CellKey, CellKind, CellQueries, CellStore, CellStoreError,
    CollectionDefRegistry, CollectionId, CollectionRef, CommitOracle, Coordinate, DeserializeRow,
    EventMarker, EventRef, KeyRow, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS, MarkerBlob,
    MarkerCheckSet, Pk, PreparedStatement, QueryRowsResult, ResolveCellError, ResolvedRow,
    Resolver, RowShape, SHARD_FANOUT_CONCURRENCY, Scan, ScanStatements, Section, Session, Stream,
    TryStreamExt, blob_weight, encode, encode_marker_payload, fetch_and_decode_cell,
    fetch_cell_rows_result, fetch_cells_batch_result, flatten_resolve, marker_delete_unit,
    marker_last_split, page_cells, peek_read, pin_mut, resolve_event_marker,
    resolve_prior_clear_before_read, smallvec, try_stream,
};

impl<O> CassandraStore<O> {
    /// Creates a Cassandra cell store for one partition assignment.
    ///
    /// The marker-check set must use the assignment cache workspace.
    #[must_use]
    pub(crate) fn new(
        session: CassandraSession,
        queries: Arc<CellQueries>,
        oracle: O,
        registry: Arc<CollectionDefRegistry>,
        checks: MarkerCheckSet,
    ) -> Self {
        Self {
            session,
            queries,
            resolver: Resolver::new(oracle, registry),
            memo: Arc::new(super::MarkerMemo::new(checks)),
            #[cfg(test)]
            counters: Arc::default(),
        }
    }

    /// Test handle on the recovery-read counters (shared across clones), for
    /// the zero-query and bounded-recovery assertions.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn recovery_reads(&self) -> Arc<RecoveryReadCounts> {
        self.counters.clone()
    }

    pub(super) fn cql(&self) -> &Session {
        self.session.session()
    }

    pub(super) async fn point_read_cell(
        &self,
        statement: &PreparedStatement,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<Cell>, CassandraCellStoreError> {
        fetch_and_decode_cell(&self.session, statement, id, cell).await
    }

    pub(super) async fn point_read_cell_result(
        &self,
        statement: &PreparedStatement,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<QueryRowsResult, CassandraCellStoreError> {
        fetch_cell_rows_result(&self.session, statement, id, cell).await
    }

    pub(super) async fn batch_read_result(
        &self,
        id: &CollectionId,
        section: Section,
        unique_coordinates: &[&Coordinate],
    ) -> Result<QueryRowsResult, CassandraCellStoreError> {
        fetch_cells_batch_result(
            &self.session,
            &self.queries,
            id,
            section,
            unique_coordinates,
        )
        .await
    }

    /// Executes the packed same-partition `UNLOGGED BATCH`es for a multi-cell
    /// mutation — the shared tail of the cell mutators. Each [`BatchUnit`] is
    /// one row (a cell mutation, or the marker row), packed into the fewest
    /// batches under the byte and statement budgets; every batch is
    /// row-disjoint by construction (the marker address is disjoint from every
    /// cell row by `kind`), so no same-batch timestamp tie can pit a delete
    /// against a write of one row.
    ///
    /// Allocation ruling (write-path buffer audit): every mutator's `units`
    /// buffer — and the `blobs` its rows borrow — stays a `Vec`, never a
    /// [`crate::state::store::CellBuffer`]/`SmallVec`.
    /// `BatchUnit<CellBatchRow>` is 320 B and `CellBlobs` 80 B, and both live
    /// across this `.await`; an inline capacity would embed hundreds of bytes
    /// to kilobytes in every stage/settle future the way `StagedCollection`
    /// tripped clippy `large_futures` (`crate::state::session`). Write sets are
    /// not `CELL_BATCH`-bounded (the packer splits by byte/statement budget
    /// downstream), and each build site is already exactly-sized
    /// `Vec::with_capacity`, so a conversion removes at most one allocation and
    /// cannot earn the footprint.
    pub(super) async fn run_batches(
        &self,
        units: &[BatchUnit<CellBatchRow<'_>>],
    ) -> Result<(), CassandraCellStoreError> {
        self.session
            .execute_unlogged_batches(
                units,
                MAX_BATCH_BYTES,
                MAX_BATCH_STATEMENTS,
                SHARD_FANOUT_CONCURRENCY,
            )
            .await?;
        Ok(())
    }

    /// Builds the resolved-write batch units for `cells` — the shared unit
    /// construction of `write_resolved` and `abort_provisional`. A present
    /// value binds the resolved-value shape; an absent value **deletes** the
    /// `kind=Cell` row (the row-absence invariant — no null-blob residue).
    /// Returns a borrowing iterator the callers extend into their pre-sized
    /// `units` — no intermediate buffer; see [`Self::run_batches`] for why the
    /// callers' `units` is a `Vec` rather than a
    /// [`crate::state::store::CellBuffer`].
    pub(super) fn resolved_units<'u>(
        &'u self,
        pk: Pk<'u>,
        ttl: Option<i32>,
        blobs: &'u [CellBlobs],
        cells: &'u [(CellKey, Option<Bytes>)],
    ) -> impl Iterator<Item = BatchUnit<CellBatchRow<'u>>> + 'u {
        let cell_stmt = if ttl.is_some() {
            &self.queries.write_resolved
        } else {
            &self.queries.write_resolved_no_ttl
        };
        blobs.iter().zip(cells).map(move |(blob, (cell, _))| {
            let addr = CellAddr::new(pk, cell);
            let row = match blob.data() {
                Some(_) => CellBatchRow {
                    statement: cell_stmt,
                    row: RowShape::Resolved(ResolvedRow {
                        ttl,
                        data: blob.data(),
                        encoding: blob.encoding(),
                        version: blob.version(),
                        addr,
                    }),
                },
                None => CellBatchRow {
                    statement: &self.queries.cell_delete,
                    row: RowShape::Key(KeyRow {
                        kind: CellKind::Cell,
                        addr,
                    }),
                },
            };
            BatchUnit::new(blob_weight(blob), smallvec![row])
        })
    }

    /// Records that durable state has no unsettled marker.
    pub(super) async fn record_marker_settled(&self, collection: &CollectionId) {
        self.memo.unsettled.remove_async(collection).await;
        self.memo.checks.set(collection).await;
    }
}

impl<O> CassandraStore<O>
where
    O: CommitOracle,
{
    /// The stage's marker half, ahead of any row building: the stage-boundary
    /// resolve, the memo mirror, and the frozen payload's encoding. Returns
    /// the marker row's blob.
    ///
    /// Resolves a prior event marker before it stores the new marker.
    ///
    /// A marker for the same event can be replaced safely.
    /// The function updates the memo before it writes durable state.
    pub(super) async fn stage_marker(
        &self,
        collection: &CollectionRef,
        marker: &EventMarker,
    ) -> Result<MarkerBlob, CellStoreError<O::Error>> {
        if let Some(unsettled) = self.unsettled_marker(collection.id()).await?
            && unsettled.event() != marker.event()
        {
            resolve_event_marker(self, self.resolver.oracle(), collection, &unsettled)
                .await
                .map_err(flatten_resolve)?;
        }
        self.memo
            .unsettled
            .upsert_async(collection.id().clone(), marker.clone())
            .await;
        self.memo.checks.set(collection.id()).await;
        let payload = encode_marker_payload(marker)
            .map_err(CassandraCellStoreError::from)
            .map_err(ResolveCellError::Store)?;
        let payload = encode(&payload)
            .map_err(CassandraCellStoreError::from)
            .map_err(ResolveCellError::Store)?;
        Ok(MarkerBlob {
            payload,
            event: marker.event(),
        })
    }

    /// The single resolving section scan, yielding each present cell's
    /// committed bytes — the body behind [`scan_cells`](CellStore::scan_cells).
    pub(super) fn scan_inner<'a, Row>(
        &'a self,
        statements: ScanStatements<'a>,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
        decode_row: fn(Row) -> Result<(CellKey, Cell), CassandraCellStoreError>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), CellStoreError<O::Error>>> + Send + 'a
    where
        Row: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata> + Send + 'a,
    {
        let limit = scan.limit;
        let collection_ref = self.resolver.collection_ref(collection);
        try_stream! {
            // Resolve a prior event's section clear before the scan starts.
            // The scan cannot return data that the clear removed.
            let marker = self.unsettled_marker(collection).await?;
            // The scan starts after this resolution, so a durable change needs no re-read.
            let _ = resolve_prior_clear_before_read(
                self,
                self.resolver.oracle(),
                &collection_ref,
                marker.as_ref(),
                own,
            )
            .await
            .map_err(flatten_resolve)?;
            // The shared paging core (`page_cells`): it selects the per-bound
            // statement, decodes each row, and applies `past_end`. It applies
            // no resolution and no limit.
            let pages = page_cells(
                &self.session,
                statements,
                collection,
                scan,
                decode_row,
            );
            pin_mut!(pages);

            let mut yielded = 0usize;
            // Deliberately sequential, not an oversight: the common `peek_read`
            // is a free no-op (own-event provisional / already-resolved cells
            // consult no oracle), so steady-state scans gain nothing from
            // fan-out; the only payoff is mid-recovery across many prior event
            // provisional cells. And because `limit` counts *present* yields —
            // knowable only post-resolve — a buffered pipeline would resolve up
            // to N−1 prior event-provisional cells past the boundary, each an extra
            // oracle read: a recovery-only win we won't pay for on a hot read
            // path. `peek_read` is read-only — it never writes a resolution
            // back durably (a scan write-back could clobber a newer `commit()`
            // of the same cell), so this posture costs no write amplification.
            while let Some((key, raw)) = pages.try_next().await.map_err(ResolveCellError::Store)? {
                // The limit bounds *yielded* (present) cells; check it before
                // processing the next row so `Some(0)` yields nothing (an absent
                // cell never consumes a slot — only a present yield does).
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                let committed = peek_read(self.resolver.oracle(), &collection_ref, own, raw)
                    .await
                    .map_err(ResolveCellError::Oracle)?;
                if let Some(bytes) = committed.into_inner() {
                    yield (key, bytes);
                    yielded += 1;
                }
            }
        }
    }

    /// Issues a settle's `units` marker-LAST: appends the collection's marker
    /// delete, then runs one atomic batch when everything fits the budget, else
    /// awaits the recovery prefix to completion before issuing the marker
    /// alone. Owning the append, the split, and the ordered await here
    /// makes marker misplacement and await reversal unrepresentable at the
    /// call sites — the coupling [`marker_last_split`]'s positional index
    /// alone cannot enforce.
    pub(super) async fn issue_marker_last<'u>(
        &'u self,
        pk: Pk<'u>,
        mut units: Vec<BatchUnit<CellBatchRow<'u>>>,
    ) -> Result<(), CellStoreError<O::Error>> {
        units.push(marker_delete_unit(pk, &self.queries));
        let split = marker_last_split(&units, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS);
        self.run_batches(&units[..split])
            .await
            .map_err(ResolveCellError::Store)?;
        if split < units.len() {
            self.run_batches(&units[split..])
                .await
                .map_err(ResolveCellError::Store)?;
        }
        Ok(())
    }
}
