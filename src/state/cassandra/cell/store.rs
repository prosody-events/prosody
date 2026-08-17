#[cfg(test)]
use super::RecoveryReadCounts;
use super::{
    Arc, BatchUnit, Bytes, CassandraCellStoreError, CassandraSession, CassandraStore, Cell,
    CellAddr, CellBatchRow, CellBlobs, CellKey, CellKind, CellQueries, CellStore, CellStoreError,
    CollectionDefRegistry, CollectionId, CollectionRef, CommitOracle, Coordinate, EventMarker,
    EventRef, KeyRow, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS, MarkerBlob, MarkerPresence, Pk,
    PreparedStatement, QueryRowsResult, ResolveCellError, ResolvedRow, Resolver, RowShape,
    SHARD_FANOUT_CONCURRENCY, Scan, ScanStatements, Section, Session, Stream, TryStreamExt,
    blob_weight, decode, encode, encode_marker_payload, fetch_and_decode_cell,
    fetch_cell_rows_result, fetch_cells_batch_result, flatten_resolve, help_read_window,
    marker_delete_unit, marker_last_split, page_cells, peek_read, pin_mut, resolve_marker,
    smallvec, try_stream,
};

impl<O> CassandraStore<O> {
    /// Creates a Cassandra cell store over an existing session, a prepared
    /// [`CellQueries`] set, the commit oracle it resolves provisional cells
    /// through, the registry that supplies per-collection TTLs, and the
    /// per-assignment [`MarkerPresence`] latch minted from the partition's
    /// fjall workspace.
    #[must_use]
    pub(crate) fn new(
        session: CassandraSession,
        queries: Arc<CellQueries>,
        oracle: O,
        registry: Arc<CollectionDefRegistry>,
        presence: MarkerPresence,
    ) -> Self {
        Self {
            session,
            queries,
            resolver: Resolver::new(oracle, registry),
            memo: Arc::default(),
            presence,
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

    /// Mirrors a successful settle into the marker memo
    /// ([`super::MarkerMemo`]'s standing map plus the presence latch): the
    /// marker is now durably deleted, so the collection is known
    /// marker-absent for the rest of the assignment.
    pub(super) async fn settle_memo(&self, collection: &CollectionId) {
        self.memo.standing.remove_async(collection).await;
        self.presence.set(collection).await;
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
    /// The boundary resolves any standing FOREIGN marker (a different event)
    /// before overwriting it, establishing marker uniqueness per collection. A
    /// same-event marker (a retry attempt re-running finalize, or the later
    /// chunk of a split stage) is overwritten, never resolved. A resolution
    /// failure fails the stage (retry middleware). The memo is updated BEFORE
    /// the durable attempt (the over-report-safe direction — see the
    /// [`super::MarkerMemo`] invariant).
    pub(super) async fn stage_marker(
        &self,
        collection: &CollectionRef,
        marker: &EventMarker,
    ) -> Result<MarkerBlob, CellStoreError<O::Error>> {
        if let Some(standing) = self.standing_marker(collection.id()).await?
            && standing.event() != marker.event()
        {
            resolve_marker(self, self.resolver.oracle(), collection, &standing)
                .await
                .map_err(flatten_resolve)?;
        }
        self.memo
            .standing
            .upsert_async(collection.id().clone(), marker.clone())
            .await;
        self.presence.set(collection.id()).await;
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
    pub(super) fn scan_inner<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), CellStoreError<O::Error>>> + Send + 'a {
        let limit = scan.limit;
        let collection_ref = self.resolver.collection_ref(collection);
        try_stream! {
            // Read-help once before the pager opens (`help_read_window`): a
            // standing foreign clears-bearing marker is resolved so the scan
            // pages post-clear truth. Memo-backed — no durable marker read
            // after the seed read. The reader-only scan
            // ([`CassandraCellResources::scan_committed`]) skips this: it
            // observes `prev`, which is committed by construction.
            let standing = self.standing_marker(collection).await?;
            help_read_window(self, self.resolver.oracle(), &collection_ref, standing.as_ref(), own)
                .await
                .map_err(flatten_resolve)?;
            // The shared paging core (`page_cells`): it selects the per-bound
            // statement, decodes each row, and applies `past_end`. It applies
            // no resolution and no limit.
            let pages = page_cells(
                &self.session,
                ScanStatements::values(&self.queries),
                collection,
                scan,
                decode::try_decode_keyed_cell,
            );
            pin_mut!(pages);

            let mut yielded = 0usize;
            // Deliberately sequential, not an oversight: the common `peek_read`
            // is a free no-op (own-event provisional / already-resolved cells
            // consult no oracle), so steady-state scans gain nothing from
            // fan-out; the only payoff is mid-recovery across many foreign
            // provisional cells. And because `limit` counts *present* yields —
            // knowable only post-resolve — a buffered pipeline would resolve up
            // to N−1 foreign-provisional cells past the boundary, each an extra
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
    /// awaits the recovery prefix to completion BEFORE issuing the marker
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
