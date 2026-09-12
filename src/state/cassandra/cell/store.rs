#[cfg(test)]
use super::CellReadCounts;
use super::decode::CellDecoder;
use super::{
    Arc, BatchUnit, Bytes, CassandraCellStoreError, CassandraSession, CassandraStore, Cell,
    CellAddr, CellBatchRow, CellBlobs, CellKey, CellKind, CellQueries, CellStoreError,
    CollectionDefRegistry, CollectionId, Coordinate, DeserializeRow, EventMarker, EvidenceLookup,
    KeyRow, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS, MarkerBlob, Pk, PreparedStatement,
    QueryRowsResult, ResolveCellError, ResolvedRow, RowShape, SHARD_FANOUT_CONCURRENCY, Scan,
    ScanStatements, Section, Stream, TryStreamExt, blob_weight, encode, encode_marker_payload,
    fetch_and_decode_cell, fetch_cell_rows_result, fetch_cells_batch_result, page_cells, pin_mut,
    smallvec, try_stream,
};

use crate::state::cell::Projection;

impl CassandraStore {
    /// Creates a Cassandra cell store for one partition assignment.
    ///
    /// The marker-check set must use the assignment cache workspace.
    #[must_use]
    pub(crate) fn new(
        session: CassandraSession,
        queries: Arc<CellQueries>,
        registry: Arc<CollectionDefRegistry>,
    ) -> Self {
        Self {
            session,
            queries,
            registry,
            #[cfg(test)]
            counters: Arc::default(),
        }
    }

    /// Returns the shared counters for durable reads.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn read_counts(&self) -> Arc<CellReadCounts> {
        self.counters.clone()
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

    /// Executes same-partition `UNLOGGED BATCH` statements for cell mutations.
    /// Each [`BatchUnit`] changes one cell or marker row. The packer uses the
    /// fewest batches within byte and statement budgets.
    /// Rows within a batch are disjoint; `kind` separates marker and cell
    /// addresses. Thus, equal timestamps cannot make a delete compete with
    /// a write to the same row.
    ///
    /// The `units` and borrowed `blobs` buffers retain exactly sized `Vec`
    /// allocations to limit future sizes.
    /// Inline buffers would enlarge every stage and settle future across this
    /// await. Write sets have no `CELL_BATCH` limit; the packer splits them
    /// downstream.
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
        ttl: i32,
        blobs: &'u [CellBlobs],
        cells: &'u [(CellKey, Option<Bytes>)],
    ) -> impl Iterator<Item = BatchUnit<CellBatchRow<'u>>> + 'u {
        blobs.iter().zip(cells).map(move |(blob, (cell, _))| {
            let addr = CellAddr::new(pk, cell);
            let row = match blob.data() {
                Some(_) => CellBatchRow {
                    statement: &self.queries.write_resolved,
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

    /// The single resolving section scan, yielding each present cell's
    /// committed bytes — the body behind
    /// [`scan_cells`](super::CellStore::scan_cells).
    pub(super) fn scan_inner<'a, Row, P: Projection>(
        &'a self,
        statements: ScanStatements<'a>,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        decode_row: CellDecoder<Row, P>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), CellStoreError>> + Send + 'a
    where
        Row: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata> + Send + 'a,
    {
        let limit = scan.limit;
        try_stream! {
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

            let mut lookup = EvidenceLookup::new(self, collection);
            let mut yielded = 0usize;
            while let Some((key, raw)) = pages.try_next().await.map_err(ResolveCellError::Store)? {
                // The limit bounds *yielded* (present) cells; check it before
                // processing the next row so `Some(0)` yields nothing (an absent
                // cell never consumes a slot — only a present yield does).
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                let committed = lookup.resolve(raw).await?;
                if let Some(bytes) = committed.into_inner() {
                    yield (key, bytes);
                    yielded += 1;
                }
            }
        }
    }

    /// Writes evidence before destructive promote chunks and deletes Staged
    /// last. A resolved cell without evidence, or residue without Staged,
    /// cannot result from a partial promote. An abort has no leading
    /// evidence unit.
    pub(super) async fn issue_markers<'u>(
        &'u self,
        leading: Option<BatchUnit<CellBatchRow<'u>>>,
        units: Vec<BatchUnit<CellBatchRow<'u>>>,
        trailing: BatchUnit<CellBatchRow<'u>>,
    ) -> Result<(), CellStoreError> {
        let (units, phases) = super::batch::settle_batches(
            leading,
            units,
            trailing,
            MAX_BATCH_BYTES,
            MAX_BATCH_STATEMENTS,
        );
        for phase in phases {
            self.run_batches(&units[phase])
                .await
                .map_err(ResolveCellError::Store)?;
        }
        Ok(())
    }
}

/// Encodes one stage payload for the marker row.
pub(super) fn stage_marker(marker: &EventMarker) -> Result<MarkerBlob, CellStoreError> {
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
