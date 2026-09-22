#[cfg(test)]
use super::CellReadCounts;
use super::projection::CassandraProjection;
use super::read::{decode_point, fetch_batch, fetch_point, page};
use super::{
    Arc, BatchUnit, Bytes, CacheBatch, CassandraCellStoreError, CassandraSession, CassandraStore,
    Cell, CellAddr, CellBatchRow, CellBlobs, CellKey, CellKind, CellQueries, CellStoreError,
    CollectionDefRegistry, CollectionId, Committed, EventMarker, EvidenceLookup, KeyRow,
    MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS, MarkerBlob, Pk, ResolveCellError, ResolvedRow, RowShape,
    SHARD_FANOUT_CONCURRENCY, Scan, Section, Stream, TryStreamExt, blob_weight, dedupe, encode,
    encode_marker_payload, expand_to_input_order, pin_mut, smallvec, try_stream,
    ttl_seconds_to_duration,
};
use crate::state::cell_key::CellRef;
use crate::state::store::{CellRead, ReadBatch};
use crate::state::store_types::Durable;

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
    ) -> impl Iterator<Item = BatchUnit<CellBatchRow<'u>>> + use<'u> {
        blobs.iter().zip(cells).map(move |(blob, (cell, _))| {
            let addr = CellAddr::new(pk, cell);
            let row = match blob.data() {
                Some(_) => CellBatchRow {
                    statement: &self.queries.cells.write_resolved,
                    row: RowShape::Resolved(ResolvedRow {
                        ttl,
                        data: blob.data(),
                        encoding: blob.encoding(),
                        version: blob.version(),
                        addr,
                    }),
                },
                None => CellBatchRow {
                    statement: &self.queries.cells.cell_delete,
                    row: RowShape::Key(KeyRow {
                        kind: CellKind::Cell,
                        addr,
                    }),
                },
            };
            BatchUnit::new(blob_weight(blob), smallvec![row])
        })
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

impl<P: CassandraProjection> CellRead<P> for CassandraStore {
    /// Reads one committed projection and its remaining durable TTL.
    async fn read(
        &self,
        id: &CollectionId,
        cell: CellRef<'_>,
    ) -> Result<Durable<P>, CellStoreError> {
        let row = fetch_point::<P>(&self.session, &self.queries, id, cell)
            .await
            .map_err(ResolveCellError::Store)?;
        let (raw, ttl) = match row {
            Some(row) => decode_point::<P>(row).map_err(ResolveCellError::Store)?,
            None => (Cell::Resolved(Committed::new(None)), None),
        };
        let committed = EvidenceLookup::new(self, id).resolve(raw).await?;
        Ok((committed, ttl_seconds_to_duration(ttl)))
    }

    /// Resolves each unique coordinate once and expands answers to input order.
    async fn read_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &ReadBatch<'_>,
    ) -> Result<CacheBatch<P>, CellStoreError> {
        let (coordinates, indices) = dedupe(batch);
        let rows = fetch_batch::<P>(&self.session, &self.queries, id, section, &coordinates)
            .await
            .map_err(ResolveCellError::Store)?;
        let mut answers = CacheBatch::<P>::with_capacity(coordinates.len());
        let mut lookup = EvidenceLookup::new(self, id);
        for row in rows {
            let (raw, ttl) = match row {
                Some(row) => decode_point::<P>(row).map_err(ResolveCellError::Store)?,
                None => (Cell::Resolved(Committed::new(None)), None),
            };
            answers.push((lookup.resolve(raw).await?, ttl_seconds_to_duration(ttl)));
        }
        Ok(expand_to_input_order(&indices, &answers))
    }

    /// Scans one section under the selected projection.
    fn scan<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), CellStoreError>> + Send + use<'a, P> {
        try_stream! {
            let pages = page::<P>(&self.session, &self.queries, collection, scan);
            pin_mut!(pages);

            let mut lookup = EvidenceLookup::new(self, collection);
            while let Some((key, raw)) = pages.try_next().await.map_err(ResolveCellError::Store)? {
                let committed = lookup.resolve(raw).await?;
                if let Some(bytes) = committed.into_inner() {
                    yield (key, bytes);
                }
            }
        }
    }
}
