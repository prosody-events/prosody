use super::decode::CellDecoder;
use super::read::fetch_marker_state;
use super::{
    Arc, Bytes, CassandraCellResources, CassandraCellStoreError, CassandraSession, CellBuffer,
    CellKey, CellQueries, CollectionId, CoordinateBatch, DeserializeRow, PresenceBatch, Scan,
    ScanStatements, Section, Stream, StreamExt, TryStreamExt, decode, decode_presence_batch_rows,
    dedupe, expand_to_input_order, fetch_and_decode_cell, fetch_cells_batch,
    fetch_presence_batch_result, page_cells, pin_mut, try_stream,
};
use crate::state::cell::{Projection, resolve_for_reader};
use crate::state::marker::ReaderEvidence;
use crate::state::resolve::sibling_committed;
use futures::try_join;

impl CassandraCellResources {
    /// Bundles the shared session and prepared cell statements.
    #[must_use]
    pub fn new(session: CassandraSession, queries: Arc<CellQueries>) -> Self {
        Self { session, queries }
    }

    async fn reader_evidence(
        &self,
        id: &CollectionId,
    ) -> Result<ReaderEvidence, CassandraCellStoreError> {
        // Outside readers do not use evidence TTL; version 1 therefore decodes with
        // None.
        let state = fetch_marker_state(&self.session, &self.queries, id, None).await?;
        let staged_committed = match &state.staged {
            Some(marker) => {
                sibling_committed(id, marker, |sibling| async move {
                    fetch_marker_state(&self.session, &self.queries, &sibling, None).await
                })
                .await?
            }
            None => false,
        };
        Ok(ReaderEvidence {
            state,
            staged_committed,
        })
    }

    /// Returns one cell for a standalone reader.
    ///
    /// This read does not resolve markers or change durable state.
    /// Positive evidence selects a provisional value; otherwise it returns
    /// `prev`. An absent cell or a cell removed by a committed clear reads
    /// `None`.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraCellStoreError`] on a store failure or a corrupt row
    /// shape.
    pub(crate) async fn read_committed(
        &self,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, CassandraCellStoreError> {
        let (value, evidence) = try_join!(
            fetch_and_decode_cell(&self.session, &self.queries.read_cell, id, cell),
            self.reader_evidence(id)
        )?;
        Ok(value
            .filter(|_| evidence.survives(cell))
            .and_then(|value| resolve_for_reader(&value, &evidence).cloned()))
    }

    /// The batch form of [`Self::read_committed`]. Reads one section's
    /// coordinates in one `IN` query. `result[i]` answers `batch[i]`.
    /// Duplicate coordinates share one
    /// lookup, and an absent coordinate reads `None`. Only the committed value
    /// is projected. The TTL column is ignored: the reader has no write-through
    /// cache to mirror it into.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraCellStoreError`] on a store failure or a corrupt row
    /// shape.
    pub(crate) async fn read_committed_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, CassandraCellStoreError> {
        let (unique_coordinates, input_indices) = dedupe(batch);
        let (rows, evidence) = try_join!(
            fetch_cells_batch(
                &self.session,
                &self.queries,
                id,
                section,
                &unique_coordinates
            ),
            self.reader_evidence(id)
        )?;
        let unique_answers: CellBuffer<Option<Bytes>> = rows
            .into_iter()
            .zip(unique_coordinates)
            .map(|(row, coordinate)| {
                let key = CellKey {
                    section,
                    coordinate: coordinate.clone(),
                };
                row.filter(|_| evidence.survives(&key))
                    .and_then(|(cell, _)| resolve_for_reader(&cell, &evidence).cloned())
            })
            .collect();
        Ok(expand_to_input_order(&input_indices, &unique_answers))
    }

    /// Reads an index-aligned batch of committed cell presence values.
    /// Uses the commit evidence and clear rules of [`Self::read_committed`].
    pub(crate) async fn read_committed_presence_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<PresenceBatch, CassandraCellStoreError> {
        // The fetch and decode pipelines differ, so a generic fold adds machinery
        // without clarity.
        let (unique_coordinates, input_indices) = dedupe(batch);
        let (result, evidence) = try_join!(
            fetch_presence_batch_result(
                &self.session,
                &self.queries,
                id,
                section,
                &unique_coordinates,
            ),
            self.reader_evidence(id),
        )?;
        let unique_answers: PresenceBatch =
            decode_presence_batch_rows(&result, &unique_coordinates)?
                .into_iter()
                .zip(unique_coordinates)
                .map(|(row, coordinate)| {
                    let key = CellKey {
                        section,
                        coordinate: coordinate.clone(),
                    };
                    row.is_some_and(|cell| {
                        evidence.survives(&key) && resolve_for_reader(&cell, &evidence).is_some()
                    })
                })
                .collect();
        Ok(expand_to_input_order(&input_indices, &unique_answers))
    }

    /// Scans cells for a standalone reader.
    ///
    /// This scan does not resolve markers or change durable state.
    /// It returns cells in coordinate order.
    /// The limit counts only returned cells.
    ///
    /// A committed clear restricts results to its frozen survivors.
    pub(crate) fn scan_committed<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), CassandraCellStoreError>> + Send + 'a {
        self.scan_committed_inner(
            ScanStatements::values(&self.queries),
            id,
            scan,
            decode::try_decode_keyed_cell,
        )
    }

    /// Scans committed keys through [`Self::scan_committed_inner`].
    pub(crate) fn scan_committed_keys<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<CellKey, CassandraCellStoreError>> + Send + 'a {
        self.scan_committed_inner(
            ScanStatements::presence(&self.queries),
            id,
            scan,
            decode::try_decode_keyed_presence,
        )
        .map(|item| item.map(|(key, ())| key))
    }

    /// Projects values or presence through commit evidence without durable
    /// writes. The limit counts only present results after committed
    /// clears.
    fn scan_committed_inner<'a, Row, P: Projection>(
        &'a self,
        statements: ScanStatements<'a>,
        id: &'a CollectionId,
        scan: Scan<'a>,
        decode_row: CellDecoder<Row, P>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), CassandraCellStoreError>> + Send + 'a
    where
        Row: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata> + Send + 'a,
    {
        let limit = scan.limit;
        try_stream! {
            let pages = page_cells(
                &self.session,
                statements,
                id,
                scan,
                decode_row,
            );
            pin_mut!(pages);
            let (evidence, mut row) = try_join!(self.reader_evidence(id), pages.try_next())?;
            let mut yielded = 0usize;
            while let Some((key, cell)) = row {
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                if evidence.survives(&key) && let Some(bytes) = resolve_for_reader(&cell, &evidence).cloned() {
                    yield (key, bytes);
                    yielded += 1;
                }
                row = pages.try_next().await?;
            }
        }
    }
}
