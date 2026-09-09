use super::read::fetch_marker_state;
use super::{
    Arc, Bytes, CassandraCellResources, CassandraCellStoreError, CassandraSession, CellBuffer,
    CellKey, CellQueries, CollectionId, CoordinateBatch, Scan, Section, Stream, TryStreamExt,
    dedupe, expand_to_input_order, fetch_and_decode_cell, fetch_cells_batch, page_cells, pin_mut,
    try_stream,
};
use crate::state::cell::{Cell, resolve_for_reader};
use crate::state::marker::ReaderEvidence;
use crate::state::resolve::sibling_committed;

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
    /// `prev`. It returns `None` for an absent cell.
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
        let Some(cell) =
            fetch_and_decode_cell(&self.session, &self.queries.read_cell, id, cell).await?
        else {
            return Ok(None);
        };
        let evidence = if matches!(cell, Cell::Provisional(_)) {
            self.reader_evidence(id).await?
        } else {
            ReaderEvidence::default()
        };
        Ok(resolve_for_reader(&cell, &evidence).cloned())
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
        let rows = fetch_cells_batch(
            &self.session,
            &self.queries,
            id,
            section,
            &unique_coordinates,
        )
        .await?;
        let evidence = if rows
            .iter()
            .flatten()
            .any(|(cell, _)| matches!(cell, Cell::Provisional(_)))
        {
            self.reader_evidence(id).await?
        } else {
            ReaderEvidence::default()
        };
        let unique_answers: CellBuffer<Option<Bytes>> = rows
            .into_iter()
            .map(|row| row.and_then(|(cell, _)| resolve_for_reader(&cell, &evidence).cloned()))
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
        let limit = scan.limit;
        try_stream! {
            let pages = page_cells(&self.session, &self.queries, id, scan);
            pin_mut!(pages);
            let (evidence, mut row) = futures::try_join!(self.reader_evidence(id), pages.try_next())?;
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
