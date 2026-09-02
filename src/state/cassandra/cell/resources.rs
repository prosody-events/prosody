use super::{
    Arc, Bytes, CassandraCellResources, CassandraCellStoreError, CassandraSession, CellBuffer,
    CellKey, CellQueries, CollectionId, CoordinateBatch, Scan, Section, Stream, TryStreamExt,
    dedupe, expand_to_input_order, fetch_and_decode_cell, fetch_cells_batch, page_cells, pin_mut,
    try_stream,
};

impl CassandraCellResources {
    /// Bundles the shared session and prepared cell statements.
    #[must_use]
    pub fn new(session: CassandraSession, queries: Arc<CellQueries>) -> Self {
        Self { session, queries }
    }

    /// Returns one cell for a standalone reader.
    ///
    /// This read does not resolve markers or change durable state.
    /// It returns `prev` for a provisional cell.
    /// It returns `None` for an absent cell.
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
        Ok(cell.project_committed().cloned())
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
        let unique_answers: CellBuffer<Option<Bytes>> = rows
            .into_iter()
            .map(|row| row.and_then(|(cell, _)| cell.project_committed().cloned()))
            .collect();
        Ok(expand_to_input_order(&input_indices, &unique_answers))
    }

    /// Scans cells for a standalone reader.
    ///
    /// This scan does not resolve markers or change durable state.
    /// It returns cells in coordinate order.
    /// The limit counts only returned cells.
    ///
    /// This scan does not resolve an unsettled section clear.
    /// The projection is still sound: a provisional row's `prev`
    /// is committed by construction, and a resolved row's `data` was committed
    /// at some earlier point. So a resolved row written before a committed but
    /// not-yet-applied section clear reads a value that was once committed but
    /// is now stale, until the owner applies the clear. That staleness is
    /// bounded (see
    /// [`Cell::project_committed`](crate::state::cell::Cell::project_committed)).
    /// It is never an uncommitted read.
    pub(crate) fn scan_committed<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), CassandraCellStoreError>> + Send + 'a {
        let limit = scan.limit;
        try_stream! {
            let pages = page_cells(&self.session, &self.queries, id, scan);
            pin_mut!(pages);
            let mut yielded = 0usize;
            while let Some((key, cell)) = pages.try_next().await? {
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                if let Some(bytes) = cell.project_committed().cloned() {
                    yield (key, bytes);
                    yielded += 1;
                }
            }
        }
    }
}
