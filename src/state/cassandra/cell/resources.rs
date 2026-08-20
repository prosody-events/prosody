use super::{
    Arc, Bytes, CassandraCellResources, CassandraCellStoreError, CassandraSession, Cell,
    CellBuffer, CellKey, CellQueries, CollectionId, CoordinateBatch, DeserializeRow, PresenceBatch,
    Scan, ScanStatements, Section, Stream, StreamExt, TryStreamExt, decode,
    decode_presence_batch_rows, dedupe, expand_to_input_order, fetch_and_decode_cell,
    fetch_cells_batch, fetch_presence_batch_result, page_cells, pin_mut, try_stream,
};

impl CassandraCellResources {
    /// Bundles the shared session and prepared cell statements.
    #[must_use]
    pub fn new(session: CassandraSession, queries: Arc<CellQueries>) -> Self {
        Self { session, queries }
    }

    /// Reads one cell's committed value without consulting the oracle. This is
    /// the read path a standalone reader uses, not the owner. It point-reads
    /// the `kind=Cell` row and projects
    /// [`crate::state::cell::Cell::project_committed`]. It never returns an
    /// in-flight provisional value and never runs owner-side repair: no
    /// `help_read_window`, no oracle. An absent row reads `None`. It
    /// decodes the borrowed row before it drops the response.
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

    /// Reads an index-aligned batch of committed cell presence values.
    /// This method applies [`crate::state::cell::Cell::project_committed`].
    /// It does not consult the oracle or run owner-side repair.
    pub(crate) async fn read_committed_presence_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<PresenceBatch, CassandraCellStoreError> {
        // The fetch and decode pipelines differ, so a generic fold adds machinery
        // without clarity.
        let (unique_coordinates, input_indices) = dedupe(batch);
        let result = fetch_presence_batch_result(
            &self.session,
            &self.queries,
            id,
            section,
            &unique_coordinates,
        )
        .await?;
        let unique_answers: PresenceBatch =
            decode_presence_batch_rows(&result, &unique_coordinates)?
                .into_iter()
                .map(|row| row.is_some_and(|cell| cell.project_committed().is_some()))
                .collect();
        Ok(expand_to_input_order(&input_indices, &unique_answers))
    }

    /// Scans committed values through [`Self::scan_committed_inner`].
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
        .map(|item| item.map(|(key, _)| key))
    }

    /// Scans committed projections without the owner's oracle or repair.
    ///
    /// This scan drives [`page_cells`] and yields committed projections in
    /// coordinate order. The limit counts only present yields.
    ///
    /// A provisional row's `prev` is committed by construction. A resolved
    /// row's `data` was committed earlier. A pending section clear can make
    /// resolved data stale until the owner applies the clear. This bounded
    /// state never exposes uncommitted data.
    fn scan_committed_inner<'a, Row>(
        &'a self,
        statements: ScanStatements<'a>,
        id: &'a CollectionId,
        scan: Scan<'a>,
        decode_row: fn(Row) -> Result<(CellKey, Cell), CassandraCellStoreError>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), CassandraCellStoreError>> + Send + 'a
    where
        Row: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata> + Send + 'a,
    {
        let limit = scan.result_limit();
        try_stream! {
            let pages = page_cells(
                &self.session,
                statements,
                id,
                scan,
                decode_row,
            );
            pin_mut!(pages);
            let mut yielded = 0usize;
            while let Some((key, cell)) = pages.try_next().await? {
                if limit.is_some_and(|n| yielded >= n.get()) {
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
