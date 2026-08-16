use super::{
    Arc, Bytes, CassandraCellResources, CassandraCellStoreError, CassandraSession, CellBuffer,
    CellKey, CellQueries, CollectionId, CoordinateBatch, Scan, Section, Stream, TryStreamExt,
    dedupe, fetch_and_decode_cell, fetch_cells_batch, page_cells, pin_mut, realign, try_stream,
};

impl CassandraCellResources {
    /// Bundles the shared session and prepared cell statements.
    #[must_use]
    pub fn new(session: CassandraSession, queries: Arc<CellQueries>) -> Self {
        Self { session, queries }
    }

    /// Reads one cell's committed value without consulting the oracle. This is
    /// the read path a standalone reader uses, not the owner. It point-reads
    /// the `kind=Cell` row and projects [`Cell::project_committed`]. It
    /// never returns an in-flight provisional value and never runs
    /// owner-side repair: no `help_read_window`, no oracle. An absent row
    /// reads `None`. It decodes the borrowed row before it drops the response.
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
    /// coordinates in one `IN` query. The result is index-aligned to `batch`,
    /// so `result[i]` answers `batch[i]`. Duplicate coordinates share one
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
        let (uniques, plan) = dedupe(batch);
        let rows = fetch_cells_batch(&self.session, &self.queries, id, section, &uniques).await?;
        let answers: CellBuffer<Option<Bytes>> = rows
            .into_iter()
            .map(|row| row.and_then(|(cell, _)| cell.project_committed().cloned()))
            .collect();
        Ok(realign(&plan, &answers))
    }

    /// Scans a section's committed values without consulting the oracle. This
    /// is the scan path a standalone reader uses, not the owner. It drives
    /// the shared [`page_cells`] pager and yields each present cell's
    /// [`Cell::project_committed`] in `coordinate` order. The scan's `limit`
    /// counts only present yields. It skips `help_read_window`, the owner-side
    /// durable repair a reader cannot and may not run.
    ///
    /// The projection is sound without that repair. A provisional row's `prev`
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
