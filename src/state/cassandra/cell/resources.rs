use super::decode::decode_body;
use super::projection::CassandraProjection;
use super::read::{fetch_batch, fetch_marker_state, fetch_point, page, split_point};
use super::{
    Arc, CassandraCellResources, CassandraCellStoreError, CassandraSession, CellBuffer, CellKey,
    CellQueries, CollectionId, CoordinateBatch, Scan, Section, Stream, TryStreamExt, dedupe,
    expand_to_input_order, pin_mut, try_stream,
};
use crate::state::cell::resolve_for_reader;
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

    /// Reads one projection through commit evidence without a durable write.
    pub(crate) async fn read_committed<P: CassandraProjection>(
        &self,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<P::Payload>, CassandraCellStoreError> {
        let (row, evidence) = try_join!(
            fetch_point::<P>(&self.session, P::statements(&self.queries), id, cell),
            self.reader_evidence(id),
        )?;
        let value = row
            .map(|row| decode_body::<P>(split_point::<P>(row).0))
            .transpose()?;
        Ok(value
            .filter(|_| evidence.survives(cell))
            .and_then(|value| resolve_for_reader(&value, &evidence).cloned()))
    }

    /// Reads one section's coordinates and returns answers in input order.
    pub(crate) async fn read_committed_many<P: CassandraProjection>(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<P::Payload>>, CassandraCellStoreError> {
        let (coordinates, indices) = dedupe(batch);
        let (rows, evidence) = try_join!(
            fetch_batch::<P>(
                &self.session,
                P::statements(&self.queries),
                id,
                section,
                &coordinates
            ),
            self.reader_evidence(id),
        )?;
        let answers: CellBuffer<Option<P::Payload>> = rows
            .into_iter()
            .zip(coordinates)
            .map(|(row, coordinate)| {
                let key = CellKey {
                    section,
                    coordinate: coordinate.clone(),
                };
                let cell = row
                    .map(|row| decode_body::<P>(split_point::<P>(row).0))
                    .transpose()?;
                Ok(cell
                    .filter(|_| evidence.survives(&key))
                    .and_then(|cell| resolve_for_reader(&cell, &evidence).cloned()))
            })
            .collect::<Result<_, CassandraCellStoreError>>()?;
        Ok(expand_to_input_order(&indices, &answers))
    }

    /// Scans committed projections in coordinate order through one evidence
    /// snapshot.
    pub(crate) fn scan_committed<'a, P: CassandraProjection>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), CassandraCellStoreError>> + Send + 'a
    {
        let limit = scan.limit;
        try_stream! {
            let pages = page::<P>(&self.session, P::statements(&self.queries), id, scan);
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
