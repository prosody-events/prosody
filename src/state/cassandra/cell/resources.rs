use super::projection::CassandraProjection;
use super::read::{decode_point, fetch_batch, fetch_marker_state, fetch_point, page, take_row};
use super::{
    Arc, CassandraCellResources, CassandraCellStoreError, CassandraSession, CellBuffer, CellKey,
    CellQueries, CollectionId, Scan, Section, Stream, TryStreamExt, distinct, pin_mut, repeated,
    try_stream,
};
use crate::state::cell::resolve_for_reader;
use crate::state::cell_key::CellRef;
use crate::state::marker::ReaderEvidence;
use crate::state::resolve::sibling_committed;
use crate::state::store::ReadBatch;
use crate::state_reader::{CellSource, CommittedCellSource};
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
        cell: CellRef<'_>,
    ) -> Result<Option<P::Payload>, CassandraCellStoreError> {
        let (row, evidence) = try_join!(
            fetch_point::<P>(&self.session, &self.queries, id, cell),
            self.reader_evidence(id),
        )?;
        let value = row
            .map(decode_point::<P>)
            .transpose()?
            .map(|(cell, _)| cell);
        Ok(value
            .filter(|_| evidence.survives(cell))
            .and_then(|value| resolve_for_reader(&value, &evidence).cloned()))
    }

    /// Reads one section's coordinates and returns answers in input order.
    pub(crate) async fn read_committed_many<P: CassandraProjection>(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &ReadBatch<'_>,
    ) -> Result<CellBuffer<Option<P::Payload>>, CassandraCellStoreError> {
        let coordinates = distinct(batch);
        let (mut rows, evidence) = try_join!(
            fetch_batch::<P>(&self.session, &self.queries, id, section, &coordinates),
            self.reader_evidence(id),
        )?;
        let mut answers = CellBuffer::with_capacity(batch.len());
        for &coordinate in batch.iter() {
            let answer = if let Some(answer) = repeated(batch, &answers, coordinate) {
                answer
            } else {
                let key = CellRef {
                    section,
                    coordinate,
                };
                let cell = take_row(&mut rows, coordinate)
                    .map(decode_point::<P>)
                    .transpose()?
                    .map(|(cell, _)| cell);
                cell.filter(|_| evidence.survives(key))
                    .and_then(|cell| resolve_for_reader(&cell, &evidence).cloned())
            };
            answers.push(answer);
        }
        Ok(answers)
    }

    /// Scans committed projections in coordinate order through one evidence
    /// snapshot.
    pub(crate) fn scan_committed<'a, P: CassandraProjection>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), CassandraCellStoreError>> + Send + use<'a, P>
    {
        try_stream! {
            let pages = page::<P>(&self.session, &self.queries, id, scan);
            pin_mut!(pages);
            let (evidence, mut row) = try_join!(self.reader_evidence(id), pages.try_next())?;
            while let Some((key, cell)) = row {
                if evidence.survives(key.as_ref()) && let Some(bytes) = resolve_for_reader(&cell, &evidence).cloned() {
                    yield (key, bytes);
                }
                row = pages.try_next().await?;
            }
        }
    }
}

impl CellSource for CassandraCellResources {
    type Error = CassandraCellStoreError;
}

impl<P: CassandraProjection> CommittedCellSource<P> for CassandraCellResources {
    async fn load(
        &self,
        id: &CollectionId,
        cell: CellRef<'_>,
    ) -> Result<Option<P::Payload>, Self::Error> {
        self.read_committed::<P>(id, cell).await
    }

    async fn load_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &ReadBatch<'_>,
    ) -> Result<CellBuffer<Option<P::Payload>>, Self::Error> {
        self.read_committed_many::<P>(id, section, batch).await
    }

    fn scan<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), Self::Error>> + Send + use<'a, P> {
        self.scan_committed::<P>(id, scan)
    }
}
