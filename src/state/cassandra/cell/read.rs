use super::decode::{
    BatchRow, Body, BorrowedMarkerRow, PointRow, ScanRow, blob_ttl, decode_body, decode_marker_row,
};
use super::projection::CassandraProjection;
use super::queries::ReadStatements;
use super::{
    CassandraCellStoreError, CassandraSession, CassandraStoreError, Cell, CellBuffer, CellKey,
    CellKind, CellQueries, CollectionId, Coordinate, Direction, Pk, Scan, ScanEdge, Section,
    Stream, TryStreamExt, cooperative, pin_mut, try_stream,
};
use crate::state::marker::MarkerState;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;

/// Fetches one projected cell with its durable TTL columns.
pub(super) async fn fetch_point<P: CassandraProjection>(
    session: &CassandraSession,
    statements: &ReadStatements,
    id: &CollectionId,
    cell: &CellKey,
) -> Result<Option<PointRow<P>>, CassandraCellStoreError> {
    let pk = Pk::of(id);
    Ok(session
        .session()
        .execute_unpaged(
            &statements.point,
            (
                pk.segment_id,
                pk.key,
                pk.state_type,
                pk.name,
                CellKind::Cell,
                i8::from(cell.section),
                &cell.coordinate,
            ),
        )
        .await
        .map_err(CassandraStoreError::from)?
        .into_rows_result()
        .map_err(CassandraStoreError::from)?
        .maybe_first_row::<PointRow<P>>()
        .map_err(CassandraStoreError::from)?)
}

/// Fetches one batch and preserves input order before semantic decode.
pub(super) async fn fetch_batch<P: CassandraProjection>(
    session: &CassandraSession,
    statements: &ReadStatements,
    id: &CollectionId,
    section: Section,
    coordinates: &[&Coordinate],
) -> Result<CellBuffer<Option<PointRow<P>>>, CassandraCellStoreError> {
    let pk = Pk::of(id);
    let result = session
        .session()
        .execute_unpaged(
            &statements.batch,
            (
                pk.segment_id,
                pk.key,
                pk.state_type,
                pk.name,
                CellKind::Cell,
                i8::from(section),
                coordinates,
            ),
        )
        .await
        .map_err(CassandraStoreError::from)?
        .into_rows_result()
        .map_err(CassandraStoreError::from)?;
    let mut rows = CellBuffer::with_capacity(coordinates.len());
    for row in result
        .rows::<BatchRow<P>>()
        .map_err(CassandraStoreError::from)?
    {
        let (coordinate, data, prev, encoding, version, event, ttl_data, ttl_prev) =
            row.map_err(CassandraStoreError::from)?;
        rows.push((
            coordinate,
            (data, prev, encoding, version, event, ttl_data, ttl_prev),
        ));
    }
    Ok(match_rows_to_coordinates(rows, coordinates))
}

pub(super) fn match_rows_to_coordinates<Row>(
    mut rows: CellBuffer<(Bytes, Row)>,
    coordinates: &[&Coordinate],
) -> CellBuffer<Option<Row>> {
    let mut out = CellBuffer::with_capacity(coordinates.len());
    for &coordinate in coordinates {
        let Some(pos) = rows
            .iter()
            .position(|(found, _)| found.as_ref() == coordinate.as_bytes())
        else {
            out.push(None);
            continue;
        };
        let (_, row) = rows.swap_remove(pos);
        out.push(Some(row));
    }
    out
}

/// Separates a point row's body and co-expiry before semantic decode.
pub(super) fn split_point<P: CassandraProjection>(row: PointRow<P>) -> (Body<P>, Option<i32>) {
    let (data, prev, encoding, version, event, ttl_data, ttl_prev) = row;
    (
        (data, prev, encoding, version, event),
        blob_ttl(ttl_data, ttl_prev),
    )
}

/// Pages projected rows within the scan bounds.
/// Callers apply commit evidence and limits after decode.
pub(super) fn page<'a, P: CassandraProjection>(
    session: &'a CassandraSession,
    statements: &'a ReadStatements,
    collection: &'a CollectionId,
    scan: Scan<'a>,
) -> impl Stream<Item = Result<(CellKey, Cell<P>), CassandraCellStoreError>> + Send + 'a {
    let section = i8::from(scan.section);
    let dir = scan.dir;
    let start = scan.start.cloned();
    let end = scan.end.cloned();
    try_stream! {
        let pk = Pk::of(collection);
        let statement = &statements.scan[dir as usize][start.kind() as usize];
        let pager = session.session().execute_iter(statement.clone(),
            (pk.segment_id, pk.key, pk.state_type, pk.name, CellKind::Cell, section, start.as_ref().anchor()),
        ).await.map_err(CassandraStoreError::from)?;
        let stream = pager.rows_stream::<ScanRow<P>>().map_err(CassandraStoreError::from)?;
        pin_mut!(stream);
        while let Some(row) = cooperative(stream.try_next()).await.map_err(CassandraStoreError::from)? {
            let (section, coordinate, data, prev, encoding, version, event) = row;
            let key = CellKey { section: Section::new(section), coordinate: Coordinate::from_bytes(coordinate) };
            let cell = decode_body::<P>((data, prev, encoding, version, event))?;
            if past_end(dir, &key, end.as_ref()) { break; }
            yield (key, cell);
        }
    }
}

/// Whether `key` has walked past the in-code `end` edge for the scan
/// direction. An `Excluded` edge also stops *on* the endpoint (the exclusive
/// variant for exclusive scan anchors); an `Unbounded` end never stops the
/// walk (the section-only fallback).
pub(super) fn past_end(dir: Direction, key: &CellKey, end: ScanEdge<&Coordinate>) -> bool {
    let coordinate = key.coordinate.as_bytes();
    match (dir, end) {
        (Direction::Forward, ScanEdge::Included(end)) => coordinate > end.as_bytes(),
        (Direction::Forward, ScanEdge::Excluded(end)) => coordinate >= end.as_bytes(),
        (Direction::Backward, ScanEdge::Included(end)) => coordinate < end.as_bytes(),
        (Direction::Backward, ScanEdge::Excluded(end)) => coordinate <= end.as_bytes(),
        (_, ScanEdge::Unbounded) => false,
    }
}

/// Reads the whole marker slice. Both marker verbs use this decoder.
pub(super) async fn fetch_marker_state(
    session: &CassandraSession,
    queries: &CellQueries,
    id: &CollectionId,
    legacy_ttl: Option<CompactDuration>,
) -> Result<MarkerState, CassandraCellStoreError> {
    let pk = Pk::of(id);
    let result = session
        .session()
        .execute_unpaged(
            &queries.cells.marker_state,
            (
                pk.segment_id,
                pk.key,
                pk.state_type,
                pk.name,
                CellKind::Marker,
            ),
        )
        .await
        .map_err(CassandraStoreError::from)?
        .into_rows_result()
        .map_err(CassandraStoreError::from)?;
    let mut state = MarkerState::default();
    for row in result
        .rows::<BorrowedMarkerRow<'_>>()
        .map_err(CassandraStoreError::from)?
    {
        decode_marker_row(
            &mut state,
            row.map_err(CassandraStoreError::from)?,
            legacy_ttl,
        )?;
    }
    Ok(state)
}
