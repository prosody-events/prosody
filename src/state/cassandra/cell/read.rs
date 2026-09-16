//! Each projection selects its own statements, so a decoder cannot receive
//! another projection's statements.

use super::decode::{
    BatchRow, BorrowedMarkerRow, PointRow, ScanRow, blob_ttl, decode_body, decode_marker_row,
};
use super::projection::CassandraProjection;
use super::{
    CassandraCellStoreError, CassandraSession, CassandraStoreError, Cell, CellBuffer, CellKey,
    CellKind, CellQueries, CollectionId, Coordinate, Direction, Pk, Scan, ScanEdge, Section,
    Stream, cooperative, try_stream,
};
use crate::state::marker::MarkerState;
use crate::state::store::FetchSchedule;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use futures::future::Either;
use futures::{TryStreamExt, pin_mut};
use scylla::response::PagingState;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;

/// Fetches one projected cell with its durable TTL columns.
pub(super) async fn fetch_point<P: CassandraProjection>(
    session: &CassandraSession,
    queries: &CellQueries,
    id: &CollectionId,
    cell: &CellKey,
) -> Result<Option<PointRow<P>>, CassandraCellStoreError> {
    let pk = Pk::of(id);
    Ok(session
        .session()
        .execute_unpaged(
            &P::statements(queries).point,
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
    queries: &CellQueries,
    id: &CollectionId,
    section: Section,
    coordinates: &[&Coordinate],
) -> Result<CellBuffer<Option<PointRow<P>>>, CassandraCellStoreError> {
    let pk = Pk::of(id);
    let result = session
        .session()
        .execute_unpaged(
            &P::statements(queries).batch,
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
        rows.push(split_batch::<P>(row.map_err(CassandraStoreError::from)?));
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

/// Decodes a point row into its cell and the remaining durable TTL.
pub(super) fn decode_point<P: CassandraProjection>(
    row: PointRow<P>,
) -> Result<(Cell<P>, Option<i32>), CassandraCellStoreError> {
    let (data, prev, encoding, version, event, ttl_data, ttl_prev) = row;
    let cell = decode_body::<P>((data, prev, encoding, version, event))?;
    Ok((cell, blob_ttl(ttl_data, ttl_prev)))
}

/// Separates a batch row's coordinate from its point row.
fn split_batch<P: CassandraProjection>(row: BatchRow<P>) -> (Bytes, PointRow<P>) {
    let (coordinate, data, prev, encoding, version, event, ttl_data, ttl_prev) = row;
    (
        coordinate,
        (data, prev, encoding, version, event, ttl_data, ttl_prev),
    )
}

/// Pages projected rows within the scan bounds.
/// Callers apply commit evidence after decode.
pub(super) fn page<'a, P: CassandraProjection>(
    session: &'a CassandraSession,
    queries: &'a CellQueries,
    collection: &'a CollectionId,
    scan: Scan<'a>,
) -> impl Stream<Item = Result<(CellKey, Cell<P>), CassandraCellStoreError>> + Send + 'a {
    let section = i8::from(scan.section);
    let dir = scan.dir;
    let start = scan.start.cloned();
    let end = scan.end.cloned();
    try_stream! {
        let pk = Pk::of(collection);
        let prepared = P::statements(queries).scan.select(dir, start.kind());
        let start = start.as_ref();
        let values = (
            pk.segment_id, pk.key, pk.state_type, pk.name,
            CellKind::Cell, section, start.anchor(),
        );
        // Scylla rejects a non-positive page size, so this fallback is unreachable.
        let page_size = NonZeroUsize::new(usize::try_from(prepared.get_page_size()).unwrap_or(0))
            .unwrap_or(NonZeroUsize::MIN);
        // Demand below one page fetches a page schedule sized to the demand.
        // Unbounded demand, or demand of a page or more, streams through the
        // driver pager, which reads one page ahead in its own task.
        let rows = match scan.fetch_hint {
            Some(first) if first < page_size => Either::Right(scheduled_rows::<P, _>(
                session, prepared, values, first, page_size,
            )),
            _ => {
                let pager = session
                    .session()
                    .execute_iter(prepared.clone(), values)
                    .await
                    .map_err(CassandraStoreError::from)?;
                Either::Left(
                    pager
                        .rows_stream::<ScanRow<P>>()
                        .map_err(CassandraStoreError::from)?
                        .map_err(CassandraStoreError::from)
                        .map_err(CassandraCellStoreError::from),
                )
            }
        };
        pin_mut!(rows);
        while let Some(row) = cooperative(rows.try_next()).await? {
            let (section, coordinate, data, prev, encoding, version, event) = row;
            let key = CellKey {
                section: Section::new(section),
                coordinate: Coordinate::from_bytes(coordinate),
            };
            let cell = decode_body::<P>((data, prev, encoding, version, event))?;
            if past_end(dir, &key, end.as_ref()) {
                break;
            }
            yield (key, cell);
        }
    }
}

/// Fetches pages one at a time. The first page holds `first` rows and each
/// later page doubles, up to `page_size`.
fn scheduled_rows<P: CassandraProjection, V: SerializeRow + Send + Sync>(
    session: &CassandraSession,
    prepared: &PreparedStatement,
    values: V,
    first: NonZeroUsize,
    page_size: NonZeroUsize,
) -> impl Stream<Item = Result<ScanRow<P>, CassandraCellStoreError>> + Send {
    try_stream! {
        let mut statement = prepared.clone();
        let mut fetch = FetchSchedule::new(Some(first), page_size);
        let mut paging_state = PagingState::start();
        loop {
            statement.set_page_size(i32::try_from(fetch.next().get()).unwrap_or(i32::MAX));
            let (result, response) = session
                .session()
                .execute_single_page(&statement, &values, paging_state)
                .await
                .map_err(CassandraStoreError::from)?;
            let rows = result.into_rows_result().map_err(CassandraStoreError::from)?;
            for row in rows.rows::<ScanRow<P>>().map_err(CassandraStoreError::from)? {
                yield row.map_err(CassandraStoreError::from)?;
            }
            match response.into_paging_control_flow() {
                ControlFlow::Break(()) => break,
                ControlFlow::Continue(next) => paging_state = next,
            }
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
