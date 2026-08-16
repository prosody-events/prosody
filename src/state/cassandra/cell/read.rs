use super::*;

/// Maps a raw Cassandra error into the resolving store error, generic only over
/// the oracle error type `E` the caller's stream carries.
pub(super) fn into_store_err<E: Error + 'static>(error: CassandraStoreError) -> CellStoreError<E> {
    ResolveCellError::Store(CassandraCellStoreError::from(error))
}

pub(super) async fn fetch_and_decode_cell(
    session: &CassandraSession,
    statement: &PreparedStatement,
    id: &CollectionId,
    cell: &CellKey,
) -> Result<Option<Cell>, CassandraCellStoreError> {
    let result = fetch_cell_rows_result(session, statement, id, cell).await?;
    result
        .maybe_first_row::<decode::BorrowedRawCellRow<'_>>()
        .map_err(CassandraStoreError::from)?
        .map(decode::try_decode_cell)
        .transpose()
}

pub(super) async fn fetch_and_decode_cell_ttl(
    session: &CassandraSession,
    statement: &PreparedStatement,
    id: &CollectionId,
    cell: &CellKey,
) -> Result<Option<(Cell, Option<i32>)>, CassandraCellStoreError> {
    let result = fetch_cell_rows_result(session, statement, id, cell).await?;
    result
        .maybe_first_row::<decode::BorrowedCellTtlRow<'_>>()
        .map_err(CassandraStoreError::from)?
        .map(decode::try_decode_cell_ttl)
        .transpose()
}

pub(super) async fn fetch_cell_rows_result(
    session: &CassandraSession,
    statement: &PreparedStatement,
    id: &CollectionId,
    cell: &CellKey,
) -> Result<QueryRowsResult, CassandraCellStoreError> {
    let pk = Pk::of(id);
    let result = session
        .session()
        .execute_unpaged(
            statement,
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
        .map_err(CassandraStoreError::from)?;
    result
        .into_rows_result()
        .map_err(CassandraStoreError::from)
        .map_err(CassandraCellStoreError::from)
}

/// Reads and decodes one bounded `IN` query in input resolution order.
/// The result owns no reference to the Scylla response frame.
pub(super) async fn fetch_cells_batch(
    session: &CassandraSession,
    queries: &CellQueries,
    id: &CollectionId,
    section: Section,
    uniques: &[&Coordinate],
) -> Result<CellBuffer<(Coordinate, Cell, Option<i32>)>, CassandraCellStoreError> {
    let pk = Pk::of(id);
    let result = session
        .session()
        .execute_unpaged(
            &queries.read_cells_batch,
            (
                pk.segment_id,
                pk.key,
                pk.state_type,
                pk.name,
                CellKind::Cell,
                i8::from(section),
                uniques,
            ),
        )
        .await
        .map_err(CassandraStoreError::from)?
        .into_rows_result()
        .map_err(CassandraStoreError::from)?;
    // At most one row per unique coordinate, so size once at the IN-list upper
    // bound rather than growing an inline buffer up to `CELL_BATCH`.
    let mut rows: CellBuffer<(Coordinate, decode::BorrowedCellTtlRow<'_>)> =
        SmallVec::with_capacity(uniques.len());
    for row in result
        .rows::<BorrowedKeyedCellTtlRow<'_>>()
        .map_err(CassandraStoreError::from)?
    {
        rows.push(split_keyed_cell_ttl(
            row.map_err(CassandraStoreError::from)?,
        ));
    }

    decode_batch_rows(rows, uniques)
}

pub(super) fn decode_batch_rows(
    mut rows: CellBuffer<(Coordinate, decode::BorrowedCellTtlRow<'_>)>,
    uniques: &[&Coordinate],
) -> Result<CellBuffer<(Coordinate, Cell, Option<i32>)>, CassandraCellStoreError> {
    let mut out = CellBuffer::with_capacity(uniques.len());
    for &coordinate in uniques {
        let Some(pos) = rows.iter().position(|(found, _)| found == coordinate) else {
            continue;
        };
        let (coordinate, row) = rows.swap_remove(pos);
        let (cell, ttl) = decode::try_decode_cell_ttl(row)?;
        out.push((coordinate, cell, ttl));
    }
    Ok(out)
}

/// The shared section-scan row pager. It opens the prepared statement for the
/// scan bounds (the six-arm `(dir, start)` selection), builds the
/// [`FramedKeyedCellRow`] stream, and yields each decoded `(CellKey, Cell)`
/// with the in-code `past_end` cutoff applied. It applies no limit and no
/// resolution. The limit counts present cells after projection, so each
/// consumer keeps it in its own loop. Two callers consume this. The owner scan
/// ([`CassandraStore::scan_inner`]) then applies `peek_read`; the reader scan
/// ([`CassandraCellResources::scan_committed`]) then applies
/// `project_committed`. Sharing this pager keeps their physical paging from
/// drifting apart. Each `try_next` is wrapped in [`cooperative`] so a drain of
/// ready rows yields to the runtime every ~128 items.
pub(super) fn page_cells<'a>(
    session: &'a CassandraSession,
    queries: &'a CellQueries,
    collection: &'a CollectionId,
    scan: Scan<'a>,
) -> impl Stream<Item = Result<(CellKey, Cell), CassandraCellStoreError>> + Send + 'a {
    let section = i8::from(scan.section);
    let dir = scan.dir;
    // Both edges are held as owned `Coordinate`s across the stream's awaits —
    // O(1) refcount bumps (`Coordinate` is `Bytes`), never byte copies.
    let start = scan.start.cloned();
    let end = scan.end.cloned();
    try_stream! {
        let pk = Pk::of(collection);
        // The section-prefix bind values every scan statement shares.
        let prefix = (pk.segment_id, pk.key, pk.state_type, pk.name, CellKind::Cell, section);
        let (seg, key, st, name, cell_kind, sect) = prefix;
        // Statement selection and binding are one match. A bounded arm appends
        // the anchor coordinate for a 7-tuple. An `Unbounded` arm binds only the
        // section prefix for a 6-tuple. Those are distinct Rust types, so the
        // pager must open inside each arm.
        let pager = match (dir, start.as_ref()) {
            (Direction::Forward, ScanEdge::Included(c)) => {
                session.session().execute_iter(queries.scan_forward_incl.clone(),
                    (seg, key, st, name, cell_kind, sect, c)).await
            }
            (Direction::Forward, ScanEdge::Excluded(c)) => {
                session.session().execute_iter(queries.scan_forward_excl.clone(),
                    (seg, key, st, name, cell_kind, sect, c)).await
            }
            (Direction::Backward, ScanEdge::Included(c)) => {
                session.session().execute_iter(queries.scan_backward_incl.clone(),
                    (seg, key, st, name, cell_kind, sect, c)).await
            }
            (Direction::Backward, ScanEdge::Excluded(c)) => {
                session.session().execute_iter(queries.scan_backward_excl.clone(),
                    (seg, key, st, name, cell_kind, sect, c)).await
            }
            (Direction::Forward, ScanEdge::Unbounded) => {
                session.session().execute_iter(queries.scan_forward_all.clone(), prefix).await
            }
            (Direction::Backward, ScanEdge::Unbounded) => {
                session.session().execute_iter(queries.scan_backward_all.clone(), prefix).await
            }
        };
        let stream = pager
            .map_err(CassandraStoreError::from)?
            .rows_stream::<FramedKeyedCellRow>()
            .map_err(CassandraStoreError::from)?;
        pin_mut!(stream);
        while let Some(row) = cooperative(stream.try_next())
            .await
            .map_err(CassandraStoreError::from)?
        {
            let (key, cell) = decode::try_decode_keyed_cell(row)?;
            if past_end(dir, &key, end.as_ref()) {
                break;
            }
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
