use super::{
    BatchUnit, CellAddr, CellBatchRow, CellKind, CellQueries, GapBetweenRow, GapEdgeRow,
    GapSectionRow, KeyRow, PER_STATEMENT_OVERHEAD, Pk, RowShape, SectionClear, smallvec,
};

/// The number of gap rows needed to erase `clears` while excluding survivors.
pub(super) fn gap_count(clears: &[SectionClear]) -> usize {
    clears.iter().map(|clear| clear.survivors().len() + 1).sum()
}

/// Appends one bounded batch unit per gap around each cleared section's sorted,
/// deduplicated survivors (`< k₁`, `(k₁,k₂)`, …, `> kₙ`; one whole-section
/// delete when empty). Coordinates borrow from the frozen [`SectionClear`]s.
pub(super) fn extend_gap_units<'u>(
    units: &mut Vec<BatchUnit<CellBatchRow<'u>>>,
    queries: &'u CellQueries,
    pk: Pk<'u>,
    clears: &'u [SectionClear],
) {
    for clear in clears {
        let section = i8::from(clear.section());
        let survivors = clear.survivors();
        let (Some(first), Some(last)) = (survivors.first(), survivors.last()) else {
            units.push(BatchUnit::new(
                PER_STATEMENT_OVERHEAD,
                smallvec![CellBatchRow {
                    statement: &queries.gap_section,
                    row: RowShape::GapSection(GapSectionRow { pk, section }),
                }],
            ));
            continue;
        };
        units.push(BatchUnit::new(
            first.as_bytes().len() as u64 + PER_STATEMENT_OVERHEAD,
            smallvec![CellBatchRow {
                statement: &queries.gap_below,
                row: RowShape::GapEdge(GapEdgeRow {
                    pk,
                    section,
                    coordinate: first.as_bytes(),
                }),
            }],
        ));
        for pair in survivors.windows(2) {
            units.push(BatchUnit::new(
                (pair[0].as_bytes().len() + pair[1].as_bytes().len()) as u64
                    + PER_STATEMENT_OVERHEAD,
                smallvec![CellBatchRow {
                    statement: &queries.gap_between,
                    row: RowShape::GapBetween(GapBetweenRow {
                        pk,
                        section,
                        low: pair[0].as_bytes(),
                        high: pair[1].as_bytes(),
                    }),
                }],
            ));
        }
        units.push(BatchUnit::new(
            last.as_bytes().len() as u64 + PER_STATEMENT_OVERHEAD,
            smallvec![CellBatchRow {
                statement: &queries.gap_above,
                row: RowShape::GapEdge(GapEdgeRow {
                    pk,
                    section,
                    coordinate: last.as_bytes(),
                }),
            }],
        ));
    }
}

/// The one-row batch unit deleting a collection's event-marker row at its
/// fixed address, appended last by
/// [`super::CassandraStore::issue_marker_last`], the shared tail of both settle
/// verbs.
pub(super) fn marker_delete_unit<'u>(
    pk: Pk<'u>,
    queries: &'u CellQueries,
) -> BatchUnit<CellBatchRow<'u>> {
    BatchUnit::new(
        PER_STATEMENT_OVERHEAD,
        smallvec![CellBatchRow {
            statement: &queries.marker_delete,
            row: RowShape::Key(KeyRow {
                kind: CellKind::Marker,
                addr: CellAddr::marker(pk),
            }),
        }],
    )
}

/// Returns the boundary between settle's cell/gap work and its final marker
/// delete. Everything returns in the first slice when one batch can carry all
/// rows atomically; otherwise the marker is the second slice's sole unit, so it
/// is issued only after every recovery-relevant mutation has completed.
///
/// **Precondition:** the marker delete is the LAST unit — the split is
/// positional (`units.len() - 1`), so a marker placed elsewhere, or a caller
/// awaiting the tail before the prefix, would issue the marker before the
/// recovery-relevant rows.
/// [`super::CassandraStore::issue_marker_last`] owns that ordering;
/// this function only decides where the split falls.
pub(super) fn marker_last_split<R>(
    units: &[BatchUnit<R>],
    max_bytes: u64,
    max_count: usize,
) -> usize {
    if fits_one_batch(units.iter().map(BatchUnit::weight), max_bytes, max_count) {
        units.len()
    } else {
        units.len().saturating_sub(1)
    }
}

/// Whether `weights` pack into a **single** batch under the byte and count
/// budgets: `chunk_boundaries` provably yields one chunk iff the weight sum
/// fits `max_bytes` and the count fits `max_count`. This one predicate
/// underlies both marker-ordering decisions — the stage's marker-FIRST choice
/// (`write_provisional`) and the settle's marker-LAST split
/// ([`marker_last_split`]): when everything fits one atomic batch the marker
/// rides along; otherwise it is isolated to its own batch — issued first at
/// stage, last at settle.
pub(super) fn fits_one_batch(
    weights: impl Iterator<Item = u64>,
    max_bytes: u64,
    max_count: usize,
) -> bool {
    let (mut total, mut count) = (0_u64, 0_usize);
    for weight in weights {
        total = total.saturating_add(weight);
        count += 1;
    }
    total <= max_bytes && count <= max_count
}
