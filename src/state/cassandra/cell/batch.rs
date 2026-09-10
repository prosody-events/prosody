use super::{
    BatchUnit, CellAddr, CellBatchRow, CellKind, CellQueries, GapBetweenRow, GapEdgeRow,
    GapSectionRow, KeyRow, PER_STATEMENT_OVERHEAD, Pk, RowShape, SectionClear, smallvec,
};
use crate::cassandra::chunk_boundaries;
use crate::state::marker::MarkerRow;
use std::iter;
use std::ops::Range;

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

/// Deletes Staged at its fixed address in one batch unit.
/// [`super::CassandraStore::issue_markers`] appends this unit last for both
/// settle operations.
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
                addr: CellAddr::marker(pk, MarkerRow::Staged),
            }),
        }],
    )
}

/// Builds the promote or abort phases under the batch budgets.
/// The leading evidence and final Staged delete remain outside split middle
/// chunks.
pub(in crate::state) fn settle_batches<R>(
    leading: Option<BatchUnit<R>>,
    mut middle: Vec<BatchUnit<R>>,
    trailing: BatchUnit<R>,
    max_bytes: u64,
    max_count: usize,
) -> (Vec<BatchUnit<R>>, [Range<usize>; 3]) {
    let start = usize::from(leading.is_some());
    if let Some(leading) = leading {
        middle.insert(0, leading);
    }
    middle.push(trailing);
    let end = middle.len();
    let phases = if fits_one_batch(middle.iter().map(BatchUnit::weight), max_bytes, max_count) {
        [0..end, end..end, end..end]
    } else {
        [0..start, start..end - 1, end - 1..end]
    };
    (middle, phases)
}

/// Splits cells into ranges with space for the marker in every batch.
/// The caller must include the marker when it executes each range.
/// No cells produces one marker-only batch.
pub(in crate::state) fn stage_batches<R>(
    marker: &BatchUnit<R>,
    cells: &[BatchUnit<R>],
    max_bytes: u64,
    max_count: usize,
) -> impl Iterator<Item = Range<usize>> {
    let cell_bytes = max_bytes.saturating_sub(marker.weight());
    let cell_limit = if marker.weight() > max_bytes {
        // An oversized marker permits only one cell, even when cells weigh zero.
        1
    } else {
        max_count.saturating_sub(1)
    };

    let ranges = chunk_boundaries(cells.iter().map(BatchUnit::weight), cell_bytes, cell_limit);

    let marker_only = cells.is_empty().then_some(0..0);
    marker_only.into_iter().chain(ranges)
}

/// Binds the marker to every atomic stage mutation.
pub(in crate::state) fn stage_chunk<'u, R>(
    marker: &'u BatchUnit<R>,
    cells: &'u [BatchUnit<R>],
    range: Range<usize>,
) -> impl Iterator<Item = &'u BatchUnit<R>> {
    iter::once(marker).chain(cells[range].iter())
}

/// Reports whether all rows fit one atomic batch.
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
