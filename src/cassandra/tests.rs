//! Offline unit/property tests for the Cassandra infrastructure module.
//!
//! These need no cluster: they pin the pure [`chunk_boundaries`] packer with
//! plain numbers. The live batch *execution* path is covered by the cell-store
//! integration tests (one real batch) composed with this proof of the
//! boundaries, so it needs no separate fixture.

use super::chunk_boundaries;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::iter::repeat_n;
use std::ops::Range;

/// Mirrors [`super::CassandraStore::execute_unlogged_batches`]' unit→row
/// flatten: pack per-**unit** weights, then within each chunk expand every unit
/// to its `row_count` rows (each tagged with its unit index). The production
/// flatten is `units[range].flat_map(|u| u.rows.iter())`, so a chunk always
/// holds **whole** units — this is the offline witness of that.
fn flatten_units_to_batches(
    units: &[(u64, usize)],
    max_bytes: u64,
    max_count: usize,
) -> Vec<Vec<usize>> {
    chunk_boundaries(units.iter().map(|(w, _)| *w), max_bytes, max_count)
        .map(|range| {
            (range.start..range.end)
                .flat_map(|unit_idx| repeat_n(unit_idx, units[unit_idx].1))
                .collect()
        })
        .collect()
}

/// The packer yields the **fewest contiguous** chunks within both limits — the
/// "as few batches as possible" invariant, proven offline.
///
/// Three falsifiable clauses over random weights / limits (including weights
/// heavier than `max_bytes`, which exercise the oversized-row arm):
///
/// * **cover + contiguity** — the chunks concatenate to exactly `0..n`, each
///   non-empty, in order (a dropped or duplicated row breaks this);
/// * **within limits** — each chunk has `len ≤ max_count` and `sum ≤
///   max_bytes`, the sole exception being a single oversized row (`len == 1`) —
///   an overfill breaks this;
/// * **maximal** — every chunk but the last is full: appending the next chunk's
///   first row would exceed `max_bytes` *or* the chunk already holds
///   `max_count` rows. This is the minimality witness; a premature flush breaks
///   it. (For a contiguous, order-preserving partition, maximal greedy
///   extension is provably the minimum number of parts — `ceil(sum /
///   max_bytes)` is **not** the right formula, since e.g. `[3,3,3]` with
///   `max_bytes = 5` needs 3 parts, not 2.)
#[quickcheck]
fn chunk_boundaries_are_minimal_and_within_limits(
    sizes: Vec<u32>,
    max_bytes_raw: u16,
    max_count_raw: u8,
) -> TestResult {
    // `+ 1` so both limits are ≥ 1 (a zero limit is not a valid budget).
    let max_bytes = u64::from(max_bytes_raw) + 1;
    let max_count = usize::from(max_count_raw) + 1;
    // `u32` weights against a `u16`-derived byte budget guarantee some weights
    // exceed `max_bytes`, exercising the oversized-row arm.
    let weights: Vec<u64> = sizes.into_iter().map(u64::from).collect();

    let ranges: Vec<Range<usize>> =
        chunk_boundaries(weights.iter().copied(), max_bytes, max_count).collect();

    // Cover + contiguity: ranges concatenate to exactly 0..n, each non-empty.
    let mut next_start = 0_usize;
    for range in &ranges {
        if range.start != next_start || range.end <= range.start {
            return TestResult::failed();
        }
        next_start = range.end;
    }
    if next_start != weights.len() {
        return TestResult::failed();
    }

    let chunk_sum = |range: &Range<usize>| {
        weights[range.start..range.end]
            .iter()
            .copied()
            .fold(0, u64::saturating_add)
    };

    for (i, range) in ranges.iter().enumerate() {
        let len = range.end - range.start;
        let sum = chunk_sum(range);

        // Within limits — `len == 1` is the unavoidable oversized-row exception.
        if len > max_count || (sum > max_bytes && len != 1) {
            return TestResult::failed();
        }

        // Maximal: every chunk but the last could not have taken one more row.
        if let Some(next) = ranges.get(i + 1) {
            let next_weight = weights[next.start];
            let byte_room = sum.saturating_add(next_weight) <= max_bytes;
            let count_room = len < max_count;
            if byte_room && count_room {
                return TestResult::failed();
            }
        }
    }

    TestResult::passed()
}

/// A cell's rows are **never split across batches** — the whole-cell atomicity
/// the per-cell batch unit enforces structurally. Over random per-unit weights
/// (some heavier than `max_bytes`, exercising the oversized arm) and row
/// counts:
///
/// * **whole units** — every row of a given unit lands in exactly one batch,
///   contiguously (a split would place a unit's rows in two batches);
/// * **cover + order** — concatenating the batches reproduces the full row list
///   (unit `0` fully, then unit `1`, …) with none dropped or reordered.
///
/// Generalizes the old single-example multi-chunk smoke: it proves the flatten
/// keeps cells whole for *any* packing, not one hand-picked split.
#[quickcheck]
fn chunk_boundaries_never_split_a_unit(
    units_raw: Vec<(u16, u8)>,
    max_bytes_raw: u16,
    max_count_raw: u8,
) -> TestResult {
    // `+ 1`: both limits ≥ 1, and every unit has ≥ 1 row.
    let max_bytes = u64::from(max_bytes_raw) + 1;
    let max_count = usize::from(max_count_raw) + 1;
    let units: Vec<(u64, usize)> = units_raw
        .into_iter()
        .map(|(w, r)| (u64::from(w), usize::from(r) + 1))
        .collect();

    let batches = flatten_units_to_batches(&units, max_bytes, max_count);

    // Whole units: each unit index occupies one contiguous run in one batch.
    for unit_idx in 0..units.len() {
        let batches_with = batches.iter().filter(|b| b.contains(&unit_idx)).count();
        if batches_with != 1 {
            return TestResult::failed();
        }
    }

    // Cover + order: the batches concatenate to the full flattened row list.
    let expected: Vec<usize> = (0..units.len())
        .flat_map(|i| repeat_n(i, units[i].1))
        .collect();
    let actual: Vec<usize> = batches.into_iter().flatten().collect();
    TestResult::from_bool(actual == expected)
}
