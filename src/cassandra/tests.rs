//! Offline unit/property tests for the Cassandra infrastructure module.
//!
//! These need no cluster: they pin the pure [`chunk_boundaries`] packer with
//! plain numbers. The live batch *execution* path is covered by the cell-store
//! integration tests (one real batch) composed with this proof of the
//! boundaries, so it needs no separate fixture.

use super::chunk_boundaries;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::ops::Range;

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
