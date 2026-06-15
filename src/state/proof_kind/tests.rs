//! `CounterKind` semantic property tests: the additive `combine`-then-`apply`
//! fold matches naive op replay, and the fold is idempotent under
//! re-application over the same base (the contract `Lane::stage` relies on for
//! an in-place transient retry).

use super::*;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

/// Generates a counter op, biased so resets are exercised but deltas
/// dominate. Deltas are bounded so a long fold cannot overflow `i64`.
impl Arbitrary for CounterOp {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            reset_first: u8::arbitrary(g) % 4 == 0,
            delta: i64::from(i16::arbitrary(g)),
        }
    }
}

/// Replays ops one at a time over the base — the naive oracle the compacted
/// `combine`-then-`apply` must match. Consumes `ops`.
fn naive_replay(base: Option<Bytes>, ops: Vec<CounterOp>) -> Option<Bytes> {
    ops.into_iter()
        .fold(base, |acc, op| CounterKind::apply(acc, &op))
}

/// Compacts ops left-to-right via `combine` (the dirty store's
/// arrival-order fold), then applies the single combined op. Consumes
/// `ops`.
fn compact_then_apply(base: Option<Bytes>, ops: Vec<CounterOp>) -> Option<Bytes> {
    let mut ops = ops.into_iter();
    match ops.next() {
        None => base,
        Some(first) => {
            let combined = ops.fold(first, CounterKind::combine);
            CounterKind::apply(base, &combined)
        }
    }
}

fn base_bytes(base: i64) -> Bytes {
    Bytes::copy_from_slice(&base.to_le_bytes())
}

/// Invariant: `combine`-then-`apply` equals naive op replay over the same
/// base — the compacted one-op-per-cell dirty store is observationally
/// identical to replaying every op. Covers the non-LWW additive fold and
/// arrival-ordered resets.
#[test]
fn prop_combine_then_apply_equals_naive_replay() {
    fn prop(ops: Vec<CounterOp>, base: i64) -> TestResult {
        let compacted = compact_then_apply(Some(base_bytes(base)), ops.clone());
        let naive = naive_replay(Some(base_bytes(base)), ops);
        TestResult::from_bool(compacted == naive)
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<CounterOp>, i64) -> TestResult);
}

/// Invariant: re-applying the same combined op over the same base is
/// idempotent — the contract `Lane::stage` relies on for an in-place
/// transient retry (the own-event committed read returns the same `prev`,
/// so the stage recomputes the identical write).
#[test]
fn prop_apply_over_same_base_is_idempotent() {
    fn prop(ops: Vec<CounterOp>, base: i64) -> TestResult {
        if ops.is_empty() {
            return TestResult::discard();
        }
        let once = compact_then_apply(Some(base_bytes(base)), ops.clone());
        let twice = compact_then_apply(Some(base_bytes(base)), ops);
        TestResult::from_bool(once == twice)
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<CounterOp>, i64) -> TestResult);
}
