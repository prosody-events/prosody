//! The `Bound`-algebra centerpiece: an [`IntervalSet`] stays sorted,
//! pairwise-disjoint and well-formed under any `cover`/`punch` sequence, and
//! [`IntervalSet::query`] returns ordered, disjoint pieces that exactly tile
//! the request — checked against a plainly-correct set-of-points model.
//!
//! Coordinates are drawn from a small pool, so every interval endpoint is a
//! pool point and the points model is a faithful oracle (continuum gaps between
//! adjacent pool points serve nothing and are not point-observable). A
//! `Bound`-polarity bug — perma-fragmentation, or merging across a real hole —
//! falsifies one of these assertions.

use super::*;
use color_eyre::eyre::{Result, eyre};
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::BTreeSet;

/// Coordinate pool: small so covers overlap, punches land inside intervals, and
/// complementary endpoints recur.
const POOL: u8 = 6;

/// A coordinate from the pool.
fn point(b: u8) -> Coordinate {
    Coordinate::from_bytes(vec![b % POOL])
}

/// A generated bound: `Unbounded`, or `Included`/`Excluded` of a pool point.
#[derive(Clone, Copy, Debug)]
enum GenBound {
    Unbounded,
    Included(u8),
    Excluded(u8),
}

impl GenBound {
    fn to_bound(self) -> Bound<Coordinate> {
        match self {
            Self::Unbounded => Bound::Unbounded,
            Self::Included(b) => Bound::Included(point(b)),
            Self::Excluded(b) => Bound::Excluded(point(b)),
        }
    }
}

impl Arbitrary for GenBound {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => Self::Unbounded,
            1 => Self::Included(u8::arbitrary(g)),
            _ => Self::Excluded(u8::arbitrary(g)),
        }
    }
}

/// A generated interval (low, high); may be empty, in which case it is ignored.
#[derive(Clone, Copy, Debug)]
struct GenInterval {
    lo: GenBound,
    hi: GenBound,
}

impl GenInterval {
    fn build(self) -> Option<Interval> {
        Interval::new(self.lo.to_bound(), self.hi.to_bound())
    }
}

impl Arbitrary for GenInterval {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            lo: GenBound::arbitrary(g),
            hi: GenBound::arbitrary(g),
        }
    }
}

/// One mutation of the set.
#[derive(Clone, Copy, Debug)]
enum CovOp {
    Cover(GenInterval),
    Punch(u8),
}

impl Arbitrary for CovOp {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Cover(GenInterval::arbitrary(g))
        } else {
            Self::Punch(u8::arbitrary(g))
        }
    }
}

/// A shrinkable op sequence plus the request intervals to query after each op.
#[derive(Clone, Debug)]
struct Script {
    ops: Vec<CovOp>,
    queries: Vec<GenInterval>,
}

impl Arbitrary for Script {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: Vec::arbitrary(g).into_iter().take(40).collect(),
            queries: Vec::arbitrary(g).into_iter().take(6).collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let queries = self.queries.clone();
        Box::new(self.ops.shrink().map(move |ops| Self {
            ops,
            queries: queries.clone(),
        }))
    }
}

/// Whether the set's intervals stay sorted by low bound, pairwise non-touching
/// (a real hole between each), and individually non-empty.
fn set_well_formed(set: &IntervalSet) -> bool {
    set.intervals.iter().all(|iv| is_nonempty(&iv.lo, &iv.hi))
        && set.intervals.windows(2).all(|w| {
            cmp_low(&w[0].lo, &w[1].lo) == Ordering::Less && !high_ge_low(&w[0].hi, &w[1].lo)
        })
}

/// Whether `coordinate` is covered by some interval in the set.
fn set_covers(set: &IntervalSet, coordinate: &Coordinate) -> bool {
    set.intervals.iter().any(|iv| iv.contains(coordinate))
}

/// Asserts `query(request)` returns ordered, disjoint pieces that exactly tile
/// the request, and that a Covered piece's points are exactly the model-covered
/// points within the request.
/// The interval underlying a piece, regardless of kind.
fn interval_of(piece: &Piece) -> &Interval {
    match piece {
        Piece::Covered(iv) | Piece::Gap(iv) => iv,
    }
}

fn query_partitions_exactly(set: &IntervalSet, model: &BTreeSet<u8>, request: &Interval) -> bool {
    let pieces = set.query(request);
    if pieces.is_empty() {
        return false;
    }
    // Tiling: first low == request low, last high == request high, and each
    // piece picks up exactly where the previous left off (complementary, so no
    // gap and no overlap).
    if cmp_low(&interval_of(&pieces[0]).lo, &request.lo) != Ordering::Equal {
        return false;
    }
    let last = interval_of(&pieces[pieces.len() - 1]);
    if cmp_high(&last.hi, &request.hi) != Ordering::Equal {
        return false;
    }
    for w in pieces.windows(2) {
        let prev = interval_of(&w[0]);
        let next = interval_of(&w[1]);
        if next.lo != complement_high(&prev.hi) {
            return false;
        }
    }
    // Membership: every pool point inside the request lands in exactly one
    // piece, Covered iff the model covers it.
    for b in 0..POOL {
        let c = point(b);
        if !request.contains(&c) {
            continue;
        }
        let hits: Vec<&Piece> = pieces
            .iter()
            .filter(|p| interval_of(p).contains(&c))
            .collect();
        if hits.len() != 1 {
            return false;
        }
        let in_covered = matches!(hits[0], Piece::Covered(_));
        if in_covered != model.contains(&b) {
            return false;
        }
    }
    true
}

/// The centerpiece: random cover/punch keeps the set well-formed and faithful
/// to the points model, and every query exactly partitions its request.
#[test]
fn prop_interval_set_bound_algebra() {
    fn prop(script: Script) -> TestResult {
        let mut set = IntervalSet::default();
        let mut model: BTreeSet<u8> = BTreeSet::new();

        for op in script.ops {
            match op {
                CovOp::Cover(gi) => {
                    let Some(interval) = gi.build() else {
                        continue;
                    };
                    set.cover(interval.clone());
                    for b in 0..POOL {
                        if interval.contains(&point(b)) {
                            model.insert(b);
                        }
                    }
                }
                CovOp::Punch(b) => {
                    let b = b % POOL;
                    set.punch(&point(b));
                    model.remove(&b);
                }
            }

            if !set_well_formed(&set) {
                return TestResult::error("interval set not sorted/disjoint/well-formed");
            }
            // The set represents exactly the model's covered points.
            for b in 0..POOL {
                if set_covers(&set, &point(b)) != model.contains(&b) {
                    return TestResult::error("set point-coverage diverged from model");
                }
            }
            for gi in &script.queries {
                if let Some(request) = gi.build()
                    && !query_partitions_exactly(&set, &model, &request)
                {
                    return TestResult::error("query did not exactly partition the request");
                }
            }
        }
        TestResult::passed()
    }

    QuickCheck::new().quickcheck(prop as fn(Script) -> TestResult);
}

/// A re-filled punch heals the split: `[a, Excluded(X))` then `[Included(X),
/// b]` rejoin to one interval — but a real hole `[a, Excluded(X))` /
/// `[Excluded(X), b]` stays two. Pins the `Bound`-polarity merge rule the
/// property relies on.
#[test]
fn complementary_endpoints_merge_but_holes_do_not() -> Result<()> {
    let x = point(3);
    let a = point(1);
    let b = point(5);
    let iv = |lo, hi| Interval::new(lo, hi).ok_or_else(|| eyre!("non-empty interval"));

    let mut healed = IntervalSet::default();
    healed.cover(iv(Bound::Included(a.clone()), Bound::Excluded(x.clone()))?);
    healed.cover(iv(Bound::Included(x.clone()), Bound::Included(b.clone()))?);
    assert_eq!(
        healed.intervals.len(),
        1,
        "complementary endpoints must rejoin"
    );

    let mut holed = IntervalSet::default();
    holed.cover(iv(Bound::Included(a), Bound::Excluded(x.clone()))?);
    holed.cover(iv(Bound::Excluded(x), Bound::Included(b))?);
    assert_eq!(holed.intervals.len(), 2, "a real hole at X must not merge");
    Ok(())
}
