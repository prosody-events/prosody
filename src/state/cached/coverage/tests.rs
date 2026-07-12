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
use std::iter::empty;

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
    /// The whole-section punch (a committed section clear's Cov-Clr eviction).
    PunchSection,
}

impl Arbitrary for CovOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 | 1 => Self::Cover(GenInterval::arbitrary(g)),
            2 => Self::Punch(u8::arbitrary(g)),
            _ => Self::PunchSection,
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
    set.intervals.iter().all(|(lo, hi)| is_nonempty(&lo.0, hi))
        && set.intervals.iter().zip(set.intervals.iter().skip(1)).all(
            |((lo_a, hi_a), (lo_b, _))| {
                cmp_low(&lo_a.0, &lo_b.0) == Ordering::Less && !high_ge_low(hi_a, &lo_b.0)
            },
        )
}

/// Whether `coordinate` is covered by some interval in the set.
fn set_covers(set: &IntervalSet, coordinate: &Coordinate) -> bool {
    set.intervals
        .iter()
        .any(|(lo, hi)| interval(lo, hi).contains(coordinate))
}

/// The interval underlying a piece, regardless of kind.
fn interval_of(piece: &Piece) -> &Interval {
    match piece {
        Piece::Covered(iv) | Piece::Gap(iv) => iv,
    }
}

/// Asserts `query(request)` returns ordered, disjoint pieces that exactly tile
/// the request, and that a Covered piece's points are exactly the model-covered
/// points within the request.
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
        if next.lo != complement(&prev.hi) {
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
                CovOp::PunchSection => {
                    set.clear();
                    model.clear();
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

/// One batch mutation of a live [`Coverage`] (as opposed to the bare
/// [`IntervalSet`] ops above): a ranged cover, a settle batch's point covers,
/// or a batch punch.
#[derive(Clone, Debug)]
enum LiveOp {
    Cover(GenInterval),
    CoverPoints(Vec<u8>),
    PunchMany(Vec<u8>),
    /// The whole-section punch ([`Coverage::punch_section`]).
    PunchSection,
}

impl Arbitrary for LiveOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let coords = |g: &mut Gen| -> Vec<u8> {
            (0..usize::arbitrary(g) % 4)
                .map(|_| u8::arbitrary(g))
                .collect()
        };
        match u8::arbitrary(g) % 4 {
            0 => Self::Cover(GenInterval::arbitrary(g)),
            1 => Self::CoverPoints(coords(g)),
            2 => Self::PunchMany(coords(g)),
            _ => Self::PunchSection,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            Self::Cover(_) | Self::PunchSection => Box::new(empty()),
            Self::CoverPoints(coords) => Box::new(coords.shrink().map(Self::CoverPoints)),
            Self::PunchMany(coords) => Box::new(coords.shrink().map(Self::PunchMany)),
        }
    }
}

/// **Memo coherence (warmth invariance).** After any op sequence, a warm
/// [`Coverage`] (answering from its memoized interval sets) and a cold one
/// freshly built over the same fjall keyspace (forced to reload the durable
/// spill) must agree with each other and with a plain set-of-points model on
/// every pool point. A memo that went stale against the fjall spill — a
/// mutation that updated one but not the other, or a batch op that diverged
/// from N singles — falsifies the triple equality.
#[test]
fn prop_coverage_memo_matches_durable_spill() {
    use crate::state::fjall::test_db;
    use crate::state::tests::support::fresh_collection;
    use crate::test_util::TEST_RUNTIME;

    fn property(ops: Vec<LiveOp>) -> Result<bool> {
        TEST_RUNTIME.block_on(async move {
            let fjall = test_db::cache("cover_memo")?;
            let warm = Coverage::new(fjall.clone());
            let id = fresh_collection("memo")?;
            let section = Section::new(0);
            let mut model: BTreeSet<u8> = BTreeSet::new();

            for (index, op) in ops.iter().enumerate() {
                match op {
                    LiveOp::Cover(gi) => {
                        let Some(interval) = gi.build() else {
                            continue;
                        };
                        warm.cover(&id, section, interval.clone()).await?;
                        for b in 0..POOL {
                            if interval.contains(&point(b)) {
                                model.insert(b);
                            }
                        }
                    }
                    LiveOp::CoverPoints(coords) => {
                        let points: Vec<Coordinate> = coords.iter().map(|&b| point(b)).collect();
                        warm.cover_points(&id, section, &points).await?;
                        model.extend(coords.iter().map(|&b| b % POOL));
                    }
                    LiveOp::PunchMany(coords) => {
                        let points: Vec<Coordinate> = coords.iter().map(|&b| point(b)).collect();
                        warm.punch_many(&id, section, &points).await?;
                        for b in coords {
                            model.remove(&(b % POOL));
                        }
                    }
                    LiveOp::PunchSection => {
                        warm.punch_section(&id, section).await?;
                        model.clear();
                    }
                }

                // A cold instance reloads the durable spill (empty memo).
                let cold = Coverage::new(fjall.clone());
                for b in 0..POOL {
                    let want = model.contains(&b);
                    if warm.covers(&id, section, &point(b)).await? != want {
                        return Err(eyre!("op {index}: warm (memoized) diverged at {b}"));
                    }
                    if cold.covers(&id, section, &point(b)).await? != want {
                        return Err(eyre!("op {index}: durable spill diverged at {b}"));
                    }
                }
            }
            Ok(true)
        })
    }
    QuickCheck::new().quickcheck(property as fn(Vec<LiveOp>) -> Result<bool>);
}

/// A `punch` racing a `cover` of a disjoint interval in the same section must
/// never be lost (the mutation-atomicity note on [`Coverage`]). Both are
/// whole-section load→mutate→store cycles: unserialized, the cover's store —
/// computed from a load that still contained the punched coordinate — can land
/// after the punch's store and resurrect its coverage, and a resurrected
/// coordinate is served verbatim for the rest of the assignment. Per-key event
/// serialization does not exclude the race: one handler can hold two `&self`
/// session ops concurrently polled (`join!`). Each round re-covers, races the
/// two mutations on the multi-threaded runtime, and asserts the punch stuck.
#[test]
fn concurrent_cover_never_resurrects_a_punched_coordinate() -> Result<()> {
    use crate::state::fjall::test_db;
    use crate::state::tests::support::fresh_collection;
    use crate::test_util::TEST_RUNTIME;
    use std::env;
    use std::slice::from_ref;

    /// A raw single-byte coordinate (not pool-folded like [`point`]).
    fn coord(b: u8) -> Coordinate {
        Coordinate::from_bytes(vec![b])
    }

    // Race repetitions: each round is one full interleaving opportunity.
    // Sourced from `QUICKCHECK_TESTS` so CI can crank the race schedule; the
    // dev default matches the in-memory property-test count.
    let rounds: u32 = env::var("QUICKCHECK_TESTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    TEST_RUNTIME.block_on(async {
        let coverage = Coverage::new(test_db::cache("cover_race")?);
        let id = fresh_collection("race")?;
        let section = Section::new(0);
        let iv = |a: u8, b: u8| {
            Interval::new(Bound::Included(coord(a)), Bound::Included(coord(b)))
                .ok_or_else(|| eyre!("non-empty interval"))
        };
        for round in 0..rounds {
            // Re-cover the punch target, then race the punch against a cover
            // of a disjoint interval in the same section.
            coverage.cover(&id, section, iv(0, 10)?).await?;
            let five = coord(5);
            let (punched, covered) = tokio::join!(
                coverage.punch_many(&id, section, from_ref(&five)),
                coverage.cover(&id, section, iv(20, 30)?),
            );
            punched?;
            covered?;
            assert!(
                !coverage.covers(&id, section, &coord(5)).await?,
                "round {round}: the concurrent cover resurrected the punched coordinate"
            );
            assert!(
                coverage.covers(&id, section, &coord(25)).await?,
                "round {round}: the concurrent cover was lost"
            );
        }
        Ok(())
    })
}

/// **`GAP_COVER_STRIDE` frontier invariant.** A gap scan dropped after
/// consuming `consumed` of `total` uncovered cells persists coverage only for
/// the stride-aligned prefix it completed: a re-scan of that prefix issues zero
/// lower scans, while a full re-scan still yields every cell (the tail past the
/// frontier falls through to the lower store). Under-covers on an early drop,
/// never over-covers. Generalizes the single fixed `(total, consumed)` scenario
/// the `gap_frontier_persists_mid_stream_and_never_over_covers` smoke pins.
#[test]
fn prop_gap_cover_stride_frontier() {
    use super::super::{Cached, GAP_COVER_STRIDE};
    use crate::state::cell_key::{CellKey, Direction, Scan, ScanEdge};
    use crate::state::event_ref::EventRef;
    use crate::state::fjall::test_db;
    use crate::state::identity::CollectionRef;
    use crate::state::memory::{MemoryCellStore, MemoryCells};
    use crate::state::registry::CollectionDefRegistry;
    use crate::state::store::CellStore;
    use crate::state::tests::cell_suite::{ScriptedOracle, bytes};
    use crate::state::tests::support::{CountingCellStore, fresh_collection};
    use crate::test_util::TEST_RUNTIME;
    use ::bytes::Bytes;
    use futures::StreamExt;
    use std::sync::Arc;
    use uuid::Uuid;

    /// Drains up to `take` cells of a forward section-0 scan over `[0, end]`,
    /// returning the count yielded (`take` bounds an early drop).
    async fn drain<L: CellStore>(
        cached: &Cached<L>,
        id: &CollectionId,
        end: ScanEdge<u8>,
        take: usize,
    ) -> Result<usize> {
        let start = Coordinate::from_bytes(vec![0]);
        let end = end.map(|b| Coordinate::from_bytes(vec![b]));
        let scan = Scan {
            section: Section::new(0),
            start: ScanEdge::Included(&start),
            dir: Direction::Forward,
            end: end.as_ref(),
            limit: None,
        };
        let probe = EventRef::Message {
            dedup_id: Uuid::from_u128(9),
        };
        let stream = cached.scan_cells(id, scan, probe);
        futures::pin_mut!(stream);
        let mut n = 0usize;
        while n < take
            && let Some(item) = stream.next().await
        {
            item?;
            n += 1;
        }
        Ok(n)
    }

    fn property(total_seed: u8, drop_seed: u8) -> Result<bool> {
        TEST_RUNTIME.block_on(async move {
            // At least two strides so the frontier advances more than once, and
            // < 256 total so coordinates stay single bytes.
            let total = 2 * GAP_COVER_STRIDE + 1 + usize::from(total_seed % 64);
            let consumed = usize::from(drop_seed) % (total + 1);
            let covered = (consumed / GAP_COVER_STRIDE) * GAP_COVER_STRIDE;

            let counting = CountingCellStore::new(MemoryCellStore::new(
                MemoryCells::new(),
                ScriptedOracle::default(),
                Arc::new(CollectionDefRegistry::default()),
            ));
            let id = fresh_collection("stride-frontier")?;
            let cref = CollectionRef::new(id.clone(), None);

            // Seed the LOWER store directly so the whole section is one gap.
            let mut seed: Vec<(CellKey, Option<Bytes>)> = Vec::with_capacity(total);
            for i in 0..total {
                let cell = CellKey {
                    section: Section::new(0),
                    coordinate: Coordinate::from_bytes(vec![u8::try_from(i)?]),
                };
                seed.push((cell, Some(bytes(1))));
            }
            counting.write_resolved(&cref, &seed, &[]).await?;

            let cached = Cached::new(test_db::cache("stride-frontier")?, counting.clone());

            // Drop the gap scan after `consumed` cells.
            if drain(&cached, &id, ScanEdge::Included(255), consumed).await? != consumed {
                return Ok(false);
            }

            // The stride-persisted prefix serves covered: zero lower scans.
            if covered > 0 {
                counting.reset();
                let end = u8::try_from(covered - 1)?;
                let prefix = drain(&cached, &id, ScanEdge::Included(end), usize::MAX).await?;
                if prefix != covered || counting.lower_scans() != 0 {
                    return Ok(false);
                }
            }

            // Never over-covered: the full re-scan still yields EVERY cell, and
            // the unpersisted tail (if any) falls through to the lower store.
            counting.reset();
            let all = drain(&cached, &id, ScanEdge::Included(255), usize::MAX).await?;
            if all != total {
                return Ok(false);
            }
            if covered < total && counting.lower_scans() == 0 {
                return Ok(false);
            }
            Ok(true)
        })
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8) -> Result<bool>);
}
