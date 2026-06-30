//! Fine-grained scan-coverage tracking for the [`Cached`](super::Cached) cache.
//!
//! Coverage answers one question per `(CollectionId, Section)`: *which
//! coordinate sub-ranges has fjall already mirrored from the durable store?* A
//! `get`/`scan` over a covered coordinate/sub-range is served entirely from
//! fjall; gaps fall through to the lower store and are covered as their cells
//! are consumed. Because the cache is **write-through**, a write covers the
//! coordinate it touched with the new committed projection — coverage *grows*
//! on both reads and writes. The only `punch` (uncover) is a failed
//! `fjall.put`, so a coordinate whose publish failed falls through on the next
//! read and self-heals.
//!
//! # Soundness invariants (the cache serves committed projections only)
//!
//! - **Cov1 — covered ⇒ fjall holds the current committed projection (and is
//!   unexpired).** Every committed-value change to a covered coordinate routes
//!   through a [`Cached`](super::Cached) mutator, which publishes the new
//!   projection on success or punches the coordinate on a `fjall.put` failure.
//!   TTL is mirrored: an expired covered entry reads as a miss and the covered
//!   serve falls through (floor rounding can expire a fjall entry just before
//!   its durable row, so an expired covered coordinate is a gap, not absence).
//! - **Cov2 — covered ⇒ fjall = oracle(lower row).** A covered point may have a
//!   provisional lower row during `[stage, commit]`, but fjall holds `prev`
//!   (the committed projection) across it: `write_provisional` publishes
//!   `prev`, and `commit_provisional`/`abort_provisional` republish
//!   `data`/`prev`. The covered serve skips the oracle and ignores `own`;
//!   resolution-on-read fires only in gaps.
//! - **Cov3 — establish-then-publish.** `lower.write` precedes
//!   [`IntervalSet::cover`]; the only mutator `punch` is on a `fjall.put`
//!   failure. Publishing before lower confirmation is forbidden.
//! - **`CovBuild` — born resolved.** [`IntervalSet::cover`] is `pub(in
//!   crate::state::cached)`; its call sites — the scan-drain, the mutator
//!   publishes, and cover-on-get — carry committed projections only (the
//!   scan-drain after `lower.scan_for_cache` oracle-resolved the gap; the
//!   mutators after the lower write established the committed value). A covered
//!   interval cannot be minted from provisional data.
//! - **`CovVolatile`.** Coverage is in-memory (dropped when
//!   [`Cached`](super::Cached) drops at partition revocation), never persisted,
//!   so "stale coverage survives a crash" is unrepresentable; a cold restart
//!   trusts nothing uncovered.
//! - **`GetNeverReadsOwnStaged`.** Cover-on-get reads the lower store on an
//!   uncovered (or expired-covered) `get` and publishes the result. It is sound
//!   because `get` is never called on a cell the current event already staged
//!   (staging is at `finalize`/`settle`, which resolves via
//!   `commit_provisional`/`abort_provisional`, not `get`; `finalize`'s
//!   prev-read precedes the stage), so the lower read always returns a settled
//!   committed projection.
//!
//! # The interval model
//!
//! An [`Interval`] is an **absolute** (direction-independent) coordinate range
//! with [`Bound`] endpoints. An [`IntervalSet`] keeps a sorted,
//! pairwise-disjoint set of them; [`IntervalSet::query`] partitions a request
//! into ordered, disjoint [`Piece`]s that exactly tile it. Endpoints are
//! compared on the *continuum* of the coordinate order (not byte-adjacency): a
//! re-filled punch heals — `[a, Excluded(X))` and `[Included(X), b]` rejoin to
//! `[a, b]` — but a real hole never merges — `[a, Excluded(X))` and
//! `[Excluded(X), b]` stay split. Treating `(p, q)` with `p < q` as non-empty
//! even when no coordinate lies strictly between is sound: such an interval
//! simply serves nothing.

use super::super::cell_key::{Coordinate, Section};
use super::super::identity::CollectionId;
use ahash::RandomState;
use scc::hash_map::Entry;
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};

/// Per-partition scan coverage, keyed by `(CollectionId, Section)`.
///
/// Shared across keys via the [`Cached`](super::Cached) `Arc`; cross-key
/// touches hit disjoint entries and same-key access is framework-serialized, so
/// the lock-free `scc::HashMap` needs no extra synchronization.
#[derive(Debug, Default)]
pub struct Coverage {
    sections: scc::HashMap<(CollectionId, Section), IntervalSet, RandomState>,
}

impl Coverage {
    /// An empty coverage map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Partitions `request` over the section's coverage into ordered, disjoint
    /// covered/gap pieces. An unseen section is one whole-request gap.
    pub async fn query(
        &self,
        collection: &CollectionId,
        section: Section,
        request: &Interval,
    ) -> SmallVec<[Piece; 4]> {
        let pieces = self
            .sections
            .read_async(&(collection.clone(), section), |_, set| set.query(request))
            .await;
        pieces.unwrap_or_else(|| {
            let mut single = SmallVec::new();
            single.push(Piece::Gap(request.clone()));
            single
        })
    }

    /// Covers `interval` in the section (union/merge). Born resolved
    /// (`CovBuild`): the callers — the scan-drain, the write-through mutator
    /// publishes, and cover-on-get — all carry committed projections only.
    pub(in crate::state::cached) async fn cover(
        &self,
        collection: &CollectionId,
        section: Section,
        interval: Interval,
    ) {
        self.sections
            .entry_async((collection.clone(), section))
            .await
            .or_default()
            .get_mut()
            .cover(interval);
    }

    /// Whether `coordinate`'s committed value is already covered — a covered
    /// fjall hit serves with no lower read (Cov1/Cov2). A covered fjall *miss*
    /// means the entry expired (write-through always leaves an entry), so the
    /// caller falls through and re-publishes.
    pub async fn covers(
        &self,
        collection: &CollectionId,
        section: Section,
        coordinate: &Coordinate,
    ) -> bool {
        self.sections
            .read_async(&(collection.clone(), section), |_, set| {
                set.covers(coordinate)
            })
            .await
            .unwrap_or(false)
    }

    /// Punches `coordinate` out of the section's coverage (splitting the
    /// containing interval). A no-op when the coordinate was not covered.
    pub async fn punch(
        &self,
        collection: &CollectionId,
        section: Section,
        coordinate: &Coordinate,
    ) {
        if let Entry::Occupied(mut entry) = self
            .sections
            .entry_async((collection.clone(), section))
            .await
        {
            entry.get_mut().punch(coordinate);
        }
    }
}

/// A non-empty, absolute coordinate interval with [`Bound`] endpoints.
///
/// `lo` is the low side, `hi` the high side, regardless of any scan direction.
/// Constructed only through [`Interval::new`], which drops empties, so an
/// ill-formed (`hi` below `lo`) or empty interval is unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interval {
    lo: Bound<Coordinate>,
    hi: Bound<Coordinate>,
}

impl Interval {
    /// Builds an interval from its low/high bounds, returning `None` when the
    /// range is empty (e.g. `[a, a)` or `(X, X)`).
    #[must_use]
    pub fn new(lo: Bound<Coordinate>, hi: Bound<Coordinate>) -> Option<Self> {
        is_nonempty(&lo, &hi).then_some(Self { lo, hi })
    }

    /// The low bound.
    #[must_use]
    pub fn low(&self) -> Bound<&Coordinate> {
        self.lo.as_ref()
    }

    /// The high bound.
    #[must_use]
    pub fn high(&self) -> Bound<&Coordinate> {
        self.hi.as_ref()
    }
}

impl RangeBounds<Coordinate> for Interval {
    fn start_bound(&self) -> Bound<&Coordinate> {
        self.lo.as_ref()
    }

    fn end_bound(&self) -> Bound<&Coordinate> {
        self.hi.as_ref()
    }
}

/// One disjoint slice of a queried request: either fjall-covered or a
/// fall-through gap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Piece {
    /// Fully mirrored in fjall — serve from the cache.
    Covered(Interval),
    /// Not mirrored — fall through to the lower store.
    Gap(Interval),
}

/// The low bound of a stored interval — the [`BTreeMap`] key in an
/// [`IntervalSet`], ordered by [`cmp_low`].
///
/// **Consistency invariant (the `BTreeMap` correctness prerequisite):**
/// `cmp_low` returns `Equal` iff the two bounds are structurally equal — both
/// `Unbounded`, or the same variant with equal coordinates. The cross-polarity
/// arms (`Included`/`Excluded` at the same coordinate) end in
/// `.then(Less/Greater)`, never `Equal`, so `a.cmp(b) == Equal ⇔ a == b`. That
/// is what keeps distinct keys from collapsing or mis-ordering.
/// `PartialEq`/`Eq` stay derived so they can never drift from `cmp_low`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct LowBound(Bound<Coordinate>);

impl Ord for LowBound {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_low(&self.0, &other.0)
    }
}

impl PartialOrd for LowBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A sorted, pairwise-disjoint set of covered [`Interval`]s for one
/// `(collection, section)`.
///
/// Stored as a [`BTreeMap`] from each interval's [`LowBound`] to its high
/// bound: the key *is* the sorted-by-low invariant (an out-of-order entry is
/// unrepresentable), and the low is recovered from the key, so it is the single
/// source of truth and cannot drift from a duplicated copy. Every mutation
/// preserves pairwise-disjointness and non-emptiness.
#[derive(Debug, Default)]
pub struct IntervalSet {
    intervals: BTreeMap<LowBound, Bound<Coordinate>>,
}

impl IntervalSet {
    /// Unions `interval` into the set, merging any intervals it touches or
    /// overlaps (complementary endpoints rejoin; real holes never merge).
    pub(in crate::state::cached) fn cover(&mut self, interval: Interval) {
        let Interval { mut lo, mut hi } = interval;

        // Left-merge: at most one predecessor can reach `lo`, since
        // disjointness puts every earlier interval's high strictly below the
        // predecessor's low. The `.map(clone)` releases the `&self` range borrow
        // before `remove`/`insert`; the clones are cheap `Bound`/Arc-`Bytes`
        // refcount bumps, not a scratch buffer.
        if let Some((key, prev_hi)) = self
            .intervals
            .range(..=LowBound(lo.clone()))
            .next_back()
            .map(|(k, v)| (k.clone(), v.clone()))
            .filter(|(_, prev_hi)| high_ge_low(prev_hi, &lo))
        {
            self.intervals.remove(&key);
            lo = key.0;
            if cmp_high(&prev_hi, &hi) == Ordering::Greater {
                hi = prev_hi;
            }
        }

        // Right-cascade: absorb every following interval the running `hi`
        // reaches, re-seeking against the extended `hi` so a cover bridging
        // several runs coalesces transitively. Monotone — `hi` only grows and
        // each step removes one entry — so it terminates.
        while let Some((key, next_hi)) = self
            .intervals
            .range(LowBound(lo.clone())..)
            .next()
            .map(|(k, v)| (k.clone(), v.clone()))
            .filter(|(next, _)| high_ge_low(&hi, &next.0))
        {
            if cmp_high(&next_hi, &hi) == Ordering::Greater {
                hi = next_hi;
            }
            self.intervals.remove(&key);
        }

        self.intervals.insert(LowBound(lo), hi);
    }

    /// Whether `coordinate` is covered by some interval in the set.
    fn covers(&self, coordinate: &Coordinate) -> bool {
        // The only candidate is the interval with the greatest low `<=` the
        // coordinate; an `Excluded(c)` low sorts after `Included(c)` and is
        // correctly excluded.
        self.intervals
            .range(..=LowBound(Bound::Included(coordinate.clone())))
            .next_back()
            .is_some_and(|(key, hi)| interval(key, hi).contains(coordinate))
    }

    /// Splits the interval containing `coordinate` into `[lo, Excluded(X))` and
    /// `(Excluded(X), hi]`, dropping either empty half. A no-op when the
    /// coordinate is not covered.
    fn punch(&mut self, coordinate: &Coordinate) {
        let Some((key, hi)) = self
            .intervals
            .range(..=LowBound(Bound::Included(coordinate.clone())))
            .next_back()
            .map(|(k, v)| (k.clone(), v.clone()))
            .filter(|(key, hi)| interval(key, hi).contains(coordinate))
        else {
            return;
        };
        self.intervals.remove(&key);
        // The left half re-inserts at the same key; the right half is a new key
        // after it. `Interval::new` drops either empty half.
        let halves = [
            Interval::new(key.0, Bound::Excluded(coordinate.clone())),
            Interval::new(Bound::Excluded(coordinate.clone()), hi),
        ]
        .into_iter()
        .flatten();
        self.intervals
            .extend(halves.map(|iv| (LowBound(iv.lo), iv.hi)));
    }

    /// Partitions `request` into ordered, disjoint covered/gap pieces that
    /// exactly tile it.
    fn query(&self, request: &Interval) -> SmallVec<[Piece; 4]> {
        let mut pieces = SmallVec::new();
        // The low bound of the next piece still to emit; advances rightward.
        let mut cursor = request.lo.clone();
        // Seek the one straddling predecessor (low strictly below the request,
        // high possibly reaching in) then every interval from the request's low
        // forward. The predecessor range is exclusive and the forward range
        // inclusive so a key exactly at `request.lo` is visited once, not twice.
        // Omitting the predecessor seek would silently drop coverage of a
        // request whose low falls mid-interval.
        let predecessor = self
            .intervals
            .range(..LowBound(request.lo.clone()))
            .next_back();
        let forward = self.intervals.range(LowBound(request.lo.clone())..);
        for (key, hi) in predecessor.into_iter().chain(forward) {
            let iv = interval(key, hi);
            // Skip intervals ending before the cursor and stop once one starts
            // past the request's high bound.
            if !high_ge_low(&iv.hi, &cursor) {
                continue;
            }
            if !high_ge_low(&request.hi, &iv.lo) {
                break;
            }
            let cov_lo = max_low(&cursor, &iv.lo);
            let cov_hi = min_high(&request.hi, &iv.hi);
            // A degenerate (empty) intersection contributes no covered slice;
            // its span stays part of a gap.
            let Some(covered) = Interval::new(cov_lo.clone(), cov_hi.clone()) else {
                continue;
            };
            // Emit the gap before this covered slice, if any.
            if cmp_low(&cov_lo, &cursor) == Ordering::Greater
                && let Some(gap) = Interval::new(cursor.clone(), complement_low(&cov_lo))
            {
                pieces.push(Piece::Gap(gap));
            }
            pieces.push(Piece::Covered(covered));
            // Reaching the request's high bound tiles it completely; stop before
            // a spurious trailing gap (`complement_high(Unbounded)` would not
            // otherwise signal completion).
            if cmp_high(&cov_hi, &request.hi) == Ordering::Equal {
                return pieces;
            }
            cursor = complement_high(&cov_hi);
        }
        if let Some(gap) = Interval::new(cursor, request.hi.clone()) {
            pieces.push(Piece::Gap(gap));
        }
        pieces
    }
}

/// Reconstructs the [`Interval`] held by a `(key, hi)` map entry. Every stored
/// entry is non-empty — inputs arrive as non-empty [`Interval`]s, merges only
/// widen, and `punch` re-inserts halves only through [`Interval::new`] — so
/// bypassing the `Interval::new` empty-check here is sound.
fn interval(key: &LowBound, hi: &Bound<Coordinate>) -> Interval {
    Interval {
        lo: key.0.clone(),
        hi: hi.clone(),
    }
}

/// Whether `[lo, hi]` contains at least one coordinate on the continuum.
fn is_nonempty(lo: &Bound<Coordinate>, hi: &Bound<Coordinate>) -> bool {
    match (lo, hi) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        (Bound::Included(a), Bound::Included(b)) => a <= b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a < b,
    }
}

/// Orders two **low** bounds by position. `Unbounded` is `-∞`; at a tie,
/// `Included` (the range starts *at* the point) precedes `Excluded` (it starts
/// just *after*).
fn cmp_low(a: &Bound<Coordinate>, b: &Bound<Coordinate>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(x), Bound::Included(y)) | (Bound::Excluded(x), Bound::Excluded(y)) => {
            x.cmp(y)
        }
        (Bound::Included(x), Bound::Excluded(y)) => x.cmp(y).then(Ordering::Less),
        (Bound::Excluded(x), Bound::Included(y)) => x.cmp(y).then(Ordering::Greater),
    }
}

/// Orders two **high** bounds by position. `Unbounded` is `+∞`; at a tie,
/// `Excluded` (the range ends just *before* the point) precedes `Included` (it
/// ends *at* the point).
fn cmp_high(a: &Bound<Coordinate>, b: &Bound<Coordinate>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Greater,
        (_, Bound::Unbounded) => Ordering::Less,
        (Bound::Included(x), Bound::Included(y)) | (Bound::Excluded(x), Bound::Excluded(y)) => {
            x.cmp(y)
        }
        (Bound::Excluded(x), Bound::Included(y)) => x.cmp(y).then(Ordering::Less),
        (Bound::Included(x), Bound::Excluded(y)) => x.cmp(y).then(Ordering::Greater),
    }
}

/// Whether a high bound reaches at least as far as a low bound — i.e. an
/// interval ending at `hi` and one starting at `lo` touch or overlap (so they
/// merge with no hole). The complementary pair `Excluded(X)` / `Included(X)`
/// touches; the real hole `Excluded(X)` / `Excluded(X)` does not.
fn high_ge_low(hi: &Bound<Coordinate>, lo: &Bound<Coordinate>) -> bool {
    match (hi, lo) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        (Bound::Excluded(x), Bound::Excluded(y)) => x > y,
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => {
            x >= y
        }
    }
}

/// The later (greater-position) of two low bounds.
fn max_low(a: &Bound<Coordinate>, b: &Bound<Coordinate>) -> Bound<Coordinate> {
    if cmp_low(a, b) == Ordering::Greater {
        a.clone()
    } else {
        b.clone()
    }
}

/// The earlier (smaller-position) of two high bounds.
fn min_high(a: &Bound<Coordinate>, b: &Bound<Coordinate>) -> Bound<Coordinate> {
    if cmp_high(a, b) == Ordering::Less {
        a.clone()
    } else {
        b.clone()
    }
}

/// The high bound that closes the gap ending just before a covered slice's low
/// bound: the gap stops where the covered slice starts (`Included(X)` low →
/// gap ends `Excluded(X)`; `Excluded(X)` low → gap ends `Included(X)`).
fn complement_low(low: &Bound<Coordinate>) -> Bound<Coordinate> {
    match low {
        Bound::Included(x) => Bound::Excluded(x.clone()),
        Bound::Excluded(x) => Bound::Included(x.clone()),
        // A covered slice's low is clamped to the request, never unbounded here.
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// The low bound that opens the next piece just after a covered slice's high
/// bound (`Included(X)` high → next starts `Excluded(X)`; `Excluded(X)` high →
/// next starts `Included(X)`).
fn complement_high(high: &Bound<Coordinate>) -> Bound<Coordinate> {
    match high {
        Bound::Included(x) => Bound::Excluded(x.clone()),
        Bound::Excluded(x) => Bound::Included(x.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[cfg(test)]
mod tests;
