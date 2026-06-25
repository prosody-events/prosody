//! Fine-grained scan-coverage tracking for the [`Cached`](super::Cached) cache.
//!
//! Coverage answers one question per `(CollectionId, Section)`: *which
//! coordinate sub-ranges has fjall already mirrored from the durable store?* A
//! scan over a covered sub-range is served entirely from fjall; the gaps fall
//! through to the lower store and are covered as their cells are consumed. A
//! write **punches** only the coordinates it touches, so a single Map-entry
//! write evicts one coordinate and the next `iter()` re-reads only that point —
//! never the whole section.
//!
//! # Soundness invariants (the cache serves committed projections only)
//!
//! The committed value of a staged coordinate flips `prev → data` at
//! marker-flush, which happens in `settle` **off-cache, with no [`Cached`]
//! hook** ([`Cached`](super::Cached)). Coverage is sound only because of:
//!
//! - **Cov1 — covered ⇒ fjall value is current.** Every committed-value change
//!   to a covered coordinate routes through a [`Cached`](super::Cached)
//!   mutator, and every mutator punches its touched coordinates *before* the
//!   lower write. So no covered coordinate's committed value ever changes
//!   without a punch.
//! - **Cov2 — covered ⇒ resolved-in-lower (never provisional).** A coordinate
//!   goes provisional only via `write_provisional`, which punches it. So a
//!   covered hit is always a prior-committed projection: the covered serve
//!   skips the oracle and ignores `own`, and resolution-on-read fires only in
//!   gaps.
//! - **`CovBuild` — resolved-by-construction.** [`IntervalSet::cover`] is
//!   `pub(in crate::state::cached)` with its sole call site the scan-drain,
//!   *after* `lower.scan_cells` has oracle-resolved the gap. A covered interval
//!   cannot be minted from provisional data.
//! - **`CovVolatile`.** Coverage is in-memory (dropped when
//!   [`Cached`](super::Cached) drops at partition revocation), never persisted,
//!   so "stale coverage survives a crash" is unrepresentable.
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

    /// Covers `interval` in the section (union/merge). Born resolved: the sole
    /// caller is the scan-drain, after the gap was oracle-resolved by the lower
    /// store (`CovBuild`).
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
    /// fjall miss then means genuine absence (Cov2), with no lower read.
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

/// A sorted, pairwise-disjoint set of covered [`Interval`]s for one
/// `(collection, section)`.
///
/// The `intervals` vector is the load-bearing structure; it is **private** and
/// every mutation preserves the sorted-and-disjoint invariant.
#[derive(Debug, Default)]
pub struct IntervalSet {
    intervals: Vec<Interval>,
}

impl IntervalSet {
    /// Unions `interval` into the set, merging any intervals it touches or
    /// overlaps (complementary endpoints rejoin; real holes never merge).
    pub(in crate::state::cached) fn cover(&mut self, interval: Interval) {
        self.intervals.push(interval);
        self.intervals.sort_by(|a, b| cmp_low(&a.lo, &b.lo));
        let mut merged: Vec<Interval> = Vec::with_capacity(self.intervals.len());
        for iv in self.intervals.drain(..) {
            match merged.last_mut() {
                // Sorted by low, so `prev.lo <= iv.lo`; merge when `prev`'s high
                // reaches `iv`'s low with no hole, extending the high outward.
                Some(prev) if high_ge_low(&prev.hi, &iv.lo) => {
                    if cmp_high(&iv.hi, &prev.hi) == Ordering::Greater {
                        prev.hi = iv.hi;
                    }
                }
                _ => merged.push(iv),
            }
        }
        self.intervals = merged;
    }

    /// Whether `coordinate` is covered by some interval in the set.
    fn covers(&self, coordinate: &Coordinate) -> bool {
        self.intervals.iter().any(|iv| iv.contains(coordinate))
    }

    /// Splits the interval containing `coordinate` into `[lo, Excluded(X))` and
    /// `(Excluded(X), hi]`, dropping either empty half. A no-op when the
    /// coordinate is not covered.
    fn punch(&mut self, coordinate: &Coordinate) {
        let Some(pos) = self.intervals.iter().position(|iv| iv.contains(coordinate)) else {
            return;
        };
        let iv = self.intervals.remove(pos);
        let halves = [
            Interval::new(iv.lo, Bound::Excluded(coordinate.clone())),
            Interval::new(Bound::Excluded(coordinate.clone()), iv.hi),
        ]
        .into_iter()
        .flatten();
        self.intervals.splice(pos..pos, halves);
    }

    /// Partitions `request` into ordered, disjoint covered/gap pieces that
    /// exactly tile it.
    fn query(&self, request: &Interval) -> SmallVec<[Piece; 4]> {
        let mut pieces = SmallVec::new();
        // The low bound of the next piece still to emit; advances rightward.
        let mut cursor = request.lo.clone();
        for iv in &self.intervals {
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
