//! Fine-grained scan-coverage tracking for the [`Cached`](super::Cached) cache.
//!
//! Coverage answers one question per `(CollectionId, Section)`: *which
//! coordinate sub-ranges has fjall already mirrored from the durable store?* A
//! `get`/`scan` over a covered coordinate/sub-range is served entirely from
//! fjall; gaps fall through to the lower store and are covered as their cells
//! are consumed. Because the cache is **write-through**, a write covers the
//! coordinate it touched with the new committed projection — coverage *grows*
//! on both reads and writes. `punch` (uncover) happens on a write-path publish
//! that could not be established in fjall (self-heal on the next read), on
//! the raw `mark_resolved` promote, which cannot project the new committed
//! value from keys alone and so drops the coordinate for the next read to
//! re-publish, and on a committed section clear
//! ([`Coverage::punch_section`]), which drops a section's coverage wholesale.
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
//!   [`IntervalSet::cover`]; a write-path mutator punches only when that fjall
//!   publish cannot be established. Publishing before lower confirmation is
//!   forbidden.
//! - **`CovBuild` — born resolved.** [`IntervalSet::cover`] is `pub(in
//!   crate::state::cached)`; its call sites — the scan-drain, the mutator
//!   publishes, and cover-on-get — carry committed projections only (the
//!   scan-drain after `lower.scan_for_cache` oracle-resolved the gap; the
//!   mutators after the lower write established the committed value). A covered
//!   interval cannot be minted from provisional data.
//! - **`CovVolatile`.** Coverage is spilled to the per-partition fjall `index`
//!   keyspace, which is **assignment-scoped**: every assignment's keyspaces are
//!   born cold under a freshly minted name
//!   ([`FjallClient::workspace`](crate::state::fjall::FjallClient::workspace)
//!   owns that invariant) and dropped at revocation, so "stale coverage
//!   survives a crash" is unrepresentable — a fresh assignment trusts nothing
//!   uncovered. Coverage is authoritative only *within* one assignment
//!   (exclusive partition ownership), never persisted across assignments.
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
use super::super::fjall::{FjallCellCache, FjallCellCacheError};
use super::super::identity::CollectionId;
use ahash::RandomState;
use educe::Educe;
use quick_cache::sync::Cache;
use smallvec::SmallVec;
use std::array::from_fn;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Shards in the coverage-mutation lock pool. Mutations of one section always
/// share a shard; distinct sections almost never collide, so unrelated keys'
/// coverage writes don't serialize.
const LOCK_SHARDS: usize = 16;

/// Sections whose materialized [`IntervalSet`] is memoized in RAM. Bounds the
/// memo regardless of how many sections an assignment touches; an evicted
/// section simply reloads from fjall on its next access.
const MEMO_SECTIONS: usize = 1024;

/// Per-partition scan coverage, keyed by `(CollectionId, Section)`, spilled to
/// the per-partition fjall `index` keyspace.
///
/// Each `(collection, section)`'s covered intervals live as a `Cover` prefix
/// range (one fjall entry per interval, keyed by its low-bound frame → high
/// bound), so the coverage map accumulates **on disk** with RAM bounded by the
/// fixed-capacity memo below plus fjall's block cache — no unbounded in-RAM
/// `scc::HashMap` over a weeks-long assignment. The materialized
/// [`IntervalSet`] of each hot section is **memoized** ([`MEMO_SECTIONS`]
/// capacity): reads answer from the memo with zero fjall I/O, and every
/// mutation runs the algebra on the memoized set and rewrites the whole
/// section range in one blocking hop (fjall is the spill + reload source; its
/// stored range order is tag-first, not low-bound order — loading re-sorts
/// the pairs into the `BTreeMap`).
///
/// The one-per-partition [`FjallCellCache`] handle is a cheap `Arc` clone of
/// the one the [`Cached`](super::Cached) cache operates, so coverage shares the
/// workspace's lifecycle: cold (empty) at a fresh assignment
/// (`CovVolatile`), dropped at revocation. Like the mutation locks, the memo
/// is sound only because this `Coverage` (through its `Arc`-shared clones) is
/// the workspace's **single owner** for the assignment — a second live instance
/// over the same keyspace would neither serialize with nor observe this one's
/// mutations.
///
/// # Mutation atomicity
///
/// Every mutation is a whole-section load→mutate→store cycle; two interleaved
/// cycles on one section would lose the first store, and a **lost punch
/// resurrects stale coverage** — a wrong covered answer for the rest of the
/// assignment. Per-key event serialization does **not** exclude this: session
/// ops are `&self`, so one handler can hold two of them concurrently polled (a
/// `join!`-ed checkpoint and scan, a scan stream held across a get). Each
/// mutation therefore serializes on a hash-sharded per-`(collection, section)`
/// async lock, which also owns every **memo write**: a mutation refreshes the
/// memo only after its store landed (and drops the entry when the store
/// failed, so the next access reloads whatever fjall actually holds), and a
/// read that misses the memo populates it under the same lock — so a stale
/// read-side
/// load can never clobber a newer mutation's entry. Memo-hit reads
/// (`covers`/`query`) stay lock-free: the memoized `Arc` snapshot is replaced
/// wholesale, so a racing read sees the set before-or-after a mutation, and an
/// under-read is a benign fall-through.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct Coverage {
    fjall: FjallCellCache,
    locks: Arc<MutationLocks>,
    #[educe(Debug(ignore))]
    memo: Arc<Cache<(CollectionId, Section), Arc<IntervalSet>>>,
}

/// The shared coverage-mutation lock pool (see [`Coverage`]'s mutation-
/// atomicity note): a fixed shard array indexed by the hash of
/// `(collection, section)`, so it is bounded regardless of how many sections
/// an assignment touches.
#[derive(Debug)]
struct MutationLocks {
    hasher: RandomState,
    shards: [Mutex<()>; LOCK_SHARDS],
}

impl MutationLocks {
    /// The shard serializing mutations of `(collection, section)`.
    fn shard(&self, collection: &CollectionId, section: Section) -> &Mutex<()> {
        // Same inputs always map to the same shard; which shard is purely a
        // spreading choice.
        let hash = self.hasher.hash_one((collection, section));
        &self.shards[(hash as usize) % LOCK_SHARDS]
    }
}

impl Coverage {
    /// Builds coverage backed by `fjall`'s `index` keyspace.
    #[must_use]
    pub fn new(fjall: FjallCellCache) -> Self {
        Self {
            fjall,
            locks: Arc::new(MutationLocks {
                hasher: RandomState::new(),
                shards: from_fn(|_| Mutex::new(())),
            }),
            memo: Arc::new(Cache::new(MEMO_SECTIONS)),
        }
    }

    /// The section's materialized interval set: the memo answer when present,
    /// else loaded from fjall **under the section's mutation lock** and
    /// memoized (see the mutation-atomicity note — the lock owns every memo
    /// write, so this fill can never clobber a concurrent mutation's newer
    /// set).
    async fn snapshot(
        &self,
        collection: &CollectionId,
        section: Section,
    ) -> Result<Arc<IntervalSet>, FjallCellCacheError> {
        let key = (collection.clone(), section);
        if let Some(set) = self.memo.get(&key) {
            return Ok(set);
        }
        let _mutation = self.locks.shard(collection, section).lock().await;
        // A mutation may have populated the memo while we awaited the lock.
        if let Some(set) = self.memo.get(&key) {
            return Ok(set);
        }
        let stored = self.fjall.cover_load(collection, section).await?;
        let set = Arc::new(IntervalSet::from_pairs(stored));
        self.memo.insert(key, set.clone());
        Ok(set)
    }

    /// The one coverage write path: under the section's mutation lock, runs
    /// `mutate` over the section's materialized set (memoized, else loaded
    /// from fjall), rewrites the whole stored range in one blocking hop, and
    /// refreshes the memo — or drops the memo entry when the store failed, so
    /// the next access reloads whatever fjall actually holds.
    async fn mutate(
        &self,
        collection: &CollectionId,
        section: Section,
        mutate: impl FnOnce(&mut IntervalSet),
    ) -> Result<(), FjallCellCacheError> {
        let _mutation = self.locks.shard(collection, section).lock().await;
        let key = (collection.clone(), section);
        let mut set = match self.memo.get(&key) {
            Some(memoized) => (*memoized).clone(),
            None => IntervalSet::from_pairs(self.fjall.cover_load(collection, section).await?),
        };
        mutate(&mut set);
        let stored = self
            .fjall
            .cover_store(collection, section, &set.to_pairs())
            .await;
        match stored {
            Ok(()) => self.memo.insert(key, Arc::new(set)),
            Err(_) => {
                self.memo.remove(&key);
            }
        }
        stored
    }

    /// Partitions `request` over the section's coverage into ordered, disjoint
    /// covered/gap pieces. An unseen section is one whole-request gap; a fjall
    /// read/decode failure degrades the same way (a slower fall-through, never
    /// a wrong answer).
    pub async fn query(
        &self,
        collection: &CollectionId,
        section: Section,
        request: &Interval,
    ) -> Result<SmallVec<[Piece; 4]>, FjallCellCacheError> {
        Ok(self.snapshot(collection, section).await?.query(request))
    }

    /// Covers `interval` in the section (union/merge). Born resolved
    /// (`CovBuild` — see the module-level bullet). A fjall read/write failure
    /// leaves the interval uncovered (the next read falls through).
    pub(in crate::state::cached) async fn cover(
        &self,
        collection: &CollectionId,
        section: Section,
        interval: Interval,
    ) -> Result<(), FjallCellCacheError> {
        self.mutate(collection, section, |set| set.cover(interval))
            .await
    }

    /// Covers each coordinate's singleton interval `[X, X]` in one
    /// load→mutate→store cycle — the batch counterpart of
    /// [`cover`](Self::cover) for a settle batch's cells, N in-memory unions
    /// for one section rewrite. Born resolved (`CovBuild`), same as `cover`. A
    /// fjall read/write failure leaves the points uncovered (the next read
    /// falls through).
    pub(in crate::state::cached) async fn cover_points(
        &self,
        collection: &CollectionId,
        section: Section,
        coordinates: &[Coordinate],
    ) -> Result<(), FjallCellCacheError> {
        self.mutate(collection, section, |set| {
            for coordinate in coordinates {
                set.cover(point_interval(coordinate));
            }
        })
        .await
    }

    /// Whether `coordinate`'s committed value is already covered — a covered
    /// fjall hit serves with no lower read (Cov1/Cov2). A covered fjall *miss*
    /// means the entry expired (write-through always leaves an entry), so the
    /// caller falls through and re-publishes. A fjall read/decode failure
    /// degrades to "uncovered" (`false`), also falling through to the lower
    /// store.
    pub async fn covers(
        &self,
        collection: &CollectionId,
        section: Section,
        coordinate: &Coordinate,
    ) -> Result<bool, FjallCellCacheError> {
        Ok(self.snapshot(collection, section).await?.covers(coordinate))
    }

    /// Punches each coordinate out of the section's coverage (splitting its
    /// containing interval) in one load→mutate→store cycle. A no-op for
    /// coordinates that were not covered.
    ///
    /// A fjall read/write failure propagates. A covered hit is served verbatim
    /// — there is **no** read-side mismatch detection — so where the punch
    /// guards a moved durable value the caller must not swallow the failure:
    /// `Cached::punch_cells_must_succeed` retries it until it lands. Only the
    /// cover-on-get fill may degrade a punch failure (its coordinate is
    /// uncovered or expired-covered, both of which fall through anyway).
    pub async fn punch_many(
        &self,
        collection: &CollectionId,
        section: Section,
        coordinates: &[Coordinate],
    ) -> Result<(), FjallCellCacheError> {
        self.mutate(collection, section, |set| {
            for coordinate in coordinates {
                set.punch(coordinate);
            }
        })
        .await
    }

    /// Punches the section's **whole** coverage — Cov-Clr's eviction
    /// primitive: a committed section clear invalidates every covered value in
    /// the section, so the interval set is dropped wholesale (never expressed
    /// as an unbounded interval; the stored range is simply emptied). Like
    /// [`punch_many`](Self::punch_many), a failure propagates and the `Cached`
    /// call sites are must-succeed (`Cached::punch_sections_must_succeed`).
    pub async fn punch_section(
        &self,
        collection: &CollectionId,
        section: Section,
    ) -> Result<(), FjallCellCacheError> {
        self.mutate(collection, section, IntervalSet::clear).await
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
#[derive(Clone, Debug, Default)]
struct IntervalSet {
    intervals: BTreeMap<LowBound, Bound<Coordinate>>,
}

impl IntervalSet {
    /// Drops every interval — the whole-section punch
    /// ([`Coverage::punch_section`]).
    fn clear(&mut self) {
        self.intervals.clear();
    }

    /// Rebuilds the set from stored `(lo, hi)` bound pairs (a fjall `Cover`
    /// range). Each pair round-trips through [`Interval::new`], so a degenerate
    /// on-disk interval is dropped — an empty interval is unrepresentable in
    /// the materialized set.
    fn from_pairs(pairs: Vec<(Bound<Coordinate>, Bound<Coordinate>)>) -> Self {
        let mut intervals = BTreeMap::new();
        for (lo, hi) in pairs {
            if let Some(iv) = Interval::new(lo, hi) {
                intervals.insert(LowBound(iv.lo), iv.hi);
            }
        }
        Self { intervals }
    }

    /// Dumps the set's intervals as `(lo, hi)` bound pairs for storage,
    /// ascending by low bound.
    fn to_pairs(&self) -> Vec<(Bound<Coordinate>, Bound<Coordinate>)> {
        self.intervals
            .iter()
            .map(|(key, hi)| (key.0.clone(), hi.clone()))
            .collect()
    }

    /// Unions `interval` into the set, merging any intervals it touches or
    /// overlaps (complementary endpoints rejoin; real holes never merge).
    fn cover(&mut self, interval: Interval) {
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

    /// The `(key, hi)` entry of the one interval that could contain
    /// `coordinate` — the interval with the greatest low `<=` the coordinate;
    /// an `Excluded(c)` low sorts after `Included(c)` and is correctly
    /// excluded — filtered to where it actually contains it. `None` when
    /// `coordinate` is not covered.
    fn containing(&self, coordinate: &Coordinate) -> Option<(LowBound, Bound<Coordinate>)> {
        self.intervals
            .range(..=LowBound(Bound::Included(coordinate.clone())))
            .next_back()
            .map(|(k, v)| (k.clone(), v.clone()))
            .filter(|(key, hi)| interval(key, hi).contains(coordinate))
    }

    /// Whether `coordinate` is covered by some interval in the set.
    fn covers(&self, coordinate: &Coordinate) -> bool {
        self.containing(coordinate).is_some()
    }

    /// Splits the interval containing `coordinate` into `[lo, Excluded(X))` and
    /// `(Excluded(X), hi]`, dropping either empty half. A no-op when the
    /// coordinate is not covered.
    fn punch(&mut self, coordinate: &Coordinate) {
        let Some((key, hi)) = self.containing(coordinate) else {
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
                && let Some(gap) = Interval::new(cursor.clone(), complement(&cov_lo))
            {
                pieces.push(Piece::Gap(gap));
            }
            pieces.push(Piece::Covered(covered));
            // Reaching the request's high bound tiles it completely; stop before
            // a spurious trailing gap (`complement(Unbounded)` would not
            // otherwise signal completion).
            if cmp_high(&cov_hi, &request.hi) == Ordering::Equal {
                return pieces;
            }
            cursor = complement(&cov_hi);
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

/// The singleton interval `[X, X]` — non-empty by construction
/// (`Included`/`Included` at one coordinate), so it skips the
/// [`Interval::new`] empty-check.
fn point_interval(coordinate: &Coordinate) -> Interval {
    Interval {
        lo: Bound::Included(coordinate.clone()),
        hi: Bound::Included(coordinate.clone()),
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

/// Flips a bound across its own coordinate — `Included(X)` ↔ `Excluded(X)`,
/// `Unbounded` ↔ `Unbounded` — used both ways in `query`: as a high bound it
/// opens the next piece just after a covered slice ends; as a low bound it
/// closes the gap just before a covered slice starts. Either reading, a
/// covered slice's bound here is always clamped to the request, never
/// unbounded.
fn complement(bound: &Bound<Coordinate>) -> Bound<Coordinate> {
    match bound {
        Bound::Included(x) => Bound::Excluded(x.clone()),
        Bound::Excluded(x) => Bound::Included(x.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[cfg(test)]
mod tests;
