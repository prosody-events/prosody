//! Write-through fjall coverage cache over the durable lower store.
//!
//! [`Cached`] fronts a lower [`CellStore`] (the durable
//! [`CassandraStore`](crate::state::cassandra::CassandraStore)) with a
//! [`FjallCellCache`] of committed cell projections **and** a fine-grained
//! `Coverage` map of which coordinate sub-ranges fjall has mirrored. As the
//! single writer of an owned partition we observe every committed change we
//! make, so the cache is **write-through**: each mutator runs `lower.write`
//! first, then publishes the committed projection of every touched cell into
//! fjall and **covers** the coordinate. A point `get` and a `scan` then serve a
//! covered coordinate/range entirely from fjall — never re-reading the durable
//! store after a write.
//!
//! Coverage is the single trust bit for both points and ranges: a point is the
//! singleton interval `[X,X]`, a scan is a range, and one coverage-aware path
//! serves both. **The success path only publishes and covers — it never
//! invalidates.** `punch` (uncover) happens for two reasons, each dropping a
//! coordinate from coverage so the next read falls through and re-publishes
//! from the lower store: (a) a write-path publish that could not be established
//! in fjall (a failed `put`/`put_batch`, or a `commit_provisional` whose stage
//! entry was missing/unreadable) — the coordinate self-heals on the next read;
//! and (b) the raw `mark_resolved` promote, which cannot project the new
//! committed `data` from keys alone — since a covered hit is served verbatim, a
//! stale `prev` must not stay covered.
//!
//! The five soundness invariants (Cov1/Cov2/Cov3, `CovBuild`, `CovVolatile`,
//! `GetNeverReadsOwnStaged`) are stated on the `coverage` module. The ones
//! carried into this file:
//!
//! - **Cov3 — establish-then-publish.** `lower.write` precedes `fjall.put`+
//!   `cover`; the write-path `punch` fires only when that fjall publish cannot
//!   be established — reason (a) above (the raw-promote `punch` in (b) is not a
//!   write path). A failed `lower.write` returns the error and leaves coverage
//!   untouched (the atomic batch changed nothing). Publishing before lower
//!   confirmation is forbidden.
//! - **The Incomplete trap.** [`Cached`]'s
//!   [`commit_provisional`](CellStore::commit_provisional) /
//!   [`abort_provisional`](CellStore::abort_provisional) overrides MUST return
//!   the **lower** `Result` verbatim and swallow fjall publish errors — else a
//!   transient fjall failure would fold into
//!   [`ApplyOutcome::Incomplete`](crate::state::session) and arm
//!   `StateRecovery` forever for a perfectly healthy durable store.
//!
//! # TTL co-expiry
//!
//! Cassandra cells expire (`USING TTL`); fjall has no native per-entry TTL, so
//! the cache mirrors the expiry. Cassandra anchors a row's death on the
//! **coordinator wall clock at whole-second resolution** and ignores the write
//! timestamp for TTL, so each path floors its anchor DOWN to the second to
//! match that resolution — shedding the sub-second remainder that would
//! otherwise deterministically overhang the row:
//!
//! * A direct write ([`write_resolved`](CellStore::write_resolved) /
//!   [`write_provisional`](CellStore::write_provisional) /
//!   [`abort_provisional`](CellStore::abort_provisional)) anchors on a clock
//!   read taken **before** the lower write and stamps `floor(stamped_at) + ttl`
//!   ([`CollectionRef::ttl`]).
//! * The promote ([`commit_provisional`](CellStore::commit_provisional))
//!   **reuses** the stage-anchored expiry already on the cell's fjall entry —
//!   `mark_resolved` does not re-stamp the durable TTL, so `data` keeps the
//!   death set at stage time; a fresh `now + ttl` would overhang it by the
//!   whole stage→commit gap.
//! * A scan-fill / cover-on-get reads each cell's *remaining* TTL from the
//!   lower store ([`CellStore::get_for_cache`] / [`CellStore::scan_for_cache`],
//!   which the Cassandra store backs with `TTL(data)`) → stamps `floor(now) +
//!   remaining` (whole seconds).
//!
//! We do **not** synchronize the client and coordinator clocks; any residual
//! difference after flooring is clock skew, which no client-side arithmetic can
//! remove. Cov1 is therefore best-effort co-expiry — floor-to-second plus
//! self-heal — not an absolute cross-node guarantee. Because flooring makes an
//! entry expire slightly *early*, an expired entry on a covered coordinate is
//! treated as a **gap, not absence**: a covered `get` on an expiry-miss falls
//! through and re-publishes, and a covered scan refills the expired sub-range
//! (see the `coverage` module, Cov1).
//!
//! The cache is a **hint**: every fjall failure is logged and degraded (a miss,
//! a skipped publish leaving the coordinate uncovered, a fall-through scan),
//! never surfaced — correctness rests on the lower store, so
//! [`Cached::Error`](CellStore::Error) is just the lower store's error.

mod coverage;

use self::coverage::{Coverage, Interval, Piece};
use super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use super::event_ref::EventRef;
use super::fjall::{CacheRead, CoverDecision, FjallCellCache, ScanHit};
use super::identity::{CollectionId, CollectionRef};
use super::store::CellStore;
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use futures::{Stream, StreamExt, pin_mut};
use smallvec::SmallVec;
use std::ops::Bound;
use tracing::warn;

/// A write-through fjall coverage cache over a lower committed [`CellStore`].
///
/// `Coverage` spills to the same per-partition fjall `index` keyspace, so it
/// accumulates on disk (RAM bounded by fjall's block cache) and shares the
/// workspace's epoch lifecycle — cold at a fresh assignment, dropped at
/// revocation (`CovVolatile`). It is a cheap `Arc`-backed handle, cloned with
/// the stack per event.
#[derive(Clone)]
pub struct Cached<L> {
    fjall: FjallCellCache,
    lower: L,
    coverage: Coverage,
}

impl<L> Cached<L> {
    /// Composes a cache over `lower`, serving committed-value hits from `fjall`
    /// and covered scan sub-ranges from the (initially empty) coverage map.
    /// Both the coverage map and the warm provisional index spill to
    /// `fjall`'s `index` keyspace.
    #[must_use]
    pub fn new(fjall: FjallCellCache, lower: L) -> Self {
        Self {
            coverage: Coverage::new(fjall.clone()),
            fjall,
            lower,
        }
    }

    /// The fjall expiry for a cell **read back** from the lower store now: the
    /// clock is read at fill time and `remaining` is the already-decremented
    /// `TTL(data)`, so [`expiry_at`] stamps `floor(now) + remaining` — flooring
    /// the anchor to the second to match Cassandra's whole-second TTL
    /// resolution (Cov1). The write-through paths instead anchor on a
    /// pre-write clock.
    fn expiry_for(&self, remaining: Option<CompactDuration>) -> u64 {
        expiry_at(self.fjall.clock().now_ms(), remaining)
    }

    /// The absolute expiry stamped on a cell's current fjall entry (`None` if
    /// absent) — the co-expiry-anchor property asserts this equals the modeled
    /// durable death after every mutation (Cov1).
    ///
    /// # Errors
    ///
    /// Propagates a fjall read/decode failure.
    #[cfg(test)]
    pub(crate) async fn stored_expiry(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<u64>, super::fjall::FjallCellCacheError> {
        self.fjall.stored_expiry(collection, cell).await
    }

    /// Publishes one cell's committed `projection` into fjall with an absolute
    /// `expiry` (`0` = never), then **covers** the coordinate on success or
    /// **punches** it on a `fjall.put` failure (Cov3: the only mutator
    /// uncover). Called only after the lower write succeeded.
    async fn publish(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
        projection: &Committed,
        expiry: u64,
    ) where
        L: CellStore,
    {
        match self.fjall.put(collection, cell, projection, expiry).await {
            Ok(()) => self.cover_point(collection, cell).await,
            Err(error) => {
                warn_skip("publish", &error);
                degrade_cover_mut(
                    "punch",
                    self.coverage
                        .punch(collection, cell.section, &cell.coordinate)
                        .await,
                );
            }
        }
    }

    /// Covers a single coordinate (the point interval `[X, X]`) after a
    /// successful publish.
    async fn cover_point(&self, collection: &CollectionId, cell: &CellKey) {
        if let Some(point) = Interval::new(
            Bound::Included(cell.coordinate.clone()),
            Bound::Included(cell.coordinate.clone()),
        ) {
            degrade_cover_mut(
                "cover",
                self.coverage.cover(collection, cell.section, point).await,
            );
        }
    }
}

impl<L> Cached<L>
where
    L: CellStore,
{
    /// Publishes each touched cell's `projection` after a successful
    /// `lower.write` (Cov3) in **one** atomic fjall batch
    /// ([`FjallCellCache::put_batch`]), then covers every coordinate (or
    /// uncovers them all if the batch failed). `stamped_at` is a clock reading
    /// taken **before** the lower write; [`expiry_at`] floors it DOWN to the
    /// second to match Cassandra's whole-second TTL resolution, so the
    /// co-expiry stamp sheds the sub-second remainder that would otherwise
    /// overhang the row (Cov1; residual clock skew self-heals via
    /// fall-through). The collection's write TTL is the full TTL (the value was
    /// just written). `project` computes each cell's committed projection from
    /// its batch entry.
    async fn publish_written<T>(
        &self,
        collection: &CollectionRef,
        cells: &[(CellKey, T)],
        stamped_at: u64,
        project: impl Fn(&T) -> Committed,
    ) {
        let expiry = expiry_at(stamped_at, collection.ttl());
        // Build the bounded batch input once, then publish atomically: a
        // multi-cell update is never torn, and the whole settle is one blocking
        // thread-hop instead of N. The coverage updates below are in-memory
        // `scc` ops, so a serial loop is correct and cheap.
        let mut batch: Vec<(CellKey, Committed, u64)> = Vec::with_capacity(cells.len());
        for (cell, value) in cells {
            batch.push((cell.clone(), project(value), expiry));
        }
        match self.fjall.put_batch(collection.id(), &batch).await {
            Ok(()) => {
                for (cell, ..) in &batch {
                    self.cover_point(collection.id(), cell).await;
                }
            }
            Err(error) => {
                warn_skip("publish", &error);
                for (cell, ..) in &batch {
                    degrade_cover_mut(
                        "punch",
                        self.coverage
                            .punch(collection.id(), cell.section, &cell.coordinate)
                            .await,
                    );
                }
            }
        }
    }

    /// Serves one covered piece from fjall, falling through to the lower store
    /// for the remainder when a covered cell is missing-expired (a gap under
    /// floor rounding) or a fjall read errors — degradation is always a slower
    /// fall-through, never a wrong answer. Ignores `own` on the covered serve
    /// (Cov2) but forwards it on the fall-through gap read, which also
    /// re-publishes the refilled cells with a fresh expiry.
    fn serve_covered<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        piece: &'a Interval,
        dir: Direction,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), L::Error>> + Send + 'a {
        try_stream! {
            let covered = self
                .fjall
                .scan_present(collection, section, piece.low(), piece.high(), dir);
            pin_mut!(covered);
            let mut last: Option<Coordinate> = None;
            // Set when the covered serve must hand the remainder to the lower
            // store: a fjall read error, or an expired covered cell (which under
            // floor rounding is a gap, not an absence — Cov1).
            let mut fall_through = false;
            while let Some(item) = covered.next().await {
                match item {
                    Ok(ScanHit::Present(cell, bytes)) => {
                        last = Some(cell.coordinate.clone());
                        yield (cell, bytes);
                    }
                    Ok(ScanHit::Expired(_)) => {
                        // The expired coordinate and everything after it in `dir`
                        // refills from the lower store; stop the fjall serve at
                        // `last` so the remainder covers from there onward.
                        fall_through = true;
                        break;
                    }
                    Err(error) => {
                        warn_skip("covered scan", &error);
                        fall_through = true;
                        break;
                    }
                }
            }
            if fall_through && let Some(remainder) = remainder_after(piece, dir, last.as_ref()) {
                let filled = self.serve_gap(collection, section, &remainder, dir, own);
                pin_mut!(filled);
                while let Some(item) = filled.next().await {
                    yield item?;
                }
            }
        }
    }

    /// Serves one gap piece from the lower store via
    /// [`CellStore::scan_for_cache`], publishing each yielded cell into
    /// fjall (best-effort, stamped with its remaining TTL) and **covering
    /// on consume**: coverage extends only over the cells actually
    /// published, contiguously from the gap edge. The whole gap interval is
    /// covered only if the scan **exhausts** with no publish failure — a
    /// `limit`/early-drop or a failed publish caps coverage at the
    /// last good cell, so a covered re-scan can never serve a
    /// published-but-uncovered present cell as absent.
    fn serve_gap<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        piece: &'a Interval,
        dir: Direction,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), L::Error>> + Send + 'a {
        try_stream! {
            let scan = scan_for_piece(section, piece, dir);
            let inner = self.lower.scan_for_cache(collection, scan, own);
            pin_mut!(inner);
            let mut contiguous = true;
            while let Some(item) = inner.next().await {
                let (cell, bytes, remaining) = item?;
                let published = self
                    .try_put(collection, &cell, &Committed::new(Some(bytes.clone())), remaining)
                    .await;
                // Cover up to (and including) this cell only while the run from
                // the gap edge is unbroken; a failed publish stops extension.
                if contiguous && published {
                    self.cover_consumed(collection, section, piece, dir, &cell.coordinate)
                        .await;
                } else {
                    contiguous = false;
                }
                yield (cell, bytes);
            }
            // Exhausted with no hole: the empty tail is genuinely absent, so
            // cover the whole gap (crucial for unbounded-end `iter()`).
            if contiguous {
                degrade_cover_mut(
                    "cover",
                    self.coverage.cover(collection, section, piece.clone()).await,
                );
            }
        }
    }

    /// Best-effort fjall publish of a present cell, returning whether it landed
    /// (a failure logs and degrades, leaving the coordinate uncovered so the
    /// next read falls through).
    async fn try_put(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
        projection: &Committed,
        remaining: Option<CompactDuration>,
    ) -> bool {
        let expiry = self.expiry_for(remaining);
        match self.fjall.put(collection, cell, projection, expiry).await {
            Ok(()) => true,
            Err(error) => {
                warn_skip("populate", &error);
                false
            }
        }
    }

    /// Covers the consumed prefix of a gap up to `coordinate`: `[gap_lo, X]`
    /// forward, `[X, gap_hi]` backward (the side already drained).
    async fn cover_consumed(
        &self,
        collection: &CollectionId,
        section: Section,
        piece: &Interval,
        dir: Direction,
        coordinate: &Coordinate,
    ) {
        let consumed = match dir {
            Direction::Forward => {
                Interval::new(piece.low().cloned(), Bound::Included(coordinate.clone()))
            }
            Direction::Backward => {
                Interval::new(Bound::Included(coordinate.clone()), piece.high().cloned())
            }
        };
        if let Some(consumed) = consumed {
            degrade_cover_mut(
                "cover",
                self.coverage.cover(collection, section, consumed).await,
            );
        }
    }
}

impl<L> CellStore for Cached<L>
where
    L: CellStore,
{
    type Error = L::Error;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<Committed, Self::Error> {
        // A coverage read failure degrades to "uncovered" (fall through), never
        // a wrong answer.
        let covered = self
            .coverage
            .covers(collection, cell.section, &cell.coordinate)
            .await
            .unwrap_or_else(|error| {
                warn_skip("covers", &error);
                false
            });
        if covered {
            match self.fjall.get(collection, cell).await {
                // A covered hit (Present value or Absent tag) is the current
                // committed projection (Cov1); serve it with zero lower reads,
                // ignoring `own` (Cov2).
                Ok(CacheRead::Hit(committed)) => return Ok(committed),
                // A covered Miss is a gap within a covered range — genuine
                // absence (no entry was ever published there): answer absent
                // with zero lower reads (Cov2).
                Ok(CacheRead::Miss) => return Ok(Committed::new(None)),
                // A covered Expired entry is a gap under floor rounding (Cov1):
                // fall through and re-publish a fresh entry — never serve it as
                // absent.
                Ok(CacheRead::Expired) => {}
                Err(error) => warn_skip("read", &error),
            }
        }
        // Uncovered (or expired-covered falling through): read the committed
        // value plus its remaining TTL, publish, and cover the point
        // (cover-on-get). Sound because `get` is never called on a cell the
        // current event staged (GetNeverReadsOwnStaged), so the lower read is a
        // settled committed projection.
        let (committed, remaining) = self.lower.get_for_cache(collection, cell, own).await?;
        let expiry = self.expiry_for(remaining);
        self.publish(collection, cell, &committed, expiry).await;
        Ok(committed)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        let section = scan.section;
        let dir = scan.dir;
        let limit = scan.limit;
        // The request interval is absolute (direction-independent); the scan's
        // direction-relative bounds map onto it (start = low forward / high
        // backward).
        let (lo, hi) = scan.low_high();
        let request = Interval::new(lo.cloned(), hi.cloned());
        try_stream! {
            // An empty request (e.g. `(a, a)`) yields nothing.
            let Some(request) = request else {
                return;
            };
            // A coverage read failure degrades to one whole-request gap (serve
            // entirely from the lower store), never a wrong answer.
            let mut pieces = self
                .coverage
                .query(collection, section, &request)
                .await
                .unwrap_or_else(|error| {
                    warn_skip("query", &error);
                    let mut single = SmallVec::new();
                    single.push(Piece::Gap(request.clone()));
                    single
                });
            // `query` yields pieces ascending; walk them in the scan direction.
            if dir == Direction::Backward {
                pieces.reverse();
            }
            let mut yielded = 0usize;
            for piece in &pieces {
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                // Disjoint pieces concatenate with no merge or dedup; the limit
                // applies to the merged output. `Either` (left/right stream)
                // unifies the two piece stream types without `dyn`.
                let sub = match piece {
                    Piece::Covered(iv) => self
                        .serve_covered(collection, section, iv, dir, own)
                        .left_stream(),
                    Piece::Gap(iv) => {
                        self.serve_gap(collection, section, iv, dir, own).right_stream()
                    }
                };
                pin_mut!(sub);
                while let Some(item) = sub.next().await {
                    if limit.is_some_and(|n| yielded >= n) {
                        break;
                    }
                    yield item?;
                    yielded += 1;
                }
            }
        }
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        // The disk-backed warm provisional index gates the recovery sweep. Warm
        // (seeded): the local fjall snapshot answers with ZERO Cassandra queries
        // (the zero-query-on-quiescence goal); an empty snapshot yields nothing.
        // Cold (a fresh epoch after crash/rebalance mints an empty `index`
        // keyspace): the lower store's unconditional bounded `kind=Index` seed
        // runs (cost ∝ #provisional, never #cells), each coordinate is recorded
        // into fjall as it streams, and the collection is marked seeded.
        //
        // The sweep touches only provisional (= gap) coordinates, never covered
        // ones, so it needs no coverage interaction (the Cov2 corollary). A warm
        // read/write failure degrades toward the cold path (re-seed from durable
        // truth), never toward trusting a possibly-incomplete warm set — the
        // fjall index is a hint over the authoritative `kind=Index` markers.
        try_stream! {
            // Resolve the warm coordinate list, or fall through to the cold seed.
            // A warm read failure — `is_seeded` OR `snapshot` — must degrade to
            // the cold durable re-seed (`None`), NEVER to an empty set: an empty
            // set is a terminal "clean" answer that would unschedule the backstop
            // and strand real provisional cells (F2). Only a genuinely-seeded,
            // successfully-read snapshot short-circuits.
            let warm_coords = match self.fjall.index_seeded(collection).await {
                Ok(true) => match self.fjall.index_snapshot(collection).await {
                    Ok(coords) => Some(coords),
                    Err(error) => {
                        warn_skip("snapshot", &error);
                        None
                    }
                },
                Ok(false) => None,
                Err(error) => {
                    warn_skip("is_seeded", &error);
                    None
                }
            };
            if let Some(coords) = warm_coords {
                // Warm: the local fjall snapshot answers with ZERO Cassandra
                // queries; an empty snapshot yields nothing.
                for cell in coords {
                    // Point-read the `kind=Cell` row; a concurrently-resolved or
                    // absent coordinate reads `None` and is dropped
                    // (over-report-safe, matching the cold path's filter).
                    if let Some(provisional) =
                        self.lower.provisional_cell_at(collection, &cell).await?
                    {
                        yield (cell, provisional);
                    }
                }
            } else {
                // Cold (fresh epoch, or a warm read failed): the lower store's
                // unconditional bounded `kind=Index` seed runs; each coordinate
                // is recorded into fjall as it streams.
                let inner = self.lower.provisional_cells(collection);
                pin_mut!(inner);
                let mut all_recorded = true;
                while let Some(item) = inner.next().await {
                    let (cell, provisional) = item?;
                    if let Err(error) = self.fjall.index_record(collection, &cell).await {
                        warn_skip("record", &error);
                        all_recorded = false;
                    }
                    yield (cell, provisional);
                }
                // Latch `seeded` only if the whole coords set landed on disk. If
                // any record failed, leave it unseeded so the next sweep re-seeds
                // cold from the durable `kind=Index` markers rather than
                // short-circuiting on an incomplete snapshot and stranding a
                // provisional cell (F4) — symmetric with `write_provisional`.
                if all_recorded {
                    degrade_cover_mut(
                        "mark_seeded",
                        self.fjall.index_mark_seeded(collection).await,
                    );
                }
            }
        }
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        self.lower.provisional_cell_at(collection, cell).await
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        // Anchor the co-expiry on a clock read taken BEFORE the lower write;
        // `expiry_at` floors it to the second to match Cassandra's whole-second
        // TTL resolution, so the stamp sheds the sub-second remainder that would
        // overhang the row (Cov1/FLOOR). Establish first (Cov3): a failed lower
        // write leaves coverage untouched.
        let stamped_at = self.fjall.clock().now_ms();
        if let Err(error) = self.lower.write_provisional(collection, writes).await {
            // A partial durable stage may have landed `kind=Index` markers the
            // warm set now misses; drop the seeded latch so the next sweep
            // re-seeds from the durable index and restores completeness (the
            // warm-index invariant), closing the strand hole (F4).
            degrade_cover_mut("unseed", self.fjall.index_unseed(collection.id()).await);
            return Err(error);
        }
        // Record each staged coordinate into the warm index after the durable
        // ack. A warm write failure drops the seeded latch so the next sweep
        // re-seeds from the durable `kind=Index` — never leaving the latch true
        // with an unaccounted coordinate (F4).
        for (cell, _) in writes {
            if let Err(error) = self.fjall.index_record(collection.id(), cell).await {
                warn_skip("record", &error);
                degrade_cover_mut("unseed", self.fjall.index_unseed(collection.id()).await);
            }
        }
        // The committed value stays `prev` while the cell is provisional (commit
        // /abort republishes), so publish `prev` — never the in-flight `data`.
        self.publish_written(collection, writes, stamped_at, |write| {
            Committed::new(write.prev().cloned())
        })
        .await;
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
    ) -> Result<(), Self::Error> {
        // Pre-write anchor (Cov1/FLOOR), establish-first (Cov3) — see
        // `write_provisional`.
        let stamped_at = self.fjall.clock().now_ms();
        self.lower.write_resolved(collection, cells).await?;
        // Rollback/committed-write resolved the cell; drop its warm provisional
        // coordinate (a no-op for a never-staged direct write). A failed clear is
        // a harmless over-report the sweep's point-read filter drops.
        for (cell, _) in cells {
            degrade_cover_mut("clear", self.fjall.index_clear(collection.id(), cell).await);
        }
        self.publish_written(collection, cells, stamped_at, |data| {
            Committed::new(data.clone())
        })
        .await;
        Ok(())
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        // Promote keeps `data` as the committed value but does not carry it here,
        // so the cache cannot publish the new committed projection from the keys
        // alone. `commit_provisional` is the promote path that *does* carry the
        // staged writes (and publishes `data`); this raw promote is only reached
        // by the recovery sweep / `resolve_cell`. Because a covered hit is served
        // verbatim (Cov1/Cov2 — `get` never falls through on a value mismatch), a
        // stale `prev` left covered would be served forever. So punch the touched
        // coordinates, dropping them from coverage; the next read falls through
        // and re-publishes the promoted value from the lower store.
        self.lower.mark_resolved(collection, cells).await?;
        for cell in cells {
            degrade_cover_mut(
                "punch",
                self.coverage
                    .punch(collection.id(), cell.section, &cell.coordinate)
                    .await,
            );
            // Promote resolved the cell; drop its warm provisional coordinate.
            degrade_cover_mut("clear", self.fjall.index_clear(collection.id(), cell).await);
        }
        Ok(())
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        // Promote in the lower store (the authoritative settle). The result is
        // returned VERBATIM below; the fjall publish is best-effort so a cache
        // failure never folds into `ApplyOutcome::Incomplete` (the Incomplete
        // trap).
        let mut keys: Vec<CellKey> = Vec::with_capacity(writes.len());
        keys.extend(writes.iter().map(|(cell, _)| cell.clone()));
        let result = self.lower.mark_resolved(collection, &keys).await;
        if result.is_ok() {
            // The provisional `data` is now the committed value; re-publish it.
            // `mark_resolved` does NOT re-stamp the durable TTL — `data` keeps
            // the death set by `write_provisional` at stage time — so the
            // co-expiry must REUSE the stage-anchored expiry already on the
            // cell's fjall entry, never a fresh `now + ttl` (which would overhang
            // the durable death by the whole stage→commit gap; Cov1). The
            // combined read-expiry → re-publish runs as one atomic batch in a
            // single blocking thread-hop ([`commit_batch`]); it returns a
            // [`CoverDecision`] per write — `Cover` when re-published, `Punch`
            // when the stage entry was missing/unreadable or the batch failed
            // (the next covered read then re-publishes via the floored read-back
            // path). The lower promote above stays a single batched call before
            // the cache work, and `result` is returned verbatim so the
            // best-effort republish never folds into `ApplyOutcome::Incomplete`
            // (the Incomplete trap).
            let decisions = self.fjall.commit_batch(collection.id(), writes).await;
            for ((cell, _), decision) in writes.iter().zip(decisions) {
                match decision {
                    CoverDecision::Cover => self.cover_point(collection.id(), cell).await,
                    CoverDecision::Punch => degrade_cover_mut(
                        "punch",
                        self.coverage
                            .punch(collection.id(), cell.section, &cell.coordinate)
                            .await,
                    ),
                }
                // The cell is resolved; drop its warm provisional coordinate.
                degrade_cover_mut("clear", self.fjall.index_clear(collection.id(), cell).await);
            }
        }
        result
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        let cells: Vec<(CellKey, Option<Bytes>)> = writes
            .iter()
            .map(|(cell, write)| (cell.clone(), write.prev().cloned()))
            .collect();
        // Pre-write anchor (Cov1/FLOOR): the rollback re-writes `prev` with a
        // fresh `USING TTL`, so it co-expires from this instant.
        let stamped_at = self.fjall.clock().now_ms();
        let result = self.lower.write_resolved(collection, &cells).await;
        if result.is_ok() {
            // The rollback resolved each cell; drop its warm provisional
            // coordinate, then publish the rolled-back `prev`.
            for (cell, _) in &cells {
                degrade_cover_mut("clear", self.fjall.index_clear(collection.id(), cell).await);
            }
            self.publish_written(collection, writes, stamped_at, |write| {
                Committed::new(write.prev().cloned())
            })
            .await;
        }
        result
    }
}

/// The absolute fjall expiry (millis; `0` = never) for a cell whose durable row
/// was (or is about to be) written at `stamped_at` with `remaining` whole
/// seconds of TTL. A `None` TTL means the durable row never expires, so the
/// entry never does either.
///
/// Cassandra anchors a row's death on the **coordinator wall clock at
/// whole-second resolution** and ignores the write timestamp for TTL. We floor
/// `stamped_at` DOWN to the same second resolution so the mirrored fjall stamp
/// sheds the 0–999 ms sub-second remainder that would otherwise
/// deterministically overhang the row (rounding to *nearest* would round up
/// half the time and still overhang by ≤500 ms). Flooring is the safe direction
/// — an early fjall expiry falls through and self-heals. Any residual is
/// cross-node clock skew, which no client-side arithmetic can remove; it too is
/// absorbed by the self-healing fall-through (Cov1).
fn expiry_at(stamped_at: u64, remaining: Option<CompactDuration>) -> u64 {
    match remaining {
        Some(remaining) => {
            let anchor = stamped_at - stamped_at % 1_000;
            anchor.saturating_add(u64::from(remaining.seconds()).saturating_mul(1_000))
        }
        None => 0,
    }
}

/// Builds the direction-relative [`Scan`] for one piece interval: forward walks
/// `low → high`, backward `high → low`. No limit — the stitched scan applies
/// the merged-output limit across pieces.
fn scan_for_piece(section: Section, piece: &Interval, dir: Direction) -> Scan<'_> {
    let (start, end) = match dir {
        Direction::Forward => (piece.low(), piece.high()),
        Direction::Backward => (piece.high(), piece.low()),
    };
    Scan {
        section,
        start,
        dir,
        end,
        limit: None,
    }
}

/// The covered piece's sub-range still unserved after a fjall error/expiry at
/// `last`: `(last, high]` forward, `[low, last)` backward, or the whole piece
/// when the stop came before the first cell. `None` when nothing remains.
fn remainder_after(
    piece: &Interval,
    dir: Direction,
    last: Option<&Coordinate>,
) -> Option<Interval> {
    match (dir, last) {
        (_, None) => Some(piece.clone()),
        (Direction::Forward, Some(c)) => {
            Interval::new(Bound::Excluded(c.clone()), piece.high().cloned())
        }
        (Direction::Backward, Some(c)) => {
            Interval::new(piece.low().cloned(), Bound::Excluded(c.clone()))
        }
    }
}

/// Logs a degraded fjall cache operation (the cache is a hint).
fn warn_skip(op: &str, error: &super::fjall::FjallCellCacheError) {
    warn!(error = %error, "committed-value cache {op} failed; degrading");
}

/// Degrades a coverage mutation (`cover`/`punch`): a fjall failure logs and
/// leaves coverage unchanged, so the next read self-heals via fall-through —
/// the cache is a hint, never surfaced.
fn degrade_cover_mut(op: &str, result: Result<(), super::fjall::FjallCellCacheError>) {
    if let Err(error) = result {
        warn_skip(op, &error);
    }
}

#[cfg(test)]
mod tests;
