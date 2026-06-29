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
//! invalidates.** The sole `punch` (uncover) is a failed `fjall.put`: the
//! coordinate drops out of coverage so the next read falls through and
//! self-heals.
//!
//! The five soundness invariants (Cov1/Cov2/Cov3, `CovBuild`, `CovVolatile`,
//! `GetNeverReadsOwnStaged`) are stated on the `coverage` module. The ones
//! carried into this file:
//!
//! - **Cov3 — establish-then-publish.** `lower.write` precedes `fjall.put`+
//!   `cover`; the only mutator `punch` is on a `fjall.put` failure. A failed
//!   `lower.write` returns the error and leaves coverage untouched (the atomic
//!   batch changed nothing). Publishing before lower confirmation is forbidden.
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
use super::fjall::{CacheRead, FjallCellCache, ScanHit};
use super::identity::{CollectionId, CollectionRef};
use super::store::CellStore;
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use futures::{Stream, StreamExt, pin_mut};
use std::ops::Bound;
use std::sync::Arc;
use tracing::warn;

/// A write-through fjall coverage cache over a lower committed [`CellStore`].
///
/// The `Coverage` map rides an [`Arc`] so `Clone` (the session clones the
/// stack per event) shares one per-partition coverage state; it drops with the
/// last clone at partition revocation (`CovVolatile`).
#[derive(Clone)]
pub struct Cached<L> {
    fjall: FjallCellCache,
    lower: L,
    coverage: Arc<Coverage>,
}

impl<L> Cached<L> {
    /// Composes a cache over `lower`, serving committed-value hits from `fjall`
    /// and covered scan sub-ranges from the (initially empty) coverage map.
    #[must_use]
    pub fn new(fjall: FjallCellCache, lower: L) -> Self {
        Self {
            fjall,
            lower,
            coverage: Arc::new(Coverage::new()),
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
            Ok(()) => {
                if let Some(point) = Interval::new(
                    Bound::Included(cell.coordinate.clone()),
                    Bound::Included(cell.coordinate.clone()),
                ) {
                    self.coverage.cover(collection, cell.section, point).await;
                }
            }
            Err(error) => {
                warn_skip("publish", &error);
                self.coverage
                    .punch(collection, cell.section, &cell.coordinate)
                    .await;
            }
        }
    }
}

impl<L> Cached<L>
where
    L: CellStore,
{
    /// Publishes each touched cell's `projection` after a successful
    /// `lower.write` (Cov3). `stamped_at` is a clock reading taken **before**
    /// the lower write; [`expiry_at`] floors it DOWN to the second to match
    /// Cassandra's whole-second TTL resolution, so the co-expiry stamp sheds
    /// the sub-second remainder that would otherwise overhang the row
    /// (Cov1; residual clock skew self-heals via fall-through). The
    /// collection's write TTL is the full TTL (the value was just written).
    /// `project` computes each cell's committed projection from its batch
    /// entry.
    async fn publish_written<'a, T, P>(
        &self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, T)],
        stamped_at: u64,
        project: P,
    ) where
        P: Fn(&T) -> Committed,
    {
        let expiry = expiry_at(stamped_at, collection.ttl());
        for (cell, value) in cells {
            self.publish(collection.id(), cell, &project(value), expiry)
                .await;
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
                self.coverage.cover(collection, section, piece.clone()).await;
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
            self.coverage.cover(collection, section, consumed).await;
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
        let covered = self
            .coverage
            .covers(collection, cell.section, &cell.coordinate)
            .await;
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
        let (lo, hi) = match dir {
            Direction::Forward => (scan.start, scan.end),
            Direction::Backward => (scan.end, scan.start),
        };
        let request = Interval::new(lo.cloned(), hi.cloned());
        try_stream! {
            // An empty request (e.g. `(a, a)`) yields nothing.
            let Some(request) = request else {
                return;
            };
            let mut pieces = self.coverage.query(collection, section, &request).await;
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
        // The sweep touches only provisional (= gap) coordinates, never covered
        // ones, so it needs no coverage interaction (the Cov2 corollary).
        self.lower.provisional_cells(collection)
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
        self.lower.write_provisional(collection, writes).await?;
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
        // by the recovery sweep / `resolve_cell`, where the next covered read
        // re-publishes from the lower store. Leave coverage untouched: the lower
        // promote did not change the committed value (provisional `data` ==
        // committed once promoted), and any stale fjall `prev` is corrected by
        // the falls-through-on-mismatch covered serve. To stay conservative,
        // punch the touched coordinates so the next read re-publishes the
        // promoted value.
        self.lower.mark_resolved(collection, cells).await?;
        for cell in cells {
            self.coverage
                .punch(collection.id(), cell.section, &cell.coordinate)
                .await;
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
        let keys: Vec<CellKey> = writes.iter().map(|(cell, _)| cell.clone()).collect();
        let result = self.lower.mark_resolved(collection, &keys).await;
        if result.is_ok() {
            // The provisional `data` is now the committed value; re-publish it.
            // `mark_resolved` does NOT re-stamp the durable TTL — `data` keeps
            // the death set by `write_provisional` at stage time — so the
            // co-expiry must REUSE the stage-anchored expiry already on the
            // cell's fjall entry, never a fresh `now + ttl` (which would overhang
            // the durable death by the whole stage→commit gap; Cov1). If the
            // stage publish never landed (no entry) or is unreadable, punch the
            // coordinate so the next covered read re-publishes via the floored
            // read-back path.
            for (cell, write) in writes {
                let stage_expiry = match self.fjall.stored_expiry(collection.id(), cell).await {
                    Ok(expiry) => expiry,
                    Err(error) => {
                        warn_skip("commit expiry", &error);
                        None
                    }
                };
                match stage_expiry {
                    Some(expiry) => {
                        let projection = Committed::new(write.data().cloned());
                        self.publish(collection.id(), cell, &projection, expiry)
                            .await;
                    }
                    None => {
                        self.coverage
                            .punch(collection.id(), cell.section, &cell.coordinate)
                            .await;
                    }
                }
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
            // The committed value rolled back to `prev`; publish it.
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

#[cfg(test)]
mod tests;
