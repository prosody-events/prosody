//! Fjall coverage cache over the durable lower store.
//!
//! [`Cached`] fronts a lower [`CellStore`] (the durable
//! [`CassandraStore`](crate::state::cassandra::CassandraStore)) with a
//! [`FjallCellCache`] of committed cell values **and** a fine-grained
//! `Coverage` map of which coordinate sub-ranges fjall has mirrored. A point
//! `get` serves a fjall hit; a `scan` *stitches* covered sub-ranges (served
//! from fjall) with bounded fall-through queries over the gaps; and a write
//! **punches** only the coordinates it touches, so one Map-entry write evicts
//! one coordinate and the next `iter()` re-reads only that point.
//!
//! Coverage holds **committed projections only** — never a provisional `data`.
//! Its three soundness invariants (Cov1: covered ⇒ fjall current; Cov2: covered
//! ⇒ resolved-in-lower; `CovBuild`: coverage born resolved from the scan-drain)
//! are stated on the `coverage` module. The one carried into this file is:
//!
//! - **Cov3 — punch-before-write.** Every mutator routes its touched cells
//!   through `invalidate_touched` — an infallible in-memory punch plus a
//!   best-effort fjall point eviction — **before** the fallible `lower.write`.
//!   Punching then failing the write costs only a re-scan; writing then
//!   skipping the punch on an `Err` would leave a covered-but-changed
//!   coordinate, a fail-open stale serve. The single choke point is the only
//!   way to satisfy Cov1 ("every mutator punches"); a new mutator that skips it
//!   is the lone regression, caught by the answer-vs-oracle and op-budget
//!   properties.
//!
//! The cache is a **hint**: every fjall failure is logged and degraded (a miss,
//! a skipped populate, a fall-through scan), never surfaced — correctness rests
//! on the lower store, so [`Cached::Error`](CellStore::Error) is just the lower
//! store's error.

mod coverage;

use self::coverage::{Coverage, Interval, Piece};
use super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use super::event_ref::EventRef;
use super::fjall::FjallCellCache;
use super::identity::{CollectionId, CollectionRef};
use super::store::CellStore;
use async_stream::try_stream;
use bytes::Bytes;
use futures::{Stream, StreamExt, pin_mut};
use std::ops::Bound;
use std::sync::Arc;
use tracing::warn;

/// A fjall coverage cache over a lower committed [`CellStore`].
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

    /// Best-effort fjall point invalidation: a failure leaves a possibly-stale
    /// entry, but the coordinate was already punched out of coverage, so the
    /// next read reaches the lower store — log and degrade.
    async fn invalidate(&self, collection: &CollectionId, cell: &CellKey) {
        if let Err(error) = self.fjall.invalidate(collection, cell).await {
            warn!(error = %error, "committed-value cache invalidation failed; may be stale");
        }
    }

    /// The single mutator choke point (Cov3): punch each touched coordinate out
    /// of coverage (infallible, in-memory), then best-effort evict its fjall
    /// point. Called **before** the fallible `lower.write` in every mutator.
    async fn invalidate_touched<'i, I>(&self, collection: &CollectionId, cells: I)
    where
        I: Iterator<Item = &'i CellKey>,
    {
        for cell in cells {
            self.coverage
                .punch(collection, cell.section, &cell.coordinate)
                .await;
            self.invalidate(collection, cell).await;
        }
    }
}

impl<L> Cached<L>
where
    L: CellStore,
{
    /// Serves one covered piece from fjall, falling through to the lower store
    /// for the remainder on a fjall read error (degradation is always a slower
    /// fall-through, never a wrong answer). Ignores `own` on the covered serve
    /// (Cov2) but forwards it on the fall-through gap read.
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
            let mut errored = false;
            while let Some(item) = covered.next().await {
                match item {
                    Ok((cell, bytes)) => {
                        last = Some(cell.coordinate.clone());
                        yield (cell, bytes);
                    }
                    Err(error) => {
                        warn_skip("covered scan", &error);
                        errored = true;
                        break;
                    }
                }
            }
            if errored && let Some(remainder) = remainder_after(piece, dir, last.as_ref()) {
                let scan = scan_for_piece(section, &remainder, dir);
                let inner = self.lower.scan_cells(collection, scan, own);
                pin_mut!(inner);
                while let Some(item) = inner.next().await {
                    yield item?;
                }
            }
        }
    }

    /// Serves one gap piece from the lower store, populating each yielded cell
    /// into fjall (best-effort) and **covering on consume**: coverage extends
    /// only over the cells actually populated, contiguously from the gap edge.
    /// The whole gap interval is covered only if the scan **exhausts** with no
    /// populate failure — a `limit`/early-drop or a failed populate caps
    /// coverage at the last good cell, so a covered re-scan can never serve a
    /// populated-but-uncovered present cell as absent.
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
            let inner = self.lower.scan_cells(collection, scan, own);
            pin_mut!(inner);
            let mut contiguous = true;
            while let Some(item) = inner.next().await {
                let (cell, bytes) = item?;
                let populated = match self
                    .fjall
                    .put(collection, &cell, &Committed::new(Some(bytes.clone())))
                    .await
                {
                    Ok(()) => true,
                    Err(error) => {
                        warn_skip("populate", &error);
                        false
                    }
                };
                // Cover up to (and including) this cell only while the run from
                // the gap edge is unbroken; a failed populate stops extension.
                if contiguous && populated {
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
        match self.fjall.get(collection, cell).await {
            Ok(Some(committed)) => return Ok(committed),
            Ok(None) => {
                // A covered miss is genuine absence (Cov2): the coordinate is a
                // prior-committed projection and fjall holds no Present/Absent
                // entry, so it is absent — answer with zero lower reads.
                if self
                    .coverage
                    .covers(collection, cell.section, &cell.coordinate)
                    .await
                {
                    return Ok(Committed::new(None));
                }
            }
            Err(error) => warn_skip("read", &error),
        }
        let committed = self.lower.get(collection, cell, own).await?;
        if let Err(error) = self.fjall.put(collection, cell, &committed).await {
            warn_skip("populate", &error);
        }
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
        self.invalidate_touched(collection.id(), writes.iter().map(cell_of))
            .await;
        self.lower.write_provisional(collection, writes).await
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
    ) -> Result<(), Self::Error> {
        self.invalidate_touched(collection.id(), cells.iter().map(cell_of))
            .await;
        self.lower.write_resolved(collection, cells).await
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        self.invalidate_touched(collection.id(), cells.iter()).await;
        self.lower.mark_resolved(collection, cells).await
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

/// The covered piece's sub-range still unserved after a fjall error at `last`:
/// `(last, high]` forward, `[low, last)` backward, or the whole piece when the
/// error came before the first cell. `None` when nothing remains.
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

/// Extracts the [`CellKey`] from a `(CellKey, _)` batch entry. A named function
/// (not a closure) so its higher-ranked lifetime unifies with
/// `invalidate_touched`'s borrowed-iterator bound.
fn cell_of<T>(entry: &(CellKey, T)) -> &CellKey {
    &entry.0
}

/// Logs a degraded fjall cache operation (the cache is a hint).
fn warn_skip(op: &str, error: &super::fjall::FjallCellCacheError) {
    warn!(error = %error, "committed-value cache {op} failed; degrading");
}

#[cfg(test)]
mod tests;
