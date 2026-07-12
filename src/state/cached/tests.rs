//! `remainder_after` arithmetic — the covered-scan fall-through tail.
//!
//! When `serve_covered` hits a fjall read error after yielding up to `last`, it
//! falls through to the lower store for the *unserved* tail of the piece. That
//! tail's bounds are direction-sensitive (Forward keeps `(last, hi]`, Backward
//! keeps `[lo, last)`), and an error at the piece's far endpoint leaves
//! nothing. The error path never fires in the answer-vs-oracle suites (fjall
//! does not error there), so this pins the bound arithmetic directly — the same
//! shape of direction/exclusivity logic that already needed a fix once in
//! `query`.

use super::coverage::Interval;
use super::{Cached, Direction, GAP_COVER_STRIDE, remainder_after};
use crate::state::cell_key::{CellKey, Coordinate, Scan, ScanEdge, Section};
use crate::state::event_ref::EventRef;
use crate::state::fjall::test_db;
use crate::state::identity::{CollectionId, CollectionRef, StateKey, StateName, StateType};
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::registry::CollectionDefRegistry;
use crate::state::store::CellStore;
use crate::state::tests::cell_suite::{ScriptedOracle, bytes, cell_at};
use crate::state::tests::support::CountingCellStore;
use crate::test_util::TEST_RUNTIME;
use ::bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::StreamExt;
use std::ops::Bound;
use std::sync::Arc;
use uuid::Uuid;

/// A single-byte coordinate.
fn c(b: u8) -> Coordinate {
    Coordinate::from_bytes(vec![b])
}

/// A non-empty interval (the test endpoints are statically non-empty).
fn iv(lo: Bound<Coordinate>, hi: Bound<Coordinate>) -> Result<Interval> {
    Interval::new(lo, hi).ok_or_else(|| eyre!("test interval is non-empty"))
}

/// Forward: the tail after `last` is `(last, hi]` — the far (high) end is kept.
#[test]
fn forward_keeps_the_high_tail_after_last() -> Result<()> {
    let piece = iv(Bound::Included(c(0)), Bound::Included(c(10)))?;
    assert_eq!(
        remainder_after(&piece, Direction::Forward, Some(&c(3))),
        Some(iv(Bound::Excluded(c(3)), Bound::Included(c(10)))?)
    );
    Ok(())
}

/// Backward: the tail after `last` is `[lo, last)` — the far (low) end is kept.
#[test]
fn backward_keeps_the_low_tail_after_last() -> Result<()> {
    let piece = iv(Bound::Included(c(0)), Bound::Included(c(10)))?;
    assert_eq!(
        remainder_after(&piece, Direction::Backward, Some(&c(3))),
        Some(iv(Bound::Included(c(0)), Bound::Excluded(c(3)))?)
    );
    Ok(())
}

/// An error before the first cell (`last == None`) re-serves the whole piece.
#[test]
fn no_last_re_serves_the_whole_piece() -> Result<()> {
    let piece = iv(Bound::Included(c(0)), Bound::Excluded(c(10)))?;
    assert_eq!(
        remainder_after(&piece, Direction::Forward, None),
        Some(piece.clone())
    );
    assert_eq!(
        remainder_after(&piece, Direction::Backward, None),
        Some(piece)
    );
    Ok(())
}

/// An error *at* the piece's far endpoint leaves an empty tail — nothing to
/// re-serve (`None`), so the fall-through scan is skipped.
#[test]
fn error_at_the_far_endpoint_leaves_no_remainder() -> Result<()> {
    let piece = iv(Bound::Included(c(0)), Bound::Included(c(10)))?;
    // Forward consumed through the high endpoint 10 → `(10, 10]` is empty.
    assert_eq!(
        remainder_after(&piece, Direction::Forward, Some(&c(10))),
        None
    );
    // Backward consumed through the low endpoint 0 → `[0, 0)` is empty.
    assert_eq!(
        remainder_after(&piece, Direction::Backward, Some(&c(0))),
        None
    );
    Ok(())
}

/// Drains up to `take` cells of a `dir` scan from `start` to `end`, mapping
/// each to its coordinate byte; `take = usize::MAX` drains to exhaustion.
async fn scan_take<L>(
    cached: &Cached<L>,
    id: &CollectionId,
    start: ScanEdge<u8>,
    dir: Direction,
    end: ScanEdge<u8>,
    take: usize,
) -> Result<Vec<u8>>
where
    L: CellStore,
{
    let start = start.map(c);
    let end = end.map(c);
    let scan = Scan {
        section: Section::new(0),
        start: start.as_ref(),
        dir,
        end: end.as_ref(),
        limit: None,
    };
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(9),
    };
    let stream = cached.scan_cells(id, scan, probe);
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while out.len() < take
        && let Some(item) = stream.next().await
    {
        let (key, _) = item?;
        out.push(key.coordinate.as_bytes()[0]);
    }
    Ok(out)
}

/// The gap-fill frontier: a gap scan dropped mid-stream (after more than one
/// [`GAP_COVER_STRIDE`] but before exhaustion) must leave the *persisted*
/// frontier covered — a re-scan of that prefix issues **zero** lower scans —
/// while everything past the frontier stays a gap: the full re-scan still
/// yields every cell (falling through for the tail), proving the frontier
/// under-covers on early drop but never over-covers.
///
/// Run in both directions: `cover_consumed`'s frontier arithmetic branches on
/// direction (forward covers `[gap_lo, X]`, backward `[X, gap_hi]`), and
/// production reaches the Backward arm via wide-window Deque reverse iteration.
/// The answer-vs-oracle suites never trip the `GAP_COVER_STRIDE` early-drop
/// window, so this pins both arms directly.
async fn gap_frontier_case(dir: Direction) -> Result<()> {
    // More than two strides of cells, consumed one stride + a partial.
    const TOTAL: usize = 2 * GAP_COVER_STRIDE + 22;
    let consumed = GAP_COVER_STRIDE + 10;

    let counting = CountingCellStore::new(MemoryCellStore::new(
        MemoryCells::new(),
        ScriptedOracle::default(),
        Arc::new(CollectionDefRegistry::default()),
    ));
    let id = CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("k")),
        StateType::Application,
        StateName::try_new("frontier")?,
    );
    let cref = CollectionRef::new(id.clone(), None);

    // Seed the LOWER store directly (bypassing the cache) so the whole
    // section is one uncovered gap.
    let mut seed: Vec<(CellKey, Option<Bytes>)> = Vec::with_capacity(TOTAL);
    for i in 0..TOTAL {
        seed.push((cell_at(u8::try_from(i)?), Some(bytes(1))));
    }
    counting.write_resolved(&cref, &seed, &[]).await?;

    let cached = Cached::new(test_db::cache("frontier")?, counting.clone());

    // The gap fill covers contiguously from the scan's leading edge: forward
    // fills from coordinate 0 upward, backward from the high edge 255 downward.
    // `boundary` is the last coordinate of the first fully persisted stride —
    // the 10 cells consumed past it were pending and lost with the drop. The
    // covered prefix `[start, boundary]` (direction-relative) must then serve
    // with zero lower scans.
    let (start, end, boundary) = match dir {
        Direction::Forward => (
            ScanEdge::Included(0u8),
            ScanEdge::Included(255u8),
            ScanEdge::Included(u8::try_from(GAP_COVER_STRIDE - 1)?),
        ),
        Direction::Backward => (
            ScanEdge::Included(255u8),
            ScanEdge::Included(0u8),
            ScanEdge::Included(u8::try_from(TOTAL - GAP_COVER_STRIDE)?),
        ),
    };

    // Drain part of the gap fill, then drop the stream mid-piece.
    let partial = scan_take(&cached, &id, start, dir, end, consumed).await?;
    assert_eq!(partial.len(), consumed);

    // The persisted frontier serves covered: zero lower scans.
    counting.reset();
    let prefix = scan_take(&cached, &id, start, dir, boundary, usize::MAX).await?;
    assert_eq!(prefix.len(), GAP_COVER_STRIDE);
    assert_eq!(
        counting.lower_scans(),
        0,
        "{dir:?}: the stride-persisted frontier must serve covered"
    );

    // Never over-covered: the full re-scan still yields EVERY cell — the
    // unpersisted tail reads as a gap and falls through, so nothing past
    // the frontier is served as covered-absent.
    counting.reset();
    let all = scan_take(&cached, &id, start, dir, end, usize::MAX).await?;
    assert_eq!(
        all.len(),
        TOTAL,
        "{dir:?}: no cell past the frontier may be lost"
    );
    assert!(
        counting.lower_scans() > 0,
        "{dir:?}: the unpersisted tail must fall through to the lower store"
    );
    Ok(())
}

#[test]
fn gap_frontier_persists_mid_stream_and_never_over_covers_forward() -> Result<()> {
    TEST_RUNTIME.block_on(gap_frontier_case(Direction::Forward))
}

#[test]
fn gap_frontier_persists_mid_stream_and_never_over_covers_backward() -> Result<()> {
    TEST_RUNTIME.block_on(gap_frontier_case(Direction::Backward))
}
