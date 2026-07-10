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
use crate::state::tests::cell_suite::{CountingCellStore, ScriptedOracle, bytes, cell_at};
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

/// Drains up to `take` cells of a forward scan over `[0, end]`, mapping each
/// to its coordinate byte; `take = usize::MAX` drains to exhaustion.
async fn scan_take<L>(
    cached: &Cached<L>,
    id: &CollectionId,
    end: ScanEdge<u8>,
    take: usize,
) -> Result<Vec<u8>>
where
    L: CellStore,
{
    let start = c(0);
    let end = end.map(c);
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
#[test]
fn gap_frontier_persists_mid_stream_and_never_over_covers() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        // More than two strides of cells, consumed one stride + a partial.
        const TOTAL: usize = 2 * GAP_COVER_STRIDE + 22;
        let consumed = GAP_COVER_STRIDE + 10;
        // The last coordinate whose cover was persisted (stride boundary);
        // the 10 consumed past it were pending and are lost with the drop.
        let boundary = u8::try_from(GAP_COVER_STRIDE - 1)?;

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

        // Drain part of the gap fill, then drop the stream mid-piece.
        let partial = scan_take(&cached, &id, ScanEdge::Included(255), consumed).await?;
        assert_eq!(partial.len(), consumed);

        // The persisted frontier `[0, boundary]` serves covered: zero lower
        // scans.
        counting.reset();
        let prefix = scan_take(&cached, &id, ScanEdge::Included(boundary), usize::MAX).await?;
        assert_eq!(prefix.len(), GAP_COVER_STRIDE);
        assert_eq!(
            counting.lower_scans(),
            0,
            "the stride-persisted frontier must serve covered"
        );

        // Never over-covered: the full re-scan still yields EVERY cell — the
        // unpersisted tail reads as a gap and falls through, so nothing past
        // the frontier is served as covered-absent.
        counting.reset();
        let all = scan_take(&cached, &id, ScanEdge::Included(255), usize::MAX).await?;
        assert_eq!(all.len(), TOTAL, "no cell past the frontier may be lost");
        assert!(
            counting.lower_scans() > 0,
            "the unpersisted tail must fall through to the lower store"
        );
        Ok(())
    })
}
