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
use super::{Direction, remainder_after};
use crate::state::cell_key::Coordinate;
use color_eyre::eyre::{Result, eyre};
use std::ops::Bound;

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
