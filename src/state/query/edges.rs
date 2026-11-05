//! Ascending query edges that only narrow.

use crate::state::Direction;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::ops::{Bound, RangeBounds};

/// The ascending edges of a query range.
///
/// Every change intersects the range with a new bound. The range never
/// widens, so the order of changes does not change the selection. A low edge
/// above the high edge selects nothing. `B` must order keys as the codec
/// orders their bytes.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct Edges<B> {
    pub(crate) low: Bound<B>,
    pub(crate) high: Bound<B>,
}

impl<B> Edges<B> {
    /// Selects every key.
    pub(crate) const fn unbounded() -> Self {
        Self {
            low: Bound::Unbounded,
            high: Bound::Unbounded,
        }
    }

    /// Narrows the edge where a walk in `dir` starts.
    pub(crate) fn start(&mut self, dir: Direction, bound: Bound<B>)
    where
        B: Ord,
    {
        match dir {
            Direction::Forward => self.narrow_low(bound),
            Direction::Backward => self.narrow_high(bound),
        }
    }

    /// Narrows the edge where a walk in `dir` ends.
    pub(crate) fn end(&mut self, dir: Direction, bound: Bound<B>)
    where
        B: Ord,
    {
        match dir {
            Direction::Forward => self.narrow_high(bound),
            Direction::Backward => self.narrow_low(bound),
        }
    }

    /// Narrows both edges to an ascending range.
    pub(crate) fn range<R: RangeBounds<B>>(&mut self, range: &R)
    where
        B: Ord + Clone,
    {
        self.narrow_low(range.start_bound().cloned());
        self.narrow_high(range.end_bound().cloned());
    }

    /// Raises the low edge when `bound` selects fewer keys.
    pub(crate) fn narrow_low(&mut self, bound: Bound<B>)
    where
        B: Ord,
    {
        if low_rank(&bound) > low_rank(&self.low) {
            self.low = bound;
        }
    }

    /// Lowers the high edge when `bound` selects fewer keys.
    pub(crate) fn narrow_high(&mut self, bound: Bound<B>)
    where
        B: Ord,
    {
        if high_rank(&bound) > high_rank(&self.high) {
            self.high = bound;
        }
    }
}

/// Ranks a low edge. A higher rank selects fewer keys.
fn low_rank<B>(bound: &Bound<B>) -> Option<(&B, bool)> {
    match bound {
        Bound::Included(key) => Some((key, false)),
        Bound::Excluded(key) => Some((key, true)),
        Bound::Unbounded => None,
    }
}

/// Ranks a high edge. A higher rank selects fewer keys.
fn high_rank<B>(bound: &Bound<B>) -> Option<Reverse<(&B, bool)>> {
    match bound {
        Bound::Included(key) => Some(Reverse((key, true))),
        Bound::Excluded(key) => Some(Reverse((key, false))),
        Bound::Unbounded => None,
    }
}
