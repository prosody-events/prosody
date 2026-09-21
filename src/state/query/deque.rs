//! Queries over deque positions, counted from the front.

use crate::state::cell_key::Direction;
use serde::{Deserialize, Serialize};
use std::mem::swap;
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};

/// An owned deque query shared by all read APIs.
/// Forward order is the default. Direction changes preserve the selected
/// bounds. Positions count from the front. Edges follow the query direction.
/// Each bound method replaces one edge. A start past the end returns no values.
/// Deque positions have no prefix operation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[must_use]
pub struct DequeQuery {
    pub(crate) dir: Direction,
    start: Bound<usize>,
    end: Bound<usize>,
    pub(crate) limit: Option<NonZeroUsize>,
}

impl DequeQuery {
    /// Creates an unbounded query in forward order.
    pub fn new() -> Self {
        Self {
            dir: Direction::Forward,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            limit: None,
        }
    }

    /// Selects order from front to back.
    pub fn forward(self) -> Self {
        self.direction(Direction::Forward)
    }

    /// Selects order from back to front. Repeated calls keep this order.
    pub fn reverse(self) -> Self {
        self.direction(Direction::Backward)
    }

    /// Selects an order supplied at runtime.
    pub fn direction(mut self, dir: Direction) -> Self {
        if self.dir != dir {
            swap(&mut self.start, &mut self.end);
            self.dir = dir;
        }
        self
    }

    /// Starts at `position`.
    pub fn from(mut self, position: usize) -> Self {
        self.start = Bound::Included(position);
        self
    }

    /// Starts after `position`.
    pub fn after(mut self, position: usize) -> Self {
        self.start = Bound::Excluded(position);
        self
    }

    /// Stops at `position`.
    pub fn to(mut self, position: usize) -> Self {
        self.end = Bound::Included(position);
        self
    }

    /// Stops before `position`.
    pub fn before(mut self, position: usize) -> Self {
        self.end = Bound::Excluded(position);
        self
    }

    /// Replaces both edges with an ascending position range, in either
    /// direction.
    pub fn range<R: RangeBounds<usize>>(mut self, range: R) -> Self {
        let low = range.start_bound().cloned();
        let high = range.end_bound().cloned();
        (self.start, self.end) = match self.dir {
            Direction::Forward => (low, high),
            Direction::Backward => (high, low),
        };
        self
    }

    /// Limits present results. Absent positions do not consume the limit.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub(crate) fn bounds(self) -> (Bound<usize>, Bound<usize>) {
        match self.dir {
            Direction::Forward => (self.start, self.end),
            Direction::Backward => (self.end, self.start),
        }
    }
}

impl Default for DequeQuery {
    fn default() -> Self {
        Self::new()
    }
}
