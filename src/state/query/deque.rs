//! Queries over deque positions, counted from the front.

use super::edges::Edges;
use crate::state::cell_key::Direction;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};

/// An owned deque query shared by all read APIs.
/// Forward order is the default. Positions count from the front. Bound
/// methods narrow the selection and never widen it, as on
/// [`KeyQuery`](super::KeyQuery). Deque positions have no prefix operation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[must_use]
pub struct DequeQuery {
    pub(crate) dir: Direction,
    edges: Edges<usize>,
    pub(crate) limit: Option<NonZeroUsize>,
}

impl DequeQuery {
    /// Creates an unbounded query in forward order.
    pub fn new() -> Self {
        Self {
            dir: Direction::Forward,
            edges: Edges::unbounded(),
            limit: None,
        }
    }

    /// Selects order from front to back.
    pub fn forward(self) -> Self {
        self.direction(Direction::Forward)
    }

    /// Selects order from back to front.
    pub fn reverse(self) -> Self {
        self.direction(Direction::Backward)
    }

    /// Selects an order supplied at runtime.
    pub fn direction(mut self, dir: Direction) -> Self {
        self.dir = dir;
        self
    }

    /// Starts at `position` in query order.
    pub fn from(mut self, position: usize) -> Self {
        self.edges.start(self.dir, Bound::Included(position));
        self
    }

    /// Starts after `position` in query order.
    pub fn after(mut self, position: usize) -> Self {
        self.edges.start(self.dir, Bound::Excluded(position));
        self
    }

    /// Stops at `position` in query order.
    pub fn to(mut self, position: usize) -> Self {
        self.edges.end(self.dir, Bound::Included(position));
        self
    }

    /// Stops before `position` in query order.
    pub fn before(mut self, position: usize) -> Self {
        self.edges.end(self.dir, Bound::Excluded(position));
        self
    }

    /// Keeps positions within an ascending range, in either direction.
    pub fn range<R: RangeBounds<usize>>(mut self, range: R) -> Self {
        self.edges.range(&range);
        self
    }

    /// Limits present results. Absent positions do not consume the limit.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Returns the ascending `(low, high)` bounds.
    pub(crate) fn bounds(self) -> (Bound<usize>, Bound<usize>) {
        (self.edges.low, self.edges.high)
    }
}

impl Default for DequeQuery {
    fn default() -> Self {
        Self::new()
    }
}
