//! Encoded map and set bounds and ordered coordinate selection.

use crate::state::cell_key::{Coordinate, Direction, ScanEdge};
use std::num::NonZeroUsize;

/// The encoded bounds, direction, and result limit of a map or set query.
#[derive(Clone, Debug)]
pub(crate) struct Query {
    pub(crate) dir: Direction,
    pub(crate) limit: Option<NonZeroUsize>,
    pub(crate) start: ScanEdge<Coordinate>,
    pub(crate) end: ScanEdge<Coordinate>,
}

impl Query {
    pub(crate) fn new(dir: Direction) -> Self {
        Self {
            dir,
            limit: None,
            start: ScanEdge::Unbounded,
            end: ScanEdge::Unbounded,
        }
    }

    /// Sets both edges to select coordinates that start with `low`.
    pub(crate) fn prefix(&mut self, low: Coordinate) {
        let high = low
            .prefix_end()
            .map_or(ScanEdge::Unbounded, ScanEdge::Excluded);
        let low = ScanEdge::Included(low);
        (self.start, self.end) = match self.dir {
            Direction::Forward => (low, high),
            Direction::Backward => (high, low),
        };
    }

    /// Keeps the ascending stored coordinates within the query bounds, in
    /// query order. The trim reuses the stored vector.
    pub(crate) fn select(&self, mut coordinates: Vec<Coordinate>) -> Vec<Coordinate> {
        let (low, high) = match self.dir {
            Direction::Forward => (&self.start, &self.end),
            Direction::Backward => (&self.end, &self.start),
        };
        let start = match low {
            ScanEdge::Included(edge) => coordinates.partition_point(|c| c < edge),
            ScanEdge::Excluded(edge) => coordinates.partition_point(|c| c <= edge),
            ScanEdge::Unbounded => 0,
        };
        let end = match high {
            ScanEdge::Included(edge) => coordinates.partition_point(|c| c <= edge),
            ScanEdge::Excluded(edge) => coordinates.partition_point(|c| c < edge),
            ScanEdge::Unbounded => coordinates.len(),
        };
        coordinates.truncate(end.max(start));
        coordinates.drain(..start);
        if self.dir == Direction::Backward {
            coordinates.reverse();
        }
        coordinates
    }
}
