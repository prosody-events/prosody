//! Owned query values shared by handler, standalone, and erased reads.

mod bounds;
mod deque;
#[cfg(test)]
pub(crate) mod tests;

pub(crate) use bounds::Query;
pub use deque::DequeQuery;

use crate::state::cell_key::{Direction, ScanEdge};
use crate::state::order_codec::{OrderedKeyCodec, Utf8KeyCodec};
use educe::Educe;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};

/// An owned map or set query with no collection, session, or backend.
/// The key codec prevents use with a collection that has a different encoding.
/// Handlers and readers accept the same query through `entries` and `keys`.
///
/// Edges follow the query direction. Each bound method replaces one edge.
/// A start past the end produces an empty stream.
#[derive(Educe)]
#[educe(Clone, Debug)]
#[must_use]
pub struct KeyQuery<KC = Utf8KeyCodec> {
    pub(crate) encoded: Query,
    codec: PhantomData<fn() -> KC>,
}

/// The string query that foreign-language clients wrap.
pub type ErasedKeyQuery = KeyQuery<Utf8KeyCodec>;

impl<KC> KeyQuery<KC> {
    /// Creates an unbounded query in `dir` order.
    pub fn new(dir: Direction) -> Self {
        Self {
            encoded: Query::new(dir),
            codec: PhantomData,
        }
    }

    /// Limits present results. Absent cells do not consume the limit.
    /// The limit also bounds the first fetch and its error boundary.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.encoded.limit = Some(limit);
        self
    }
}

impl<KC: OrderedKeyCodec> KeyQuery<KC> {
    /// Starts at `key`.
    pub fn from(mut self, key: &KC::Borrowed) -> Self {
        self.encoded.start = ScanEdge::Included(KC::encode(key));
        self
    }

    /// Starts after `key`.
    pub fn after(mut self, key: &KC::Borrowed) -> Self {
        self.encoded.start = ScanEdge::Excluded(KC::encode(key));
        self
    }

    /// Stops at `key`.
    pub fn to(mut self, key: &KC::Borrowed) -> Self {
        self.encoded.end = ScanEdge::Included(KC::encode(key));
        self
    }

    /// Stops before `key`.
    pub fn before(mut self, key: &KC::Borrowed) -> Self {
        self.encoded.end = ScanEdge::Excluded(KC::encode(key));
        self
    }

    /// Replaces both edges with an ascending key range, in either direction.
    pub fn range<R: RangeBounds<KC::Borrowed>>(mut self, range: R) -> Self {
        let edge = |bound| match bound {
            Bound::Included(key) => ScanEdge::Included(KC::encode(key)),
            Bound::Excluded(key) => ScanEdge::Excluded(KC::encode(key)),
            Bound::Unbounded => ScanEdge::Unbounded,
        };
        let low = edge(range.start_bound());
        let high = edge(range.end_bound());
        (self.encoded.start, self.encoded.end) = match self.encoded.dir {
            Direction::Forward => (low, high),
            Direction::Backward => (high, low),
        };
        self
    }

    /// Replaces both edges to select keys with this encoded prefix.
    /// For fixed-width keys, the range contains only that key.
    /// A later cursor must start with the prefix to stay within this range.
    /// A cursor outside the prefix replaces its edge and can expand the range.
    pub fn prefix(mut self, key: &KC::Borrowed) -> Self {
        self.encoded.prefix(KC::encode(key));
        self
    }
}

impl<KC> Default for KeyQuery<KC> {
    fn default() -> Self {
        Self::new(Direction::Forward)
    }
}
