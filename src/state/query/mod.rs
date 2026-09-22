//! Query settings shared by handler, standalone, and erased reads.

mod bounds;
mod deque;
mod edges;
mod read;
#[cfg(test)]
pub(crate) mod tests;

pub(crate) use bounds::Query;
pub use deque::DequeQuery;
pub use read::{DequeRead, KeyRead, ReadQuery, ReadSource};

use crate::state::Direction;
use crate::state::order_codec::{OrderedKeyCodec, PrefixKeyCodec, Utf8KeyCodec};
use edges::Edges;
use educe::Educe;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};

/// Map or set query settings with caller-selected bound storage.
/// Typed collection builders borrow bounds. The erased specialization owns
/// strings. The codec must match the collection. Encoding starts when the
/// stream is polled.
///
/// Execution reuses a thread-local encoding buffer. Each live stream retains
/// its buffer. Sequential reads reuse its capacity after the stream drops.
/// A cold pool, larger bounds, or overlapping streams can require allocation.
///
/// Forward order is the default. Bound and prefix methods narrow the
/// selection and never widen it, so their order does not change the selected
/// keys. `from`, `after`, `to`, and `before` use the direction set before the
/// call. A later direction change keeps the selection and changes only the
/// order. A start past the end produces an empty stream.
#[derive(Educe, Serialize, Deserialize)]
#[serde(bound(serialize = "B: Serialize", deserialize = "B: Deserialize<'de>"))]
#[educe(
    Clone(bound = "B: Clone"),
    Copy(bound = "B: Copy"),
    Debug(bound = "B: std::fmt::Debug")
)]
#[must_use]
pub struct KeyQuery<KC: OrderedKeyCodec = Utf8KeyCodec, B = <KC as OrderedKeyCodec>::Key> {
    pub(crate) dir: Direction,
    pub(crate) limit: Option<NonZeroUsize>,
    edges: Edges<B>,
    prefix: Option<B>,
    #[serde(skip)]
    codec: PhantomData<fn() -> KC>,
}

/// Query settings that borrow keys through the codec's input view.
pub type BorrowedKeyQuery<'a, KC = Utf8KeyCodec> =
    KeyQuery<KC, &'a <KC as OrderedKeyCodec>::Borrowed>;

/// The owned string query that foreign-language clients wrap.
pub type ErasedKeyQuery = KeyQuery<Utf8KeyCodec, String>;

impl<KC: OrderedKeyCodec, B> KeyQuery<KC, B> {
    /// Creates an unbounded query in forward order.
    pub fn new() -> Self {
        Self {
            dir: Direction::Forward,
            limit: None,
            edges: Edges::unbounded(),
            prefix: None,
            codec: PhantomData,
        }
    }

    /// Selects ascending key order.
    pub fn forward(self) -> Self {
        self.direction(Direction::Forward)
    }

    /// Selects descending key order.
    pub fn reverse(self) -> Self {
        self.direction(Direction::Backward)
    }

    /// Selects an order supplied at runtime.
    pub fn direction(mut self, dir: Direction) -> Self {
        self.dir = dir;
        self
    }

    /// Limits present results. Absent cells do not consume the limit.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Starts at `key` in query order.
    pub fn from<K: Into<B>>(mut self, key: K) -> Self
    where
        B: Ord,
    {
        self.edges.start(self.dir, Bound::Included(key.into()));
        self
    }

    /// Starts after `key` in query order.
    pub fn after<K: Into<B>>(mut self, key: K) -> Self
    where
        B: Ord,
    {
        self.edges.start(self.dir, Bound::Excluded(key.into()));
        self
    }

    /// Stops at `key` in query order.
    pub fn to<K: Into<B>>(mut self, key: K) -> Self
    where
        B: Ord,
    {
        self.edges.end(self.dir, Bound::Included(key.into()));
        self
    }

    /// Stops before `key` in query order.
    pub fn before<K: Into<B>>(mut self, key: K) -> Self
    where
        B: Ord,
    {
        self.edges.end(self.dir, Bound::Excluded(key.into()));
        self
    }

    /// Keeps keys within an ascending range, in either direction.
    pub fn range<R: RangeBounds<B>>(mut self, range: R) -> Self
    where
        B: Ord + Clone,
    {
        self.edges.range(&range);
        self
    }

    /// Keeps keys that start with `key`.
    pub fn prefix<K: Into<B>>(mut self, key: K) -> Self
    where
        KC: PrefixKeyCodec,
        B: Borrow<KC::Borrowed> + Ord,
    {
        let prefix = key.into();
        match &self.prefix {
            Some(current) if KC::starts_with(current.borrow(), prefix.borrow()) => {}
            Some(current) if !KC::starts_with(prefix.borrow(), current.borrow()) => {
                // Disjoint prefixes select no keys. The new prefix lies wholly
                // below or above the current prefix range. As an edge on that
                // side, it empties the range.
                if &prefix < current {
                    self.edges.narrow_high(Bound::Excluded(prefix));
                } else {
                    self.edges.narrow_low(Bound::Included(prefix));
                }
            }
            _ => self.prefix = Some(prefix),
        }
        self
    }

    /// Borrows the stored bounds without encoding or copying them.
    pub fn borrowed(&self) -> KeyQuery<KC, &KC::Borrowed>
    where
        B: Borrow<KC::Borrowed>,
    {
        KeyQuery {
            dir: self.dir,
            limit: self.limit,
            edges: Edges {
                low: self.edges.low.as_ref().map(Borrow::borrow),
                high: self.edges.high.as_ref().map(Borrow::borrow),
            },
            prefix: self.prefix.as_ref().map(Borrow::borrow),
            codec: PhantomData,
        }
    }
}

impl<KC: OrderedKeyCodec, B> Default for KeyQuery<KC, B> {
    fn default() -> Self {
        Self::new()
    }
}
