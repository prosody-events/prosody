//! Query settings shared by handler, standalone, and erased reads.

mod bounds;
mod deque;
mod read;
#[cfg(test)]
pub(crate) mod tests;

pub(crate) use bounds::Query;
pub use deque::DequeQuery;
pub use read::{DequeRead, KeyRead, ReadQuery, ReadSource};

use crate::state::Direction;
use crate::state::order_codec::{OrderedKeyCodec, Utf8KeyCodec};
use educe::Educe;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::mem::swap;
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};

/// Map or set query settings with caller-selected bound storage.
/// Typed collection builders borrow bounds. The erased specialization owns
/// strings. The codec must match the collection. Encoding starts when the
/// stream is polled.
///
/// Typed reads require reusable encoding storage. Allocate it before the hot
/// loop. The stream retains that storage. Encoding rejects insufficient
/// capacity before it writes bytes; it never grows the supplied buffer.
/// Use [`Self::required_capacity`] to size storage for saved settings.
///
/// Forward order is the default. Direction changes preserve selected bounds.
/// Edges follow the query direction. Each bound method replaces one edge.
/// A start past the end produces an empty stream.
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
    start: Edge<B>,
    end: Edge<B>,
    #[serde(skip)]
    codec: PhantomData<fn() -> KC>,
}

/// Query settings that borrow keys through the codec's input view.
pub type BorrowedKeyQuery<'a, KC = Utf8KeyCodec> =
    KeyQuery<KC, &'a <KC as OrderedKeyCodec>::Borrowed>;

/// The owned string query that foreign-language clients wrap.
pub type ErasedKeyQuery = KeyQuery<Utf8KeyCodec, String>;

/// A logical edge. A prefix endpoint stays logical until the codec writes it.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum Edge<B> {
    Bound(Bound<B>),
    PrefixEnd(B),
}

impl<KC: OrderedKeyCodec, B> KeyQuery<KC, B> {
    /// Creates an unbounded query in forward order.
    pub fn new() -> Self {
        Self {
            dir: Direction::Forward,
            limit: None,
            start: Edge::Bound(Bound::Unbounded),
            end: Edge::Bound(Bound::Unbounded),
            codec: PhantomData,
        }
    }

    /// Selects ascending key order.
    pub fn forward(self) -> Self {
        self.direction(Direction::Forward)
    }

    /// Selects descending key order. Repeated calls keep this order.
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

    /// Limits present results. Absent cells do not consume the limit.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Starts at `key`.
    pub fn from<K: Into<B>>(mut self, key: K) -> Self {
        self.start = Edge::Bound(Bound::Included(key.into()));
        self
    }

    /// Starts after `key`.
    pub fn after<K: Into<B>>(mut self, key: K) -> Self {
        self.start = Edge::Bound(Bound::Excluded(key.into()));
        self
    }

    /// Stops at `key`.
    pub fn to<K: Into<B>>(mut self, key: K) -> Self {
        self.end = Edge::Bound(Bound::Included(key.into()));
        self
    }

    /// Stops before `key`.
    pub fn before<K: Into<B>>(mut self, key: K) -> Self {
        self.end = Edge::Bound(Bound::Excluded(key.into()));
        self
    }

    /// Replaces both edges with ascending bounds, in either direction.
    pub fn range<R: RangeBounds<B>>(mut self, range: R) -> Self
    where
        B: Clone,
    {
        let low = Edge::Bound(range.start_bound().cloned());
        let high = Edge::Bound(range.end_bound().cloned());
        (self.start, self.end) = match self.dir {
            Direction::Forward => (low, high),
            Direction::Backward => (high, low),
        };
        self
    }

    /// Selects keys with this encoded prefix, replacing both edges.
    /// Later bound methods can replace an edge and expand the prefix range.
    pub fn prefix<K: Into<B>>(mut self, key: K) -> Self
    where
        B: Clone,
    {
        let key = key.into();
        let low = Edge::Bound(Bound::Included(key.clone()));
        let high = Edge::PrefixEnd(key);
        (self.start, self.end) = match self.dir {
            Direction::Forward => (low, high),
            Direction::Backward => (high, low),
        };
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
            start: self.start.borrowed(),
            end: self.end.borrowed(),
            codec: PhantomData,
        }
    }
}

impl<B> Edge<B> {
    fn borrowed<Q: ?Sized>(&self) -> Edge<&Q>
    where
        B: Borrow<Q>,
    {
        match self {
            Self::Bound(bound) => Edge::Bound(bound.as_ref().map(Borrow::borrow)),
            Self::PrefixEnd(key) => Edge::PrefixEnd(key.borrow()),
        }
    }
}

impl<KC: OrderedKeyCodec, B> Default for KeyQuery<KC, B> {
    fn default() -> Self {
        Self::new()
    }
}
