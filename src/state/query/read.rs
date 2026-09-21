//! Fluent query settings bound to a read source.

use super::{BorrowedKeyQuery, DequeQuery, KeyQuery};
use crate::state::Direction;
use crate::state::order_codec::OrderedKeyCodec;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::RangeBounds;

/// A source that creates a stream or an erased cursor from query settings.
/// The associated output keeps concrete stream types in the public API.
pub trait ReadSource {
    /// The settings consumed by this source.
    type Query;
    /// The concrete stream or cursor returned by this source.
    type Output;

    /// Creates the stream without a storage read.
    fn stream(self, query: Self::Query) -> Self::Output;
}

impl<Q, F, O> ReadSource for (F, PhantomData<fn(Q)>)
where
    F: FnOnce(Q) -> O,
{
    type Output = O;
    type Query = Q;

    fn stream(self, query: Q) -> O {
        self.0(query)
    }
}

/// Query settings bound to a collection and a result projection.
/// Construction performs no storage reads. Poll the stream to start the read.
#[must_use]
pub struct ReadQuery<Q, S> {
    query: Q,
    source: S,
}

/// A fluent map or set read with codec-specific bounds.
pub type KeyRead<'a, KC, S> = ReadQuery<BorrowedKeyQuery<'a, KC>, S>;

/// A fluent deque read with bounds counted from the front.
pub type DequeRead<S> = ReadQuery<DequeQuery, S>;

impl<Q, F> ReadQuery<Q, (F, PhantomData<fn(Q)>)> {
    pub(crate) fn new(query: Q, source: F) -> Self {
        Self {
            query,
            source: (source, PhantomData),
        }
    }
}

impl<Q, S> ReadQuery<Q, S> {
    /// Replaces all query settings.
    pub fn with_query(mut self, query: Q) -> Self {
        self.query = query;
        self
    }

    /// Returns the settings without a collection or read source.
    pub fn into_query(self) -> Q {
        self.query
    }

    /// Creates a lazy stream or cursor. The first poll starts the read.
    pub fn stream(self) -> S::Output
    where
        S: ReadSource<Query = Q>,
    {
        self.source.stream(self.query)
    }
}

impl<KC: OrderedKeyCodec, B, S> ReadQuery<KeyQuery<KC, B>, S> {
    /// Selects ascending order.
    pub fn forward(mut self) -> Self {
        self.query = self.query.forward();
        self
    }

    /// Selects descending order. Repeated calls keep this order.
    pub fn reverse(mut self) -> Self {
        self.query = self.query.reverse();
        self
    }

    /// Selects an order supplied at runtime.
    pub fn direction(mut self, dir: Direction) -> Self {
        self.query = self.query.direction(dir);
        self
    }

    /// Limits present results.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.query = self.query.limit(limit);
        self
    }

    /// Applies [`KeyQuery::from`] to this read.
    pub fn from<'a>(mut self, bound: &'a KC::Borrowed) -> Self
    where
        B: From<&'a KC::Borrowed>,
    {
        self.query = self.query.from(bound);
        self
    }

    /// Applies [`KeyQuery::after`] to this read.
    pub fn after<'a>(mut self, bound: &'a KC::Borrowed) -> Self
    where
        B: From<&'a KC::Borrowed>,
    {
        self.query = self.query.after(bound);
        self
    }

    /// Applies [`KeyQuery::to`] to this read.
    pub fn to<'a>(mut self, bound: &'a KC::Borrowed) -> Self
    where
        B: From<&'a KC::Borrowed>,
    {
        self.query = self.query.to(bound);
        self
    }

    /// Applies [`KeyQuery::before`] to this read.
    pub fn before<'a>(mut self, bound: &'a KC::Borrowed) -> Self
    where
        B: From<&'a KC::Borrowed>,
    {
        self.query = self.query.before(bound);
        self
    }

    /// Applies [`KeyQuery::range`] to this read.
    pub fn range<R: RangeBounds<B>>(mut self, range: R) -> Self
    where
        B: Clone,
    {
        self.query = self.query.range(range);
        self
    }

    /// Applies [`KeyQuery::prefix`] to this read.
    pub fn prefix<'a>(mut self, bound: &'a KC::Borrowed) -> Self
    where
        B: From<&'a KC::Borrowed> + Clone,
    {
        self.query = self.query.prefix(bound);
        self
    }
}

impl<S> ReadQuery<DequeQuery, S> {
    /// Selects ascending order.
    pub fn forward(mut self) -> Self {
        self.query = self.query.forward();
        self
    }

    /// Selects descending order. Repeated calls keep this order.
    pub fn reverse(mut self) -> Self {
        self.query = self.query.reverse();
        self
    }

    /// Selects an order supplied at runtime.
    pub fn direction(mut self, dir: Direction) -> Self {
        self.query = self.query.direction(dir);
        self
    }

    /// Limits present results.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.query = self.query.limit(limit);
        self
    }

    /// Applies [`DequeQuery::from`] to this read.
    pub fn from(mut self, bound: usize) -> Self {
        self.query = self.query.from(bound);
        self
    }

    /// Applies [`DequeQuery::after`] to this read.
    pub fn after(mut self, bound: usize) -> Self {
        self.query = self.query.after(bound);
        self
    }

    /// Applies [`DequeQuery::to`] to this read.
    pub fn to(mut self, bound: usize) -> Self {
        self.query = self.query.to(bound);
        self
    }

    /// Applies [`DequeQuery::before`] to this read.
    pub fn before(mut self, bound: usize) -> Self {
        self.query = self.query.before(bound);
        self
    }

    /// Applies [`DequeQuery::range`] to this read.
    pub fn range<R: RangeBounds<usize>>(mut self, range: R) -> Self {
        self.query = self.query.range(range);
        self
    }
}
