//! Named fluent builders for foreign-language collection wrappers.

use super::BoxStateCursor;
use crate::state::{DequeQuery, ErasedKeyQuery, ReadQuery, ReadSource};
use std::sync::Arc;

/// An owned map or set query. Only the returned item type remains generic.
pub type ErasedKeyRead<Item> = ReadQuery<ErasedKeyQuery, ErasedReadSource<ErasedKeyQuery, Item>>;

/// An owned deque query. Positions count from the front.
pub type ErasedDequeRead<Item> = ReadQuery<DequeQuery, ErasedReadSource<DequeQuery, Item>>;

/// An owned source for an erased query.
/// Clones share the source. Each call creates an independent cursor.
/// Clients need no backend or source type parameters.
/// Construction performs no reads. The cursor starts reads on its first pull.
pub struct ErasedReadSource<Q, Item>(Arc<dyn Fn(Q) -> BoxStateCursor<Item> + Send + Sync>);

impl<Q, Item> Clone for ErasedReadSource<Q, Item> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<Q, Item> ReadSource for ErasedReadSource<Q, Item> {
    type Output = BoxStateCursor<Item>;
    type Query = Q;

    fn stream(self, query: Q) -> Self::Output {
        self.0(query)
    }
}

pub(crate) fn read<Q: Default, Item>(
    source: impl Fn(Q) -> BoxStateCursor<Item> + Send + Sync + 'static,
) -> ReadQuery<Q, ErasedReadSource<Q, Item>> {
    ReadQuery::from_source(Q::default(), ErasedReadSource(Arc::new(source)))
}
