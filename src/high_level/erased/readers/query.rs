//! Fluent queries for erased collection reads.

use super::{ErasedDequeReader, ErasedMapReader, ErasedSetReader};
use crate::codec::Codec;
use crate::consumer::event_context::BoxStateCursor;
use crate::state::{DequeQuery, DequeRead, ErasedKeyQuery, ReadQuery, ReadSource};

impl<C: Codec> dyn ErasedMapReader<C> + '_ {
    /// Builds a query over committed entries.
    /// The cursor performs reads when `next` is called.
    pub fn entries(
        &self,
        key: String,
    ) -> ReadQuery<
        ErasedKeyQuery,
        impl ReadSource<Query = ErasedKeyQuery, Output = BoxStateCursor<(String, C::Payload)>> + '_,
    > {
        ReadQuery::new(ErasedKeyQuery::new(), move |query| {
            self.read_entries(key, query)
        })
    }

    /// Builds a query over committed keys.
    /// The cursor performs reads when `next` is called.
    pub fn keys(
        &self,
        key: String,
    ) -> ReadQuery<
        ErasedKeyQuery,
        impl ReadSource<Query = ErasedKeyQuery, Output = BoxStateCursor<String>> + '_,
    > {
        ReadQuery::new(ErasedKeyQuery::new(), move |query| {
            self.read_keys(key, query)
        })
    }
}

impl dyn ErasedSetReader + '_ {
    /// Builds a query over committed keys.
    /// The cursor performs reads when `next` is called.
    pub fn keys(
        &self,
        key: String,
    ) -> ReadQuery<
        ErasedKeyQuery,
        impl ReadSource<Query = ErasedKeyQuery, Output = BoxStateCursor<String>> + '_,
    > {
        ReadQuery::new(ErasedKeyQuery::new(), move |query| {
            self.read_keys(key, query)
        })
    }
}

impl<C: Codec> dyn ErasedDequeReader<C> + '_ {
    /// Builds a query over committed values.
    /// The cursor performs reads when `next` is called.
    pub fn values(
        &self,
        key: String,
    ) -> DequeRead<impl ReadSource<Query = DequeQuery, Output = BoxStateCursor<C::Payload>> + '_>
    {
        ReadQuery::new(DequeQuery::new(), move |query| self.read_values(key, query))
    }
}
