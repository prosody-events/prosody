//! Fluent queries for erased collection reads.

use super::{BoxStateCursor, DynDequeState, DynMapState, DynSetState};
use crate::codec::SerializeBufGuard;
use crate::state::{DequeQuery, DequeRead, ErasedKeyQuery, ReadQuery, ReadSource};

impl<Item: Send + 'static> dyn DynMapState<Item> + '_ {
    /// Builds a query over live entries.
    /// The cursor performs reads when `next` is called.
    pub fn entries(
        &self,
    ) -> ReadQuery<
        ErasedKeyQuery,
        impl ReadSource<Query = ErasedKeyQuery, Output = BoxStateCursor<(String, Item)>> + '_,
    > {
        ReadQuery::new(ErasedKeyQuery::new(), move |query| self.read_entries(query))
    }

    /// Builds a query over live keys.
    /// The cursor performs reads when `next` is called.
    pub fn keys(
        &self,
    ) -> ReadQuery<
        ErasedKeyQuery,
        impl ReadSource<Query = ErasedKeyQuery, Output = BoxStateCursor<String>> + '_,
    > {
        ReadQuery::new(ErasedKeyQuery::new(), move |query| self.read_keys(query))
    }
}

impl dyn DynSetState + '_ {
    /// Builds a query over live keys.
    /// The cursor performs reads when `next` is called.
    pub fn keys(
        &self,
    ) -> ReadQuery<
        ErasedKeyQuery,
        impl ReadSource<Query = ErasedKeyQuery, Output = BoxStateCursor<String>> + '_,
    > {
        ReadQuery::new(ErasedKeyQuery::new(), move |query| self.read_keys(query))
    }
}

impl<Item: Send + 'static> dyn DynDequeState<Item> + '_ {
    /// Builds a query over live values.
    /// The cursor performs reads when `next` is called.
    pub fn values(
        &self,
    ) -> DequeRead<impl ReadSource<Query = DequeQuery, Output = BoxStateCursor<Item>> + '_> {
        ReadQuery::new(DequeQuery::new(), move |query| self.read_values(query))
    }
}

/// Supplies encoding storage for one erased read.
pub(crate) fn encoding_buffer(query: &ErasedKeyQuery) -> SerializeBufGuard {
    let mut buffer = SerializeBufGuard::acquire();
    buffer.reserve(query.required_capacity());
    buffer
}
