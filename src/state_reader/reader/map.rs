//! Standalone reads of committed map entries.

use super::StateReader;
use crate::Key;
use crate::codec::Codec;
use crate::state::cell::{Presence, Values};
use crate::state::descriptor::{CellType, ContextOf, FromSession, MapDescriptor, ResolvedOf};
use crate::state::order_codec::{OrderedKeyCodec, UnitKey};
use crate::state::{BorrowedKeyQuery, CellBuffer, KeyQuery, KeyRead, ReadQuery, ReadSource};
use crate::state_reader::session::ReadSession;
use crate::state_reader::{ReaderBackend, StateReaderError};
use futures::Stream;
use std::borrow::Borrow;
use std::fmt::Display;

/// One committed map entry or the error that ended the stream.
pub type MapReadItem<KC, V> =
    Result<(<KC as OrderedKeyCodec>::Key, ResolvedOf<V>), StateReaderError>;

impl<KC, V, C, B> StateReader<MapDescriptor<KC, V>, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    KC: OrderedKeyCodec + 'static,
    V: CellType<Key = UnitKey>,
{
    /// Reads and resolves the committed value for map entry `map_key` under
    /// partition `key`.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn get<K: Into<Key>>(
        &self,
        key: K,
        map_key: &KC::Borrowed,
    ) -> Result<Option<ResolvedOf<V>>, StateReaderError>
    where
        for<'s> ContextOf<'s, V>: FromSession<'s, ReadSession<C, B>>,
        KC::Borrowed: Display,
    {
        let handle = self.bound(key.into()).await?;
        handle
            .get(map_key)
            .await
            .map_err(|e| StateReaderError::store(&e))
    }

    /// Reports whether a committed map entry exists without decoding its value.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn contains_key<K: Into<Key>>(
        &self,
        key: K,
        map_key: &KC::Borrowed,
    ) -> Result<bool, StateReaderError>
    where
        KC::Borrowed: Display,
    {
        let handle = self.bound(key.into()).await?;
        handle
            .contains_key(map_key)
            .await
            .map_err(|e| StateReaderError::store(&e))
    }

    /// Reports whether the committed map is empty.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn is_empty<K: Into<Key>>(&self, key: K) -> Result<bool, StateReaderError> {
        let handle = self.bound(key.into()).await?;
        handle
            .is_empty()
            .await
            .map_err(|e| StateReaderError::store(&e))
    }

    /// Reads the committed values for `map_keys` as one aligned batch,
    /// index-aligned to the input.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn get_many<'a, K: Into<Key>, Q, I>(
        &self,
        key: K,
        map_keys: I,
    ) -> Result<CellBuffer<Option<ResolvedOf<V>>>, StateReaderError>
    where
        for<'s> ContextOf<'s, V>: FromSession<'s, ReadSession<C, B>>,
        Q: Borrow<KC::Borrowed> + ?Sized + 'a,
        I: IntoIterator<Item = &'a Q>,
        I::IntoIter: Send,
    {
        let handle = self.bound(key.into()).await?;
        handle
            .get_many(map_keys)
            .await
            .map_err(|e| StateReaderError::store(&e))
    }

    /// Tests committed presence for `map_keys` as one aligned batch. Each
    /// result answers the same input position.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn contains_many<'a, K: Into<Key>, Q, I>(
        &self,
        key: K,
        map_keys: I,
    ) -> Result<CellBuffer<bool>, StateReaderError>
    where
        Q: Borrow<KC::Borrowed> + ?Sized + 'a,
        I: IntoIterator<Item = &'a Q>,
        I::IntoIter: Send,
    {
        let handle = self.bound(key.into()).await?;
        handle
            .contains_many(map_keys)
            .await
            .map_err(|e| StateReaderError::store(&e))
    }

    /// Builds a query over committed entries, in ascending key order.
    /// The stream borrows the reader. Its first poll acquires a session.
    /// Acquisition and read errors appear as stream items.
    pub fn entries<'a, K: Into<Key>>(
        &'a self,
        key: K,
    ) -> KeyRead<
        'a,
        KC,
        impl ReadSource<
            Query = BorrowedKeyQuery<'a, KC>,
            Output: Stream<Item = MapReadItem<KC, V>> + Send,
        > + Clone
        + use<'a, K, KC, V, C, B>,
    >
    where
        for<'s> ContextOf<'s, V>: FromSession<'s, ReadSession<C, B>>,
    {
        let key = key.into();
        ReadQuery::new(KeyQuery::new(), move |query: BorrowedKeyQuery<'a, KC>| {
            self.projected::<Values>(key, query)
        })
    }

    /// Builds a query over committed keys, in ascending key order.
    /// The stream borrows the reader. Its first poll acquires a session.
    /// Acquisition and read errors appear as stream items.
    pub fn keys<'a, K: Into<Key>>(
        &'a self,
        key: K,
    ) -> KeyRead<
        'a,
        KC,
        impl ReadSource<
            Query = BorrowedKeyQuery<'a, KC>,
            Output: Stream<Item = Result<KC::Key, StateReaderError>> + Send,
        > + Clone
        + use<'a, K, KC, V, C, B>,
    > {
        let key = key.into();
        ReadQuery::new(KeyQuery::new(), move |query: BorrowedKeyQuery<'a, KC>| {
            self.projected::<Presence>(key, query)
        })
    }
}
