//! Standalone reads of committed map entries.

use super::StateReader;
use crate::Key;
use crate::codec::Codec;
use crate::state::cell::{Presence, Values};
use crate::state::descriptor::{CellType, ContextOf, FromSession, MapDescriptor, ResolvedOf};
use crate::state::order_codec::{OrderedKeyCodec, UnitKey};
use crate::state::{BorrowedKeyQuery, KeyQuery, KeyRead, ReadQuery, ReadSource};
use crate::state_reader::session::ReadSession;
use crate::state_reader::{ReaderBackend, StateReaderError};
use futures::Stream;
use std::borrow::Borrow;
use std::fmt::Display;
use std::ops::DerefMut;

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
    ) -> Result<Vec<Option<ResolvedOf<V>>>, StateReaderError>
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
    ) -> Result<Vec<bool>, StateReaderError>
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
    /// The stream owns the reader state. Its first poll acquires a session.
    /// Acquisition and read errors appear as stream items.
    /// Supply reusable encoding storage as described by [`KeyQuery`].
    pub fn entries<'q, K: Into<Key>, E: DerefMut<Target = Vec<u8>> + Send + 'q>(
        &self,
        key: K,
        buffer: E,
    ) -> KeyRead<
        'q,
        KC,
        impl ReadSource<
            Query = BorrowedKeyQuery<'q, KC>,
            Output: Stream<Item = MapReadItem<KC, V>> + Send + 'q,
        >
        + 'q
        + use<'q, K, KC, V, C, B, E>,
    >
    where
        V: 'static,
        for<'s> ContextOf<'s, V>: FromSession<'s, ReadSession<C, B>>,
        ResolvedOf<V>: 'static,
    {
        let reader = self.clone();
        let key = key.into();
        ReadQuery::new(KeyQuery::new(), move |query: BorrowedKeyQuery<'q, KC>| {
            reader.projected::<Values, _>(key, query, buffer)
        })
    }

    /// Builds a query over committed keys, in ascending key order.
    /// The stream owns the reader state. Its first poll acquires a session.
    /// Acquisition and read errors appear as stream items.
    /// Supply reusable encoding storage as described by [`KeyQuery`].
    pub fn keys<'q, K: Into<Key>, E: DerefMut<Target = Vec<u8>> + Send + 'q>(
        &self,
        key: K,
        buffer: E,
    ) -> KeyRead<
        'q,
        KC,
        impl ReadSource<
            Query = BorrowedKeyQuery<'q, KC>,
            Output: Stream<Item = Result<KC::Key, StateReaderError>> + Send + 'q,
        >
        + 'q
        + use<'q, K, KC, V, C, B, E>,
    >
    where
        V: 'static,
    {
        let reader = self.clone();
        let key = key.into();
        ReadQuery::new(KeyQuery::new(), move |query: BorrowedKeyQuery<'q, KC>| {
            reader.projected::<Presence, _>(key, query, buffer)
        })
    }
}
