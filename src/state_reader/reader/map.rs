//! Standalone reads of committed map entries.

use super::{MapReaderQuery, StateReader};
use crate::Key;
use crate::codec::Codec;
use crate::state::cell_key::Direction;
use crate::state::descriptor::map::Query;
use crate::state::descriptor::{CellType, ContextOf, FromSession, MapDescriptor, ResolvedOf};
use crate::state::order_codec::{OrderedKeyCodec, UnitKey};
use crate::state_reader::session::ReadSession;
use crate::state_reader::{ReaderBackend, StateReaderError};
use futures::Stream;
use std::borrow::Borrow;
use std::fmt::Display;

impl<KC, V, C, B> StateReader<MapDescriptor<KC, V>, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    KC: OrderedKeyCodec + 'static,
    V: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, V>: FromSession<'s, ReadSession<C, B>>,
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

    /// Streams the committed live entries of the map under partition `key` in
    /// key order (ascending for [`Direction::Forward`]).
    ///
    /// The stream owns its session and can outlive the reader's borrow.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`] from acquiring the session (empty key,
    /// acquisition/identity failures); per-source read failures surface as
    /// stream items.
    pub async fn stream<K: Into<Key>>(
        &self,
        key: K,
        dir: Direction,
    ) -> Result<
        impl Stream<Item = Result<(KC::Key, ResolvedOf<V>), StateReaderError>> + 'static,
        StateReaderError,
    >
    where
        V: 'static,
        ResolvedOf<V>: 'static,
    {
        self.query(key, dir).entries().await
    }

    /// Streams committed live keys without decoding or resolving values.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`] from acquiring the session. Per-source read
    /// failures surface as stream items.
    pub async fn keys<K: Into<Key>>(
        &self,
        key: K,
        dir: Direction,
    ) -> Result<impl Stream<Item = Result<KC::Key, StateReaderError>> + 'static, StateReaderError>
    where
        V: 'static,
        KC::Key: 'static,
    {
        self.query(key, dir).keys().await
    }

    /// Builds a directional stream query for partition `key`.
    pub fn query<K: Into<Key>>(&self, key: K, dir: Direction) -> MapReaderQuery<'_, KC, V, C, B> {
        MapReaderQuery {
            reader: self,
            key: key.into(),
            query: Query::new(dir),
        }
    }
}
