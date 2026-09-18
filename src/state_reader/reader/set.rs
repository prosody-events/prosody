//! Standalone reads of committed set membership.

use super::{SetReaderQuery, StateReader};
use crate::Key;
use crate::codec::Codec;
use crate::state::cell_key::Direction;
use crate::state::descriptor::SetDescriptor;
use crate::state::descriptor::map::Query;
use crate::state::order_codec::OrderedKeyCodec;
use crate::state_reader::{ReaderBackend, StateReaderError};
use futures::Stream;
use std::borrow::Borrow;
use std::fmt::Display;

impl<KC, C, B> StateReader<SetDescriptor<KC>, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    KC: OrderedKeyCodec + 'static,
{
    /// Reports whether the committed set contains `member`.
    ///
    /// # Errors
    ///
    /// Returns an error when session acquisition or handle binding fails.
    pub async fn contains<K: Into<Key>, Q>(
        &self,
        key: K,
        member: &Q,
    ) -> Result<bool, StateReaderError>
    where
        Q: Borrow<KC::Borrowed> + ?Sized,
        KC::Borrowed: Display,
    {
        let handle = self.bound(key.into()).await?;
        handle
            .contains(member)
            .await
            .map_err(|error| StateReaderError::store(&error))
    }

    /// Tests committed membership for each input key.
    ///
    /// # Errors
    ///
    /// Returns an error when session acquisition or handle binding fails.
    pub async fn contains_many<'a, K: Into<Key>, Q, I>(
        &self,
        key: K,
        members: I,
    ) -> Result<Vec<bool>, StateReaderError>
    where
        Q: Borrow<KC::Borrowed> + Sync + ?Sized + 'a,
        I: IntoIterator<Item = &'a Q>,
        I::IntoIter: Send,
    {
        let handle = self.bound(key.into()).await?;
        handle
            .contains_many(members)
            .await
            .map_err(|error| StateReaderError::store(&error))
    }

    /// Reports whether the committed set has no members.
    ///
    /// # Errors
    ///
    /// Returns an error when session acquisition or handle binding fails.
    pub async fn is_empty<K: Into<Key>>(&self, key: K) -> Result<bool, StateReaderError> {
        let handle = self.bound(key.into()).await?;
        handle
            .is_empty()
            .await
            .map_err(|error| StateReaderError::store(&error))
    }

    /// Streams committed set members in the direction `dir`.
    ///
    /// # Errors
    ///
    /// Returns an error when session acquisition or handle binding fails.
    pub async fn keys<K: Into<Key>>(
        &self,
        key: K,
        dir: Direction,
    ) -> Result<impl Stream<Item = Result<KC::Key, StateReaderError>> + 'static, StateReaderError>
    {
        self.query(key, dir).keys().await
    }

    /// Builds a directional set query for partition `key`.
    pub fn query<K: Into<Key>>(&self, key: K, dir: Direction) -> SetReaderQuery<'_, KC, C, B> {
        SetReaderQuery {
            reader: self,
            key: key.into(),
            query: Query::new(dir),
        }
    }
}
