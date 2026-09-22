//! Standalone reads of committed set membership.

use super::StateReader;
use crate::Key;
use crate::codec::Codec;
use crate::state::cell::Presence;
use crate::state::descriptor::SetDescriptor;
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::{BorrowedKeyQuery, KeyQuery, KeyRead, ReadQuery, ReadSource};
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
    /// Returns an error when session acquisition, handle binding, key
    /// encoding, or the read fails.
    pub async fn contains<K: Into<Key>>(
        &self,
        key: K,
        member: &KC::Borrowed,
    ) -> Result<bool, StateReaderError>
    where
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
    /// Returns an error when session acquisition, handle binding, key
    /// encoding, or the read fails.
    pub async fn contains_many<'a, K: Into<Key>, Q, I>(
        &self,
        key: K,
        members: I,
    ) -> Result<Vec<bool>, StateReaderError>
    where
        Q: Borrow<KC::Borrowed> + ?Sized + 'a,
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
    /// Returns an error when session acquisition, handle binding, key
    /// encoding, or the read fails.
    pub async fn is_empty<K: Into<Key>>(&self, key: K) -> Result<bool, StateReaderError> {
        let handle = self.bound(key.into()).await?;
        handle
            .is_empty()
            .await
            .map_err(|error| StateReaderError::store(&error))
    }

    /// Builds a query over committed keys, in ascending key order.
    /// The stream owns the reader state. Its first poll acquires a session.
    /// Acquisition and read errors appear as stream items.
    pub fn keys<'q, K: Into<Key>>(
        &self,
        key: K,
    ) -> KeyRead<
        'q,
        KC,
        impl ReadSource<
            Query = BorrowedKeyQuery<'q, KC>,
            Output: Stream<Item = Result<KC::Key, StateReaderError>> + Send + 'q,
        >
        + Clone
        + 'q
        + use<'q, K, KC, C, B>,
    > {
        let reader = self.clone();
        let key = key.into();
        ReadQuery::new(KeyQuery::new(), move |query: BorrowedKeyQuery<'q, KC>| {
            reader.projected::<Presence>(key, query)
        })
    }
}
