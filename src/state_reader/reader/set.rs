//! Standalone reads of committed set membership.

use super::StateReader;
use crate::Key;
use crate::codec::Codec;
use crate::state::cell_key::Direction;
use crate::state::descriptor::{SetDescriptor, SetHandle, StateDescriptor};
use crate::state::order_codec::OrderedKeyCodec;
use crate::state_reader::{ReaderBackend, StateReaderError};
use futures::{Stream, StreamExt};
use std::fmt::Display;
use tokio::task::coop::cooperative;

impl<KC, C, B> StateReader<SetDescriptor<KC>, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    KC: OrderedKeyCodec + 'static,
    KC::Key: Display + 'static,
{
    /// Reports whether the committed set contains `member`.
    ///
    /// # Errors
    ///
    /// Returns an error when session acquisition or handle binding fails.
    pub async fn contains<K: Into<Key>>(
        &self,
        key: K,
        member: &KC::Key,
    ) -> Result<bool, StateReaderError> {
        let session = self.session(key.into()).await?;
        let handle: SetHandle<_, KC> = self.descriptor.bind(&session)?;
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
    pub async fn contains_many<K: Into<Key>>(
        &self,
        key: K,
        members: &[KC::Key],
    ) -> Result<Vec<bool>, StateReaderError> {
        let session = self.session(key.into()).await?;
        let handle: SetHandle<_, KC> = self.descriptor.bind(&session)?;
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
        let session = self.session(key.into()).await?;
        let handle: SetHandle<_, KC> = self.descriptor.bind(&session)?;
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
        let session = self.session(key.into()).await?;
        let handle: SetHandle<_, KC> = self.descriptor.bind(&session)?;
        Ok(async_stream::try_stream! {
            let inner = handle.keys(dir);
            futures::pin_mut!(inner);
            while let Some(item) = cooperative(inner.next()).await {
                yield item.map_err(|error| StateReaderError::store(&error))?;
            }
        })
    }
}
