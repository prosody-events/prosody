//! Standalone map query values and their owned stream adapter.

use super::StateReader;
use crate::Key;
use crate::codec::Codec;
use crate::state::cell::{Presence, Values};
use crate::state::collection::{StreamProjection, sealed};
use crate::state::descriptor::map::Query;
use crate::state::descriptor::{
    CellType, ContextOf, FromSession, Keyed, MapDescriptor, MapHandle, ResolvedOf, StateDescriptor,
};
use crate::state::order_codec::{OrderedKeyCodec, UnitKey};
use crate::state_reader::error::StateReaderError;
use crate::state_reader::session::ReadSession;
use crate::state_reader::{MemoryReaderBackend, ReaderBackend};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use std::fmt::Display;
use std::num::NonZeroUsize;
use tokio::task::coop::cooperative;

/// A directional map stream query for a standalone reader.
/// Terminals acquire a session before they return an owned stream.
///
/// See [`crate::state::descriptor::MapQuery::limit`] for the limit contract.
#[must_use]
pub struct MapReaderQuery<'a, KC, V, C: Codec, B = MemoryReaderBackend<C>> {
    pub(super) reader: &'a StateReader<MapDescriptor<KC, V>, C, B>,
    pub(super) key: Key,
    pub(super) query: Query,
}

impl<KC, V, C, B> MapReaderQuery<'_, KC, V, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    KC: OrderedKeyCodec + 'static,
    KC::Key: Display,
    V: CellType<Key = UnitKey>,
{
    /// Sets the maximum number of present items that the stream yields.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    /// Streams committed live entries in the query direction.
    ///
    /// # Errors
    ///
    /// Returns an error when session acquisition or handle binding fails.
    pub async fn entries(
        self,
    ) -> Result<
        impl Stream<Item = Result<(KC::Key, ResolvedOf<V>), StateReaderError>> + 'static,
        StateReaderError,
    >
    where
        V: 'static,
        ResolvedOf<V>: 'static,
        for<'s> ContextOf<'s, V>: FromSession<'s, ReadSession<C, B>>,
    {
        self.projected::<Values>().await
    }

    /// Streams committed live keys in the query direction.
    ///
    /// # Errors
    ///
    /// Returns an error when session acquisition or handle binding fails.
    pub async fn keys(
        self,
    ) -> Result<impl Stream<Item = Result<KC::Key, StateReaderError>> + 'static, StateReaderError>
    where
        V: 'static,
        KC::Key: 'static,
    {
        self.projected::<Presence>().await
    }

    async fn projected<P>(
        self,
    ) -> Result<impl Stream<Item = Result<P::Item, StateReaderError>> + 'static, StateReaderError>
    where
        V: 'static,
        P: StreamProjection<ReadSession<C, B>, Keyed<KC, V>>,
        P::Item: 'static,
        <ReadSession<C, B> as sealed::Session>::Engine: sealed::Reads<ReadSession<C, B>, P>,
    {
        let session = self.reader.session(self.key).await?;
        let handle: MapHandle<_, KC, V> = self.reader.descriptor.bind(&session)?;
        let query = self.query;
        Ok(try_stream! {
            let inner = query.run::<P, _, _, _>(&handle);
            futures::pin_mut!(inner);
            while let Some(item) = cooperative(inner.next()).await {
                yield item.map_err(|e| StateReaderError::store(&e))?;
            }
        })
    }
}
