//! Standalone map and set query values and their owned stream adapter.

use super::StateReader;
use crate::Key;
use crate::codec::Codec;
use crate::state::cell::{Presence, Values};
use crate::state::cell_key::ScanEdge;
use crate::state::collection::{StreamProjection, sealed};
use crate::state::descriptor::map::{KeysetLayout, KeysetQuery, MapKind, Query};
use crate::state::descriptor::set::SetKind;
use crate::state::descriptor::{
    BorrowedKeyOf, CellType, CollectionSpec, ContextOf, Descriptor, FromSession, ResolvedOf,
};
use crate::state::order_codec::{OrderedKeyCodec, UnitKey};
use crate::state_reader::error::StateReaderError;
use crate::state_reader::session::ReadSession;
use crate::state_reader::{MemoryReaderBackend, ReaderBackend};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use std::num::NonZeroUsize;
use tokio::task::coop::cooperative;

/// A directional map or set stream query for a standalone reader.
/// Terminals acquire a session before they return an owned stream.
///
/// See [`crate::state::descriptor::MapQuery::limit`] for the limit contract.
#[must_use]
pub struct KeysetReaderQuery<'a, L, C: Codec, B = MemoryReaderBackend<C>> {
    pub(super) reader: &'a StateReader<Descriptor<L>, C, B>,
    pub(super) key: Key,
    pub(super) query: Query,
}

/// A standalone map query.
pub type MapReaderQuery<'a, KC, V, C, B = MemoryReaderBackend<C>> =
    KeysetReaderQuery<'a, MapKind<KC, V>, C, B>;

/// A standalone set query.
pub type SetReaderQuery<'a, KC, C, B = MemoryReaderBackend<C>> =
    KeysetReaderQuery<'a, SetKind<KC>, C, B>;

impl<L, C, B> KeysetReaderQuery<'_, L, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    L: CollectionSpec,
{
    /// Starts at `key`.
    pub fn from(mut self, key: &BorrowedKeyOf<L::Cell>) -> Self {
        self.query.start = ScanEdge::Included(<L::Cell as CellType>::Key::encode(key));
        self
    }

    /// Starts after `key`.
    pub fn after(mut self, key: &BorrowedKeyOf<L::Cell>) -> Self {
        self.query.start = ScanEdge::Excluded(<L::Cell as CellType>::Key::encode(key));
        self
    }

    /// Stops at `key`.
    pub fn to(mut self, key: &BorrowedKeyOf<L::Cell>) -> Self {
        self.query.end = ScanEdge::Included(<L::Cell as CellType>::Key::encode(key));
        self
    }

    /// Stops before `key`.
    pub fn before(mut self, key: &BorrowedKeyOf<L::Cell>) -> Self {
        self.query.end = ScanEdge::Excluded(<L::Cell as CellType>::Key::encode(key));
        self
    }

    /// Sets the maximum number of present items that the stream yields.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    async fn projected<P>(
        self,
    ) -> Result<impl Stream<Item = Result<P::Item, StateReaderError>> + 'static, StateReaderError>
    where
        L: KeysetLayout + 'static,
        P: StreamProjection<ReadSession<C, B>, L::Cell>,
        P::Item: 'static,
        <ReadSession<C, B> as sealed::Session>::Engine: sealed::Reads<ReadSession<C, B>, P>,
    {
        let session = self.reader.session(self.key).await?;
        let cells = self.reader.descriptor.bind_collection(&session)?;
        let query = self.query;
        Ok(try_stream! {
            let inner = KeysetQuery::new(&cells, query).projected::<P>();
            futures::pin_mut!(inner);
            while let Some(item) = cooperative(inner.next()).await {
                yield item.map_err(|e| StateReaderError::store(&e))?;
            }
        })
    }
}

impl<KC, V, C, B> KeysetReaderQuery<'_, MapKind<KC, V>, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    KC: OrderedKeyCodec + 'static,
    V: CellType<Key = UnitKey>,
{
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
}

impl<KC, C, B> KeysetReaderQuery<'_, SetKind<KC>, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    KC: OrderedKeyCodec + 'static,
{
    /// Streams committed live keys in the query direction.
    ///
    /// # Errors
    ///
    /// Returns an error when session acquisition or handle binding fails.
    pub async fn keys(
        self,
    ) -> Result<impl Stream<Item = Result<KC::Key, StateReaderError>> + 'static, StateReaderError>
    where
        KC::Key: 'static,
    {
        self.projected::<Presence>().await
    }
}
