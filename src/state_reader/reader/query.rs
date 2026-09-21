//! Streams over committed entries with borrowed query bounds.

use super::StateReader;
use crate::Key;
use crate::codec::Codec;
use crate::state::KeyQuery;
use crate::state::collection::{StreamProjection, sealed};
use crate::state::descriptor::map::{KeysetLayout, projected};
use crate::state::descriptor::{BorrowedKeyOf, CellType, CollectionSpec, Descriptor};
use crate::state_reader::session::ReadSession;
use crate::state_reader::{ReaderBackend, StateReaderError};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use std::ops::DerefMut;
use tokio::task::coop::cooperative;

impl<L, C, B> StateReader<Descriptor<L>, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    L: CollectionSpec,
{
    pub(super) fn projected<'q, P, E>(
        self,
        key: Key,
        query: KeyQuery<<L::Cell as CellType>::Key, &'q BorrowedKeyOf<L::Cell>>,
        buffer: E,
    ) -> impl Stream<Item = Result<P::Item, StateReaderError>> + Send + 'q
    where
        L: KeysetLayout + 'static,
        E: DerefMut<Target = Vec<u8>> + Send + 'q,
        P: StreamProjection<ReadSession<C, B>, L::Cell>,
        P::Item: 'static,
        <ReadSession<C, B> as sealed::Session>::Engine: sealed::Reads<ReadSession<C, B>, P>,
    {
        try_stream! {
            let session = self.session(key).await?;
            let cells = self.descriptor.bind_collection(&session)?;
            let inner = projected::<_, _, P, _>(&cells, query, buffer);
            futures::pin_mut!(inner);
            while let Some(item) = cooperative(inner.next()).await {
                yield item.map_err(|error| StateReaderError::store(&error))?;
            }
        }
    }
}
