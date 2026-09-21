//! Owned streams over committed map and set entries.

use super::StateReader;
use crate::Key;
use crate::codec::Codec;
use crate::state::collection::{StreamProjection, sealed};
use crate::state::descriptor::map::{KeysetLayout, projected};
use crate::state::descriptor::{CollectionSpec, Descriptor};
use crate::state::query::Query;
use crate::state_reader::session::ReadSession;
use crate::state_reader::{ReaderBackend, StateReaderError};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use tokio::task::coop::cooperative;

impl<L, C, B> StateReader<Descriptor<L>, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    L: CollectionSpec,
{
    pub(super) async fn projected<P>(
        &self,
        key: Key,
        query: Query,
    ) -> Result<impl Stream<Item = Result<P::Item, StateReaderError>> + 'static, StateReaderError>
    where
        L: KeysetLayout + 'static,
        P: StreamProjection<ReadSession<C, B>, L::Cell>,
        P::Item: 'static,
        <ReadSession<C, B> as sealed::Session>::Engine: sealed::Reads<ReadSession<C, B>, P>,
    {
        let session = self.session(key).await?;
        let cells = self.descriptor.bind_collection(&session)?;
        Ok(try_stream! {
            let inner = projected::<_, _, P>(&cells, query);
            futures::pin_mut!(inner);
            while let Some(item) = cooperative(inner.next()).await {
                yield item.map_err(|error| StateReaderError::store(&error))?;
            }
        })
    }
}
