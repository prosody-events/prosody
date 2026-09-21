//! The shared map and set executor and its result types.

use super::MapStateError;
use super::membership::{self, KeysetLayout};
use crate::codec::SerializeBufGuard;
use crate::state::KeyQuery;
use crate::state::collection::{Collection, StateSession, StreamProjection, sealed};
use crate::state::descriptor::{
    BorrowedKeyOf, CellCodecError, CellStateError, CellType, CollectionSpec, KeyOf, ResolvedOf,
};
use crate::state::order_codec::OrderedKeyCodec;
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use tracing::Instrument;

/// One decoded key or the error that ended the stream.
pub type KeyItem<L> = Result<
    KeyOf<<L as CollectionSpec>::Cell>,
    MapStateError<CellCodecError<<L as CollectionSpec>::Cell>>,
>;

/// One decoded map entry or the error that ended the stream.
pub type MapStreamItem<KC, V> =
    Result<(<KC as OrderedKeyCodec>::Key, ResolvedOf<V>), MapStateError<CellCodecError<V>>>;

/// Executes a map or set query under one projection.
pub(crate) fn projected<'a, S, L, P>(
    cells: &'a Collection<S, L>,
    query: KeyQuery<<L::Cell as CellType>::Key, &'a BorrowedKeyOf<L::Cell>>,
) -> impl Stream<Item = Result<P::Item, MapStateError<CellCodecError<L::Cell>>>> + use<'a, S, L, P>
where
    S: StateSession,
    L: KeysetLayout,
    P: StreamProjection<S, L::Cell>,
    S::Engine: sealed::Reads<S, P>,
{
    let span = L::stream_span(cells.name(), query.dir, P::NAME);
    try_stream! {
        let mut buf = SerializeBufGuard::acquire();
        let query = query.encode(&mut buf).map_err(CellStateError::Key)?;
        let plan = cells.read(async |op| membership::plan(op, &query).await).instrument(span.clone()).await?;
        let inner = plan.with_limit(query.limit).projected::<P>();
        futures::pin_mut!(inner);
        while let Some(item) = inner.next().instrument(span.clone()).await {
            yield item?;
        }
    }
}
