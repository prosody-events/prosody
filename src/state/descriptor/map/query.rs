//! Map and set query values and their projected stream executor.

use super::membership::{self, KeysetLayout};
use super::{MapKind, MapStateError};
use crate::state::cell::{Presence, Values};
use crate::state::cell_key::{Coordinate, Direction, ScanEdge};
use crate::state::collection::{Collection, StateSession, StreamProjection, sealed};
use crate::state::descriptor::set::SetKind;
use crate::state::descriptor::{
    BorrowedKeyOf, CellCodecError, CellType, CollectionSpec, ContextOf, FromSession, KeyOf,
    ResolvedOf,
};
use crate::state::order_codec::{OrderedKeyCodec, UnitKey};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use std::num::NonZeroUsize;
use tracing::Instrument;

/// The encoded bounds, direction, and result limit of a map or set query.
#[derive(Clone, Debug)]
pub(crate) struct Query {
    pub(crate) dir: Direction,
    pub(crate) limit: Option<NonZeroUsize>,
    pub(crate) start: ScanEdge<Coordinate>,
    pub(crate) end: ScanEdge<Coordinate>,
}

/// One decoded key or the error that ended the stream.
pub type KeyItem<L> = Result<
    KeyOf<<L as CollectionSpec>::Cell>,
    MapStateError<CellCodecError<<L as CollectionSpec>::Cell>>,
>;

/// One decoded map entry or the error that ended the stream.
pub type MapStreamItem<KC, V> =
    Result<(<KC as OrderedKeyCodec>::Key, ResolvedOf<V>), MapStateError<CellCodecError<V>>>;

/// A directional map query.
pub type MapQuery<'a, S, KC, V> = KeysetQuery<'a, S, MapKind<KC, V>>;

/// A directional set query.
pub type SetQuery<'a, S, KC> = KeysetQuery<'a, S, SetKind<KC>>;

/// A directional map or set query.
/// Edges follow the query direction. A later call replaces the same edge.
/// A start past the end produces an empty stream.
#[must_use]
pub struct KeysetQuery<'a, S, L> {
    cells: &'a Collection<S, L>,
    query: Query,
}

impl<'a, S, L> KeysetQuery<'a, S, L>
where
    S: StateSession,
    L: CollectionSpec,
{
    /// Binds the query to a collection.
    pub(crate) fn new(cells: &'a Collection<S, L>, query: Query) -> Self {
        Self { cells, query }
    }

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

    /// Bounds the present items the stream yields. Missing cells do not consume
    /// the limit. The limit sizes the first fetch, so it also sets the first
    /// error boundary.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    /// Runs the query under projection `P`.
    pub(crate) fn projected<P>(
        self,
    ) -> impl Stream<Item = Result<P::Item, MapStateError<CellCodecError<L::Cell>>>> + 'a
    where
        L: KeysetLayout,
        P: StreamProjection<S, L::Cell>,
        S::Engine: sealed::Reads<S, P>,
    {
        let span = L::stream_span(self.cells.name(), self.query.dir, P::NAME);
        try_stream! {
            let plan = self.cells.read(async |op| membership::plan(op, &self.query).await).instrument(span.clone()).await?;
            let inner = plan.with_limit(self.query.limit).projected::<P>();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().instrument(span.clone()).await {
                yield item?;
            }
        }
    }
}

impl<'a, S, KC, V> KeysetQuery<'a, S, MapKind<KC, V>>
where
    S: StateSession,
    KC: OrderedKeyCodec + 'static,
    V: CellType<Key = UnitKey>,
{
    /// Streams live keys in the query direction.
    pub fn keys(self) -> impl Stream<Item = KeyItem<MapKind<KC, V>>> + 'a {
        self.projected::<Presence>()
    }

    /// Streams live entries in the query direction.
    pub fn entries(self) -> impl Stream<Item = MapStreamItem<KC, V>> + 'a
    where
        for<'s> ContextOf<'s, V>: FromSession<'s, S>,
    {
        self.projected::<Values>()
    }
}

impl<'a, S, KC> KeysetQuery<'a, S, SetKind<KC>>
where
    S: StateSession,
    KC: OrderedKeyCodec + 'static,
{
    /// Streams live keys in the query direction.
    pub fn keys(self) -> impl Stream<Item = KeyItem<SetKind<KC>>> + 'a {
        self.projected::<Presence>()
    }
}

impl Query {
    pub(crate) fn new(dir: Direction) -> Self {
        Self {
            dir,
            limit: None,
            start: ScanEdge::Unbounded,
            end: ScanEdge::Unbounded,
        }
    }

    /// Keeps the ascending stored coordinates within the query bounds, in
    /// query order. The trim reuses the stored vector.
    pub(super) fn select(&self, mut coordinates: Vec<Coordinate>) -> Vec<Coordinate> {
        let (low, high) = match self.dir {
            Direction::Forward => (&self.start, &self.end),
            Direction::Backward => (&self.end, &self.start),
        };
        let start = match low {
            ScanEdge::Included(edge) => coordinates.partition_point(|c| c < edge),
            ScanEdge::Excluded(edge) => coordinates.partition_point(|c| c <= edge),
            ScanEdge::Unbounded => 0,
        };
        let end = match high {
            ScanEdge::Included(edge) => coordinates.partition_point(|c| c <= edge),
            ScanEdge::Excluded(edge) => coordinates.partition_point(|c| c < edge),
            ScanEdge::Unbounded => coordinates.len(),
        };
        coordinates.truncate(end.max(start));
        coordinates.drain(..start);
        if self.dir == Direction::Backward {
            coordinates.reverse();
        }
        coordinates
    }
}
