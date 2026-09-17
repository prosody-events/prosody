//! Map query values and their projected stream executor.

use super::{MapHandle, MapKeyItem, MapStateError, MapStreamItem};
use crate::state::cell::{Presence, Values};
use crate::state::cell_key::{Coordinate, Direction, ScanEdge};
use crate::state::collection::{StateSession, StreamProjection, sealed};
use crate::state::descriptor::{CellCodecError, CellType, ContextOf, FromSession, Keyed};
use crate::state::order_codec::{OrderedKeyCodec, UnitKey};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use std::fmt::Display;
use std::num::NonZeroUsize;
use tracing::{Instrument, info_span};

/// The encoded bounds, direction, and result limit of a map query.
#[derive(Clone, Debug)]
pub(crate) struct Query {
    pub(crate) dir: Direction,
    pub(crate) limit: Option<NonZeroUsize>,
    pub(crate) start: ScanEdge<Coordinate>,
    pub(crate) end: ScanEdge<Coordinate>,
}

/// A directional map stream query.
///
/// Build one with [`MapHandle::query`]. Finish with [`keys`](Self::keys) or
/// [`entries`](Self::entries).
/// Edges follow the query direction. A later call replaces the same edge.
/// A start past the end produces an empty stream.
#[must_use]
pub struct MapQuery<'a, S, KC, V> {
    handle: &'a MapHandle<S, KC, V>,
    query: Query,
}

impl<'a, S, KC, V> MapQuery<'a, S, KC, V>
where
    S: StateSession,
    KC: OrderedKeyCodec + 'static,
    KC::Key: Display,
    V: CellType<Key = UnitKey>,
{
    /// Binds `query` to `handle`. The reader builds one after it binds a
    /// handle to an acquired session.
    pub(crate) fn new(handle: &'a MapHandle<S, KC, V>, query: Query) -> Self {
        Self { handle, query }
    }

    /// Starts at `key`.
    pub fn from(mut self, key: &KC::Key) -> Self {
        self.query.start = ScanEdge::Included(KC::encode(key));
        self
    }

    /// Starts after `key`.
    pub fn after(mut self, key: &KC::Key) -> Self {
        self.query.start = ScanEdge::Excluded(KC::encode(key));
        self
    }

    /// Stops at `key`.
    pub fn to(mut self, key: &KC::Key) -> Self {
        self.query.end = ScanEdge::Included(KC::encode(key));
        self
    }

    /// Stops before `key`.
    pub fn before(mut self, key: &KC::Key) -> Self {
        self.query.end = ScanEdge::Excluded(KC::encode(key));
        self
    }

    /// Bounds the present items the stream yields. Missing cells do not consume
    /// the limit. The limit sizes the first fetch, so it also sets the first
    /// error boundary.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    /// Streams live entries in the query direction.
    pub fn entries(self) -> impl Stream<Item = MapStreamItem<KC, V>> + 'a
    where
        for<'s> ContextOf<'s, V>: FromSession<'s, S>,
    {
        self.projected::<Values>()
    }

    /// Streams live keys in the query direction.
    pub fn keys(self) -> impl Stream<Item = MapKeyItem<KC, V>> + 'a {
        self.projected::<Presence>()
    }

    /// Runs the query under projection `P`.
    /// This is the one home of the `map.stream` span.
    pub(crate) fn projected<P>(
        self,
    ) -> impl Stream<Item = Result<P::Item, MapStateError<CellCodecError<V>>>> + 'a
    where
        P: StreamProjection<S, Keyed<KC, V>>,
        S::Engine: sealed::Reads<S, P>,
    {
        let span = info_span!(
            "map.stream",
            collection = self.handle.cells.name().as_str(),
            direction = ?self.query.dir,
            projection = P::NAME,
        );
        try_stream! {
            let plan = self.handle.stream_plan(&self.query).instrument(span.clone()).await?;
            let inner = plan.with_limit(self.query.limit).projected::<P>();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().instrument(span.clone()).await {
                yield item?;
            }
        }
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

    /// Selects the stored coordinates within the query bounds and decodes
    /// them in query order. Every selected key must encode back to its
    /// coordinate. A key that does not selects a scan.
    pub(super) fn keys<KC: OrderedKeyCodec>(
        &self,
        coordinates: &[Coordinate],
    ) -> Option<Vec<KC::Key>> {
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
        let selected = coordinates.get(start..end).unwrap_or_default();

        let mut keys = Vec::with_capacity(selected.len());
        for coordinate in selected {
            let Ok(key) = KC::decode(coordinate.as_bytes()) else {
                return None;
            };
            if KC::encode(&key) != *coordinate {
                return None;
            }
            keys.push(key);
        }
        if self.dir == Direction::Backward {
            keys.reverse();
        }
        Some(keys)
    }
}
