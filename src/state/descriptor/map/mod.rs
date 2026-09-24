//! Ordered key-value maps with bounded membership tracking.
//!
//! Each map stores one entry per key and one keyset cell. Small maps read
//! tracked coordinates. An overflowed keyset selects a range scan.
//! [`MapHandle`] binds the collection to an owner or reader session.
//! [`map_state`] declares its key and value codecs.
//!
//! A mutation updates the entry and keyset in one scoped operation.
//! Removal subtracts membership. Clear resets both cell families.
//! On collections with a TTL, every insertion refreshes the keyset.
//! Thus, the keyset expires no earlier than the newest entry.

mod layout;
mod query;
#[cfg(test)]
use layout::FrozenLayout;
pub use layout::MapKind;
mod keyset;
pub(super) mod membership;
use keyset::Keyset;
pub use keyset::KeysetFrameError;
pub(crate) use keyset::{MapKeysetCodec, MapKeysetKey};
pub(crate) use membership::KeysetLayout;

use crate::state::cell::{Presence, Values};
use crate::state::query::Query;
use crate::state::{BorrowedKeyQuery, KeyQuery, KeyRead, ReadQuery, ReadSource};
pub use query::{KeyItem, MapStreamItem};
pub(crate) use query::{projected, selected};

use super::{
    CellCodecError, CellStateError, CellType, CollectionSpec, ContextOf, Descriptor, FromSession,
    Keyed, ResolvedOf, WriteOf,
};
#[cfg(test)]
use crate::codec::Codec;
use crate::codec::JsonCodec;
use crate::error::{ClassifyError, ErrorCategory};
#[cfg(test)]
use crate::state::cell_key::{CellKey, Coordinate};
#[cfg(test)]
use crate::state::collection::CollectionLayout;
use crate::state::collection::{
    Collection, CollectionRead, CollectionWrite, StateSession, WritableStateSession,
    collection_methods,
};
use crate::state::order_codec::{OrderedKeyCodec, UnitKey};
use crate::state::{CollectionKindId, StateAccessError, StoreOutcome};
use educe::Educe;
use futures::stream::Stream;
use std::borrow::Borrow;
use std::error::Error;
use std::fmt::Display;
use thiserror::Error;
use tracing::{Span, field::Empty, instrument};

/// Descriptor for a codec-backed ordered map collection. Generic over an
/// [`OrderedKeyCodec`] `KC` (the key encoding, frozen into the identity) and a
/// value [`CellType`] `V` — a plain [`crate::codec::Codec`] (JSON by default)
/// or a codec paired with a resolver via [`WithResolver`](super::WithResolver).
/// Declare via [`map_state`].
pub type MapDescriptor<KC, V = JsonCodec> = Descriptor<MapKind<KC, V>>;

impl<KC, V> CollectionSpec for MapKind<KC, V>
where
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
{
    type Cell = Keyed<KC, V>;
    type Handle<S: StateSession> = MapHandle<S, KC, V>;

    const KIND: CollectionKindId = CollectionKindId::Map;

    fn handle<S: StateSession>(collection: Collection<S, Self>) -> MapHandle<S, KC, V> {
        MapHandle { cells: collection }
    }
}

/// Typed, owned handle over a codec-backed ordered map.
///
/// Owns the bound collection, whose session clone is `Clone + Send + Sync +
/// 'static` (an FFI requirement). Each method opens exactly one scoped
/// operation; the streams run a short planning operation and then drive the
/// plan it returns. Cheap `Clone`.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct MapHandle<S, KC, V> {
    cells: Collection<S, MapKind<KC, V>>,
}

#[collection_methods(field = cells, session = S)]
impl<S, KC, V> MapHandle<S, KC, V>
where
    S: StateSession,
    KC: OrderedKeyCodec + 'static,
    V: CellType<Key = UnitKey>,
{
    /// Reads and resolves the value for `key` (`None` when absent).
    ///
    /// # Errors
    ///
    /// Returns a key codec error (`Permanent`) when `key` does not encode, a
    /// codec error (`Permanent`) when the cell does not decode, a resolution
    /// error, or an access error from the session.
    #[instrument(
        name = "map.get",
        skip_all,
        fields(collection = self.cells.name().as_str(), map.key = %key),
        err
    )]
    #[read(op)]
    pub async fn get(
        &self,
        key: &KC::Borrowed,
    ) -> Result<Option<ResolvedOf<V>>, MapStateError<CellCodecError<V>>>
    where
        KC::Borrowed: Display,
    {
        Ok(op.get(MapKind::<KC, V>::ENTRIES, key).await?)
    }

    /// Tests cell presence without value decoding or resolution.
    /// Owner reads include buffered changes. Standalone readers see committed
    /// cells. A message reference can remain present after its Kafka
    /// message expires.
    ///
    /// # Errors
    ///
    /// Returns a key codec error (`Permanent`) when a key does not encode, or a
    /// session access error.
    #[instrument(
        name = "map.contains_key",
        skip_all,
        fields(collection = self.cells.name().as_str(), map.key = %key),
        err
    )]
    #[read(op)]
    pub async fn contains_key(
        &self,
        key: &KC::Borrowed,
    ) -> Result<bool, MapStateError<CellCodecError<V>>>
    where
        KC::Borrowed: Display,
    {
        Ok(op.contains(MapKind::<KC, V>::ENTRIES, key).await?)
    }

    /// Reads one value per input key, in input order. Duplicate keys retain
    /// their positions. One scoped operation prevents session mutations
    /// between batch reads. Reads address cells directly without a keyset
    /// lookup. Result buffers reserve the iterator's lower size estimate
    /// and grow as needed.
    ///
    /// # Errors
    ///
    /// Returns a key codec, codec, resolution, or session access error. Errors
    /// return no partial result.
    #[instrument(
        name = "map.get_many",
        skip_all,
        fields(collection = self.cells.name().as_str(), keys = Empty),
        err
    )]
    #[read(op)]
    pub async fn get_many<'a, Q, I>(
        &self,
        keys: I,
    ) -> Result<Vec<Option<ResolvedOf<V>>>, MapStateError<CellCodecError<V>>>
    where
        Q: Borrow<KC::Borrowed> + ?Sized + 'a,
        I: IntoIterator<Item = &'a Q>,
        I::IntoIter: Send,
    {
        let values = op
            .get_many(
                MapKind::<KC, V>::ENTRIES,
                keys.into_iter().map(Borrow::borrow),
            )
            .await?
            .into_vec();
        Span::current().record("keys", values.len() as i64);
        Ok(values)
    }

    /// Tests `keys` for presence as one aligned batch. `results[i]` answers
    /// `keys[i]`. Duplicate keys keep their positions.
    ///
    /// # Errors
    ///
    /// Returns a key codec error (`Permanent`) when a key does not encode, or a
    /// session access error.
    #[instrument(
        name = "map.contains_many",
        skip_all,
        fields(collection = self.cells.name().as_str(), keys = Empty),
        err
    )]
    #[read(op)]
    pub async fn contains_many<'a, Q, I>(
        &self,
        keys: I,
    ) -> Result<Vec<bool>, MapStateError<CellCodecError<V>>>
    where
        Q: Borrow<KC::Borrowed> + ?Sized + 'a,
        I: IntoIterator<Item = &'a Q>,
        I::IntoIter: Send,
    {
        let present = op
            .contains_many(
                MapKind::<KC, V>::ENTRIES,
                keys.into_iter().map(Borrow::borrow),
            )
            .await?
            .into_vec();
        Span::current().record("keys", present.len() as i64);
        Ok(present)
    }

    /// Inserts or overwrites `key` and updates the tracked membership.
    ///
    /// # Errors
    ///
    /// Returns a key codec error (`Permanent`) when `key` does not encode, a
    /// codec error (`Permanent`) when `value` does not encode, or an access
    /// error from the session.
    #[instrument(
        name = "map.set",
        skip_all,
        fields(collection = self.cells.name().as_str(), map.key = %key),
        err
    )]
    #[write(op)]
    pub async fn set(
        &self,
        key: &KC::Borrowed,
        value: WriteOf<'_, V>,
    ) -> Result<(), MapStateError<CellCodecError<V>>>
    where
        KC::Borrowed: Display,
    {
        membership::insert(op, key, value).await
    }

    /// Removes `key` and subtracts it from the tracked keyset.
    /// Removal can bring an oversized tracked keyset below its limit.
    /// An overflowed keyset stays overflowed until clear or expiry.
    ///
    /// # Errors
    ///
    /// Returns a key codec error (`Permanent`) when a key does not encode, or a
    /// session access error.
    #[instrument(
        name = "map.remove",
        skip_all,
        fields(collection = self.cells.name().as_str(), map.key = %key),
        err
    )]
    #[write(op)]
    pub async fn remove(&self, key: &KC::Borrowed) -> Result<(), MapStateError<CellCodecError<V>>>
    where
        KC::Borrowed: Display,
    {
        membership::remove(op, key).await
    }

    /// Clears both cell families. Later writes start a new keyset.
    /// Rollback restores the last committed state.
    ///
    /// # Errors
    ///
    /// Returns a session access error.
    #[instrument(
        name = "map.clear",
        skip_all,
        fields(collection = self.cells.name().as_str()),
        err
    )]
    #[write(op)]
    pub async fn clear(&self) -> Result<(), MapStateError<CellCodecError<V>>> {
        op.clear_collection();
        Ok(())
    }

    /// Builds a query over live entries in ascending key order.
    /// Call [`ReadQuery::stream`] to create the lazy stream.
    ///
    /// A tracked keyset fixes membership when the stream starts. Values remain
    /// live: each chunk reads current values and skips absent cells. Later key
    /// additions do not appear. A chunk resolves all its values before
    /// emission; a failed chunk emits only its error. [`KeyQuery::limit`]
    /// sizes each fetch.
    ///
    /// An overflowed or invalid keyset selects a range scan. This scan captures
    /// the dirty writes when it starts and reads durable pages as needed.
    /// It hides cleared cells but can observe later commits ahead of the
    /// cursor. An absent keyset produces an empty stream without entry
    /// reads.
    ///
    /// Planning and point fetches hold session admission. Resolution and yields
    /// hold no admission; range scans run without admission after planning.
    /// The handler can mutate this map between items. Every completion checks
    /// the attempt fence, including errors and exhaustion.
    pub fn entries<'a>(
        &'a self,
    ) -> KeyRead<
        'a,
        KC,
        impl ReadSource<
            Query = BorrowedKeyQuery<'a, KC>,
            Output: Stream<Item = MapStreamItem<KC, V>> + Send,
        > + Clone
        + use<'a, S, KC, V>,
    >
    where
        for<'s> ContextOf<'s, V>: FromSession<'s, S>,
    {
        ReadQuery::new(KeyQuery::new(), move |query: BorrowedKeyQuery<'a, KC>| {
            projected::<_, _, Values>(&self.cells, query)
        })
    }

    /// Builds a query over live keys without value decoding or resolution.
    /// Message-backed maps perform no Kafka fetches. Storage presence reads
    /// still occur, and a corrupt value does not hide its key.
    /// Source selection, consistency, and admission follow [`Self::entries`].
    pub fn keys<'a>(
        &'a self,
    ) -> KeyRead<
        'a,
        KC,
        impl ReadSource<
            Query = BorrowedKeyQuery<'a, KC>,
            Output: Stream<Item = KeyItem<MapKind<KC, V>>> + Send,
        > + Clone
        + use<'a, S, KC, V>,
    > {
        ReadQuery::new(KeyQuery::new(), move |query: BorrowedKeyQuery<'a, KC>| {
            projected::<_, _, Presence>(&self.cells, query)
        })
    }

    /// Reports whether the map holds no live entries.
    ///
    /// This reads the entries section, not the keyset. After a split commit
    /// leaves keyset residue, it can report a live entry that `keys` does not
    /// list.
    ///
    /// # Errors
    ///
    /// Returns a key codec error or an access error from the session.
    #[instrument(name = "map.is_empty", skip_all, fields(collection = self.cells.name().as_str()), err)]
    pub async fn is_empty(&self) -> Result<bool, MapStateError<CellCodecError<V>>> {
        membership::is_empty(&self.cells).await
    }

    /// Durably commits this map's buffered ops mid-handler — entries and keyset
    /// together. At-least-once; the mid-handler durability section of the
    /// [`collection`](crate::state::collection) module states the contract,
    /// including the over-budget batch split.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(
        name = "map.commit",
        skip_all,
        fields(collection = self.cells.name().as_str()),
        err
    )]
    pub async fn commit(&self) -> Result<StoreOutcome, MapStateError<CellCodecError<V>>>
    where
        S: WritableStateSession,
    {
        Ok(self.cells.commit().await?)
    }

    /// Discards this map's buffered uncommitted ops — entries and keyset
    /// together — reverting reads to the last [`commit`](Self::commit), or the
    /// pre-event committed state if none. Infallible; the mid-handler
    /// durability section of the [`collection`](crate::state::collection)
    /// module states the contract.
    #[instrument(
        name = "map.rollback",
        skip_all,
        fields(collection = self.cells.name().as_str())
    )]
    pub async fn rollback(&self) -> StoreOutcome
    where
        S: WritableStateSession,
    {
        self.cells.rollback().await
    }
}

/// Declares a codec-backed ordered map collection named `name` over key codec
/// `KC` and value cell type `V` (JSON values by default). See
/// [`Descriptor::new`](super::Descriptor::new) for the `name` contract.
#[must_use]
pub fn map_state<KC, V>(name: &str) -> MapDescriptor<KC, V>
where
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
{
    MapDescriptor::new(name)
}

/// Test-only: the entry cell at key coordinate `coordinate`, so a test can
/// seed raw entry cells directly — including a coordinate that does not decode
/// as the collection's key codec — to exercise the real store's scan path.
#[cfg(test)]
pub(crate) fn entry_cell_for(coordinate: &Coordinate) -> CellKey {
    CellKey {
        section: FrozenLayout::ENTRIES.section(),
        coordinate: coordinate.clone(),
    }
}

/// Test-only: the keyset cell at its frozen address (section 0, coordinate
/// `[2]`), so a test can read the stored keyset frame directly.
#[cfg(test)]
pub(crate) fn keyset_cell() -> CellKey {
    CellKey {
        section: FrozenLayout::KEYSET.section(),
        coordinate: MapKeysetKey::encode(&()),
    }
}

impl<KC, V> Descriptor<MapKind<KC, V>> {
    /// Sets the number of live distinct keys a map or set tracks before
    /// overflow. The default is `128`. Registration rejects limits above
    /// `4096`. A limit of `0` makes the first member write overflow.
    /// Removal subtracts membership, so collections within the bound keep
    /// tracked reads. An overflowed collection uses range scans until clear
    /// or expiry.
    ///
    /// The bound controls source selection for owner reads only.
    /// Standalone readers use the global validated ceiling for the same map or
    /// set. Thus, they can use tracked reads when the owner's lower bound
    /// selects a scan. Value and deque descriptors do not expose this
    /// method.
    #[must_use]
    pub fn keyset_limit(mut self, limit: usize) -> Self {
        self.def.keyset_limit = limit;
        self
    }
}

/// Error returned by [`MapHandle`] operations.
///
/// The entry cells go through the typed collection commands, so their
/// failures — access, value-codec, or a stored key that no longer decodes — are
/// already a [`CellStateError`]. A corrupt keyset *frame* is the keyset codec's
/// own error, kept separate because it never surfaces from a well-behaved
/// handle: a malformed frame degrades reads to the scan (never errors upward)
/// and the encoder's guards are bounded away by the registration cap. The arm
/// exists to keep the keyset cell's error mapping total. (Mirrors
/// [`DequeStateError::MetaFrame`](super::deque::DequeStateError::MetaFrame).)
#[derive(Debug, Error)]
pub enum MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    /// A typed cell op failed: an access error, a value-codec failure, or a
    /// stored key that did not decode.
    #[error(transparent)]
    Cell(#[from] CellStateError<E>),

    /// The stored keyset frame was corrupt (see the type doc — never produced
    /// by a well-behaved handle).
    #[error(transparent)]
    KeysetFrame(#[from] KeysetFrameError),
}

/// An access refusal reaches the handle as the access arm of a cell error —
/// the shape the scoped write invocation's final fence reports.
impl<E> From<StateAccessError> for MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn from(error: StateAccessError) -> Self {
        Self::Cell(CellStateError::Access(error))
    }
}

impl<E> ClassifyError for MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Cell(e) => e.classify_error(),
            // A corrupt keyset frame will not decode on retry.
            Self::KeysetFrame(_) => ErrorCategory::Permanent,
        }
    }
}

#[cfg(test)]
mod tests;
