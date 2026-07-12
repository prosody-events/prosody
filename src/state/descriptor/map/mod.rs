//! Ordered key→value map collection.
//!
//! A Map stores one cell per entry, keyed by an order-preserving encoding of
//! the logical key, plus loose min/max bounds that anchor an ordered scan. The
//! [`MapHandle`] composes the uniform `CellView` typed cell interface — there
//! is no Map-specific store, session, or backend. Build a descriptor with
//! [`map_state`], register it with the consumer, and bind the
//! [`Registered`](super::Registered) handle through
//! [`EventContext::state`](crate::consumer::event_context::EventContext::state).
//!
//! # Layout
//!
//! Two sections separate bookkeeping from data (`MapNs`):
//!
//! * `Meta` holds two cells, addressed by `MapBound::Min`/`MapBound::Max`, each
//!   storing an extreme entry's *key* through `KC` itself (a key codec is its
//!   own payload codec — the byte-identity law on `OrderedKeyCodec`). A bound
//!   cell's payload bytes are `KC::encode(key)` — the very bytes the entry cell
//!   is addressed by — so a bound reads back as a logical `KC::Key`. The two
//!   bounds are stored separately (not one cell) so each ratchets
//!   independently. An empty Map has no `Meta` cells.
//! * `Entries` holds one cell per key, addressed by the key codec's
//!   order-preserving coordinate and typed by the entries cell type
//!   [`Keyed`]`<KC, V>`.
//!
//! # Invariant: min/max are a loose outward superset
//!
//! `set` ratchets the min/max bounds **outward only** and never reads the entry
//! being written, so a blind last-writer-wins entry write is preserved and the
//! bounds may name a key that was since removed — a *superset* of the live key
//! range, never a subset. `stream` anchors **both** edges of its scan on the
//! bounds — `[min, max]` ascending, mirrored descending — reading them
//! concurrently, and relies on the store hiding cleared cells, so it yields
//! exactly the live entries in key order regardless of stale bounds: a residue
//! beyond the live range is never reached, and a live key can never sit outside
//! its bounds, so bounding the scan to `[min, max]` drops no entry. When either
//! bound is absent the map holds no live entries (see the TTL-refresh
//! invariant), so no scan is issued at all. The bounds are therefore
//! self-healing — never an exact count, by design (a count would force a
//! read-before-write on every mutation). [`MapHandle::clear`] removes the
//! bounds with the entries, so the absent-bounds ⇔ empty-map reading survives
//! a cleared map.
//!
//! # Invariant: on a TTL'd map the bounds outlive every entry
//!
//! A bound cell carries the collection's TTL, but an entry `set` within the
//! existing bounds would refresh only the entry's TTL — so, left alone, the
//! bounds could expire while live entries remain. To prevent that skew, on a
//! collection with a TTL every `set` rewrites *both* bound cells (with the
//! ratcheted extremes), refreshing their TTL. So a bound cell always lives at
//! least as long as the newest entry, and **absent bounds ⇔ no live entries**.
//! A non-TTL'd map keeps the extend-only write — nothing expires, so the
//! invariant holds vacuously.

use super::{
    CellCodecError, CellScope, CellStateError, CellType, CellView, CollectionSpec, ContextOf,
    Descriptor, FromSession, Keyed, ResolvedOf, WriteOf,
};
use crate::codec::{Codec, JsonCodec};
use crate::error::{ClassifyError, ErrorCategory};
#[cfg(test)]
use crate::state::cell_key::CellKey;
use crate::state::cell_key::{Coordinate, Direction, ScanEdge, Section};
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec, UnitKey};
use crate::state::session::CellSession;
use crate::state::{CollectionKindId, StoreOutcome};
use async_stream::try_stream;
use educe::Educe;
use futures::stream::{Stream, StreamExt};
use std::error::Error;
use std::fmt::Display;
use std::marker::PhantomData;
use thiserror::Error;
use tracing::{Instrument, info_span, instrument};

/// The `Meta` section, holding the two bound cells.
const META_SECTION: Section = Section::new(MapNs::Meta as i8);

/// The `Entries` section, holding one cell per key.
const ENTRY_SECTION: Section = Section::new(MapNs::Entries as i8);

/// Map's section enum, lowered to the opaque [`Section`]. Frozen: the
/// discriminants are a durable wire contract (the `section tinyint` column),
/// pinned by `map_layout_is_frozen` and the [`TryFrom`] round-trip.
#[repr(i8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MapNs {
    /// Bookkeeping: the min/max bound cells.
    Meta = 0,

    /// Data: one cell per key.
    Entries = 1,
}

impl From<MapNs> for i8 {
    fn from(section: MapNs) -> Self {
        section as i8
    }
}

impl TryFrom<i8> for MapNs {
    type Error = UnknownMapSection;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Meta),
            1 => Ok(Self::Entries),
            _ => Err(UnknownMapSection(value)),
        }
    }
}

/// The two bound cells' logical address within the `Meta` section: the loose
/// lower and upper bounds of the live key range. Its encoding (`Min`→`[0]`,
/// `Max`→`[1]`) is a frozen durable contract, pinned by `map_layout_is_frozen`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum MapBound {
    /// The loose lower bound of the live key range.
    Min = 0,

    /// The loose upper bound of the live key range.
    Max = 1,
}

/// Address codec for the two [`MapBound`] cells. Module-fixed by the Map kind
/// (the section byte already pins the meta cells), so its
/// [`FORMAT_ID`](Codec::FORMAT_ID) never rides a collection's durable identity.
/// The meta cells are only ever read at their two fixed addresses, so `decode`
/// is a defensive round-trip, never a scan step.
#[derive(Clone, Copy, Debug, Default)]
struct MapBoundKey;

impl OrderedKeyCodec for MapBoundKey {
    type Key = MapBound;

    fn encode(key: &MapBound) -> Coordinate {
        Coordinate::from_bytes(vec![*key as u8])
    }

    fn decode(bytes: &[u8]) -> Result<MapBound, KeyCodecError> {
        match bytes {
            [0] => Ok(MapBound::Min),
            [1] => Ok(MapBound::Max),
            &[actual] => Err(KeyCodecError::BadDiscriminant { actual }),
            _ => Err(KeyCodecError::BadLength {
                expected: 1,
                actual: bytes.len(),
            }),
        }
    }
}

/// The payload half of `MapBoundKey` — delegates to `encode`/`decode`, so the
/// byte-identity law on [`OrderedKeyCodec`] holds by construction. Only the
/// *entries* key codec rides a bound cell's payload; this impl exists to
/// satisfy the supertrait, not to store bounds by.
impl Codec for MapBoundKey {
    type Error = KeyCodecError;
    type Payload = MapBound;

    const FORMAT_ID: &'static str = "map-bound.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<MapBound, KeyCodecError> {
        Self::decode(buf)
    }

    fn serialize(&mut self, payload: MapBound, buf: &mut Vec<u8>) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(Self::encode(&payload).as_bytes());
        Ok(())
    }
}

/// One item [`MapHandle::stream`] yields: a decoded key paired with its
/// resolved value, or the error that ended the stream.
type MapStreamItem<KC, V> =
    Result<(<KC as OrderedKeyCodec>::Key, ResolvedOf<V>), MapStateError<CellCodecError<V>>>;

/// Descriptor for a codec-backed ordered map collection. Generic over an
/// [`OrderedKeyCodec`] `KC` (the key encoding, frozen into the identity) and a
/// value [`CellType`] `V` — a plain [`Codec`] (JSON by default) or a codec
/// paired with a resolver via [`WithResolver`](super::WithResolver). Declare
/// via [`map_state`].
pub type MapDescriptor<KC, V = JsonCodec> = Descriptor<MapKind<KC, V>>;

/// The Map [`CollectionSpec`]: one cell per key plus the min/max bound cells;
/// the key codec is frozen into the identity.
pub struct MapKind<KC, V>(PhantomData<fn() -> (KC, V)>);

impl<KC, V> CollectionSpec for MapKind<KC, V>
where
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
{
    type Cell = Keyed<KC, V>;
    type Handle<S: CellSession> = MapHandle<S, KC, V>;

    const KIND: CollectionKindId = CollectionKindId::Map;

    fn handle<S: CellSession>(scope: CellScope<S>) -> MapHandle<S, KC, V> {
        MapHandle {
            entries: scope.typed(ENTRY_SECTION),
            meta: scope.typed(META_SECTION),
        }
    }
}

/// Typed, owned handle over a codec-backed ordered map — a thin composition
/// over two typed `CellView`s: `entries` (the per-key data cells, typed
/// [`Keyed`]`<KC, V>`) and `meta` (the min/max bound cells, each storing an
/// extreme entry's key). Every operation guards on session termination through
/// the views. Cheap `Clone`.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct MapHandle<S, KC, V> {
    entries: CellView<S, Keyed<KC, V>>,
    meta: CellView<S, Keyed<MapBoundKey, KC>>,
}

// `KC::Key: Display` exists only so the operation spans can record the map
// key as a joinable attribute (Debug would quote strings); every real key
// (`String`, `i64`, `u64`) already satisfies it, and no other map machinery
// needs it.
impl<S, KC, V> MapHandle<S, KC, V>
where
    S: CellSession,
    KC: OrderedKeyCodec + 'static,
    KC::Key: Display,
    V: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, V>: FromSession<'s, S>,
{
    /// Reads and resolves the value for `key` (`None` when absent).
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the cell does not decode, a
    /// resolution error, or an access error from the session.
    #[instrument(
        name = "map.get",
        skip_all,
        fields(collection = self.entries.name().as_str(), map.key = %key),
        err
    )]
    pub async fn get(
        &self,
        key: &KC::Key,
    ) -> Result<Option<ResolvedOf<V>>, MapStateError<CellCodecError<V>>> {
        let _permit = self.entries.read_permit().await;
        Ok(self.entries.get(key).await?)
    }

    /// Streams the live entries in key order — ascending for
    /// [`Direction::Forward`], descending for [`Direction::Backward`]. Each
    /// entry's value is resolved as it is yielded, and the entries view decodes
    /// each yielded key, so the anchors are logical keys rather than
    /// coordinates. See the module's loose-superset invariant: the scan is
    /// bounded to `[min, max]`, so a stale bound never drops a live entry, it
    /// only ever costs an extra scanned-but-cleared cell. When either bound is
    /// absent the map holds no live entries, so no scan is issued.
    pub fn stream(&self, dir: Direction) -> impl Stream<Item = MapStreamItem<KC, V>> + '_ {
        // Hand-built span: `#[instrument]` cannot follow a returned `Stream`,
        // so each inner await is instrumented with a clone instead; the
        // span's recorded time is the stream's own work. Unlike the sibling
        // ops' `err`, failures are yielded per item rather than recorded on
        // the span — a failing scan ends with an OK-status span, and the
        // yielded `Err` surfaces to the caller inside this span's scope.
        let span = info_span!(
            "map.stream",
            collection = self.entries.name().as_str(),
            direction = ?dir,
        );
        try_stream! {
            // The scan-path split contract (see `SessionGate`): the gate is
            // held only for the init metadata read — both bound cells, read
            // concurrently — and dropped before the per-item live scan, so it
            // is never held across a yield to user code. Both present or both
            // absent are the only states (extend-only writes + the TTL-refresh
            // invariant); both absent ⇒ empty map ⇒ no scan.
            let permit = self.entries.read_permit().instrument(span.clone()).await;
            let (min, max) = tokio::try_join!(
                self.read_bound(MapBound::Min).instrument(span.clone()),
                self.read_bound(MapBound::Max).instrument(span.clone())
            )?;
            drop(permit);
            let (Some(min), Some(max)) = (min, max) else {
                return;
            };
            let (start, end) = match dir {
                Direction::Forward => (ScanEdge::Included(&min), ScanEdge::Included(&max)),
                Direction::Backward => (ScanEdge::Included(&max), ScanEdge::Included(&min)),
            };
            let inner = self.entries.scan(start, dir, end, None);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().instrument(span.clone()).await {
                yield item?;
            }
        }
    }

    /// Reads a bound cell's stored extreme key (`None` when absent). The bound
    /// stores the key through `KC` itself (byte-identity law), so it reads
    /// back as a logical `KC::Key`.
    async fn read_bound(
        &self,
        bound: MapBound,
    ) -> Result<Option<KC::Key>, MapStateError<CellCodecError<V>>> {
        self.meta.get(&bound).await.map_err(meta_err)
    }

    /// Inserts or overwrites `key`'s value (a blind last-writer-wins write —
    /// the entry is never read first), ratcheting the min/max bounds
    /// outward.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when `value` does not encode, or an
    /// access error from the session.
    #[instrument(
        name = "map.set",
        skip_all,
        fields(collection = self.entries.name().as_str(), map.key = %key),
        err
    )]
    pub async fn set(
        &self,
        key: KC::Key,
        value: WriteOf<'_, V>,
    ) -> Result<(), MapStateError<CellCodecError<V>>>
    where
        KC::Key: Clone,
    {
        // ONE permit across the entry set AND the bound ratchet: the ratchet
        // is a read-modify-write over the two bound cells, so an interleaved
        // sibling `set` could lose an extension without the shared hold.
        let _permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        self.entries.set(&key, value).await?;
        self.ratchet_bounds(key).await?;
        Ok(())
    }

    /// Removes `key` (a blind clear; the bounds are left untouched, so they
    /// remain a loose superset).
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(
        name = "map.remove",
        skip_all,
        fields(collection = self.entries.name().as_str(), map.key = %key),
        err
    )]
    pub async fn remove(&self, key: &KC::Key) -> Result<(), MapStateError<CellCodecError<V>>> {
        let _permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        Ok(self.entries.clear(key).await?)
    }

    /// Removes every entry and both bounds: within the event the map reads
    /// empty from this program point (later `set`s repopulate it); committed,
    /// exactly the repopulated entries survive; aborted, the map is
    /// untouched. O(handler writes) — the entry section rides the durable
    /// section clear; only the fixed-cardinality bookkeeping cells take the
    /// per-cell path.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(
        name = "map.clear",
        skip_all,
        fields(collection = self.entries.name().as_str()),
        err
    )]
    pub async fn clear(&self) -> Result<(), MapStateError<CellCodecError<V>>> {
        let _permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        self.entries.clear_all().await?;
        self.meta.clear(&MapBound::Min).await.map_err(meta_err)?;
        self.meta.clear(&MapBound::Max).await.map_err(meta_err)?;
        Ok(())
    }

    /// Durably commits this map's buffered ops mid-handler — entries and
    /// bound ratchets together, in one batch. At-least-once; see
    /// [`CellSession::commit`] for the contract.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(
        name = "map.commit",
        skip_all,
        fields(collection = self.entries.name().as_str()),
        err
    )]
    pub async fn commit(&self) -> Result<StoreOutcome, MapStateError<CellCodecError<V>>> {
        let _permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        Ok(self.entries.commit().await?)
    }

    /// Discards this map's buffered uncommitted ops — entries and bound
    /// ratchets together — reverting reads to the last
    /// [`commit`](Self::commit), or the pre-event committed state if none.
    /// Infallible; see [`CellSession::rollback`] for the contract.
    #[instrument(
        name = "map.rollback",
        skip_all,
        fields(collection = self.entries.name().as_str())
    )]
    pub async fn rollback(&self) -> StoreOutcome {
        self.entries.rollback().await
    }

    /// Ratchets the min/max bounds outward to `key`. Reads both bound cells
    /// concurrently (they are independent), then writes the ratcheted extremes.
    ///
    /// On a **TTL'd** collection both bound cells are rewritten every `set`,
    /// even when neither extreme moves: that unconditional rewrite refreshes
    /// the bounds' TTL so they outlive every entry (the module's
    /// TTL-refresh invariant). On a **non-TTL'd** collection only a bound
    /// that `key` extends by [`Ord`] is written — nothing expires, so a
    /// rewrite would be pure churn. Either way the conditional sets stay
    /// sequential (cheap in-memory buffering, nothing to overlap): `key` is
    /// cloned into the lower write and moved into the upper.
    async fn ratchet_bounds(&self, key: KC::Key) -> Result<(), MapStateError<CellCodecError<V>>>
    where
        KC::Key: Clone,
    {
        let (min, max) = tokio::try_join!(
            self.read_bound(MapBound::Min),
            self.read_bound(MapBound::Max)
        )?;
        if self.meta.has_ttl() {
            // Compute both ratcheted extremes before the moves: `new_min` may
            // clone `key`, `new_max` consumes it.
            let new_min = min.filter(|min| *min <= key).unwrap_or_else(|| key.clone());
            let new_max = max.filter(|max| *max >= key).unwrap_or(key);
            self.meta
                .set(&MapBound::Min, new_min)
                .await
                .map_err(meta_err)?;
            self.meta
                .set(&MapBound::Max, new_max)
                .await
                .map_err(meta_err)?;
            return Ok(());
        }
        let extend_min = min.is_none_or(|min| key < min);
        let extend_max = max.is_none_or(|max| key > max);
        match (extend_min, extend_max) {
            (true, true) => {
                // Both bounds extend: clone once for Min, move into Max.
                self.meta
                    .set(&MapBound::Min, key.clone())
                    .await
                    .map_err(meta_err)?;
                self.meta.set(&MapBound::Max, key).await.map_err(meta_err)?;
            }
            (true, false) => self.meta.set(&MapBound::Min, key).await.map_err(meta_err)?,
            (false, true) => self.meta.set(&MapBound::Max, key).await.map_err(meta_err)?,
            (false, false) => {}
        }
        Ok(())
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

/// Re-homes a meta-cell access error under the map's value-codec error
/// parameter. The `meta` view's payload codec is the entries key codec `KC`,
/// so a meta codec failure means a stored bound's key bytes no longer decode —
/// a key-decode failure, folded under [`CellStateError::Key`].
fn meta_err<E>(err: CellStateError<KeyCodecError>) -> MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    match err {
        CellStateError::Access(e) => CellStateError::Access(e).into(),
        CellStateError::Codec(e) | CellStateError::Key(e) => CellStateError::Key(e).into(),
    }
}

/// Error converting an `i8` that matches no [`MapNs`] variant.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown map section discriminant: {0}")]
struct UnknownMapSection(i8);

/// Error returned by [`MapHandle`] operations.
///
/// A single typed-cell arm: both the per-key entry cells and the bound cells go
/// through the `CellView` interface, so every failure — access, value-codec,
/// or a stored key that no longer decodes — is already a [`CellStateError`].
#[derive(Debug, Error)]
pub enum MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    /// A typed cell op failed: an access error, a value-codec failure, or a
    /// stored key that did not decode.
    #[error(transparent)]
    Cell(#[from] CellStateError<E>),
}

impl<E> ClassifyError for MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Cell(e) => e.classify_error(),
        }
    }
}

/// Test-only: the entry cell at key coordinate `coordinate`, so a test can
/// seed raw entry cells directly — including a coordinate that does not decode
/// as the collection's key codec — to exercise the real store's scan path.
#[cfg(test)]
pub(crate) fn entry_cell_for(coordinate: &Coordinate) -> CellKey {
    CellKey {
        section: ENTRY_SECTION,
        coordinate: coordinate.clone(),
    }
}

/// Test-only: the `(Min, Max)` bound cells, so a test can read the stored
/// bounds directly and assert the loose-superset containment invariant.
#[cfg(test)]
pub(crate) fn bound_cells() -> (CellKey, CellKey) {
    let cell = |bound: MapBound| CellKey {
        section: META_SECTION,
        coordinate: MapBoundKey::encode(&bound),
    };
    (cell(MapBound::Min), cell(MapBound::Max))
}

#[cfg(test)]
mod tests;
