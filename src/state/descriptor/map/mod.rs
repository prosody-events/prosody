//! Ordered key→value map collection.
//!
//! A Map stores one cell per entry, keyed by an order-preserving encoding of
//! the logical key, plus loose min/max bounds that anchor an ordered scan. The
//! [`MapHandle`] composes the uniform [`CellView`] cell interface — there is no
//! Map-specific store, session, or backend. Build a descriptor with
//! [`map_state`], register it with the consumer, and bind the
//! [`Registered`](super::Registered) handle through
//! [`EventContext::state`](crate::consumer::event_context::EventContext::state).
//!
//! # Layout
//!
//! Two sections separate bookkeeping from data (`MapNs`):
//!
//! * `Meta` holds two cells, `META_MIN` and `META_MAX`, each storing the entry
//!   coordinate bytes of an extreme key. They are stored separately (not one
//!   cell) so each ratchets independently. An empty Map has no `Meta` cells.
//! * `Entries` holds one cell per key, addressed by the key codec's
//!   order-preserving coordinate ([`OrderedKeyCodec::encode`]).
//!
//! # Invariant: min/max are a loose outward superset
//!
//! `set` ratchets `META_MIN`/`META_MAX` **outward only** and never reads the
//! entry being written, so a blind last-writer-wins entry write is preserved
//! and the bounds may point at a key that was since removed — a *superset* of
//! the live key range, never a subset. `stream` anchors its forward scan at
//! `META_MIN` (falling back to the empty coordinate when the bound is missing
//! or expired) and relies on the store hiding cleared cells, so it yields
//! exactly the live entries in key order regardless of stale bounds: a
//! below-min residue is never reached, a live key can never sit above max, and
//! the missing-bound fallback scans from the start. The bounds are therefore
//! self-healing — never an exact count, by design (a count would force a
//! read-before-write on every mutation).

use super::{
    CellView, DescriptorIdentity, StateDescriptor, StructuralIdentity, decode_cell, encode_cell,
    ensure_live, intern_descriptor_str,
};
use crate::codec::{Codec, JsonCodec};
use crate::consumer::event_context::StateAccessError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec};
use crate::state::registry::CollectionDef;
use crate::state::session::{CellRead, CellSession};
use crate::state::{CollectionKindId, CommitMode, StateName, StateType};
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use futures::stream::{Stream, StreamExt};
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use thiserror::Error;

/// The `Meta` section, holding the two bound cells.
const META_SECTION: Section = Section::new(MapNs::Meta as i8);

/// The `Entries` section, holding one cell per key.
const ENTRY_SECTION: Section = Section::new(MapNs::Entries as i8);

/// Map's section enum, lowered to the opaque [`Section`]. Frozen: the
/// discriminants are a durable wire contract (the `section tinyint` column), so
/// the [`TryFrom`] guard rejects any other value as
/// [`MapStateError::Section`] (`Permanent`).
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

/// One streamed map entry: a decoded key and value, or the error that decoding
/// it raised. Factored out of [`MapHandle::stream`]'s return type.
pub type MapEntry<KC, VC> = Result<
    (<KC as OrderedKeyCodec>::Key, <VC as Codec>::Payload),
    MapStateError<<VC as Codec>::Error>,
>;

/// Descriptor for a codec-backed ordered map collection. Generic over an
/// [`OrderedKeyCodec`] (the key encoding, frozen into the identity) and a value
/// [`Codec`] (JSON by default). There is no resolver. Declare via
/// [`map_state`].
pub struct MapDescriptor<KC, VC = JsonCodec> {
    name: &'static str,
    def: CollectionDef,
    _marker: PhantomData<fn() -> (KC, VC)>,
}

impl<KC, VC> Clone for MapDescriptor<KC, VC> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<KC, VC> Copy for MapDescriptor<KC, VC> {}

impl<KC, VC> fmt::Debug for MapDescriptor<KC, VC> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapDescriptor")
            .field("name", &self.name)
            .field("def", &self.def)
            .finish()
    }
}

impl<KC, VC> MapDescriptor<KC, VC> {
    /// Declares a map collection named `name`. `name` may be any runtime string
    /// and is interned (it stays `Copy`); an empty name fails loudly at
    /// registration, the fallible boundary.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: intern_descriptor_str(name),
            def: CollectionDef::new(None),
            _marker: PhantomData,
        }
    }

    /// Sets the collection's per-write TTL, validated at registration.
    #[must_use]
    pub fn ttl(mut self, ttl: CompactDuration) -> Self {
        self.def.ttl = Some(ttl);
        self
    }

    /// Clears the collection's TTL, selecting indefinite retention (the
    /// default).
    #[must_use]
    pub fn no_ttl(mut self) -> Self {
        self.def.ttl = None;
        self
    }

    /// Selects [`CommitMode::ReadCommitted`] (the default).
    #[must_use]
    pub fn read_committed(mut self) -> Self {
        self.def.commit_mode = CommitMode::ReadCommitted;
        self
    }

    /// Selects [`CommitMode::ReadUncommitted`].
    #[must_use]
    pub fn read_uncommitted(mut self) -> Self {
        self.def.commit_mode = CommitMode::ReadUncommitted;
        self
    }
}

impl<KC, VC> DescriptorIdentity for MapDescriptor<KC, VC>
where
    KC: OrderedKeyCodec,
    VC: Codec,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Map,
            codec_id: VC::CODEC_ID,
            resolver_id: None,
            key_codec_id: Some(KC::KEY_CODEC_ID),
        }
    }
}

impl<KC, VC> StateDescriptor for MapDescriptor<KC, VC>
where
    KC: OrderedKeyCodec,
    VC: Codec,
{
    type Handle<S: CellRead> = MapHandle<S, KC, VC>;

    fn bind<S: CellRead>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError> {
        let name = session.verify_state_registration(
            self.name,
            self.state_type(),
            &self.structural_identity(),
        )?;
        Ok(MapHandle::new(session.clone(), self.state_type(), name))
    }

    fn collection_def(&self) -> CollectionDef {
        self.def
    }
}

/// Typed, owned handle over a codec-backed ordered map — a thin composition
/// over a [`CellView`]. Reads (`get`/`stream`) need only [`CellRead`]; the
/// mutators (`set`/`remove`) need [`CellSession`], so a reader-minted handle
/// has the readers but **cannot name** a mutator (the read-only-handle
/// invariant). Every operation guards on session termination.
pub struct MapHandle<S, KC, VC> {
    view: CellView<S>,
    _marker: PhantomData<fn() -> (KC, VC)>,
}

impl<S, KC, VC> MapHandle<S, KC, VC> {
    /// Wraps a verified session and the binding descriptor's `(state_type,
    /// name)`. Codec-agnostic so [`StateDescriptor::bind`] mints it without the
    /// codec bounds.
    fn new(session: S, state_type: StateType, name: StateName) -> Self {
        Self {
            view: CellView::new(session, state_type, name),
            _marker: PhantomData,
        }
    }
}

impl<S: Clone, KC, VC> Clone for MapHandle<S, KC, VC> {
    fn clone(&self) -> Self {
        Self {
            view: self.view.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, KC, VC> MapHandle<S, KC, VC>
where
    S: CellRead,
    KC: OrderedKeyCodec,
    VC: Codec,
{
    /// Reads the value for `key` (`None` when absent).
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the cell does not decode, or an
    /// access error from the session.
    pub async fn get(
        &self,
        key: &KC::Key,
    ) -> Result<Option<VC::Payload>, MapStateError<VC::Error>> {
        ensure_live(self.view.session())?;
        let coordinate = KC::encode(key);
        self.view
            .get(&entry_cell(&coordinate))
            .await?
            .map(|bytes| decode_cell::<VC>(bytes).map_err(MapStateError::Codec))
            .transpose()
    }

    /// Streams the live entries in key order.
    ///
    /// Anchors the scan at `META_MIN` (falling back to the empty coordinate
    /// when the bound is missing) and skips cleared cells, so a stale bound
    /// never drops a live entry (the loose-superset invariant).
    pub fn stream(&self) -> impl Stream<Item = MapEntry<KC, VC>> + '_ {
        try_stream! {
            ensure_live(self.view.session())?;
            let start = self
                .read_bound(&meta_min_cell())
                .await?
                .unwrap_or_else(Coordinate::empty);
            let scan = Scan {
                section: ENTRY_SECTION,
                start: &start,
                dir: Direction::Forward,
                end: None,
                limit: None,
            };
            let inner = self.view.scan(scan);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                let (key, bytes) = item.map_err(|e| MapStateError::Access(StateAccessError::store(&e)))?;
                entry_section_guard(key.section)?;
                let logical = KC::decode(key.coordinate.as_bytes()).map_err(MapStateError::Key)?;
                let value = decode_cell::<VC>(bytes).map_err(MapStateError::Codec)?;
                yield (logical, value);
            }
        }
    }

    /// Reads a bound cell's stored entry coordinate (`None` when absent).
    async fn read_bound(
        &self,
        cell: &CellKey,
    ) -> Result<Option<Coordinate>, MapStateError<VC::Error>> {
        Ok(self.view.get(cell).await?.map(Coordinate::from_bytes))
    }
}

impl<S, KC, VC> MapHandle<S, KC, VC>
where
    S: CellSession,
    KC: OrderedKeyCodec,
    VC: Codec,
{
    /// Inserts or overwrites `key`'s value (a blind last-writer-wins write —
    /// the entry is never read first), ratcheting the min/max bounds
    /// outward.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when `value` does not encode, or an
    /// access error from the session.
    pub async fn set(
        &self,
        key: &KC::Key,
        value: VC::Payload,
    ) -> Result<(), MapStateError<VC::Error>> {
        ensure_live(self.view.session())?;
        let coordinate = KC::encode(key);
        let buf = encode_cell::<VC>(value).map_err(MapStateError::Codec)?;
        self.view.set(&entry_cell(&coordinate), &buf).await?;
        self.ratchet_min(&coordinate).await?;
        self.ratchet_max(&coordinate).await?;
        Ok(())
    }

    /// Removes `key` (a blind clear; the bounds are left untouched, so they
    /// remain a loose superset).
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub async fn remove(&self, key: &KC::Key) -> Result<(), MapStateError<VC::Error>> {
        ensure_live(self.view.session())?;
        let coordinate = KC::encode(key);
        self.view.clear(&entry_cell(&coordinate)).await?;
        Ok(())
    }

    /// Lowers `META_MIN` to `coordinate` when it is a new minimum (or absent).
    async fn ratchet_min(&self, coordinate: &Coordinate) -> Result<(), MapStateError<VC::Error>> {
        let min_cell = meta_min_cell();
        let extend = self
            .read_bound(&min_cell)
            .await?
            .is_none_or(|min| coordinate.as_bytes() < min.as_bytes());
        if extend {
            self.view.set(&min_cell, coordinate.as_bytes()).await?;
        }
        Ok(())
    }

    /// Raises `META_MAX` to `coordinate` when it is a new maximum (or absent).
    async fn ratchet_max(&self, coordinate: &Coordinate) -> Result<(), MapStateError<VC::Error>> {
        let max_cell = meta_max_cell();
        let extend = self
            .read_bound(&max_cell)
            .await?
            .is_none_or(|max| coordinate.as_bytes() > max.as_bytes());
        if extend {
            self.view.set(&max_cell, coordinate.as_bytes()).await?;
        }
        Ok(())
    }
}

/// Declares a codec-backed ordered map collection named `name` over key codec
/// `KC` and value codec `VC` (JSON values by default).
///
/// `name` may be any runtime string and is interned (it stays `Copy`); an empty
/// name fails loudly at registration, the fallible boundary.
#[must_use]
pub fn map_state<KC, VC>(name: &str) -> MapDescriptor<KC, VC>
where
    KC: OrderedKeyCodec,
    VC: Codec,
{
    MapDescriptor::new(name)
}

/// The entry cell at key coordinate `coordinate`.
fn entry_cell(coordinate: &Coordinate) -> CellKey {
    CellKey {
        section: ENTRY_SECTION,
        coordinate: coordinate.clone(),
    }
}

/// Test-only: the entry cell for a key coordinate, so a test can seed raw
/// entries directly (entries with a missing `META_MIN`) and prove the
/// missing-bound fallback against the real store.
#[cfg(test)]
pub(crate) fn entry_cell_for(coordinate: &Coordinate) -> CellKey {
    entry_cell(coordinate)
}

/// Test-only: the `(META_MIN, META_MAX)` bound cells, so a test can read the
/// stored bounds directly and assert the loose-superset containment invariant.
#[cfg(test)]
pub(crate) fn bound_cells() -> (CellKey, CellKey) {
    (meta_min_cell(), meta_max_cell())
}

/// The `META_MIN` bound cell.
fn meta_min_cell() -> CellKey {
    CellKey {
        section: META_SECTION,
        coordinate: Coordinate::from_bytes(vec![0u8]),
    }
}

/// The `META_MAX` bound cell.
fn meta_max_cell() -> CellKey {
    CellKey {
        section: META_SECTION,
        coordinate: Coordinate::from_bytes(vec![1u8]),
    }
}

/// Asserts a scanned cell sits in the `Entries` section — a defensive,
/// forward-compatible guard (today's scan is already section-scoped). Drives
/// the [`MapNs`] `TryFrom` so a stale cell carrying an unknown or `Meta`
/// section fails loudly rather than being decoded as an entry.
fn entry_section_guard<E>(section: Section) -> Result<(), MapStateError<E>>
where
    E: Error + Send + Sync + 'static,
{
    let raw = i8::from(section);
    match MapNs::try_from(raw) {
        Ok(MapNs::Entries) => Ok(()),
        Ok(MapNs::Meta) | Err(UnknownMapSection(_)) => Err(MapStateError::Section(raw)),
    }
}

/// Error converting an `i8` that matches no [`MapNs`] variant.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown map section discriminant: {0}")]
struct UnknownMapSection(i8);

/// Error returned by [`MapHandle`] operations.
#[derive(Debug, Error)]
pub enum MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    /// The context refused or failed the state access.
    #[error(transparent)]
    Access(#[from] StateAccessError),

    /// The value codec failed to encode or decode a cell.
    #[error("map value codec failed")]
    Codec(#[source] E),

    /// A stored key coordinate did not decode back to a logical key.
    #[error(transparent)]
    Key(#[from] KeyCodecError),

    /// A scanned cell carried a section that is not the entry section — a
    /// defensive guard; `Permanent`.
    #[error("unexpected map section discriminant: {0}")]
    Section(i8),
}

impl<E> ClassifyError for MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Access(e) => e.classify_error(),
            Self::Key(e) => e.classify_error(),
            // A cell that does not round-trip, or a cell in an unexpected
            // section, will not begin to behave on retry.
            Self::Codec(_) | Self::Section(_) => ErrorCategory::Permanent,
        }
    }
}

#[cfg(test)]
mod tests;
