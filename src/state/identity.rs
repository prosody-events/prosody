//! Typed collection identity.
//!
//! The collection kind is carried both statically (the [`CollectionKind`]
//! type parameter) and at runtime ([`CollectionKindId`]) so future
//! collection families cannot share durable state by accident. A
//! [`CollectionRef`] pairs an identity with a per-write TTL; the TTL is a
//! hint, not part of identity.

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::transaction::Read;
use crate::timers::duration::CompactDuration;
use crate::{Key, SegmentId};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;

/// Stable runtime discriminator for a collection kind.
///
/// `#[serde(into = "i8", try_from = "i8")]` routes serde through the
/// [`From`]/[`TryFrom`] pair, so the only durable wire surface is the `i8`
/// discriminator the Cassandra `kind` column stores, never the variant name.
/// A variant rename therefore cannot drift the on-wire encoding away from the
/// type it encodes; an unknown discriminator decodes as
/// [`UnknownCollectionKindId`], which classifies `Permanent`.
#[repr(i8)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "i8", try_from = "i8")]
pub enum CollectionKindId {
    /// A single optional byte payload.
    Value = 1,

    /// Test-only fixture kind used by the identity property tests to
    /// exercise discriminator round-trip and unknown-discriminator
    /// detection before any production kind other than [`Self::Value`]
    /// exists.
    #[cfg(test)]
    TestSecondary = 2,
}

impl From<CollectionKindId> for i8 {
    fn from(id: CollectionKindId) -> Self {
        id as i8
    }
}

impl TryFrom<i8> for CollectionKindId {
    type Error = UnknownCollectionKindId;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Value),
            #[cfg(test)]
            2 => Ok(Self::TestSecondary),
            _ => Err(UnknownCollectionKindId(value)),
        }
    }
}

/// Type-level marker for a keyed-state collection family.
///
/// # Per-kind divergence: two semantic hooks, plus their byte bridge
///
/// A collection's whole per-kind behaviour reduces to two pure functions on
/// its [`Op`](Self::Op):
///
/// * [`combine`](Self::combine) compacts ops in **arrival order at write
///   time**, so a hot write-loop on one cell stays O(1) (one combined op per
///   cell, matching the original compact-on-write dirty store). It folds
///   `combine(existing, newest)` left-to-right, so it need not be commutative —
///   only consistent with replaying the ops one at a time.
/// * [`apply`](Self::apply) folds the one combined op over the committed base
///   **at stage time**, producing the cell's new committed bytes.
///
/// Value is last-writer-wins: `combine` keeps the newest op and `apply` ignores
/// the base. An additive sketch (Count-Min, a counter) makes `combine` add
/// deltas and `apply` add the combined delta to the base. A kind needing the
/// full op history sets `Op` to a sequence and `combine` to concatenation, so
/// these hooks **subsume** a fold-over-vector rather than foreclose it.
///
/// [`set_op`](Self::set_op) / [`clear_op`](Self::clear_op) /
/// [`read_overlay`](Self::read_overlay) are the byte bridge between the
/// session's uniform point-cell API (set/clear/read raw bytes at a
/// [`CellAddr`](Self::CellAddr)) and the kind's `Op`: they are mechanical, not
/// semantic — the divergence lives in `combine`/`apply`.
pub trait CollectionKind: Clone + Copy + fmt::Debug + Send + Sync + 'static {
    /// Runtime discriminator stored beside durable identity.
    const ID: CollectionKindId;

    /// Ordered operation persisted for this collection kind.
    type Op: Clone + fmt::Debug + Eq + Send + Sync + 'static;

    /// Address of one cell within a collection.
    ///
    /// A collection is a set of independently durable cells, each carrying
    /// its own [`Cell`](crate::state::cell::Cell). Value is a single cell,
    /// so its address is `()`. Map's address is the encoded entry key;
    /// Deque's is the slot index or header marker. The durability layer is
    /// written once over this address; only the per-kind table shape and the
    /// read-side fold differ.
    type CellAddr: Clone + fmt::Debug + Hash + Eq + Send + Sync + 'static;

    /// Builds the op for "set this cell to these bytes" — the byte bridge for
    /// the session's uniform `set_cell`. Value wraps the bytes verbatim; an
    /// additive kind decodes them into a delta.
    fn set_op(cell: &[u8]) -> Self::Op;

    /// Builds the op for "clear this cell" — the byte bridge for the session's
    /// uniform `clear_cell`.
    fn clear_op() -> Self::Op;

    /// Compacts two ops in **arrival order** into one combined op. The dirty
    /// store folds `combine(existing, newest)` per write, so this is a left
    /// fold over the touched cell's op history.
    fn combine(existing: Self::Op, newest: Self::Op) -> Self::Op;

    /// The cell's visible value from a buffered op **alone**, when the op fully
    /// determines it without the committed base (Value `Set`/`Clear`).
    /// [`Read::Unknown`] means the buffered op cannot answer the read on its
    /// own (an additive delta), so the read falls through to the committed
    /// base.
    fn read_overlay(op: &Self::Op) -> Read<Bytes>;

    /// Folds the one combined op over the committed base at stage time,
    /// producing the cell's new committed bytes (`None` = cleared). Value
    /// ignores the base; an additive kind adds the combined delta to it.
    fn apply(committed_base: Option<Bytes>, op: &Self::Op) -> Option<Bytes>;
}

/// Key qualified by the timer segment that owns the Kafka partition.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct StateKey {
    /// Segment that owns this state key.
    pub segment_id: SegmentId,

    /// Application key within the segment.
    pub key: Key,
}

impl StateKey {
    /// Creates a segment-qualified state key.
    #[must_use]
    pub fn new(segment_id: SegmentId, key: Key) -> Self {
        Self { segment_id, key }
    }
}

/// Logical state namespace.
///
/// The wire discriminator persisted beside durable identity is the `i8` the
/// [`From`]/[`TryFrom`] pair round-trips through, so the on-wire encoding
/// cannot drift from the type it encodes.
#[repr(i8)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum StateType {
    /// User application state.
    Application = 0,

    /// Test-only fixture namespace used by the identity property tests to
    /// prove a name is namespaced by `state_type` (a second namespace can
    /// share a name with [`Self::Application`] without colliding) before a
    /// second production namespace exists.
    #[cfg(test)]
    Framework = 1,
}

impl From<StateType> for i8 {
    fn from(state_type: StateType) -> Self {
        state_type as i8
    }
}

impl TryFrom<i8> for StateType {
    type Error = UnknownStateType;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Application),
            #[cfg(test)]
            1 => Ok(Self::Framework),
            _ => Err(UnknownStateType(value)),
        }
    }
}

/// Human-readable state collection name.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct StateName(Arc<str>);

impl StateName {
    /// Creates a non-empty state name.
    ///
    /// # Errors
    ///
    /// Returns [`StateNameError`] when the trimmed name is empty.
    pub fn try_new<N>(name: N) -> Result<Self, StateNameError>
    where
        N: AsRef<str>,
    {
        let name = name.as_ref().trim();
        if name.is_empty() {
            return Err(StateNameError);
        }

        Ok(Self(Arc::from(name)))
    }

    /// Returns the state name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for StateName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Lets registry maps keyed by [`StateName`] resolve `&str` lookups without
/// allocating. Sound because the derived `Hash`/`Eq` delegate to the inner
/// `str`, matching `str`'s own implementations.
impl Borrow<str> for StateName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

/// Fully qualified typed collection identity.
///
/// Equality, hashing, and ordering use only the data fields; the phantom
/// `K` is hand-rolled out of those impls so `CollectionId<K>` is `Hash`/`Eq`
/// without requiring `K: Hash + Eq` (the kind is carried for type safety,
/// not identity). `Clone`/`Debug` derive cleanly because [`CollectionKind`]
/// already requires `Clone + Copy + Debug`.
#[derive(Clone, Debug)]
pub struct CollectionId<K> {
    state_key: StateKey,
    state_type: StateType,
    name: StateName,
    _kind: PhantomData<K>,
}

impl<K> PartialEq for CollectionId<K> {
    fn eq(&self, other: &Self) -> bool {
        self.state_key == other.state_key
            && self.state_type == other.state_type
            && self.name == other.name
    }
}

impl<K> Eq for CollectionId<K> {}

impl<K> Hash for CollectionId<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.state_key.hash(state);
        self.state_type.hash(state);
        self.name.hash(state);
    }
}

impl<K> CollectionId<K> {
    /// Creates a collection identity for the type-level kind `K`.
    #[must_use]
    pub fn new(state_key: StateKey, state_type: StateType, name: StateName) -> Self {
        Self {
            state_key,
            state_type,
            name,
            _kind: PhantomData,
        }
    }

    /// Returns the segment-qualified state key.
    #[must_use]
    pub fn state_key(&self) -> &StateKey {
        &self.state_key
    }

    /// Returns the state namespace.
    #[must_use]
    pub fn state_type(&self) -> StateType {
        self.state_type
    }

    /// Returns the collection name.
    #[must_use]
    pub fn name(&self) -> &StateName {
        &self.name
    }

    /// Returns the runtime collection kind discriminator.
    #[must_use]
    pub fn kind(&self) -> CollectionKindId
    where
        K: CollectionKind,
    {
        K::ID
    }
}

/// Lightweight typed reference to a collection plus the application's
/// per-collection TTL.
///
/// The TTL is `Option<CompactDuration>`: `Some(d)` binds a TTL via
/// `USING TTL ?` on every Cassandra write the store issues for this
/// collection; `None` writes via the `*_no_ttl` query variants and means the
/// application opted into indefinite retention. An over-ceiling `Some(d)`
/// (Cassandra rejects `USING TTL ?` values above `630_720_000` seconds) is
/// rejected at `CollectionDefRegistry::register` time — never silently
/// collapsed to `None`, which would turn a finite retention into permanent
/// storage.
///
/// Production callers either supply a per-collection TTL explicitly (from the
/// registry) or read a store's `default_ttl` field (set once at construction
/// from `CassandraStore::base_ttl()`). The keyed-state stores never reach
/// into a sibling type for TTL: each store owns its own `default_ttl` and
/// threads it through `ValueStore::set` / `clear` and through recovery
/// writes. `None` is therefore a deliberate value, not a forgotten one.
/// Reads do not see the TTL; recovery callers re-supply it from the
/// store-owned default.
///
/// # Identity invariant
///
/// Equality, hashing, and ordering use **only** the inner [`CollectionId`].
/// Two refs to the same logical collection compare equal regardless of TTL;
/// the TTL is a per-write hint, not part of the collection's identity. The
/// `Hash`/`Eq` impls are hand-rolled (not derived) to keep a future change
/// to the struct from silently folding `ttl` into equality.
#[derive(Clone, Debug)]
pub struct CollectionRef<K> {
    id: CollectionId<K>,
    ttl: Option<CompactDuration>,
}

impl<K> CollectionRef<K> {
    /// Creates a typed collection reference. Pass `Some(ttl)` to bind a
    /// TTL on every write; pass `None` for indefinite retention. The TTL
    /// choice is always explicit at the callsite, and an over-ceiling TTL
    /// was already rejected at registration.
    #[must_use]
    pub fn new(id: CollectionId<K>, ttl: Option<CompactDuration>) -> Self {
        Self { id, ttl }
    }

    /// Returns the typed collection identity.
    #[must_use]
    pub fn id(&self) -> &CollectionId<K> {
        &self.id
    }

    /// Returns the per-collection TTL, if any.
    #[must_use]
    pub fn ttl(&self) -> Option<CompactDuration> {
        self.ttl
    }
}

impl<K> PartialEq for CollectionRef<K>
where
    K: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<K> Eq for CollectionRef<K> where K: Eq {}

impl<K> Hash for CollectionRef<K>
where
    K: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// Error returned for an empty state name.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("state name must not be empty")]
pub struct StateNameError;

impl ClassifyError for StateNameError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

/// Error converting an `i8` that matches no [`StateType`] variant.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown state type discriminator: {0}")]
pub struct UnknownStateType(i8);

/// Error converting an `i8` that matches no [`CollectionKindId`] variant.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown collection kind discriminator: {0}")]
pub struct UnknownCollectionKindId(i8);
