//! Collection identity.
//!
//! A collection is one Cassandra partition `(segment_id, key, state_type,
//! name)`; [`CollectionId`] names it and [`CollectionRef`] pairs it with a
//! per-write TTL (a hint, not part of identity). The collection *kind* is not
//! representable below the descriptor layer — the cell core addresses by
//! [`CellKey`](super::cell_key::CellKey) and never names Value/Map/Deque — so
//! the runtime [`CollectionKindId`] survives only as the durable identity
//! token the descriptor layer validates.

use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::duration::CompactDuration;
use crate::{Key, SegmentId};
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use thiserror::Error;

/// Stable runtime discriminator for a collection kind.
///
/// The only durable wire surface is the `i8` the [`From`] impl produces for
/// the Cassandra `kind` column, so a variant rename cannot drift the on-wire
/// encoding away from the type it encodes. There is no decode direction: the
/// identity read path **compares** the stored `i8` against the asserted
/// durable identity's raw field, never reconstructing the enum. The
/// variant→byte assignments are pinned by
/// `durable_identity_wire_contract_is_frozen`.
#[repr(i8)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CollectionKindId {
    /// A single optional byte payload.
    Value = 1,

    /// An ordered key→value map.
    Map = 2,

    /// An index-addressed double-ended queue.
    Deque = 3,

    /// A presence-only ordered set.
    Set = 4,
}

impl From<CollectionKindId> for i8 {
    fn from(id: CollectionKindId) -> Self {
        id as i8
    }
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
/// [`From`] impl produces; like [`CollectionKindId`], reads compare the stored
/// raw `i8`, never decoding it back into the enum.
#[repr(i8)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
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

/// Human-readable state collection name.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
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

/// Shares the name's backing allocation, so an error or log field that needs an
/// owned name never copies the bytes.
impl From<&StateName> for Arc<str> {
    fn from(name: &StateName) -> Self {
        Arc::clone(&name.0)
    }
}

/// Lets registry maps keyed by [`StateName`] resolve `&str` lookups without
/// allocating (`CollectionDefRegistry::lookup` passes the descriptor's
/// `&'static str` straight to `HashMap::get_key_value`). Sound because the
/// derived `Hash`/`Eq` delegate to the inner `str`, matching `str`'s own
/// implementations.
impl Borrow<str> for StateName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

/// Fully qualified collection identity: the four columns of one Cassandra
/// partition `(segment_id, key, state_type, name)`.
///
/// Carries no collection kind — the cell core addresses cells by
/// [`CellKey`](super::cell_key::CellKey) and never names a collection family.
/// The kind lives only in the durable [`StructuralIdentity`] the descriptor
/// layer validates.
///
/// [`StructuralIdentity`]: super::descriptor::StructuralIdentity
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CollectionId {
    state_key: StateKey,
    state_type: StateType,
    name: StateName,
}

impl CollectionId {
    /// Creates a collection identity.
    #[must_use]
    pub fn new(state_key: StateKey, state_type: StateType, name: StateName) -> Self {
        Self {
            state_key,
            state_type,
            name,
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
/// The per-collection TTL is sourced from the shared
/// `CollectionDefRegistry`: the
/// session builds a `CollectionRef` at stage time, and the bottom store builds
/// one for its resolution write-backs, so both bind the same TTL. `None` is a
/// deliberate value (indefinite retention), not a forgotten one. Reads do not
/// see the TTL.
///
/// # Identity invariant
///
/// Equality, hashing, and ordering use **only** the inner [`CollectionId`].
/// Two refs to the same logical collection compare equal regardless of TTL;
/// the TTL is a per-write hint, not part of the collection's identity. The
/// `Hash`/`Eq` impls are hand-rolled (not derived) to keep a future change
/// to the struct from silently folding `ttl` into equality.
#[derive(Clone, Debug)]
pub struct CollectionRef {
    id: CollectionId,
    ttl: Option<CompactDuration>,
}

impl CollectionRef {
    /// Creates a collection reference. Pass `Some(ttl)` to bind a TTL on every
    /// write; pass `None` for indefinite retention. The TTL choice is always
    /// explicit at the callsite, and an over-ceiling TTL was already rejected
    /// at registration.
    #[must_use]
    pub fn new(id: CollectionId, ttl: Option<CompactDuration>) -> Self {
        Self { id, ttl }
    }

    /// Returns the collection identity.
    #[must_use]
    pub fn id(&self) -> &CollectionId {
        &self.id
    }

    /// Returns the per-collection TTL, if any.
    #[must_use]
    pub fn ttl(&self) -> Option<CompactDuration> {
        self.ttl
    }
}

impl PartialEq for CollectionRef {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for CollectionRef {}

impl Hash for CollectionRef {
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
