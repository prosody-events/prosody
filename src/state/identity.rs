//! Typed collection identity.
//!
//! The collection kind is carried both statically (the [`CollectionKind`]
//! type parameter) and at runtime ([`CollectionKindId`]) so future
//! collection families cannot share durable state by accident. A
//! [`CollectionRef`] pairs an identity with a per-write TTL; the TTL is a
//! hint, not part of identity.

use crate::Key;
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;

/// Stable runtime discriminator for a collection kind.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectionKindId {
    /// A single optional byte payload.
    Value = 1,

    /// Test-only fixture kind used by the encoding property tests to
    /// exercise WAL kind-mismatch detection before any production kind
    /// other than [`Self::Value`] exists.
    #[cfg(test)]
    TestSecondary = 2,
}

impl CollectionKindId {
    /// Wire discriminator persisted beside durable identity.
    ///
    /// Paired with [`Self::from_i8`]; the two are inverses by construction,
    /// so the on-wire encoding cannot drift from the type it encodes.
    pub(crate) fn as_i8(self) -> i8 {
        self as u8 as i8
    }

    /// Recovers a collection kind from its wire discriminator, or `None`
    /// for an unknown byte. Inverse of [`Self::as_i8`].
    pub(crate) fn from_i8(value: i8) -> Option<Self> {
        match value {
            1 => Some(Self::Value),
            #[cfg(test)]
            2 => Some(Self::TestSecondary),
            _ => None,
        }
    }
}

/// Type-level marker for a keyed-state collection family.
pub trait CollectionKind: Clone + Copy + fmt::Debug + Send + Sync + 'static {
    /// Runtime discriminator stored beside durable identity.
    const ID: CollectionKindId;

    /// Ordered operation persisted for this collection kind.
    type Op: Clone + fmt::Debug + Eq + Send + Sync + 'static;

    /// Authoritative applied state for this collection kind.
    type Applied: Clone + fmt::Debug + Eq + Send + Sync + 'static;

    /// Dirty read overlay for this collection kind.
    type Overlay: Clone + fmt::Debug + Eq + Send + Sync + 'static;
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
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum StateType {
    /// User application state.
    Application,
}

impl StateType {
    /// Wire discriminator for the state namespace.
    ///
    /// Paired with [`Self::from_i8`]; the two are inverses by construction,
    /// so the on-wire encoding cannot drift from the type it encodes.
    pub(crate) fn as_i8(self) -> i8 {
        match self {
            Self::Application => 0,
        }
    }

    /// Recovers a state namespace from its wire discriminator, or `None`
    /// for an unknown byte. Inverse of [`Self::as_i8`].
    pub(crate) fn from_i8(value: i8) -> Option<Self> {
        match value {
            0 => Some(Self::Application),
            _ => None,
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
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CollectionId<K>
where
    K: CollectionKind,
{
    state_key: StateKey,
    state_type: StateType,
    name: StateName,
    _kind: PhantomData<K>,
}

impl<K> CollectionId<K>
where
    K: CollectionKind,
{
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
    pub fn kind(&self) -> CollectionKindId {
        K::ID
    }
}

/// Lightweight typed reference to a collection plus the application's
/// per-collection TTL.
///
/// The TTL is `Option<CompactDuration>`: `Some(d)` binds a TTL via
/// `USING TTL ?` on every Cassandra write the store issues for this
/// collection; `None` writes via the `*_no_ttl` query variants. `None`
/// covers two first-class cases:
///
/// 1. The application opted into indefinite retention.
/// 2. The Cassandra over-20-year overflow fallback collapsed a computed TTL
///    into `None` at the wiring layer (Cassandra rejects `USING TTL ?` values
///    above `630_720_000` seconds).
///
/// Production callers either supply a per-write TTL explicitly or read it
/// from a store's `default_ttl` field (set once at construction from
/// `CassandraStore::base_ttl()`). The keyed-state stores never reach into
/// a sibling type for TTL: each store owns its own `default_ttl` and
/// threads it through `ValueStore::set` / `clear` and through recovery
/// writes. `None` is therefore a deliberate value, not a forgotten one.
/// Reads do not see the TTL; recovery callers re-supply it from the
/// store-owned default. Per-collection registry overrides are future work.
///
/// # Identity invariant
///
/// Equality, hashing, and ordering use **only** the inner [`CollectionId`].
/// Two refs to the same logical collection compare equal regardless of TTL;
/// the TTL is a per-write hint, not part of the collection's identity. The
/// `Hash`/`Eq` impls are hand-rolled (not derived) to keep a future change
/// to the struct from silently folding `ttl` into equality.
#[derive(Clone, Debug)]
pub struct CollectionRef<K>
where
    K: CollectionKind,
{
    id: CollectionId<K>,
    ttl: Option<CompactDuration>,
}

impl<K> CollectionRef<K>
where
    K: CollectionKind,
{
    /// Creates a typed collection reference. Pass `Some(ttl)` to bind a
    /// TTL on every write; pass `None` for indefinite retention or the
    /// Cassandra over-20-year overflow fallback. The TTL choice is
    /// always explicit at the callsite.
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
    K: CollectionKind + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<K> Eq for CollectionRef<K> where K: CollectionKind + Eq {}

impl<K> Hash for CollectionRef<K>
where
    K: CollectionKind + Hash,
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
