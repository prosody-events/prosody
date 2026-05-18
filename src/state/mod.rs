//! Keyed application state protocol types.
//!
//! This module defines the shared typed identity and transaction state shapes
//! used by keyed state stores. The first implemented collection kind is
//! [`ValueKind`], but collection identities carry the kind both statically and
//! at runtime so future collection families cannot share state by accident.

use crate::Key;
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::store::SegmentId;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::Arc;
use thiserror::Error;

pub mod memory;
pub mod value;

#[cfg(test)]
mod tests;

pub use value::{StoredPayload, ValueApplied, ValueKind, ValueOp, ValueOverlay};

/// Stable runtime discriminator for a collection kind.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CollectionKindId {
    /// A single optional byte payload.
    Value = 1,
}

/// Type-level marker for a keyed-state collection family.
pub trait CollectionKind: Clone + Copy + fmt::Debug + Send + Sync + 'static {
    /// Runtime discriminator stored beside durable identity.
    const ID: CollectionKindId;
}

/// Collection kind with typed operations and applied state.
pub trait StatefulCollectionKind: CollectionKind {
    /// Ordered operation persisted for this collection kind.
    type Op: Clone + fmt::Debug + Eq + Send + Sync + 'static;

    /// Authoritative applied state for this collection kind.
    type Applied: Clone + fmt::Debug + Eq + Send + Sync + 'static;
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

/// Fully qualified typed collection identity.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CollectionId<K>
where
    K: CollectionKind,
{
    state_key: StateKey,
    state_type: StateType,
    name: StateName,
    kind: CollectionKindId,
    marker: PhantomData<K>,
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
            kind: K::ID,
            marker: PhantomData,
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
        self.kind
    }
}

/// Per-event scope identity used by commit recovery.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct EventScopeId(u128);

impl EventScopeId {
    /// Creates an event scope identifier.
    #[must_use]
    pub fn new(id: u128) -> Self {
        Self(id)
    }

    /// Returns the raw identifier value.
    #[must_use]
    pub fn get(self) -> u128 {
        self.0
    }
}

/// Event kind associated with a local transaction.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum EventRef {
    /// Kafka message event.
    Message(EventScopeId),

    /// Timer event.
    Timer(EventScopeId),
}

/// Result of resolving a sealed durable transaction.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CommitDecision {
    /// The sealed operations were committed.
    Committed,

    /// No sealed operations were committed.
    NotCommitted,
}

/// Persistence mode for local state changes.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CommitMode {
    /// Seal dirty operations before the event commit oracle resolves them.
    Wal,

    /// Apply dirty operations directly with no sealed write-ahead state.
    Direct,
}

/// Three-valued read used by overlays.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum Read<T> {
    /// Value is present.
    Present(T),

    /// Value is known absent.
    Absent,

    /// This layer has not observed the value.
    Unknown,
}

impl<T> Read<T> {
    /// Maps a present value while preserving absence and unknown.
    pub fn map<U, F>(self, f: F) -> Read<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Self::Present(value) => Read::Present(f(value)),
            Self::Absent => Read::Absent,
            Self::Unknown => Read::Unknown,
        }
    }
}

/// Durable collection state is either idle or sealed for one event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurableState<K>
where
    K: StatefulCollectionKind,
{
    /// No sealed operations are pending; `applied` is authoritative.
    Idle {
        /// Collection identity.
        collection: CollectionId<K>,

        /// Authoritative applied state.
        applied: K::Applied,
    },

    /// A non-empty ordered operation list is sealed for recovery.
    Sealed(SealedCollection<K>),
}

/// Local transaction state for one collection and event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalTx<K>
where
    K: StatefulCollectionKind,
{
    /// No dirty operations are pending.
    Clean(CollectionRef<K>),

    /// Dirty operations are buffered in the local pending store.
    Dirty(DirtyCollection<K>),

    /// Dirty operations have been sealed durably.
    Sealed(SealedCollection<K>),

    /// The transaction was resolved and must not transition again.
    Finished,
}

/// Lightweight typed reference to a collection.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CollectionRef<K>
where
    K: CollectionKind,
{
    id: CollectionId<K>,
}

impl<K> CollectionRef<K>
where
    K: CollectionKind,
{
    /// Creates a typed collection reference.
    #[must_use]
    pub fn new(id: CollectionId<K>) -> Self {
        Self { id }
    }

    /// Returns the typed collection identity.
    #[must_use]
    pub fn id(&self) -> &CollectionId<K> {
        &self.id
    }
}

/// Dirty collection marker with a statically non-zero operation count.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DirtyCollection<K>
where
    K: CollectionKind,
{
    collection: CollectionRef<K>,
    operation_count: NonZeroU64,
}

impl<K> DirtyCollection<K>
where
    K: CollectionKind,
{
    /// Creates a dirty marker from a non-zero operation count.
    #[must_use]
    pub fn new(collection: CollectionRef<K>, operation_count: NonZeroU64) -> Self {
        Self {
            collection,
            operation_count,
        }
    }

    /// Creates a dirty marker from a pending operation slice length.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyOperationsError`] when `operation_count` is zero.
    pub fn try_from_count(
        collection: CollectionRef<K>,
        operation_count: usize,
    ) -> Result<Self, EmptyOperationsError> {
        let Some(operation_count) = NonZeroU64::new(operation_count as u64) else {
            return Err(EmptyOperationsError);
        };

        Ok(Self::new(collection, operation_count))
    }

    /// Returns the collection reference.
    #[must_use]
    pub fn collection(&self) -> &CollectionRef<K> {
        &self.collection
    }

    /// Returns the number of buffered operations.
    #[must_use]
    pub fn operation_count(&self) -> NonZeroU64 {
        self.operation_count
    }
}

/// Durable sealed state for one event and non-empty ordered operation list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedCollection<K>
where
    K: StatefulCollectionKind,
{
    collection: CollectionRef<K>,
    event: EventRef,
    applied: K::Applied,
    ops: Vec<K::Op>,
    operation_count: NonZeroU64,
}

impl<K> SealedCollection<K>
where
    K: StatefulCollectionKind,
{
    /// Creates sealed state from a non-empty ordered operation list.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyOperationsError`] when `ops` is empty.
    pub fn try_new(
        collection: CollectionRef<K>,
        event: EventRef,
        applied: K::Applied,
        ops: Vec<K::Op>,
    ) -> Result<Self, EmptyOperationsError> {
        let Some(operation_count) = NonZeroU64::new(ops.len() as u64) else {
            return Err(EmptyOperationsError);
        };

        Ok(Self {
            collection,
            event,
            applied,
            ops,
            operation_count,
        })
    }

    /// Returns the collection reference.
    #[must_use]
    pub fn collection(&self) -> &CollectionRef<K> {
        &self.collection
    }

    /// Returns the event that owns the sealed operations.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.event
    }

    /// Returns the state that was authoritative before sealing.
    #[must_use]
    pub fn applied(&self) -> &K::Applied {
        &self.applied
    }

    /// Returns ordered sealed operations.
    #[must_use]
    pub fn ops(&self) -> &[K::Op] {
        &self.ops
    }

    /// Decomposes sealed state into its pre-seal state and operation list.
    #[must_use]
    pub fn into_applied_and_ops(self) -> (K::Applied, Vec<K::Op>) {
        (self.applied, self.ops)
    }

    /// Returns the number of sealed operations.
    #[must_use]
    pub fn operation_count(&self) -> NonZeroU64 {
        self.operation_count
    }
}

/// Typed WAL payload shape before final byte encoding exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalBlob<K>
where
    K: StatefulCollectionKind,
{
    collection: CollectionId<K>,
    kind: CollectionKindId,
    event: EventRef,
    ops: Vec<K::Op>,
}

impl<K> WalBlob<K>
where
    K: StatefulCollectionKind,
{
    /// Creates a typed WAL payload.
    #[must_use]
    pub fn new(collection: CollectionId<K>, event: EventRef, ops: Vec<K::Op>) -> Self {
        Self {
            collection,
            kind: K::ID,
            event,
            ops,
        }
    }

    /// Returns the collection identity.
    #[must_use]
    pub fn collection(&self) -> &CollectionId<K> {
        &self.collection
    }

    /// Returns the runtime collection kind discriminator.
    #[must_use]
    pub fn kind(&self) -> CollectionKindId {
        self.kind
    }

    /// Returns the event that owns this WAL payload.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.event
    }

    /// Returns ordered WAL operations.
    #[must_use]
    pub fn ops(&self) -> &[K::Op] {
        &self.ops
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

/// Error returned when a non-empty operation list is required.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("state operation list must not be empty")]
pub struct EmptyOperationsError;

impl ClassifyError for EmptyOperationsError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
