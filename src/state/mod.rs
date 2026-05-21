//! Keyed application state protocol types.
//!
//! This module defines the shared typed identity and transaction state shapes
//! used by keyed state stores. The first implemented collection kind is
//! [`ValueKind`], but collection identities carry the kind both statically and
//! at runtime so future collection families cannot share state by accident.

use crate::Key;
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::SegmentId;
use bytes::Bytes;
use std::fmt;
use std::iter;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

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

/// Durable reference to the upstream event that owns a sealed WAL.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum EventRef {
    /// Kafka message event identified by its deduplication marker.
    Message {
        /// Deduplication row identifier written at the event commit point.
        dedup_id: Uuid,
    },

    /// Timer event identified by its durable timer row coordinates.
    Timer(TimerEventRef),
}

/// Durable timer identity stored in a sealed WAL.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct TimerEventRef {
    /// Timer namespace.
    pub timer_type: TimerType,

    /// Scheduled fire time.
    pub time: CompactDateTime,

    /// Timer row tag observed when the WAL was sealed.
    pub tag: i32,
}

impl TimerEventRef {
    /// Creates a durable timer event reference.
    #[must_use]
    pub fn new(timer_type: TimerType, time: CompactDateTime, tag: i32) -> Self {
        Self {
            timer_type,
            time,
            tag,
        }
    }
}

/// Oracle verdict on a sealed WAL for one event.
///
/// Returned by [`value::DurableWalStore::apply_sealed`] and
/// [`value::DurableWalStore::rollback_sealed`]. Distinct from
/// [`FlushOutcome`], which describes whether a storage step applied any
/// operations.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CommitDecision {
    /// The sealed operations were committed.
    Committed,

    /// No sealed operations were committed.
    NotCommitted,
}

/// Result of a storage step that applies dirty operations.
///
/// Returned by [`value::DirectApplyStore::direct_apply`],
/// [`value::TransactionValueStore::flush`], and
/// [`value::TransactionValueStore::direct_apply`]. Carries the
/// "did this step apply anything" signal that used to overload
/// [`CommitDecision`].
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum FlushOutcome {
    /// At least one operation was applied to authoritative state.
    Applied,

    /// No operations were applied.
    NoOp,
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
    K: CollectionKind,
{
    /// No sealed operations are pending; `applied` is authoritative.
    Idle {
        /// Authoritative applied state.
        applied: K::Applied,
    },

    /// A non-empty WAL is sealed for recovery.
    Sealed {
        /// Authoritative applied state observed before the WAL was sealed.
        applied: K::Applied,

        /// Durable sealed WAL.
        wal: SealedWal<K>,
    },
}

/// Local transaction state for one collection and event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalTx<K>
where
    K: CollectionKind,
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
///
/// Today this carries only the collection identity; later slices will extend
/// it with the per-event `commit_mode` and `EventScopeId` described in
/// `docs/keyed-state/design-summary.md` §"Local State". Keep the scaffold so
/// the next slice has somewhere to put those fields without re-flattening
/// callers back into [`CollectionId`].
// TODO(slice-8): carry commit_mode and scope per design-summary §Local State
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

/// Durable sealed state for one event and non-empty WAL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedWal<K>
where
    K: CollectionKind,
{
    event: EventRef,
    wal: WalEnvelope<K>,
}

impl<K> SealedWal<K>
where
    K: CollectionKind,
{
    /// Creates durable sealed state from a non-empty WAL.
    #[must_use]
    pub fn new(event: EventRef, wal: WalEnvelope<K>) -> Self {
        Self { event, wal }
    }

    /// Creates durable sealed state from a non-empty ordered operation list.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyOperationsError`] when `ops` is empty.
    pub fn try_new(event: EventRef, ops: Vec<K::Op>) -> Result<Self, EmptyOperationsError> {
        Ok(Self::new(event, WalEnvelope::try_from_ops(ops)?))
    }

    /// Returns the event that owns the sealed operations.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.event
    }

    /// Returns the sealed WAL.
    #[must_use]
    pub fn wal(&self) -> &WalEnvelope<K> {
        &self.wal
    }

    /// Returns ordered sealed operations.
    #[must_use]
    pub fn ops(&self) -> NonEmptyOpsSlice<'_, K::Op> {
        self.wal.ops()
    }

    /// Decomposes sealed state into the ordered operation list.
    #[must_use]
    pub fn into_ops(self) -> Vec<K::Op> {
        self.wal.into_ops()
    }

    /// Returns the number of sealed operations.
    #[must_use]
    pub fn operation_count(&self) -> NonZeroU64 {
        self.wal.operation_count()
    }
}

/// Local proof that dirty operations were sealed for an event.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct SealedCollection<K>
where
    K: CollectionKind,
{
    collection: CollectionRef<K>,
    event: EventRef,
}

impl<K> SealedCollection<K>
where
    K: CollectionKind,
{
    /// Creates a sealed local transition marker.
    #[must_use]
    pub fn new(collection: CollectionRef<K>, event: EventRef) -> Self {
        Self { collection, event }
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
}

/// Non-empty ordered operation list.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NonEmptyOps<T> {
    first: T,
    rest: Vec<T>,
    operation_count: NonZeroU64,
}

impl<T> NonEmptyOps<T> {
    /// Creates a non-empty operation list.
    #[must_use]
    pub fn new(first: T, rest: Vec<T>) -> Self {
        let operation_count = NonZeroU64::MIN.saturating_add(rest.len() as u64);
        Self {
            first,
            rest,
            operation_count,
        }
    }

    /// Creates a non-empty operation list from a vector.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyOperationsError`] when `ops` is empty.
    pub fn try_from_vec(ops: Vec<T>) -> Result<Self, EmptyOperationsError> {
        let mut iter = ops.into_iter();
        let first = iter.next().ok_or(EmptyOperationsError)?;
        Ok(Self::new(first, iter.collect()))
    }

    /// Returns ordered operations.
    #[must_use]
    pub fn as_slice(&self) -> NonEmptyOpsSlice<'_, T> {
        NonEmptyOpsSlice {
            first: &self.first,
            rest: &self.rest,
        }
    }

    /// Decomposes the list into a vector.
    #[must_use]
    pub fn into_vec(self) -> Vec<T> {
        let mut ops = Vec::with_capacity(1 + self.rest.len());
        ops.push(self.first);
        ops.extend(self.rest);
        ops
    }

    /// Returns the number of operations.
    #[must_use]
    pub fn len(&self) -> NonZeroU64 {
        self.operation_count
    }
}

/// Borrowed view of a non-empty operation list.
#[derive(Clone, Copy, Debug)]
pub struct NonEmptyOpsSlice<'a, T> {
    first: &'a T,
    rest: &'a [T],
}

impl<'a, T> NonEmptyOpsSlice<'a, T> {
    /// Returns the first operation.
    #[must_use]
    pub fn first(self) -> &'a T {
        self.first
    }

    /// Returns operations after the first.
    #[must_use]
    pub fn rest(self) -> &'a [T] {
        self.rest
    }

    /// Iterates over every operation in order.
    pub fn iter(self) -> impl Iterator<Item = &'a T> {
        iter::once(self.first).chain(self.rest.iter())
    }
}

/// Typed WAL payload shape before final byte encoding exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalEnvelope<K>
where
    K: CollectionKind,
{
    ops: NonEmptyOps<K::Op>,
    _kind: PhantomData<K>,
}

impl<K> WalEnvelope<K>
where
    K: CollectionKind,
{
    /// Creates a typed WAL payload from a non-empty operation list.
    #[must_use]
    pub fn new(ops: NonEmptyOps<K::Op>) -> Self {
        Self {
            ops,
            _kind: PhantomData,
        }
    }

    /// Creates a typed WAL payload from ordered operations.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyOperationsError`] when `ops` is empty.
    pub fn try_from_ops(ops: Vec<K::Op>) -> Result<Self, EmptyOperationsError> {
        Ok(Self::new(NonEmptyOps::try_from_vec(ops)?))
    }

    /// Returns ordered WAL operations.
    #[must_use]
    pub fn ops(&self) -> NonEmptyOpsSlice<'_, K::Op> {
        self.ops.as_slice()
    }

    /// Decomposes this WAL into ordered operations.
    #[must_use]
    pub fn into_ops(self) -> Vec<K::Op> {
        self.ops.into_vec()
    }

    /// Returns the number of WAL operations.
    #[must_use]
    pub fn operation_count(&self) -> NonZeroU64 {
        self.ops.len()
    }
}

/// Encoded WAL bytes tagged with their collection kind.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct WalBlob<K>
where
    K: CollectionKind,
{
    bytes: Bytes,
    operation_count: NonZeroU64,
    _kind: PhantomData<K>,
}

impl<K> WalBlob<K>
where
    K: CollectionKind,
{
    /// Creates an encoded typed WAL.
    #[must_use]
    pub fn new(bytes: Bytes, operation_count: NonZeroU64) -> Self {
        Self {
            bytes,
            operation_count,
            _kind: PhantomData,
        }
    }

    /// Returns the encoded WAL bytes.
    #[must_use]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns the number of encoded operations.
    #[must_use]
    pub fn operation_count(&self) -> NonZeroU64 {
        self.operation_count
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
