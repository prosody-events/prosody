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
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

pub mod cassandra;
pub mod encoding;
pub mod fjall;
pub mod layered;
pub mod memory;
pub mod middleware;
pub mod oracle;
pub mod pending;
pub mod recovering;
pub mod value;

#[cfg(test)]
mod dirty_value_test_suite;
#[cfg(test)]
mod encoding_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod value_test_suite;

pub use encoding::{EncodingError, PayloadEncoding, WalFormat};
pub use value::{KafkaMessageRef, StoredPayload, ValueApplied, ValueKind, ValueOp, ValueOverlay};

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
///
/// The keyed-state middleware mints a fresh scope per handler invocation
/// (via [`Self::fresh`]) so dirty workspaces can be keyed by scope without
/// colliding across events. The Fjall dirty workspace will key on
/// [`EventScopeId`] in a later slice; today this identity is consumed by
/// the in-memory middleware workspace and is sufficient to distinguish
/// concurrent events at the type level.
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

    /// Mints a fresh random scope identifier. Used by the keyed-state
    /// middleware to scope per-event dirty workspaces.
    #[must_use]
    pub fn fresh() -> Self {
        Self(Uuid::new_v4().as_u128())
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
/// Returned by the commit oracle when it resolves a [`SealedWal`]'s
/// [`EventRef`] against the upstream commit source (deduplication store
/// for messages, timer-row tag for timers per
/// `docs/keyed-state/design-summary.md` §"Recovery"). Distinct from
/// [`StoreOutcome`], which is the durable store's "did this call mutate
/// state" signal: the oracle decides, the store acts on the decision.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CommitDecision {
    /// The sealed operations were committed.
    Committed,

    /// No sealed operations were committed.
    NotCommitted,
}

/// Did this store call mutate authoritative state.
///
/// Returned by store-side methods that may or may not have work to do:
/// [`value::DurableWalStore::apply_sealed`] (WAL present → folded),
/// [`value::DurableWalStore::rollback_sealed`] (WAL present → cleared),
/// [`value::DirectApplyStore::direct_apply`] (ops non-empty → folded),
/// and the [`value::TransactionValueStore`] wrappers around them.
///
/// Distinct from [`CommitDecision`]: the oracle decides whether a sealed
/// WAL should be committed, the store reports whether it actually
/// changed durable state when called. A second call with the same
/// arguments observes [`StoreOutcome::NoOp`].
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum StoreOutcome {
    /// The call mutated authoritative state.
    Applied,

    /// No durable state changed (idempotent no-op).
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
/// store-owned default (Slice 7+; per-collection registry overrides land
/// in Slice 8).
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
    wal: WalBlob<K>,
    payload_encoding: PayloadEncoding,
}

impl<K> SealedWal<K>
where
    K: CollectionKind,
{
    /// Creates durable sealed state from an encoded WAL blob.
    #[must_use]
    pub fn new(event: EventRef, wal: WalBlob<K>, payload_encoding: PayloadEncoding) -> Self {
        Self {
            event,
            wal,
            payload_encoding,
        }
    }

    /// Returns the event that owns the sealed operations.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.event
    }

    /// Returns the sealed WAL blob.
    #[must_use]
    pub fn wal(&self) -> &WalBlob<K> {
        &self.wal
    }

    /// Returns the payload encoding for stored payload cells in this partition.
    #[must_use]
    pub fn payload_encoding(&self) -> PayloadEncoding {
        self.payload_encoding
    }
}

impl<K> SealedWal<K>
where
    K: CollectionKind,
    K::Op: encoding::EncodableOp,
{
    /// Creates durable sealed state by encoding a non-empty ordered operation
    /// list.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError`] when `ops` is empty or the WAL encoder fails.
    pub fn try_new(
        event: EventRef,
        ops: Vec<K::Op>,
        format: WalFormat,
        payload_encoding: PayloadEncoding,
    ) -> Result<Self, EncodingError> {
        let envelope = WalEnvelope::<K>::try_from_ops(ops)?;
        let wal = encoding::encode_wal::<K>(&envelope, format)?;
        Ok(Self::new(event, wal, payload_encoding))
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

/// Pending operations for one collection, with a typed non-empty proof.
///
/// Returned by [`value::PendingOpSource::pending_ops`] wrapped in
/// [`Option`]: `None` means no dirty work is buffered for the collection,
/// `Some(PendingOps { count, ops })` means at least one operation exists
/// and `count` matches the iterator. The [`NonZeroU64`] count lets callers
/// construct a [`DirtyCollection`] without materializing `ops` first; the
/// iterator yields the operations themselves in order when the seal or
/// direct-apply path needs them.
pub struct PendingOps<I>
where
    I: Iterator + Send,
{
    /// Number of operations the iterator will yield.
    pub count: NonZeroU64,

    /// Ordered pending operations.
    pub ops: I,
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

/// Materialized typed WAL payload.
///
/// The on-wire WAL header (`version`, `kind`, `op_count`) is an encoding
/// detail owned by [`encoding::encode_wal`] and [`encoding::decode_wal`].
/// Persisting it here would create two operation-count sources that could
/// disagree; instead the header is constructed at encode time from
/// [`Self::operation_count`] and validated and discarded at decode time.
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
///
/// The encoded body owns the operation count via its [`WalFormat`]-specific
/// header frame, so this type carries only the bytes and the format
/// discriminator. Callers needing the materialized op stream call
/// [`encoding::decode_wal`] to recover a [`WalEnvelope`].
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct WalBlob<K>
where
    K: CollectionKind,
{
    bytes: Bytes,
    format: WalFormat,
    _kind: PhantomData<K>,
}

impl<K> WalBlob<K>
where
    K: CollectionKind,
{
    /// Creates an encoded typed WAL.
    #[must_use]
    pub fn new(bytes: Bytes, format: WalFormat) -> Self {
        Self {
            bytes,
            format,
            _kind: PhantomData,
        }
    }

    /// Returns the encoded WAL bytes.
    #[must_use]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns the encoded WAL format discriminator.
    #[must_use]
    pub fn format(&self) -> WalFormat {
        self.format
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
