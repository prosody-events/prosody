//! Value collection contracts and transaction wrapper.

use super::{
    CollectionId, CollectionKind, CollectionKindId, CollectionRef, CommitMode, DirtyCollection,
    DurableState, EventRef, LocalTx, PendingOps, Read, SealedCollection, StoreOutcome,
};
use crate::error::{ClassifyError, ErrorCategory};
use crate::{Offset, Partition, Topic};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::future::Future;
use thiserror::Error;

type DirtyStoreError<S> = <S as ValueStore>::Error;
type DurableStoreError<D> = <D as DurableWalStore<ValueKind>>::Error;
type TxError<S, D> = TransactionValueStoreError<DirtyStoreError<S>, DurableStoreError<D>>;

/// Opaque payload stored in a Value collection.
///
/// Either an inline byte blob (the common case) or a typed reference to a
/// Kafka message body that the framework will load on demand. `MsgPack`
/// serialization is adjacently tagged (`{"v": "inline", "d": <bytes>}` or
/// `{"v": "kafka_message", "d": {...}}`); new variants get new tags rather
/// than repurposing existing ones.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "v", content = "d", rename_all = "snake_case")]
pub enum StoredPayload {
    /// Inline byte payload.
    Inline(Bytes),

    /// Reference to a Kafka message body.
    KafkaMessage(KafkaMessageRef),
}

/// Durable pointer to a Kafka message body.
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaMessageRef {
    /// Kafka topic.
    #[serde(with = "topic_serde")]
    pub topic: Topic,

    /// Kafka partition.
    pub partition: Partition,

    /// Kafka offset within the partition.
    pub offset: Offset,
}

mod topic_serde {
    use crate::Topic;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(topic: &Topic, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(topic.as_ref())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Topic, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = <String as Deserialize<'de>>::deserialize(deserializer)?;
        Ok(Topic::from(value.as_str()))
    }
}

/// Applied Value state.
pub type ValueApplied = Option<StoredPayload>;

/// Dirty Value overlay.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ValueOverlay {
    /// No buffered operation has been observed for this collection.
    #[default]
    Untouched,

    /// A clear is buffered.
    BufferedClear,

    /// A set is buffered.
    BufferedSet(StoredPayload),
}

/// Type marker for Value collections.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct ValueKind;

impl CollectionKind for ValueKind {
    type Applied = ValueApplied;
    type Op = ValueOp;
    type Overlay = ValueOverlay;

    const ID: CollectionKindId = CollectionKindId::Value;
}

/// Ordered operation for a Value collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ValueOp {
    /// Replace the current payload.
    Set {
        /// Stored payload.
        payload: StoredPayload,
    },

    /// Remove the current payload.
    Clear,
}

/// Store interface for normal Value reads and writes.
pub trait ValueStore: Send + Sync + 'static {
    /// Error type for Value store operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Reads the visible value for a collection.
    fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> impl Future<Output = Result<Read<StoredPayload>, Self::Error>> + Send + 'a;

    /// Buffers or applies a Value set.
    fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Buffers or applies a Value clear.
    fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Source of compacted pending operations for a collection kind.
pub trait PendingOpSource<K>: Clone + Send + Sync + 'static
where
    K: CollectionKind,
{
    /// Error type for pending operation access.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Iterator returned alongside the operation count.
    ///
    /// `'a` is the borrow lifetime of `&self` at the call site; concrete
    /// implementations may own the iterator (`Send + 'static`) or borrow
    /// from the store's internal state.
    type Ops<'a>: Iterator<Item = K::Op> + Send + 'a
    where
        Self: 'a;

    /// Returns the pending operation stream for a collection when any exist.
    ///
    /// `None` means no operations are buffered for this collection;
    /// `Some(PendingOps { count, ops })` means `count` ordered operations
    /// are available and `ops` will yield exactly that many. The non-zero
    /// count lets callers construct a [`DirtyCollection`] without
    /// materializing the iterator.
    ///
    /// # Errors
    ///
    /// Returns a store error if pending operations cannot be read.
    fn pending_ops<'a>(
        &'a self,
        collection: &'a CollectionId<K>,
    ) -> Result<Option<PendingOps<Self::Ops<'a>>>, Self::Error>;

    /// Clears compacted pending operations for the collection.
    ///
    /// # Errors
    ///
    /// Returns a store error if pending operations cannot be cleared.
    fn clear_pending_ops(&self, collection: &CollectionId<K>) -> Result<(), Self::Error>;
}

/// Durable write-ahead storage for a collection kind.
pub trait DurableWalStore<K>: Clone + Send + Sync + 'static
where
    K: CollectionKind,
{
    /// Error type for durable WAL operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Reads the durable partition state.
    fn read_partition<'a>(
        &'a self,
        collection: &'a CollectionId<K>,
    ) -> impl Future<Output = Result<DurableState<K>, Self::Error>> + Send + 'a;

    /// Seals non-empty ordered operations for an event.
    ///
    /// The collection ref's TTL is bound onto the durable write that creates
    /// the WAL cells.
    fn seal<'a, I>(
        &'a self,
        collection: &'a CollectionRef<K>,
        event: EventRef,
        ops: I,
    ) -> impl Future<Output = Result<SealedCollection<K>, Self::Error>> + Send + 'a
    where
        I: IntoIterator<Item = K::Op> + Send + 'a;

    /// Applies sealed operations when they belong to `expected_event`.
    ///
    /// Returns [`StoreOutcome::Applied`] when a sealed WAL for
    /// `expected_event` was folded into authoritative state and cleared,
    /// or [`StoreOutcome::NoOp`] when no WAL is present (the call is a
    /// safe idempotent no-op after a prior resolution). The collection
    /// ref's TTL is bound onto the durable write that refreshes the
    /// applied cells.
    fn apply_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<K>,
        expected_event: EventRef,
    ) -> impl Future<Output = Result<StoreOutcome, Self::Error>> + Send + 'a;

    /// Rolls back sealed operations when they belong to `expected_event`.
    ///
    /// Returns [`StoreOutcome::Applied`] when a sealed WAL for
    /// `expected_event` was cleared, or [`StoreOutcome::NoOp`] when no
    /// WAL is present.
    fn rollback_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<K>,
        expected_event: EventRef,
    ) -> impl Future<Output = Result<StoreOutcome, Self::Error>> + Send + 'a;
}

/// Durable direct-apply storage for a collection kind.
pub trait DirectApplyStore<K>: Clone + Send + Sync + 'static
where
    K: CollectionKind,
{
    /// Error type for direct apply operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Applies ordered operations directly to authoritative state.
    ///
    /// Returns [`StoreOutcome::Applied`] when at least one operation was
    /// folded into authoritative state, or [`StoreOutcome::NoOp`] when
    /// `ops` is empty. The collection ref's TTL is bound onto the
    /// durable write that refreshes the applied cells.
    fn direct_apply<'a, I>(
        &'a self,
        collection: &'a CollectionRef<K>,
        ops: I,
    ) -> impl Future<Output = Result<StoreOutcome, Self::Error>> + Send + 'a
    where
        I: IntoIterator<Item = K::Op> + Send + 'a;
}

/// Value transaction backed by dirty local state and durable state.
///
/// State-machine methods take `&mut self` so the single-writer-per-key
/// invariant becomes a type-level fact for transaction-side callers. The
/// `ValueStore` trait methods still take `&self` and use [`Mutex`] to
/// coordinate the [`LocalTx`] transition; this lock is the only reason the
/// field is wrapped — there is no cross-thread sharing of a transaction.
#[derive(Debug)]
pub struct TransactionValueStore<D, S> {
    durable: D,
    dirty: S,
    collection: CollectionRef<ValueKind>,
    event: EventRef,
    mode: CommitMode,
    tx: Mutex<LocalTx<ValueKind>>,
}

impl<D, S> TransactionValueStore<D, S> {
    /// Creates a Value transaction for one collection and event.
    #[must_use]
    pub fn new(
        durable: D,
        dirty: S,
        collection: CollectionRef<ValueKind>,
        event: EventRef,
        mode: CommitMode,
    ) -> Self {
        let initial = LocalTx::Clean(collection.clone());
        Self {
            durable,
            dirty,
            collection,
            event,
            mode,
            tx: Mutex::new(initial),
        }
    }

    /// Returns the local transaction state.
    #[must_use]
    pub fn local_tx(&self) -> LocalTx<ValueKind> {
        self.tx.lock().clone()
    }

    /// Returns the typed collection identity.
    #[must_use]
    pub fn collection_id(&self) -> &CollectionId<ValueKind> {
        self.collection.id()
    }
}

impl<D, S> TransactionValueStore<D, S>
where
    D: ValueStore<Error = <D as DurableWalStore<ValueKind>>::Error>
        + DurableWalStore<ValueKind>
        + DirectApplyStore<ValueKind, Error = <D as DurableWalStore<ValueKind>>::Error>,
    S: ValueStore + PendingOpSource<ValueKind, Error = <S as ValueStore>::Error>,
{
    /// Seals dirty operations in WAL mode.
    ///
    /// # Errors
    ///
    /// Returns a transaction error if the mode or local state does not permit
    /// sealing, or if either backing store fails.
    pub async fn seal(&mut self) -> Result<SealedCollection<ValueKind>, TxError<S, D>> {
        self.ensure_mode(CommitMode::Wal)?;

        match self.local_tx() {
            LocalTx::Clean(_) => Err(TransactionValueStoreError::NoPendingOps),
            LocalTx::Dirty(_) => {
                let ops = self
                    .collect_pending_ops()?
                    .ok_or(TransactionValueStoreError::NoPendingOps)?;
                let sealed = self
                    .durable
                    .seal(&self.collection, self.event, ops)
                    .await
                    .map_err(TransactionValueStoreError::Durable)?;
                self.dirty
                    .clear_pending_ops(self.collection.id())
                    .map_err(TransactionValueStoreError::Dirty)?;
                self.set_local_tx(LocalTx::Sealed(sealed.clone()));
                Ok(sealed)
            }
            LocalTx::Sealed(_) => Err(TransactionValueStoreError::AlreadySealed),
            LocalTx::Finished => Err(TransactionValueStoreError::Finished),
        }
    }

    /// Applies sealed WAL state for this transaction event.
    ///
    /// # Errors
    ///
    /// Returns a transaction error if the transaction is finished or durable
    /// storage rejects the sealed event.
    pub async fn apply_sealed(&mut self) -> Result<StoreOutcome, TxError<S, D>> {
        self.ensure_not_finished()?;

        let outcome = self
            .durable
            .apply_sealed(&self.collection, self.event)
            .await
            .map_err(TransactionValueStoreError::Durable)?;
        self.set_local_tx(LocalTx::Finished);
        Ok(outcome)
    }

    /// Rolls back sealed WAL state for this transaction event.
    ///
    /// # Errors
    ///
    /// Returns a transaction error if the transaction is finished or durable
    /// storage rejects the sealed event.
    pub async fn rollback_sealed(&mut self) -> Result<StoreOutcome, TxError<S, D>> {
        self.ensure_not_finished()?;

        let outcome = self
            .durable
            .rollback_sealed(&self.collection, self.event)
            .await
            .map_err(TransactionValueStoreError::Durable)?;
        self.set_local_tx(LocalTx::Finished);
        Ok(outcome)
    }

    /// Applies dirty operations directly and leaves the transaction clean.
    ///
    /// # Errors
    ///
    /// Returns a transaction error if the transaction is sealed, finished, or a
    /// backing store fails.
    pub async fn flush(&mut self) -> Result<StoreOutcome, TxError<S, D>> {
        match self.local_tx() {
            LocalTx::Clean(_) => Ok(StoreOutcome::NoOp),
            LocalTx::Dirty(_) => {
                let outcome = self.apply_dirty_directly().await?;
                self.set_local_tx(LocalTx::Clean(self.collection.clone()));
                Ok(outcome)
            }
            LocalTx::Sealed(_) => Err(TransactionValueStoreError::AlreadySealed),
            LocalTx::Finished => Err(TransactionValueStoreError::Finished),
        }
    }

    /// Resolves a direct-mode transaction.
    ///
    /// # Errors
    ///
    /// Returns a transaction error if the mode is not [`CommitMode::Direct`],
    /// the transaction is sealed or finished, or a backing store fails.
    pub async fn direct_apply(&mut self) -> Result<StoreOutcome, TxError<S, D>> {
        self.ensure_mode(CommitMode::Direct)?;

        let outcome = match self.local_tx() {
            LocalTx::Clean(_) => StoreOutcome::NoOp,
            LocalTx::Dirty(_) => self.apply_dirty_directly().await?,
            LocalTx::Sealed(_) => return Err(TransactionValueStoreError::AlreadySealed),
            LocalTx::Finished => return Err(TransactionValueStoreError::Finished),
        };
        self.set_local_tx(LocalTx::Finished);
        Ok(outcome)
    }

    /// Resolves this transaction as aborted.
    ///
    /// # Errors
    ///
    /// Returns a transaction error if a backing store fails or the local state
    /// cannot transition.
    pub async fn abort(&mut self) -> Result<StoreOutcome, TxError<S, D>> {
        match self.local_tx() {
            LocalTx::Clean(_) => {
                self.set_local_tx(LocalTx::Finished);
                Ok(StoreOutcome::NoOp)
            }
            LocalTx::Dirty(_) => {
                self.dirty
                    .clear_pending_ops(self.collection.id())
                    .map_err(TransactionValueStoreError::Dirty)?;
                self.set_local_tx(LocalTx::Finished);
                Ok(StoreOutcome::NoOp)
            }
            LocalTx::Sealed(_) => self.rollback_sealed().await,
            LocalTx::Finished => Err(TransactionValueStoreError::Finished),
        }
    }

    async fn apply_dirty_directly(&self) -> Result<StoreOutcome, TxError<S, D>> {
        let Some(ops) = self.collect_pending_ops()? else {
            return Ok(StoreOutcome::NoOp);
        };

        let outcome = self
            .durable
            .direct_apply(&self.collection, ops)
            .await
            .map_err(TransactionValueStoreError::Durable)?;
        self.dirty
            .clear_pending_ops(self.collection.id())
            .map_err(TransactionValueStoreError::Dirty)?;
        Ok(outcome)
    }

    /// Collects pending operations into an owned `Vec`.
    ///
    /// Returns `None` when no operations are buffered. Owning the ops as
    /// a `'static` `Vec` decouples them from any borrow the dirty store
    /// held, so the durable seal / direct-apply call can run before
    /// `clear_pending_ops` releases the dirty workspace.
    fn collect_pending_ops(&self) -> Result<Option<Vec<ValueOp>>, TxError<S, D>> {
        let Some(pending) = self
            .dirty
            .pending_ops(self.collection.id())
            .map_err(TransactionValueStoreError::Dirty)?
        else {
            return Ok(None);
        };
        Ok(Some(pending.ops.collect()))
    }

    /// Returns an error when the transaction's pinned commit mode is not
    /// `expected`.
    fn ensure_mode(&self, expected: CommitMode) -> Result<(), TxError<S, D>> {
        if self.mode == expected {
            Ok(())
        } else {
            Err(TransactionValueStoreError::WrongCommitMode {
                expected,
                actual: self.mode,
            })
        }
    }

    /// Returns an error unless `collection` is the one this transaction owns.
    ///
    /// Invariant: a transaction operates only on its own collection.
    fn ensure_collection(&self, collection: &CollectionId<ValueKind>) -> Result<(), TxError<S, D>> {
        if collection == self.collection.id() {
            Ok(())
        } else {
            Err(TransactionValueStoreError::WrongCollection)
        }
    }

    /// Returns an error when the transaction has already been resolved.
    ///
    /// Invariant: [`LocalTx::Finished`] is terminal — no operation
    /// transitions out of it.
    fn ensure_not_finished(&self) -> Result<(), TxError<S, D>> {
        if matches!(self.local_tx(), LocalTx::Finished) {
            Err(TransactionValueStoreError::Finished)
        } else {
            Ok(())
        }
    }

    fn set_local_tx(&mut self, tx: LocalTx<ValueKind>) {
        *self.tx.get_mut() = tx;
    }

    fn mark_dirty(&self) -> Result<(), TxError<S, D>> {
        let pending = self
            .dirty
            .pending_ops(self.collection.id())
            .map_err(TransactionValueStoreError::Dirty)?
            .ok_or(TransactionValueStoreError::NoPendingOps)?;
        let dirty = DirtyCollection::new(self.collection.clone(), pending.count);
        *self.tx.lock() = LocalTx::Dirty(dirty);
        Ok(())
    }

    fn can_write(&self) -> Result<(), TxError<S, D>> {
        match self.local_tx() {
            LocalTx::Clean(_) | LocalTx::Dirty(_) => Ok(()),
            LocalTx::Sealed(_) => Err(TransactionValueStoreError::AlreadySealed),
            LocalTx::Finished => Err(TransactionValueStoreError::Finished),
        }
    }
}

impl<D, S> ValueStore for TransactionValueStore<D, S>
where
    D: ValueStore<Error = <D as DurableWalStore<ValueKind>>::Error>
        + DurableWalStore<ValueKind>
        + DirectApplyStore<ValueKind, Error = <D as DurableWalStore<ValueKind>>::Error>
        + fmt::Debug,
    S: ValueStore + PendingOpSource<ValueKind, Error = <S as ValueStore>::Error> + fmt::Debug,
{
    type Error = TxError<S, D>;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<StoredPayload>, Self::Error> {
        self.ensure_collection(collection)?;
        self.ensure_not_finished()?;

        match self
            .dirty
            .get(collection)
            .await
            .map_err(TransactionValueStoreError::Dirty)?
        {
            Read::Present(payload) => Ok(Read::Present(payload)),
            Read::Absent => Ok(Read::Absent),
            Read::Unknown => self
                .durable
                .get(collection)
                .await
                .map_err(TransactionValueStoreError::Durable),
        }
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
    ) -> Result<(), Self::Error> {
        self.ensure_collection(collection)?;
        self.can_write()?;
        self.dirty
            .set(collection, payload)
            .await
            .map_err(TransactionValueStoreError::Dirty)?;
        self.mark_dirty()
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        self.ensure_collection(collection)?;
        self.can_write()?;
        self.dirty
            .clear(collection)
            .await
            .map_err(TransactionValueStoreError::Dirty)?;
        self.mark_dirty()
    }
}

/// Folds ordered Value operations into applied state.
///
/// Value operations are last-writer-wins, so only the final op in the slice
/// affects the applied state.
#[must_use]
pub fn fold_value_ops<'a, I>(applied: ValueApplied, ops: I) -> ValueApplied
where
    I: IntoIterator<Item = &'a ValueOp>,
{
    ops.into_iter().last().map_or(applied, |op| match op {
        ValueOp::Set { payload } => Some(payload.clone()),
        ValueOp::Clear => None,
    })
}

/// Error returned by [`TransactionValueStore`].
#[derive(Debug, Error)]
pub enum TransactionValueStoreError<DirtyError, DurableError>
where
    DirtyError: ClassifyError + Error + Send + Sync + 'static,
    DurableError: ClassifyError + Error + Send + Sync + 'static,
{
    /// Dirty local store failed.
    #[error("dirty value store failed")]
    Dirty(#[source] DirtyError),

    /// Durable store failed.
    #[error("durable value store failed")]
    Durable(#[source] DurableError),

    /// Transaction has already been resolved.
    #[error("value transaction is finished")]
    Finished,

    /// Transaction is already sealed.
    #[error("value transaction is already sealed")]
    AlreadySealed,

    /// The operation requires dirty state.
    #[error("value transaction has no pending operations")]
    NoPendingOps,

    /// The transaction mode did not match the requested operation.
    #[error("wrong commit mode: expected {expected:?}, got {actual:?}")]
    WrongCommitMode {
        /// Required mode.
        expected: CommitMode,

        /// Actual transaction mode.
        actual: CommitMode,
    },

    /// Operation used a collection outside this transaction.
    #[error("value transaction used with the wrong collection")]
    WrongCollection,
}

impl<DirtyError, DurableError> ClassifyError
    for TransactionValueStoreError<DirtyError, DurableError>
where
    DirtyError: ClassifyError + Error + Send + Sync + 'static,
    DurableError: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Dirty(error) => error.classify_error(),
            Self::Durable(error) => error.classify_error(),
            Self::Finished
            | Self::AlreadySealed
            | Self::NoPendingOps
            | Self::WrongCommitMode { .. }
            | Self::WrongCollection => ErrorCategory::Permanent,
        }
    }
}
