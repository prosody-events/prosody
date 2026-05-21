//! Value collection contracts and transaction wrapper.

use super::{
    CollectionId, CollectionKind, CollectionKindId, CollectionRef, CommitDecision, CommitMode,
    DirtyCollection, DurableState, EventRef, LocalTx, Read, SealedCollection,
};
use crate::error::{ClassifyError, ErrorCategory};
use bytes::Bytes;
use parking_lot::Mutex;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;

type DirtyStoreError<S> = <S as ValueStore>::Error;
type DurableStoreError<D> = <D as DurableWalStore<ValueKind>>::Error;
type TxError<S, D> = TransactionValueStoreError<DirtyStoreError<S>, DurableStoreError<D>>;

/// Opaque payload stored in a Value collection.
pub type StoredPayload = Bytes;

/// Applied Value state.
pub type ValueApplied = Option<Bytes>;

/// Dirty Value overlay.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ValueOverlay {
    /// No buffered operation has been observed for this collection.
    #[default]
    Untouched,

    /// A clear is buffered.
    BufferedClear,

    /// A set is buffered.
    BufferedSet(Bytes),
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
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum ValueOp {
    /// Replace the current payload.
    Set {
        /// Opaque payload bytes.
        payload: Bytes,
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
        payload: Bytes,
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

    /// Returns ordered compacted pending operations.
    ///
    /// # Errors
    ///
    /// Returns a store error if pending operations cannot be read.
    fn pending_ops(
        &self,
        collection: &CollectionId<K>,
    ) -> Result<impl Iterator<Item = K::Op> + Send, Self::Error>;

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
    fn seal<'a, I>(
        &'a self,
        collection: &'a CollectionId<K>,
        event: EventRef,
        ops: I,
    ) -> impl Future<Output = Result<SealedCollection<K>, Self::Error>> + Send + 'a
    where
        I: IntoIterator<Item = K::Op> + Send + 'a;

    /// Applies sealed operations when they belong to `expected_event`.
    fn apply_sealed<'a>(
        &'a self,
        collection: &'a CollectionId<K>,
        expected_event: EventRef,
    ) -> impl Future<Output = Result<CommitDecision, Self::Error>> + Send + 'a;

    /// Rolls back sealed operations when they belong to `expected_event`.
    fn rollback_sealed<'a>(
        &'a self,
        collection: &'a CollectionId<K>,
        expected_event: EventRef,
    ) -> impl Future<Output = Result<CommitDecision, Self::Error>> + Send + 'a;
}

/// Durable direct-apply storage for a collection kind.
pub trait DirectApplyStore<K>: Clone + Send + Sync + 'static
where
    K: CollectionKind,
{
    /// Error type for direct apply operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Applies ordered operations directly to authoritative state.
    fn direct_apply<'a, I>(
        &'a self,
        collection: &'a CollectionId<K>,
        ops: I,
    ) -> impl Future<Output = Result<CommitDecision, Self::Error>> + Send + 'a
    where
        I: IntoIterator<Item = K::Op> + Send + 'a;
}

/// Value transaction backed by dirty local state and durable state.
#[derive(Debug)]
pub struct TransactionValueStore<D, S> {
    durable: D,
    dirty: S,
    collection: CollectionId<ValueKind>,
    event: EventRef,
    mode: CommitMode,
    tx: Arc<Mutex<LocalTx<ValueKind>>>,
}

impl<D, S> TransactionValueStore<D, S> {
    /// Creates a Value transaction for one collection and event.
    #[must_use]
    pub fn new(
        durable: D,
        dirty: S,
        collection: CollectionId<ValueKind>,
        event: EventRef,
        mode: CommitMode,
    ) -> Self {
        let reference = CollectionRef::new(collection.clone());
        Self {
            durable,
            dirty,
            collection,
            event,
            mode,
            tx: Arc::new(Mutex::new(LocalTx::Clean(reference))),
        }
    }

    /// Returns the local transaction state.
    #[must_use]
    pub fn local_tx(&self) -> LocalTx<ValueKind> {
        self.tx.lock().clone()
    }
}

impl<D, S> TransactionValueStore<D, S>
where
    D: DurableWalStore<ValueKind>
        + DirectApplyStore<ValueKind, Error = <D as DurableWalStore<ValueKind>>::Error>,
    S: ValueStore + PendingOpSource<ValueKind, Error = <S as ValueStore>::Error>,
{
    /// Seals dirty operations in WAL mode.
    ///
    /// # Errors
    ///
    /// Returns a transaction error if the mode or local state does not permit
    /// sealing, or if either backing store fails.
    pub async fn seal(&self) -> Result<SealedCollection<ValueKind>, TxError<S, D>> {
        if self.mode != CommitMode::Wal {
            return Err(TransactionValueStoreError::WrongCommitMode {
                expected: CommitMode::Wal,
                actual: self.mode,
            });
        }

        match self.local_tx() {
            LocalTx::Clean(_) => Err(TransactionValueStoreError::NoPendingOps),
            LocalTx::Dirty(_) => {
                let ops = self.pending_ops_vec()?;

                let sealed = self
                    .durable
                    .seal(&self.collection, self.event, ops)
                    .await
                    .map_err(TransactionValueStoreError::Durable)?;
                self.dirty
                    .clear_pending_ops(&self.collection)
                    .map_err(TransactionValueStoreError::Dirty)?;
                *self.tx.lock() = LocalTx::Sealed(sealed.clone());
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
    pub async fn apply_sealed(&self) -> Result<CommitDecision, TxError<S, D>> {
        if self.local_tx() == LocalTx::Finished {
            return Err(TransactionValueStoreError::Finished);
        }

        let decision = self
            .durable
            .apply_sealed(&self.collection, self.event)
            .await
            .map_err(TransactionValueStoreError::Durable)?;
        *self.tx.lock() = LocalTx::Finished;
        Ok(decision)
    }

    /// Rolls back sealed WAL state for this transaction event.
    ///
    /// # Errors
    ///
    /// Returns a transaction error if the transaction is finished or durable
    /// storage rejects the sealed event.
    pub async fn rollback_sealed(&self) -> Result<CommitDecision, TxError<S, D>> {
        if self.local_tx() == LocalTx::Finished {
            return Err(TransactionValueStoreError::Finished);
        }

        let decision = self
            .durable
            .rollback_sealed(&self.collection, self.event)
            .await
            .map_err(TransactionValueStoreError::Durable)?;
        *self.tx.lock() = LocalTx::Finished;
        Ok(decision)
    }

    /// Applies dirty operations directly and leaves the transaction clean.
    ///
    /// # Errors
    ///
    /// Returns a transaction error if the transaction is sealed, finished, or a
    /// backing store fails.
    pub async fn flush(&self) -> Result<CommitDecision, TxError<S, D>> {
        match self.local_tx() {
            LocalTx::Clean(_) => Ok(CommitDecision::NotCommitted),
            LocalTx::Dirty(_) => {
                let decision = self.apply_dirty_directly().await?;
                *self.tx.lock() = LocalTx::Clean(CollectionRef::new(self.collection.clone()));
                Ok(decision)
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
    pub async fn direct_apply(&self) -> Result<CommitDecision, TxError<S, D>> {
        if self.mode != CommitMode::Direct {
            return Err(TransactionValueStoreError::WrongCommitMode {
                expected: CommitMode::Direct,
                actual: self.mode,
            });
        }

        let decision = match self.local_tx() {
            LocalTx::Clean(_) => CommitDecision::NotCommitted,
            LocalTx::Dirty(_) => self.apply_dirty_directly().await?,
            LocalTx::Sealed(_) => return Err(TransactionValueStoreError::AlreadySealed),
            LocalTx::Finished => return Err(TransactionValueStoreError::Finished),
        };
        *self.tx.lock() = LocalTx::Finished;
        Ok(decision)
    }

    /// Resolves this transaction as committed according to its commit mode.
    ///
    /// # Errors
    ///
    /// Returns a transaction error if a backing store fails or the local state
    /// cannot transition.
    pub async fn commit(&self) -> Result<CommitDecision, TxError<S, D>> {
        match self.mode {
            CommitMode::Wal => match self.local_tx() {
                LocalTx::Clean(_) => {
                    *self.tx.lock() = LocalTx::Finished;
                    Ok(CommitDecision::NotCommitted)
                }
                LocalTx::Dirty(_) => {
                    let _sealed = self.seal().await?;
                    self.apply_sealed().await
                }
                LocalTx::Sealed(_) => self.apply_sealed().await,
                LocalTx::Finished => Err(TransactionValueStoreError::Finished),
            },
            CommitMode::Direct => self.direct_apply().await,
        }
    }

    /// Resolves this transaction as aborted.
    ///
    /// # Errors
    ///
    /// Returns a transaction error if a backing store fails or the local state
    /// cannot transition.
    pub async fn abort(&self) -> Result<CommitDecision, TxError<S, D>> {
        match self.local_tx() {
            LocalTx::Clean(_) => {
                *self.tx.lock() = LocalTx::Finished;
                Ok(CommitDecision::NotCommitted)
            }
            LocalTx::Dirty(_) => {
                self.dirty
                    .clear_pending_ops(&self.collection)
                    .map_err(TransactionValueStoreError::Dirty)?;
                *self.tx.lock() = LocalTx::Finished;
                Ok(CommitDecision::NotCommitted)
            }
            LocalTx::Sealed(_) => self.rollback_sealed().await,
            LocalTx::Finished => Err(TransactionValueStoreError::Finished),
        }
    }

    async fn apply_dirty_directly(&self) -> Result<CommitDecision, TxError<S, D>> {
        let ops = self.pending_ops_vec()?;
        if ops.is_empty() {
            return Ok(CommitDecision::NotCommitted);
        }

        let decision = self
            .durable
            .direct_apply(&self.collection, ops)
            .await
            .map_err(TransactionValueStoreError::Durable)?;
        self.dirty
            .clear_pending_ops(&self.collection)
            .map_err(TransactionValueStoreError::Dirty)?;
        Ok(decision)
    }

    fn mark_dirty(&self) -> Result<(), TxError<S, D>> {
        let ops = self.pending_ops_vec()?;
        let dirty =
            DirtyCollection::try_from_count(CollectionRef::new(self.collection.clone()), ops.len())
                .map_err(|_| TransactionValueStoreError::NoPendingOps)?;
        *self.tx.lock() = LocalTx::Dirty(dirty);
        Ok(())
    }

    fn pending_ops_vec(&self) -> Result<Vec<ValueOp>, TxError<S, D>> {
        Ok(self
            .dirty
            .pending_ops(&self.collection)
            .map_err(TransactionValueStoreError::Dirty)?
            .collect())
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
    D: DurableWalStore<ValueKind>
        + DirectApplyStore<ValueKind, Error = <D as DurableWalStore<ValueKind>>::Error>
        + fmt::Debug,
    S: ValueStore + PendingOpSource<ValueKind, Error = <S as ValueStore>::Error> + fmt::Debug,
{
    type Error = TxError<S, D>;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<StoredPayload>, Self::Error> {
        if collection != &self.collection {
            return Err(TransactionValueStoreError::WrongCollection);
        }
        if matches!(self.local_tx(), LocalTx::Finished) {
            return Err(TransactionValueStoreError::Finished);
        }

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
                .read_partition(collection)
                .await
                .map(read_value_from_durable)
                .map_err(TransactionValueStoreError::Durable),
        }
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: Bytes,
    ) -> Result<(), Self::Error> {
        if collection != &self.collection {
            return Err(TransactionValueStoreError::WrongCollection);
        }

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
        if collection != &self.collection {
            return Err(TransactionValueStoreError::WrongCollection);
        }

        self.can_write()?;
        self.dirty
            .clear(collection)
            .await
            .map_err(TransactionValueStoreError::Dirty)?;
        self.mark_dirty()
    }
}

/// Folds ordered Value operations into applied state.
#[must_use]
pub fn fold_value_ops<'a, I>(applied: ValueApplied, ops: I) -> ValueApplied
where
    I: IntoIterator<Item = &'a ValueOp>,
{
    ops.into_iter().fold(applied, |_, op| match op {
        ValueOp::Set { payload } => Some(payload.clone()),
        ValueOp::Clear => None,
    })
}

fn read_value_from_durable(state: DurableState<ValueKind>) -> Read<StoredPayload> {
    let applied = match state {
        DurableState::Idle { applied } => applied,
        DurableState::Sealed { wal } => wal.applied().clone(),
    };

    applied.map_or(Read::Absent, Read::Present)
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
