//! Keyed-state context, value handle, and the access/bundle traits.

use super::registry::CollectionDefRegistry;
use crate::consumer::event_context::{EventContext, TerminationSignals};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::value::{
    DirectApplyStore, DurableWalStore, PendingOpSource, StoredPayload, TransactionValueStore,
    TransactionValueStoreError, ValueKind, ValueStore,
};
use crate::state::{
    CollectionId, CollectionRef, CommitMode, EventRef, Read, StateKey, StateName, StateNameError,
    StateType, StoreOutcome,
};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use parking_lot::Mutex as SyncMutex;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

/// Extension trait on [`EventContext`] that exposes keyed-state access.
///
/// Handlers that want to read or write Value collections add
/// `+ KeyedStateAccess` to their context bound. The middleware's wrapped
/// context [`KeyedStateContext`] satisfies this trait; unwrapped contexts
/// (e.g. [`crate::consumer::event_context::TimerContext`]) do not.
pub trait KeyedStateAccess: EventContext {
    /// Concrete handle returned by [`Self::value`].
    type ValueHandle: ValueAccessor;

    /// Error returned by [`Self::value`] when the requested handle cannot
    /// be constructed (e.g. the collection name was empty).
    type ValueError: ClassifyError + Error + Send + Sync + 'static;

    /// Returns a Value handle bound to the collection identified by
    /// `name` for the current event scope.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is empty or the handle cannot be
    /// allocated.
    fn value(&self, name: &str) -> Result<Self::ValueHandle, Self::ValueError>;
}

/// Handler-facing operations on a Value collection.
///
/// Returned by [`KeyedStateAccess::value`].
pub trait ValueAccessor: Clone + Send + Sync {
    /// Error type for value operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Reads the current visible value.
    fn get(&self) -> impl Future<Output = Result<Option<StoredPayload>, Self::Error>> + Send;

    /// Buffers a set operation for this collection.
    fn set(&self, payload: StoredPayload) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Buffers a clear operation for this collection.
    fn clear(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Drains buffered ops directly to authoritative state and returns
    /// the transaction to `Clean`.
    ///
    /// Mirrors the design's `Dirty → flush() → Clean` and
    /// `Clean → flush() → Clean` transitions: the visible state observed
    /// before flush is durable afterwards. `flush()` from the
    /// [`crate::state::CommitMode::Wal`] `Sealed` phase is illegal and
    /// surfaces [`crate::state::value::TransactionValueStoreError::AlreadySealed`].
    fn flush(&self) -> impl Future<Output = Result<StoreOutcome, Self::Error>> + Send;
}

/// Bundle bound for the dirty Value store the middleware composes with.
///
/// Pulled out so impl blocks downstream can reference the bound without
/// repeating the eight individual trait constraints. Both
/// [`crate::state::memory::MemoryDirtyValueStore`] and
/// [`crate::state::fjall::FjallDirtyValueStore`] satisfy it.
pub trait DirtyValueBundle:
    ValueStore + PendingOpSource<ValueKind, Error = <Self as ValueStore>::Error> + Clone
{
}

impl<T> DirtyValueBundle for T where
    T: ValueStore + PendingOpSource<ValueKind, Error = <T as ValueStore>::Error> + Clone
{
}

/// Bundle bound for the durable Value store the middleware composes with.
pub trait DurableValueBundle:
    ValueStore<Error = <Self as DurableWalStore<ValueKind>>::Error>
    + DurableWalStore<ValueKind>
    + DirectApplyStore<ValueKind, Error = <Self as DurableWalStore<ValueKind>>::Error>
    + fmt::Debug
    + Clone
    + Send
    + Sync
    + 'static
{
}

impl<T> DurableValueBundle for T where
    T: ValueStore<Error = <T as DurableWalStore<ValueKind>>::Error>
        + DurableWalStore<ValueKind>
        + DirectApplyStore<ValueKind, Error = <T as DurableWalStore<ValueKind>>::Error>
        + fmt::Debug
        + Clone
        + Send
        + Sync
        + 'static
{
}

/// Shared per-event transaction handle for a single Value collection.
///
/// Multiple `KeyedStateContext` clones share the inner `Arc<Mutex<...>>` so
/// repeated `ctx.value(name)` calls in the same handler return handles that
/// accumulate ops into the same transaction.
pub struct ValueHandle<D, S> {
    tx: Arc<AsyncMutex<TransactionValueStore<D, S>>>,
    collection: CollectionId<ValueKind>,
}

impl<D, S> Clone for ValueHandle<D, S> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            collection: self.collection.clone(),
        }
    }
}

impl<D, S> ValueAccessor for ValueHandle<D, S>
where
    D: DurableValueBundle,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    type Error = TransactionValueStoreError<
        <S as ValueStore>::Error,
        <D as DurableWalStore<ValueKind>>::Error,
    >;

    async fn get(&self) -> Result<Option<StoredPayload>, Self::Error> {
        let guard = self.tx.lock().await;
        let read = guard.get(&self.collection).await?;
        Ok(match read {
            Read::Present(payload) => Some(payload),
            Read::Absent | Read::Unknown => None,
        })
    }

    async fn set(&self, payload: StoredPayload) -> Result<(), Self::Error> {
        let guard = self.tx.lock().await;
        guard.set(&self.collection, payload).await
    }

    async fn clear(&self) -> Result<(), Self::Error> {
        let guard = self.tx.lock().await;
        guard.clear(&self.collection).await
    }

    async fn flush(&self) -> Result<StoreOutcome, Self::Error> {
        let mut guard = self.tx.lock().await;
        guard.flush().await
    }
}

type ValueTx<D, S> = Arc<AsyncMutex<TransactionValueStore<D, S>>>;

#[derive(Default)]
struct ContextInner<D, S> {
    transactions: SyncMutex<HashMap<StateName, ValueTx<D, S>>>,
}

pub(super) type ContextTxError<D, S> =
    TransactionValueStoreError<<S as ValueStore>::Error, <D as DurableWalStore<ValueKind>>::Error>;

/// Wraps an inner [`EventContext`] with keyed-state access.
///
/// One [`KeyedStateContext`] is constructed per handler invocation by
/// [`KeyedStateMiddleware`](super::KeyedStateMiddleware). Clones share the
/// inner transaction map so
/// repeated `ctx.value(name)` calls return handles that accumulate into
/// the same transaction.
pub struct KeyedStateContext<C, D, S> {
    inner: C,
    durable: D,
    dirty: S,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    pub(super) event: EventRef,
    ctx: Arc<ContextInner<D, S>>,
}

impl<C, D, S> Clone for KeyedStateContext<C, D, S>
where
    C: Clone,
    D: Clone,
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            durable: self.durable.clone(),
            dirty: self.dirty.clone(),
            registry: self.registry.clone(),
            state_key: self.state_key.clone(),
            event: self.event,
            ctx: self.ctx.clone(),
        }
    }
}

impl<C, D, S> KeyedStateContext<C, D, S>
where
    C: EventContext,
    D: DurableValueBundle,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    pub(super) fn new(
        inner: C,
        durable: D,
        dirty: S,
        registry: Arc<CollectionDefRegistry>,
        state_key: StateKey,
        event: EventRef,
    ) -> Self {
        Self {
            inner,
            durable,
            dirty,
            registry,
            state_key,
            event,
            ctx: Arc::new(ContextInner {
                transactions: SyncMutex::new(HashMap::new()),
            }),
        }
    }

    fn collection_id_for(&self, name: &StateName) -> CollectionId<ValueKind> {
        CollectionId::new(self.state_key.clone(), StateType::Application, name.clone())
    }

    fn collection_ref_for(&self, name: &StateName) -> CollectionRef<ValueKind> {
        CollectionRef::new(self.collection_id_for(name), self.registry.ttl_for(name))
    }

    fn open_transaction(&self, name: &StateName) -> ValueTx<D, S> {
        let mut txs = self.ctx.transactions.lock();
        if let Some(existing) = txs.get(name) {
            return existing.clone();
        }

        let collection = self.collection_ref_for(name);
        let mode = self.registry.commit_mode_for(name);
        let tx = TransactionValueStore::new(
            self.durable.clone(),
            self.dirty.clone(),
            collection,
            self.event,
            mode,
        );
        let handle = Arc::new(AsyncMutex::new(tx));
        txs.insert(name.clone(), handle.clone());
        handle
    }

    /// Returns the names of every collection touched by this event.
    fn dirty_collection_names(&self) -> Vec<StateName> {
        self.ctx.transactions.lock().keys().cloned().collect()
    }

    /// Walks every dirty collection and dispatches based on its
    /// per-collection commit mode. `Wal` collections seal; `Direct`
    /// collections direct-apply. Returns the WAL-sealed list so the
    /// middleware can route apply hooks and decide whether to schedule
    /// the `StateRecovery` timer.
    pub(super) async fn resolve_per_collection(
        &self,
    ) -> Result<Vec<CollectionRef<ValueKind>>, ContextTxError<D, S>> {
        let names = self.dirty_collection_names();
        let mut sealed = Vec::new();
        for name in names {
            let tx = self.open_transaction(&name);
            let mut guard = tx.lock().await;
            match self.registry.commit_mode_for(&name) {
                CommitMode::Wal => match guard.seal().await {
                    Ok(sealed_collection) => sealed.push(sealed_collection.collection().clone()),
                    Err(TransactionValueStoreError::NoPendingOps) => {
                        // Touched the collection (e.g. via `get`) but
                        // never mutated it — nothing to seal.
                    }
                    Err(err) => return Err(err),
                },
                CommitMode::Direct => match guard.direct_apply().await {
                    Ok(_) | Err(TransactionValueStoreError::NoPendingOps) => {}
                    Err(err) => return Err(err),
                },
            }
        }
        Ok(sealed)
    }
}

impl<C, D, S> TerminationSignals for KeyedStateContext<C, D, S>
where
    C: EventContext + Clone + Send + Sync,
    D: Clone + Send + Sync,
    S: Clone + Send + Sync,
{
    fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown()
    }

    fn is_message_cancelled(&self) -> bool {
        self.inner.is_message_cancelled()
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_shutdown()
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_message_cancelled()
    }
}

impl<C, D, S> EventContext for KeyedStateContext<C, D, S>
where
    C: EventContext + Clone + Send + Sync,
    D: DurableValueBundle,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    type Error = C::Error;

    fn should_cancel(&self) -> bool {
        self.inner.should_cancel()
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_cancel()
    }

    fn cancel(&self) {
        self.inner.cancel();
    }

    fn uncancel(&self) {
        self.inner.uncancel();
    }

    async fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        self.inner.schedule(time, timer_type).await
    }

    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        self.inner.clear_and_schedule(time, timer_type).await
    }

    async fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        self.inner.unschedule(time, timer_type).await
    }

    async fn clear_scheduled(&self, timer_type: TimerType) -> Result<(), Self::Error> {
        self.inner.clear_scheduled(timer_type).await
    }

    fn invalidate(self) {
        self.inner.invalidate();
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        self.inner.scheduled(timer_type)
    }
}

impl<C, D, S> KeyedStateAccess for KeyedStateContext<C, D, S>
where
    C: EventContext + Clone + Send + Sync,
    D: DurableValueBundle,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    type ValueError = KeyedStateAccessError;
    type ValueHandle = ValueHandle<D, S>;

    fn value(&self, name: &str) -> Result<Self::ValueHandle, Self::ValueError> {
        let parsed = StateName::try_new(name)?;
        let tx = self.open_transaction(&parsed);
        let collection_id = self.collection_id_for(&parsed);
        Ok(ValueHandle {
            tx,
            collection: collection_id,
        })
    }
}

/// Error returned by [`KeyedStateAccess::value`].
#[derive(Debug, Error)]
pub enum KeyedStateAccessError {
    /// The collection name was empty or invalid.
    #[error(transparent)]
    Name(#[from] StateNameError),
}

impl ClassifyError for KeyedStateAccessError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Name(e) => e.classify_error(),
        }
    }
}
