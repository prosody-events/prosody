//! Keyed-state context, the byte-handle substrate, and the bundle traits.

use super::registry::CollectionDefRegistry;
use crate::consumer::event_context::{EventContext, TerminationSignals};
use crate::state::descriptor::{KafkaMessageRef, StateDescriptor};
use crate::state::value::{
    DirectApplyStore, DurableWalStore, PendingOpSource, TransactionValueStore,
    TransactionValueStoreError, ValueKind, ValueStore,
};
use crate::state::{
    CollectionId, CollectionRef, CommitMode, EventRef, Read, StateKey, StateName, StateType,
    StoreOutcome,
};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::{Offset, Partition, Topic};
use bytes::Bytes;
use parking_lot::Mutex as SyncMutex;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

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

/// Event-scope marker for timer handlers.
///
/// Carries nothing: a timer event has no Kafka coordinates, so
/// [`KeyedStateContext::message_ref`] does not exist on timer-scoped
/// contexts — referencing it is a compile error, not a runtime one.
#[derive(Clone, Copy, Debug, Default)]
pub struct TimerScope;

/// Event-scope marker for message handlers: the Kafka coordinates of the
/// message being processed.
#[derive(Clone, Copy, Debug)]
pub struct MessageScope {
    topic: Topic,
    partition: Partition,
    offset: Offset,
}

impl MessageScope {
    pub(super) fn new(topic: Topic, partition: Partition, offset: Offset) -> Self {
        Self {
            topic,
            partition,
            offset,
        }
    }
}

/// Shared per-event byte-transaction handle for a single Value collection —
/// the substrate every typed descriptor handle wraps.
///
/// Multiple `KeyedStateContext` clones share the inner `Arc<Mutex<...>>` so
/// repeated bindings of the same collection in one handler return handles
/// that accumulate ops into the same transaction.
pub(crate) struct ByteValueHandle<D, S> {
    tx: Arc<AsyncMutex<TransactionValueStore<D, S>>>,
    collection: CollectionId<ValueKind>,
}

impl<D, S> Clone for ByteValueHandle<D, S> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            collection: self.collection.clone(),
        }
    }
}

impl<D, S> ByteValueHandle<D, S>
where
    D: DurableValueBundle,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    /// Reads the current visible cell bytes.
    pub(crate) async fn get(&self) -> Result<Option<Bytes>, ContextTxError<D, S>> {
        let guard = self.tx.lock().await;
        let read = guard.get(&self.collection).await?;
        Ok(match read {
            Read::Present(payload) => Some(payload),
            Read::Absent | Read::Unknown => None,
        })
    }

    /// Buffers a set operation for this collection.
    pub(crate) async fn set(&self, payload: Bytes) -> Result<(), ContextTxError<D, S>> {
        let guard = self.tx.lock().await;
        guard.set(&self.collection, payload).await
    }

    /// Buffers a clear operation for this collection.
    pub(crate) async fn clear(&self) -> Result<(), ContextTxError<D, S>> {
        let guard = self.tx.lock().await;
        guard.clear(&self.collection).await
    }

    /// Drains buffered ops directly to authoritative state and returns
    /// the transaction to `Clean`.
    ///
    /// Mirrors the design's `Dirty → flush() → Clean` and
    /// `Clean → flush() → Clean` transitions: the visible state observed
    /// before flush is durable afterwards. `flush()` from the
    /// [`crate::state::CommitMode::Wal`] `Sealed` phase is illegal and
    /// surfaces [`crate::state::value::TransactionValueStoreError::AlreadySealed`].
    pub(crate) async fn flush(&self) -> Result<StoreOutcome, ContextTxError<D, S>> {
        let mut guard = self.tx.lock().await;
        guard.flush().await
    }
}

type ValueTx<D, S> = Arc<AsyncMutex<TransactionValueStore<D, S>>>;

struct ContextInner<D, S> {
    transactions: SyncMutex<HashMap<StateName, ValueTx<D, S>>>,
}

pub(super) type ContextTxError<D, S> =
    TransactionValueStoreError<<S as ValueStore>::Error, <D as DurableWalStore<ValueKind>>::Error>;

/// Wraps an inner [`EventContext`] with typed keyed-state access.
///
/// One [`KeyedStateContext`] is constructed per handler invocation by
/// [`KeyedStateMiddleware`](super::KeyedStateMiddleware). Clones share the
/// inner transaction map so repeated [`Self::state`] bindings of the same
/// collection accumulate into the same transaction.
///
/// `L` is the message loader Kafka-message handles resolve through;
/// `Scope` is [`MessageScope`] or [`TimerScope`], gating
/// [`Self::message_ref`] at the type level.
pub struct KeyedStateContext<C, D, S, L, Scope> {
    inner: C,
    durable: D,
    dirty: S,
    loader: L,
    scope: Scope,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    pub(super) event: EventRef,
    ctx: Arc<ContextInner<D, S>>,
}

impl<C, D, S, L, Scope> Clone for KeyedStateContext<C, D, S, L, Scope>
where
    C: Clone,
    D: Clone,
    S: Clone,
    L: Clone,
    Scope: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            durable: self.durable.clone(),
            dirty: self.dirty.clone(),
            loader: self.loader.clone(),
            scope: self.scope.clone(),
            registry: self.registry.clone(),
            state_key: self.state_key.clone(),
            event: self.event,
            ctx: self.ctx.clone(),
        }
    }
}

/// Construction parameters for [`KeyedStateContext`], bundled so the
/// constructor stays readable as the context grows.
pub(crate) struct ContextParts<C, D, S, L, Scope> {
    pub(crate) inner: C,
    pub(crate) durable: D,
    pub(crate) dirty: S,
    pub(crate) loader: L,
    pub(crate) scope: Scope,
    pub(crate) registry: Arc<CollectionDefRegistry>,
    pub(crate) state_key: StateKey,
    pub(crate) event: EventRef,
}

impl<C, D, S, L, Scope> KeyedStateContext<C, D, S, L, Scope>
where
    D: Clone,
    S: Clone,
{
    pub(crate) fn new(parts: ContextParts<C, D, S, L, Scope>) -> Self {
        let ContextParts {
            inner,
            durable,
            dirty,
            loader,
            scope,
            registry,
            state_key,
            event,
        } = parts;
        Self {
            inner,
            durable,
            dirty,
            loader,
            scope,
            registry,
            state_key,
            event,
            ctx: Arc::new(ContextInner {
                transactions: SyncMutex::new(HashMap::new()),
            }),
        }
    }

    /// Binds `descriptor` to this event scope, returning its typed handle.
    ///
    /// Repeated bindings of the same collection in one handler share the
    /// per-event transaction.
    ///
    /// # Errors
    ///
    /// Returns the descriptor's bind error when the collection is
    /// unregistered or asserts a different structural identity.
    pub fn state<DESC>(&self, descriptor: DESC) -> Result<DESC::Handle, DESC::Error>
    where
        DESC: StateDescriptor<Self>,
    {
        descriptor.bind(self)
    }

    /// Returns the registered collections for descriptor validation.
    pub(crate) fn registry(&self) -> &CollectionDefRegistry {
        &self.registry
    }

    /// Returns the loader Kafka-message handles resolve through.
    pub(crate) fn loader(&self) -> &L {
        &self.loader
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

    /// Returns the byte-level handle for `name`, opening (or joining) the
    /// per-event transaction for that collection.
    ///
    /// Internal substrate — typed access goes through descriptors.
    pub(crate) fn byte_handle(&self, name: &StateName) -> ByteValueHandle<D, S> {
        ByteValueHandle {
            tx: self.open_transaction(name),
            collection: self.collection_id_for(name),
        }
    }
}

impl<C, D, S, L> KeyedStateContext<C, D, S, L, MessageScope> {
    /// Returns the durable reference to the message this handler is
    /// processing — the production source of a [`KafkaMessageRef`] for
    /// Kafka-message collections.
    ///
    /// Exists only on message-scoped contexts; timer handlers cannot name
    /// it.
    #[must_use]
    pub fn message_ref(&self) -> KafkaMessageRef {
        KafkaMessageRef {
            topic: self.scope.topic,
            partition: self.scope.partition,
            offset: self.scope.offset,
        }
    }
}

impl<C, D, S, L, Scope> KeyedStateContext<C, D, S, L, Scope>
where
    D: DurableValueBundle,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    /// Walks every dirty collection and dispatches based on its
    /// per-collection commit mode. `Wal` collections seal; `Direct`
    /// collections direct-apply. Returns the WAL-sealed list so the
    /// middleware can route apply hooks and decide whether to schedule
    /// the `StateRecovery` timer.
    pub(super) async fn resolve_per_collection(
        &self,
    ) -> Result<Vec<CollectionRef<ValueKind>>, ContextTxError<D, S>> {
        // Snapshot every touched collection under one lock, then drop the
        // sync guard before awaiting — the per-event map only grows, so
        // every name already maps to its open transaction.
        let transactions: Vec<(StateName, ValueTx<D, S>)> = {
            let txs = self.ctx.transactions.lock();
            txs.iter().map(|(n, tx)| (n.clone(), tx.clone())).collect()
        };
        let mut sealed = Vec::new();
        for (name, tx) in transactions {
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

impl<C, D, S, L, Scope> TerminationSignals for KeyedStateContext<C, D, S, L, Scope>
where
    C: EventContext,
    D: Clone + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    Scope: Clone + Send + Sync + 'static,
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

impl<C, D, S, L, Scope> EventContext for KeyedStateContext<C, D, S, L, Scope>
where
    C: EventContext,
    D: Clone + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    Scope: Clone + Send + Sync + 'static,
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
