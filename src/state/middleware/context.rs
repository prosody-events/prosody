//! Keyed-state context and the bundle traits.

use super::registry::CollectionDefRegistry;
use crate::consumer::event_context::{EventContext, StateAccessError, TerminationSignals};
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::defer::message::MessageLoader;
use crate::state::descriptor::{KafkaMessageRef, StructuralIdentity};
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

type ValueTx<D, S> = Arc<AsyncMutex<TransactionValueStore<D, S>>>;

struct ContextInner<D, S> {
    transactions: SyncMutex<HashMap<StateName, ValueTx<D, S>>>,
}

pub(super) type ContextTxError<D, S> =
    TransactionValueStoreError<<S as ValueStore>::Error, <D as DurableWalStore<ValueKind>>::Error>;

/// Wraps an inner [`EventContext`], overriding its keyed-state
/// capabilities with store-backed implementations.
///
/// One [`KeyedStateContext`] is constructed per handler invocation by
/// [`KeyedStateMiddleware`](super::KeyedStateMiddleware). Clones share the
/// inner transaction map, so repeated [`EventContext::state`] bindings of
/// the same collection accumulate into the same per-event transaction.
///
/// `L` is the message loader Kafka-message handles resolve through; its
/// payload is pinned to the inner context's
/// ([`MessageLoader::Payload`]` = C::Payload`), which is what keeps
/// [`EventContext::load_state_message`] fully typed in generic handlers.
pub struct KeyedStateContext<C, D, S, L> {
    inner: C,
    durable: D,
    dirty: S,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    pub(super) event: EventRef,
    ctx: Arc<ContextInner<D, S>>,
}

impl<C, D, S, L> Clone for KeyedStateContext<C, D, S, L>
where
    C: Clone,
    D: Clone,
    S: Clone,
    L: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            durable: self.durable.clone(),
            dirty: self.dirty.clone(),
            loader: self.loader.clone(),
            registry: self.registry.clone(),
            state_key: self.state_key.clone(),
            event: self.event,
            ctx: self.ctx.clone(),
        }
    }
}

/// Construction parameters for [`KeyedStateContext`], bundled so the
/// constructor stays readable as the context grows.
pub(crate) struct ContextParts<C, D, S, L> {
    pub(crate) inner: C,
    pub(crate) durable: D,
    pub(crate) dirty: S,
    pub(crate) loader: L,
    pub(crate) registry: Arc<CollectionDefRegistry>,
    pub(crate) state_key: StateKey,
    pub(crate) event: EventRef,
}

impl<C, D, S, L> KeyedStateContext<C, D, S, L>
where
    D: Clone,
    S: Clone,
{
    pub(crate) fn new(parts: ContextParts<C, D, S, L>) -> Self {
        let ContextParts {
            inner,
            durable,
            dirty,
            loader,
            registry,
            state_key,
            event,
        } = parts;
        Self {
            inner,
            durable,
            dirty,
            loader,
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
}

impl<C, D, S, L> KeyedStateContext<C, D, S, L>
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

impl<C, D, S, L> TerminationSignals for KeyedStateContext<C, D, S, L>
where
    C: EventContext,
    D: Clone + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
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

impl<C, D, S, L> EventContext for KeyedStateContext<C, D, S, L>
where
    C: EventContext,
    D: DurableValueBundle,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
    L: MessageLoader<Payload = C::Payload> + 'static,
{
    type Error = C::Error;
    type Payload = C::Payload;

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

    fn verify_state_registration(
        &self,
        name: &'static str,
        identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        let Some((state_name, registered)) = self.registry.lookup(name) else {
            return Err(StateAccessError::Unregistered { name });
        };
        if registered.identity != *identity {
            return Err(StateAccessError::IdentityMismatch {
                stored: registered.identity.clone(),
                asserted: identity.clone(),
            });
        }
        Ok(state_name.clone())
    }

    async fn state_cell(&self, name: &StateName) -> Result<Option<Bytes>, StateAccessError> {
        let tx = self.open_transaction(name);
        let guard = tx.lock().await;
        let read = guard
            .get(&self.collection_id_for(name))
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        Ok(match read {
            Read::Present(payload) => Some(payload),
            Read::Absent | Read::Unknown => None,
        })
    }

    async fn set_state_cell(&self, name: &StateName, cell: Bytes) -> Result<(), StateAccessError> {
        let tx = self.open_transaction(name);
        let guard = tx.lock().await;
        guard
            .set(&self.collection_id_for(name), cell)
            .await
            .map_err(|e| StateAccessError::store(&e))
    }

    async fn clear_state_cell(&self, name: &StateName) -> Result<(), StateAccessError> {
        let tx = self.open_transaction(name);
        let guard = tx.lock().await;
        guard
            .clear(&self.collection_id_for(name))
            .await
            .map_err(|e| StateAccessError::store(&e))
    }

    /// Drains buffered ops directly to authoritative state and returns
    /// the transaction to `Clean`.
    ///
    /// Mirrors the design's `Dirty → flush() → Clean` and
    /// `Clean → flush() → Clean` transitions: the visible state observed
    /// before flush is durable afterwards. `flush()` from the
    /// [`CommitMode::Wal`] `Sealed` phase is illegal and surfaces
    /// [`TransactionValueStoreError::AlreadySealed`] type-erased into
    /// [`StateAccessError::Store`].
    async fn flush_state_cell(&self, name: &StateName) -> Result<StoreOutcome, StateAccessError> {
        let tx = self.open_transaction(name);
        let mut guard = tx.lock().await;
        guard.flush().await.map_err(|e| StateAccessError::store(&e))
    }

    async fn load_state_message(
        &self,
        message_ref: KafkaMessageRef,
    ) -> Result<ConsumerMessage<C::Payload>, StateAccessError> {
        self.loader
            .load_message(message_ref.topic, message_ref.partition, message_ref.offset)
            .await
            .map_err(|e| StateAccessError::load(&e))
    }
}
