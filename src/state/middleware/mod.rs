//! Keyed-state middleware that wires handlers into the durable value bundle.
//!
//! This middleware is the runtime glue between user handlers and the keyed
//! state stack. It provides:
//!
//! * [`KeyedStateAccess`] — extension trait on [`EventContext`] that lets
//!   handlers call `ctx.value(name)` to operate on a Value collection.
//! * [`KeyedStateContext`] — wrapped context constructed per event; delegates
//!   `EventContext` calls to the inner context and exposes keyed-state access
//!   through [`KeyedStateAccess`].
//! * [`ValueHandle`] — the concrete handle returned by
//!   [`KeyedStateAccess::value`]; drives a [`TransactionValueStore`] per
//!   `(event, collection)`.
//! * [`KeyedStateMiddleware`] — the [`HandlerMiddleware`] implementation that
//!   wraps the handler, drives [`CommitMode::Wal`] seal + `StateRecovery` timer
//!   scheduling, [`CommitMode::Direct`] direct apply, and the apply hook
//!   routing for `apply_sealed` / `rollback_sealed`.
//!
//! # Hook lifecycle
//!
//! For each user-handler dispatch in [`CommitMode::Wal`]:
//!
//! 1. `on_message` / `on_timer` creates a fresh [`KeyedStateContext`] and
//!    invokes the inner handler.
//! 2. On `Ok`, the middleware seals every dirty collection captured by the
//!    context and schedules a single [`TimerType::StateRecovery`] timer if any
//!    collection was sealed. The seal results travel through
//!    [`KeyedStateOutput`] so the apply hooks can finalize them.
//! 3. On `Err`, the dirty workspace is dropped — nothing was sealed.
//! 4. `after_commit(Ok(_))` applies every sealed collection and clears the
//!    `StateRecovery` timer.
//! 5. `after_commit(Err(_))` / `after_abort` rolls every sealed collection back
//!    and clears the timer.
//!
//! For [`CommitMode::Direct`] the middleware skips the seal/recovery
//! ceremony and calls `direct_apply` on every dirty collection during
//! `on_message` / `on_timer`. The recovery timer is **never** scheduled
//! in direct mode — that branch literally has no access to the schedule
//! helper.
//!
//! # `StateRecovery` timer
//!
//! When the recovery timer fires, the middleware streams `scan_pending`
//! over the `(segment, key)` partition. For each Value entry it consults
//! the oracle and dispatches to `apply_sealed` or `rollback_sealed`. Idle
//! partitions with a stale pending row are cleaned up via
//! [`PendingIndexStore::delete_pending`]. Non-Value kinds are logged at
//! WARN and skipped; future kinds plug in by extending the dispatch
//! match.

use crate::Key;
use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::{EventContext, TerminationSignals};
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::deduplication::dedup_uuid;
use crate::consumer::middleware::defer::segment::compute_segment_id;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::oracle::CommitOracle;
use crate::state::pending::{PendingIndexScanner, PendingIndexStore};
use crate::state::value::{
    DirectApplyStore, DurableWalStore, PendingOpSource, StoredPayload, TransactionValueStore,
    TransactionValueStoreError, ValueKind, ValueStore,
};
use crate::state::{
    CollectionId, CollectionKindId, CollectionRef, CommitDecision, CommitMode, DirtyStoreFactory,
    DirtyStoreProvider, DurableState, EventRef, EventScopeId, Read, StateKey, StateName,
    StateNameError, StateType, StoreOutcome, TimerEventRef,
};
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use crate::{Partition, Topic};
use futures::StreamExt;
use parking_lot::Mutex as SyncMutex;
use std::collections::HashMap;
#[cfg(test)]
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// Per-collection metadata that overrides middleware defaults.
///
/// Carries the collection's TTL and [`CommitMode`]. `ttl` is `None` for
/// "do not bind a TTL" (explicit opt-out / Cassandra over-20-year
/// overflow fallback). `commit_mode` decides whether handler-side dirty
/// ops produce a sealed WAL on success ([`CommitMode::Wal`]) or apply
/// straight to authoritative state ([`CommitMode::Direct`]).
///
/// Core Invariant #6: "A collection has one `CommitMode` while a handler
/// is running" — pinned here at registration time, not at event scope
/// creation time.
#[derive(Clone, Debug)]
pub struct CollectionDef {
    /// Per-collection TTL override.
    pub ttl: Option<CompactDuration>,

    /// Per-collection commit mode.
    pub commit_mode: CommitMode,
}

impl CollectionDef {
    /// Creates a collection definition with the supplied TTL; commit
    /// mode defaults to [`CommitMode::Wal`].
    #[must_use]
    pub fn new(ttl: Option<CompactDuration>) -> Self {
        Self {
            ttl,
            commit_mode: CommitMode::Wal,
        }
    }

    /// Builder-style override for the commit mode.
    #[must_use]
    pub fn with_commit_mode(mut self, mode: CommitMode) -> Self {
        self.commit_mode = mode;
        self
    }
}

/// Registry of [`CollectionDef`] entries plus middleware-wide defaults
/// for collections that are not explicitly registered.
#[derive(Clone, Debug)]
pub struct CollectionDefRegistry {
    defs: HashMap<StateName, CollectionDef>,
    default_ttl: Option<CompactDuration>,
    default_commit_mode: CommitMode,
}

impl Default for CollectionDefRegistry {
    fn default() -> Self {
        Self {
            defs: HashMap::new(),
            default_ttl: None,
            default_commit_mode: CommitMode::Wal,
        }
    }
}

impl CollectionDefRegistry {
    /// Creates a registry with the supplied middleware-wide default TTL
    /// and [`CommitMode::Wal`] as the default commit mode.
    #[must_use]
    pub fn new(default_ttl: Option<CompactDuration>) -> Self {
        Self {
            defs: HashMap::new(),
            default_ttl,
            default_commit_mode: CommitMode::Wal,
        }
    }

    /// Overrides the default commit mode used for collections not in the
    /// registry.
    #[must_use]
    pub fn with_default_commit_mode(mut self, mode: CommitMode) -> Self {
        self.default_commit_mode = mode;
        self
    }

    /// Registers a per-collection definition. Returns the previous value
    /// for the same name, if any.
    pub fn insert(&mut self, name: StateName, def: CollectionDef) -> Option<CollectionDef> {
        self.defs.insert(name, def)
    }

    /// Returns the TTL bound to `name`, falling back to the
    /// middleware-wide default.
    #[must_use]
    pub fn ttl_for(&self, name: &StateName) -> Option<CompactDuration> {
        self.defs.get(name).map_or(self.default_ttl, |def| def.ttl)
    }

    /// Returns the commit mode bound to `name`, falling back to the
    /// middleware-wide default.
    #[must_use]
    pub fn commit_mode_for(&self, name: &StateName) -> CommitMode {
        self.defs
            .get(name)
            .map_or(self.default_commit_mode, |def| def.commit_mode)
    }
}

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

type ContextTxError<D, S> =
    TransactionValueStoreError<<S as ValueStore>::Error, <D as DurableWalStore<ValueKind>>::Error>;

/// Wraps an inner [`EventContext`] with keyed-state access.
///
/// One [`KeyedStateContext`] is constructed per handler invocation by
/// [`KeyedStateMiddleware`]. Clones share the inner transaction map so
/// repeated `ctx.value(name)` calls return handles that accumulate into
/// the same transaction.
pub struct KeyedStateContext<C, D, S> {
    inner: C,
    durable: D,
    dirty: S,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    event: EventRef,
    #[allow(dead_code)]
    scope: EventScopeId,
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
            scope: self.scope,
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
    fn new(
        inner: C,
        durable: D,
        dirty: S,
        registry: Arc<CollectionDefRegistry>,
        state_key: StateKey,
        event: EventRef,
        scope: EventScopeId,
    ) -> Self {
        Self {
            inner,
            durable,
            dirty,
            registry,
            state_key,
            event,
            scope,
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
    async fn resolve_per_collection(
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

/// Configurable defaults for [`KeyedStateMiddleware`].
const DEFAULT_RECOVERY_DELAY_SECS: u32 = 30;

/// Builder for [`KeyedStateMiddleware`].
///
/// Captures the durable bundle, the scanner, the oracle, the dirty-store
/// factory, the middleware-wide default TTL, the recovery delay, and any
/// per-collection definitions before producing the middleware.
pub struct KeyedStateMiddlewareBuilder<D, Sc, O, F, P> {
    durable: Option<D>,
    scanner: Option<Sc>,
    oracle: Option<O>,
    factory: Option<F>,
    consumer_group: Option<Arc<str>>,
    default_ttl: Option<CompactDuration>,
    default_commit_mode: CommitMode,
    recovery_delay: CompactDuration,
    defs: HashMap<StateName, CollectionDef>,
    _payload: PhantomData<fn() -> P>,
}

impl<D, Sc, O, F, P> Default for KeyedStateMiddlewareBuilder<D, Sc, O, F, P> {
    fn default() -> Self {
        Self {
            durable: None,
            scanner: None,
            oracle: None,
            factory: None,
            consumer_group: None,
            default_ttl: None,
            default_commit_mode: CommitMode::Wal,
            recovery_delay: CompactDuration::new(DEFAULT_RECOVERY_DELAY_SECS),
            defs: HashMap::new(),
            _payload: PhantomData,
        }
    }
}

impl<D, Sc, O, F, P> KeyedStateMiddlewareBuilder<D, Sc, O, F, P> {
    /// Sets the durable Value bundle.
    #[must_use]
    pub fn durable(mut self, durable: D) -> Self {
        self.durable = Some(durable);
        self
    }

    /// Sets the pending-index scanner.
    #[must_use]
    pub fn scanner(mut self, scanner: Sc) -> Self {
        self.scanner = Some(scanner);
        self
    }

    /// Sets the commit oracle.
    #[must_use]
    pub fn oracle(mut self, oracle: O) -> Self {
        self.oracle = Some(oracle);
        self
    }

    /// Sets the dirty-store factory.
    ///
    /// The factory mints per-Kafka-partition dirty workspaces during
    /// partition assignment. For in-memory tests pass
    /// [`crate::state::memory::MemoryDirtyValueStoreFactory::default()`];
    /// for production pass a `FjallDirtyValueStoreFactory` constructed
    /// from a shared [`crate::state::fjall::FjallClient`].
    #[must_use]
    pub fn dirty_factory(mut self, factory: F) -> Self {
        self.factory = Some(factory);
        self
    }

    /// Sets the consumer group used to compute the per-partition
    /// `(topic, partition, consumer_group)` segment id.
    #[must_use]
    pub fn consumer_group(mut self, group: Arc<str>) -> Self {
        self.consumer_group = Some(group);
        self
    }

    /// Sets the middleware-wide default TTL for Value collections that do
    /// not appear in the registry.
    #[must_use]
    pub fn default_ttl(mut self, ttl: Option<CompactDuration>) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Sets the default [`CommitMode`] applied to collections that are
    /// not registered via [`Self::collection_def`] (defaults to
    /// [`CommitMode::Wal`]).
    #[must_use]
    pub fn default_commit_mode(mut self, mode: CommitMode) -> Self {
        self.default_commit_mode = mode;
        self
    }

    /// Sets the delay between sealing and the `StateRecovery` sweep.
    #[must_use]
    pub fn recovery_delay(mut self, delay: CompactDuration) -> Self {
        self.recovery_delay = delay;
        self
    }

    /// Registers (or overrides) the per-collection definition for
    /// `name`. Returns the builder for chaining.
    #[must_use]
    pub fn collection_def(mut self, name: StateName, def: CollectionDef) -> Self {
        self.defs.insert(name, def);
        self
    }

    /// Builds the middleware.
    ///
    /// # Errors
    ///
    /// Returns [`KeyedStateMiddlewareBuildError::Missing`] if a required
    /// field was not supplied.
    pub fn build(
        self,
    ) -> Result<KeyedStateMiddleware<D, Sc, O, F, P>, KeyedStateMiddlewareBuildError> {
        let durable = self
            .durable
            .ok_or(KeyedStateMiddlewareBuildError::Missing("durable"))?;
        let scanner = self
            .scanner
            .ok_or(KeyedStateMiddlewareBuildError::Missing("scanner"))?;
        let oracle = self
            .oracle
            .ok_or(KeyedStateMiddlewareBuildError::Missing("oracle"))?;
        let factory = self
            .factory
            .ok_or(KeyedStateMiddlewareBuildError::Missing("dirty_factory"))?;
        let consumer_group = self
            .consumer_group
            .ok_or(KeyedStateMiddlewareBuildError::Missing("consumer_group"))?;
        let registry = Arc::new(CollectionDefRegistry {
            defs: self.defs,
            default_ttl: self.default_ttl,
            default_commit_mode: self.default_commit_mode,
        });
        Ok(KeyedStateMiddleware {
            durable,
            scanner,
            oracle,
            factory,
            consumer_group,
            registry,
            recovery_delay: self.recovery_delay,
            _payload: PhantomData,
        })
    }
}

/// Errors raised by [`KeyedStateMiddlewareBuilder::build`].
#[derive(Debug, Error)]
pub enum KeyedStateMiddlewareBuildError {
    /// A required builder field was not supplied.
    #[error("keyed-state middleware builder missing required field: {0}")]
    Missing(&'static str),
}

impl ClassifyError for KeyedStateMiddlewareBuildError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

/// Keyed-state middleware.
///
/// Wires user handlers into the durable Value bundle, drives per-collection
/// `Wal` seal + recovery timer scheduling and `Direct` apply, and routes
/// apply hooks to `apply_sealed` / `rollback_sealed`. See the module docs
/// for the hook lifecycle.
pub struct KeyedStateMiddleware<D, Sc, O, F, P> {
    durable: D,
    scanner: Sc,
    oracle: O,
    factory: F,
    consumer_group: Arc<str>,
    registry: Arc<CollectionDefRegistry>,
    recovery_delay: CompactDuration,
    _payload: PhantomData<fn() -> P>,
}

impl<D, Sc, O, F, P> Clone for KeyedStateMiddleware<D, Sc, O, F, P>
where
    D: Clone,
    Sc: Clone,
    O: Clone,
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            durable: self.durable.clone(),
            scanner: self.scanner.clone(),
            oracle: self.oracle.clone(),
            factory: self.factory.clone(),
            consumer_group: self.consumer_group.clone(),
            registry: self.registry.clone(),
            recovery_delay: self.recovery_delay,
            _payload: PhantomData,
        }
    }
}

impl<D, Sc, O, F, P> fmt::Debug for KeyedStateMiddleware<D, Sc, O, F, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyedStateMiddleware")
            .field("consumer_group", &self.consumer_group)
            .field("recovery_delay", &self.recovery_delay)
            .finish_non_exhaustive()
    }
}

impl<D, Sc, O, F, P> KeyedStateMiddleware<D, Sc, O, F, P> {
    /// Returns a fresh builder for the middleware.
    #[must_use]
    pub fn builder() -> KeyedStateMiddlewareBuilder<D, Sc, O, F, P> {
        KeyedStateMiddlewareBuilder::default()
    }
}

/// Output forwarded by the middleware to the apply hooks.
///
/// Distinguishes three lifecycles:
///
/// * [`Self::Inner`] — the wrapped handler ran. May carry zero or more sealed
///   collections (zero in [`CommitMode::Direct`] and when the handler did not
///   touch any collection).
/// * [`Self::Recovery`] — a [`TimerType::StateRecovery`] timer fired and was
///   handled by the middleware. The wrapped handler did not run; the apply
///   hooks suppress both branches. Mirrors
///   [`TimerDeferOutput::NoInner`](crate::consumer::middleware::defer::timer)
///   precedent.
pub enum KeyedStateOutput<O> {
    /// Wrapped handler ran and produced an output.
    Inner {
        /// Output value produced by the wrapped handler.
        inner: O,

        /// Event under which the seals fired, if any.
        sealed_event: Option<EventRef>,

        /// Collections sealed during this event. Apply hooks consume
        /// this list and call `apply_sealed` / `rollback_sealed` on
        /// each.
        sealed_collections: Vec<CollectionRef<ValueKind>>,
    },

    /// `StateRecovery` timer fired; recovery sweep completed without
    /// running the inner handler.
    Recovery,
}

/// Errors raised by the middleware itself.
#[derive(Debug, Error)]
pub enum KeyedStateMiddlewareError<InnerErr, DirtyErr, DurableErr, ScannerErr, OracleErr, TimerErr>
where
    InnerErr: ClassifyError + Error + Send + 'static,
    DirtyErr: ClassifyError + Error + Send + Sync + 'static,
    DurableErr: ClassifyError + Error + Send + Sync + 'static,
    ScannerErr: ClassifyError + Error + Send + Sync + 'static,
    OracleErr: ClassifyError + Error + Send + Sync + 'static,
    TimerErr: ClassifyError + Error + Send + Sync + 'static,
{
    /// The wrapped handler returned an error.
    #[error("wrapped handler failed")]
    Inner(#[source] InnerErr),

    /// A durable Value store operation failed.
    #[error("keyed-state durable store failed")]
    Durable(#[source] DurableErr),

    /// A scanner pull failed.
    #[error("keyed-state pending scanner failed")]
    Scanner(#[source] ScannerErr),

    /// The commit oracle failed.
    #[error("keyed-state commit oracle failed")]
    Oracle(#[source] OracleErr),

    /// The dirty-store factory failed at partition assignment time.
    /// Surfaced on every dispatch for the affected partition until
    /// revocation.
    #[error("keyed-state dirty factory failed at partition assignment")]
    Factory(#[source] BoxedFactoryError),

    /// Scheduling or unscheduling the recovery timer failed.
    #[error("keyed-state recovery timer failed")]
    Timer(#[source] TimerErr),

    /// The keyed-state transaction state machine refused the requested
    /// transition (e.g. sealing in direct mode).
    #[error("keyed-state transaction failed")]
    Transaction(#[source] TransactionValueStoreError<DirtyErr, DurableErr>),

    /// `CompactDateTime` arithmetic failed when computing the recovery
    /// fire time.
    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),
}

impl<InnerErr, DirtyErr, DurableErr, ScannerErr, OracleErr, TimerErr> ClassifyError
    for KeyedStateMiddlewareError<InnerErr, DirtyErr, DurableErr, ScannerErr, OracleErr, TimerErr>
where
    InnerErr: ClassifyError + Error + Send + 'static,
    DirtyErr: ClassifyError + Error + Send + Sync + 'static,
    DurableErr: ClassifyError + Error + Send + Sync + 'static,
    ScannerErr: ClassifyError + Error + Send + Sync + 'static,
    OracleErr: ClassifyError + Error + Send + Sync + 'static,
    TimerErr: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Inner(e) => e.classify_error(),
            Self::Durable(e) => e.classify_error(),
            Self::Scanner(e) => e.classify_error(),
            Self::Oracle(e) => e.classify_error(),
            Self::Factory(e) => e.classify_error(),
            Self::Timer(e) => e.classify_error(),
            Self::Transaction(e) => e.classify_error(),
            Self::DateTime(e) => e.classify_error(),
        }
    }
}

/// Factory error captured at partition assignment time and surfaced on
/// every dispatch for that partition until revocation.
///
/// The original `F::Error` is type-erased to a stable boxed shape so the
/// handler can clone it on each dispatch (the original `F::Error` is not
/// required to be `Clone`).
#[derive(Clone, Debug, Error)]
#[error("keyed-state factory error: {message}")]
pub struct BoxedFactoryError {
    message: String,
    category: ErrorCategory,
}

impl BoxedFactoryError {
    fn new<E>(err: &E) -> Self
    where
        E: ClassifyError + Error + ?Sized,
    {
        Self {
            message: format!("{err}"),
            category: err.classify_error(),
        }
    }
}

impl ClassifyError for BoxedFactoryError {
    fn classify_error(&self) -> ErrorCategory {
        self.category
    }
}

type MiddlewareError<T, D, Sc, O, S, C> = KeyedStateMiddlewareError<
    <T as FallibleHandler>::Error,
    <S as ValueStore>::Error,
    <D as DurableWalStore<ValueKind>>::Error,
    <Sc as PendingIndexScanner>::Error,
    <O as CommitOracle>::Error,
    <C as EventContext>::Error,
>;

type BoxedMiddlewareError<T, D, Sc, O, S> = KeyedStateMiddlewareError<
    <T as FallibleHandler>::Error,
    <S as ValueStore>::Error,
    <D as DurableWalStore<ValueKind>>::Error,
    <Sc as PendingIndexScanner>::Error,
    <O as CommitOracle>::Error,
    BoxedContextError,
>;

/// Per-partition keyed-state handler produced by [`KeyedStateMiddleware`].
///
/// `provider` is `Ok` when the factory succeeded at partition assignment
/// time; `Err` carries a type-erased clone of the factory error so every
/// subsequent dispatch can surface it via
/// [`KeyedStateMiddlewareError::Factory`].
pub struct KeyedStateHandler<T, D, Sc, O, P>
where
    P: DirtyStoreProvider<ValueKind>,
{
    inner: T,
    durable: D,
    scanner: Sc,
    oracle: O,
    provider: Result<P, BoxedFactoryError>,
    consumer_group: Arc<str>,
    registry: Arc<CollectionDefRegistry>,
    segment_id: SegmentId,
    recovery_delay: CompactDuration,
}

impl<T, D, Sc, O, P> KeyedStateHandler<T, D, Sc, O, P>
where
    T: FallibleHandler,
    D: DurableValueBundle + PendingIndexStore<Error = <D as DurableWalStore<ValueKind>>::Error>,
    Sc: PendingIndexScanner,
    O: CommitOracle,
    P: DirtyStoreProvider<ValueKind>,
    P::Store: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    #[allow(
        clippy::type_complexity,
        reason = "fully-typed handler bind needs Result<context, full middleware error>; \
                  introducing a type alias here would add an opaque indirection without \
                  simplifying read"
    )]
    fn build_context<C>(
        &self,
        inner: C,
        key: Key,
        event: EventRef,
    ) -> Result<KeyedStateContext<C, D, P::Store>, MiddlewareError<T, D, Sc, O, P::Store, C>>
    where
        C: EventContext + Clone + Send + Sync,
    {
        let provider = self
            .provider
            .as_ref()
            .map_err(|e| KeyedStateMiddlewareError::Factory(e.clone()))?;
        let scope = EventScopeId::fresh();
        let dirty = provider.for_scope(scope);
        Ok(KeyedStateContext::new(
            inner,
            self.durable.clone(),
            dirty,
            self.registry.clone(),
            StateKey::new(self.segment_id, key),
            event,
            scope,
        ))
    }

    async fn resolve_seal_results<C>(
        &self,
        wrapped: KeyedStateContext<C, D, P::Store>,
        context: C,
    ) -> Result<
        (Vec<CollectionRef<ValueKind>>, Option<EventRef>),
        MiddlewareError<T, D, Sc, O, P::Store, C>,
    >
    where
        C: EventContext + Clone + Send + Sync + 'static,
    {
        let event = wrapped.event;
        let sealed = wrapped
            .resolve_per_collection()
            .await
            .map_err(KeyedStateMiddlewareError::Transaction)?;
        if sealed.is_empty() {
            Ok((Vec::new(), None))
        } else {
            let now = CompactDateTime::now().map_err(KeyedStateMiddlewareError::DateTime)?;
            let fire = now
                .add_duration(self.recovery_delay)
                .map_err(KeyedStateMiddlewareError::DateTime)?;
            context
                .schedule(fire, TimerType::StateRecovery)
                .await
                .map_err(KeyedStateMiddlewareError::Timer)?;
            Ok((sealed, Some(event)))
        }
    }

    async fn handle_state_recovery<C>(
        &self,
        context: &C,
        key: Key,
    ) -> Result<(), MiddlewareError<T, D, Sc, O, P::Store, C>>
    where
        C: EventContext,
    {
        let state_key = StateKey::new(self.segment_id, key);
        let stream = self.scanner.scan_pending(&state_key);
        futures::pin_mut!(stream);
        while let Some(entry) = stream.next().await {
            let entry = entry.map_err(KeyedStateMiddlewareError::Scanner)?;
            if entry.kind != CollectionKindId::Value {
                warn!(
                    kind = ?entry.kind,
                    name = entry.name.as_str(),
                    "skipping unsupported pending kind in state recovery"
                );
                continue;
            }
            let id = CollectionId::<ValueKind>::new(
                state_key.clone(),
                entry.state_type,
                entry.name.clone(),
            );
            let ttl = self.registry.ttl_for(&entry.name);
            let collection_ref = CollectionRef::new(id.clone(), ttl);
            match DurableWalStore::read_partition(&self.durable, &id)
                .await
                .map_err(KeyedStateMiddlewareError::Durable)?
            {
                DurableState::Idle { .. } => {
                    PendingIndexStore::delete_pending::<ValueKind>(&self.durable, &id)
                        .await
                        .map_err(KeyedStateMiddlewareError::Durable)?;
                }
                DurableState::Sealed { wal, .. } => {
                    let decision = self
                        .oracle
                        .resolve(&id, wal.event())
                        .await
                        .map_err(KeyedStateMiddlewareError::Oracle)?;
                    match decision {
                        CommitDecision::Committed => {
                            self.durable
                                .apply_sealed(&collection_ref, wal.event())
                                .await
                                .map_err(KeyedStateMiddlewareError::Durable)?;
                        }
                        CommitDecision::NotCommitted => {
                            self.durable
                                .rollback_sealed(&collection_ref, wal.event())
                                .await
                                .map_err(KeyedStateMiddlewareError::Durable)?;
                        }
                    }
                }
            }
        }
        context
            .clear_scheduled(TimerType::StateRecovery)
            .await
            .map_err(KeyedStateMiddlewareError::Timer)?;
        Ok(())
    }

    fn derive_dedup_id_for_message(&self, message: &ConsumerMessage<T::Payload>) -> Uuid {
        dedup_uuid(
            "",
            self.consumer_group.as_ref(),
            message.topic().as_ref(),
            message.partition(),
            message.key().as_bytes(),
            None,
            message.offset(),
        )
    }
}

impl<T, D, Sc, O, P> FallibleHandler for KeyedStateHandler<T, D, Sc, O, P>
where
    T: FallibleHandler,
    D: DurableValueBundle + PendingIndexStore<Error = <D as DurableWalStore<ValueKind>>::Error>,
    Sc: PendingIndexScanner,
    O: CommitOracle,
    P: DirtyStoreProvider<ValueKind>,
    P::Store: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    type Error = BoxedMiddlewareError<T, D, Sc, O, P::Store>;
    type Output = KeyedStateOutput<T::Output>;
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<T::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        let dedup_id = self.derive_dedup_id_for_message(&message);
        let event = EventRef::Message { dedup_id };
        let key = message.key().clone();
        let wrapped = self
            .build_context(context.clone(), key, event)
            .map_err(box_context_error::<T, D, Sc, O, P::Store, C>)?;

        let inner_result = self
            .inner
            .on_message(wrapped.clone(), message, demand_type)
            .await;

        let inner_output = match inner_result {
            Ok(output) => output,
            Err(error) => return Err(KeyedStateMiddlewareError::Inner(error)),
        };

        let (sealed_collections, sealed_event) = self
            .resolve_seal_results(wrapped, context)
            .await
            .map_err(box_context_error::<T, D, Sc, O, P::Store, C>)?;
        Ok(KeyedStateOutput::Inner {
            inner: inner_output,
            sealed_event,
            sealed_collections,
        })
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        if trigger.timer_type == TimerType::StateRecovery {
            // Recovery sweep is owned entirely by the middleware; the
            // user handler never sees a `StateRecovery` trigger. The
            // [`KeyedStateOutput::Recovery`] variant signals "no inner
            // ran" so the apply hooks suppress both branches (mirroring
            // the [`TimerDeferOutput::NoInner`] precedent in the timer
            // defer middleware).
            self.handle_state_recovery(&context, trigger.key.clone())
                .await
                .map_err(box_context_error::<T, D, Sc, O, P::Store, C>)?;
            return Ok(KeyedStateOutput::Recovery);
        }

        let event = EventRef::Timer(TimerEventRef::new(
            trigger.timer_type,
            trigger.time,
            trigger.tag,
        ));
        let key = trigger.key.clone();
        let wrapped = self
            .build_context(context.clone(), key, event)
            .map_err(box_context_error::<T, D, Sc, O, P::Store, C>)?;

        let inner_result = self
            .inner
            .on_timer(wrapped.clone(), trigger, demand_type)
            .await;

        let inner_output = match inner_result {
            Ok(output) => output,
            Err(error) => return Err(KeyedStateMiddlewareError::Inner(error)),
        };

        let (sealed_collections, sealed_event) = self
            .resolve_seal_results(wrapped, context)
            .await
            .map_err(box_context_error::<T, D, Sc, O, P::Store, C>)?;
        Ok(KeyedStateOutput::Inner {
            inner: inner_output,
            sealed_event,
            sealed_collections,
        })
    }

    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        match result {
            Ok(KeyedStateOutput::Inner {
                inner,
                sealed_event: Some(event),
                sealed_collections,
            }) => {
                for collection_ref in &sealed_collections {
                    if let Err(error) = self.durable.apply_sealed(collection_ref, event).await {
                        warn!(error = ?error, "apply_sealed failed in after_commit");
                    }
                }
                if let Err(error) = context.clear_scheduled(TimerType::StateRecovery).await {
                    warn!(error = ?error, "failed to clear StateRecovery timer in after_commit");
                }
                self.inner.after_commit(context, Ok(inner)).await;
            }
            Ok(KeyedStateOutput::Inner {
                inner,
                sealed_event: None,
                ..
            }) => {
                self.inner.after_commit(context, Ok(inner)).await;
            }
            Err(KeyedStateMiddlewareError::Inner(error)) => {
                self.inner.after_commit(context, Err(error)).await;
            }
            // - `Ok(Recovery)`: recovery sweep, no inner ran, suppress both hooks (matches
            //   `TimerDeferOutput::NoInner`).
            // - `Err(_)` (non-Inner): middleware-internal failure (durable, scanner, oracle, timer,
            //   transaction, datetime); the inner either never ran or returned Ok and we failed to
            //   seal afterwards. Suppress the inner hook — the design's best-effort hooks contract
            //   permits it and the next dispatch recovers via first-touch or the `StateRecovery`
            //   timer.
            Ok(KeyedStateOutput::Recovery) | Err(_) => {}
        }
    }

    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        match result {
            Ok(KeyedStateOutput::Inner {
                inner,
                sealed_event: Some(event),
                sealed_collections,
            }) => {
                for collection_ref in &sealed_collections {
                    if let Err(error) = self.durable.rollback_sealed(collection_ref, event).await {
                        warn!(error = ?error, "rollback_sealed failed in after_abort");
                    }
                }
                if let Err(error) = context.clear_scheduled(TimerType::StateRecovery).await {
                    warn!(error = ?error, "failed to clear StateRecovery timer in after_abort");
                }
                self.inner.after_abort(context, Ok(inner)).await;
            }
            Ok(KeyedStateOutput::Inner {
                inner,
                sealed_event: None,
                ..
            }) => {
                self.inner.after_abort(context, Ok(inner)).await;
            }
            Err(KeyedStateMiddlewareError::Inner(error)) => {
                self.inner.after_abort(context, Err(error)).await;
            }
            // See `after_commit` for the rationale; suppression is symmetric.
            Ok(KeyedStateOutput::Recovery) | Err(_) => {}
        }
    }

    async fn shutdown(self) {
        self.inner.shutdown().await;
    }
}

/// Boxed context-error type used inside [`KeyedStateMiddlewareError::Timer`].
///
/// `FallibleHandler::Error` cannot depend on the wrapping `C` lifetime so
/// timer failures from the inner context are boxed into a stable error
/// type.
#[derive(Debug, Error)]
#[error("boxed context error")]
pub struct BoxedContextError {
    #[source]
    source: Box<dyn Error + Send + Sync + 'static>,
    category: ErrorCategory,
}

impl ClassifyError for BoxedContextError {
    fn classify_error(&self) -> ErrorCategory {
        self.category
    }
}

fn box_context_error<T, D, Sc, O, S, C>(
    err: MiddlewareError<T, D, Sc, O, S, C>,
) -> BoxedMiddlewareError<T, D, Sc, O, S>
where
    T: FallibleHandler,
    D: DurableWalStore<ValueKind>,
    Sc: PendingIndexScanner,
    O: CommitOracle,
    S: ValueStore,
    C: EventContext,
{
    match err {
        KeyedStateMiddlewareError::Inner(e) => KeyedStateMiddlewareError::Inner(e),
        KeyedStateMiddlewareError::Durable(e) => KeyedStateMiddlewareError::Durable(e),
        KeyedStateMiddlewareError::Scanner(e) => KeyedStateMiddlewareError::Scanner(e),
        KeyedStateMiddlewareError::Oracle(e) => KeyedStateMiddlewareError::Oracle(e),
        KeyedStateMiddlewareError::Factory(e) => KeyedStateMiddlewareError::Factory(e),
        KeyedStateMiddlewareError::Timer(e) => {
            let category = e.classify_error();
            KeyedStateMiddlewareError::Timer(BoxedContextError {
                source: Box::new(e),
                category,
            })
        }
        KeyedStateMiddlewareError::Transaction(e) => KeyedStateMiddlewareError::Transaction(e),
        KeyedStateMiddlewareError::DateTime(e) => KeyedStateMiddlewareError::DateTime(e),
    }
}

/// Per-partition provider for [`KeyedStateMiddleware`].
pub struct KeyedStateProvider<T, D, Sc, O, F> {
    inner_provider: T,
    durable: D,
    scanner: Sc,
    oracle: O,
    factory: F,
    consumer_group: Arc<str>,
    registry: Arc<CollectionDefRegistry>,
    recovery_delay: CompactDuration,
}

impl<T, D, Sc, O, F> FallibleHandlerProvider for KeyedStateProvider<T, D, Sc, O, F>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler,
    D: DurableValueBundle + PendingIndexStore<Error = <D as DurableWalStore<ValueKind>>::Error>,
    Sc: PendingIndexScanner,
    O: CommitOracle,
    F: DirtyStoreFactory<ValueKind>,
    F::Provider: DirtyStoreProvider<ValueKind>,
    <F::Provider as DirtyStoreProvider<ValueKind>>::Store:
        DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    type Handler = KeyedStateHandler<T::Handler, D, Sc, O, F::Provider>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        let inner = self.inner_provider.handler_for_partition(topic, partition);
        let segment_id = compute_segment_id(topic, partition, &self.consumer_group);
        let provider = self.factory.for_partition(topic, partition).map_err(|err| {
            let boxed = BoxedFactoryError::new(&err);
            tracing::error!(
                topic = ?topic,
                partition,
                error = %err,
                "keyed-state factory failed to mint dirty workspace; \
                 every dispatch on this partition will surface the failure"
            );
            boxed
        });
        KeyedStateHandler {
            inner,
            durable: self.durable.clone(),
            scanner: self.scanner.clone(),
            oracle: self.oracle.clone(),
            provider,
            consumer_group: self.consumer_group.clone(),
            registry: self.registry.clone(),
            segment_id,
            recovery_delay: self.recovery_delay,
        }
    }
}

impl<D, Sc, O, F, P> HandlerMiddleware<P> for KeyedStateMiddleware<D, Sc, O, F, P>
where
    D: DurableValueBundle + PendingIndexStore<Error = <D as DurableWalStore<ValueKind>>::Error>,
    Sc: PendingIndexScanner,
    O: CommitOracle,
    F: DirtyStoreFactory<ValueKind>,
    F::Provider: DirtyStoreProvider<ValueKind>,
    <F::Provider as DirtyStoreProvider<ValueKind>>::Store:
        DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
    P: Send + Sync + 'static,
{
    type Provider<T>
        = KeyedStateProvider<T, D, Sc, O, F>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

    fn with_provider<T>(&self, inner_provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>,
    {
        KeyedStateProvider {
            inner_provider,
            durable: self.durable.clone(),
            scanner: self.scanner.clone(),
            oracle: self.oracle.clone(),
            factory: self.factory.clone(),
            consumer_group: self.consumer_group.clone(),
            registry: self.registry.clone(),
            recovery_delay: self.recovery_delay,
        }
    }
}

/// Helper used in unit tests: drives the `StateRecovery` timer arm
/// without a full timer plumbing setup.
#[cfg(test)]
pub(crate) async fn run_state_recovery_for_tests<C, D, Sc, O>(
    context: &C,
    durable: &D,
    scanner: &Sc,
    oracle: &O,
    registry: &CollectionDefRegistry,
    state_key: StateKey,
) -> Result<
    (),
    KeyedStateMiddlewareError<
        Infallible,
        <D as DurableWalStore<ValueKind>>::Error,
        <D as DurableWalStore<ValueKind>>::Error,
        Sc::Error,
        O::Error,
        C::Error,
    >,
>
where
    C: EventContext,
    D: DurableWalStore<ValueKind>
        + DirectApplyStore<ValueKind, Error = <D as DurableWalStore<ValueKind>>::Error>
        + PendingIndexStore<Error = <D as DurableWalStore<ValueKind>>::Error>
        + Clone,
    Sc: PendingIndexScanner,
    O: CommitOracle,
{
    let stream = scanner.scan_pending(&state_key);
    futures::pin_mut!(stream);
    while let Some(entry) = stream.next().await {
        let entry = entry.map_err(KeyedStateMiddlewareError::Scanner)?;
        if entry.kind != CollectionKindId::Value {
            warn!(
                kind = ?entry.kind,
                "skipping unsupported pending kind in test recovery"
            );
            continue;
        }
        let id =
            CollectionId::<ValueKind>::new(state_key.clone(), entry.state_type, entry.name.clone());
        let ttl = registry.ttl_for(&entry.name);
        let collection_ref = CollectionRef::new(id.clone(), ttl);
        match DurableWalStore::read_partition(durable, &id)
            .await
            .map_err(KeyedStateMiddlewareError::Durable)?
        {
            DurableState::Idle { .. } => {
                PendingIndexStore::delete_pending::<ValueKind>(durable, &id)
                    .await
                    .map_err(KeyedStateMiddlewareError::Durable)?;
            }
            DurableState::Sealed { wal, .. } => {
                let decision = oracle
                    .resolve(&id, wal.event())
                    .await
                    .map_err(KeyedStateMiddlewareError::Oracle)?;
                match decision {
                    CommitDecision::Committed => {
                        durable
                            .apply_sealed(&collection_ref, wal.event())
                            .await
                            .map_err(KeyedStateMiddlewareError::Durable)?;
                    }
                    CommitDecision::NotCommitted => {
                        durable
                            .rollback_sealed(&collection_ref, wal.event())
                            .await
                            .map_err(KeyedStateMiddlewareError::Durable)?;
                    }
                }
            }
        }
    }
    context
        .clear_scheduled(TimerType::StateRecovery)
        .await
        .map_err(KeyedStateMiddlewareError::Timer)?;
    Ok(())
}
