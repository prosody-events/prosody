//! Middleware builder, handler, provider, and the recovery sweep.

use super::context::{DirtyValueBundle, DurableValueBundle, KeyedStateContext};
use super::error::{
    BoxedFactoryError, BoxedMiddlewareError, BuildContextResult, KeyedStateMiddlewareError,
    MiddlewareError, RecoveryError, box_context_error,
};
use super::registry::{CollectionDef, CollectionDefRegistry};
use crate::Key;
use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::deduplication::dedup_uuid;
use crate::consumer::middleware::defer::segment::compute_segment_id;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::oracle::CommitOracle;
use crate::state::pending::{PendingIndexScanner, PendingIndexStore};
use crate::state::value::{DurableWalStore, ValueKind};
use crate::state::{
    CollectionId, CollectionKindId, CollectionRef, CommitDecision, CommitMode, DirtyStoreFactory,
    DirtyStoreProvider, DurableState, EventRef, EventScopeId, StateKey, StateName, TimerEventRef,
};
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use crate::{Partition, Topic};
use futures::StreamExt;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

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
    pub(super) inner: T,
    pub(super) durable: D,
    pub(super) scanner: Sc,
    pub(super) oracle: O,
    pub(super) provider: Result<P, BoxedFactoryError>,
    pub(super) consumer_group: Arc<str>,
    pub(super) registry: Arc<CollectionDefRegistry>,
    pub(super) segment_id: SegmentId,
    pub(super) recovery_delay: CompactDuration,
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
    fn build_context<C>(
        &self,
        inner: C,
        key: Key,
        event: EventRef,
    ) -> BuildContextResult<C, T, D, Sc, O, P::Store>
    where
        C: EventContext + Clone + Send + Sync,
    {
        let provider = self
            .provider
            .as_ref()
            .map_err(|e| KeyedStateMiddlewareError::Factory(e.clone()))?;
        let dirty = provider.for_scope(EventScopeId::fresh());
        Ok(KeyedStateContext::new(
            inner,
            self.durable.clone(),
            dirty,
            self.registry.clone(),
            StateKey::new(self.segment_id, key),
            event,
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

    /// Shared tail of `on_message` / `on_timer`: resolve the per-collection
    /// seals for the wrapped context and package them into
    /// [`KeyedStateOutput::Inner`] for the apply hooks.
    async fn finalize_inner<C>(
        &self,
        wrapped: KeyedStateContext<C, D, P::Store>,
        context: C,
        inner_output: T::Output,
    ) -> Result<KeyedStateOutput<T::Output>, BoxedMiddlewareError<T, D, Sc, O, P::Store>>
    where
        C: EventContext + Clone + Send + Sync + 'static,
    {
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

    async fn handle_state_recovery<C>(
        &self,
        context: &C,
        key: Key,
    ) -> Result<(), MiddlewareError<T, D, Sc, O, P::Store, C>>
    where
        C: EventContext,
    {
        let state_key = StateKey::new(self.segment_id, key);
        recover_pending_entries(
            context,
            &self.durable,
            &self.scanner,
            &self.oracle,
            &self.registry,
            state_key,
        )
        .await?;
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

        self.finalize_inner(wrapped, context, inner_output).await
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

        self.finalize_inner(wrapped, context, inner_output).await
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
                clear_recovery_timer(&context).await;
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
                clear_recovery_timer(&context).await;
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

/// Best-effort clear of the `StateRecovery` timer after a keyed-state
/// apply hook resolves its sealed collections.
///
/// Shared by the `after_commit` / `after_abort` tails — the apply vs
/// rollback arm and the inner-hook delegation stay explicit in each hook
/// because they are semantically opposite; only this clear is symmetric.
/// A failure here is logged and swallowed: the next dispatch re-clears via
/// first-touch or a later `StateRecovery` fire.
async fn clear_recovery_timer<C>(context: &C)
where
    C: EventContext,
{
    if let Err(error) = context.clear_scheduled(TimerType::StateRecovery).await {
        warn!(error = ?error, "failed to clear StateRecovery timer in keyed-state apply hook");
    }
}

/// Runs the keyed-state recovery sweep over the `(segment, key)` pending
/// partition.
///
/// Shared by production [`KeyedStateHandler::handle_state_recovery`] and
/// the middleware recovery tests so both exercise identical logic. For
/// each Value entry it reads the durable partition and either cleans a
/// stale pending row over an idle partition
/// ([`PendingIndexStore::delete_pending`]) or resolves a sealed WAL
/// against the oracle and dispatches to `apply_sealed` / `rollback_sealed`.
/// Non-Value kinds are logged at WARN and skipped; future kinds plug in by
/// extending the dispatch match. The `StateRecovery` timer is cleared once
/// the partition is drained.
///
/// The error type omits the inner-handler slot the loop can never emit;
/// the production caller lifts it into [`KeyedStateMiddlewareError`] via `?`.
pub(crate) async fn recover_pending_entries<C, D, Sc, O>(
    context: &C,
    durable: &D,
    scanner: &Sc,
    oracle: &O,
    registry: &CollectionDefRegistry,
    state_key: StateKey,
) -> Result<
    (),
    RecoveryError<<D as DurableWalStore<ValueKind>>::Error, Sc::Error, O::Error, C::Error>,
>
where
    C: EventContext,
    D: DurableWalStore<ValueKind>
        + PendingIndexStore<Error = <D as DurableWalStore<ValueKind>>::Error>,
    Sc: PendingIndexScanner,
    O: CommitOracle,
{
    let stream = scanner.scan_pending(&state_key);
    futures::pin_mut!(stream);
    while let Some(entry) = stream.next().await {
        let entry = entry.map_err(RecoveryError::Scanner)?;
        if entry.kind != CollectionKindId::Value {
            warn!(
                kind = ?entry.kind,
                name = entry.name.as_str(),
                "skipping unsupported pending kind in state recovery"
            );
            continue;
        }
        let id =
            CollectionId::<ValueKind>::new(state_key.clone(), entry.state_type, entry.name.clone());
        let ttl = registry.ttl_for(&entry.name);
        let collection_ref = CollectionRef::new(id.clone(), ttl);
        match DurableWalStore::read_partition(durable, &id)
            .await
            .map_err(RecoveryError::Durable)?
        {
            DurableState::Idle { .. } => {
                PendingIndexStore::delete_pending::<ValueKind>(durable, &id)
                    .await
                    .map_err(RecoveryError::Durable)?;
            }
            DurableState::Sealed { wal, .. } => {
                let decision = oracle
                    .resolve(&id, wal.event())
                    .await
                    .map_err(RecoveryError::Oracle)?;
                match decision {
                    CommitDecision::Committed => {
                        durable
                            .apply_sealed(&collection_ref, wal.event())
                            .await
                            .map_err(RecoveryError::Durable)?;
                    }
                    CommitDecision::NotCommitted => {
                        durable
                            .rollback_sealed(&collection_ref, wal.event())
                            .await
                            .map_err(RecoveryError::Durable)?;
                    }
                }
            }
        }
    }
    context
        .clear_scheduled(TimerType::StateRecovery)
        .await
        .map_err(RecoveryError::Timer)?;
    Ok(())
}
