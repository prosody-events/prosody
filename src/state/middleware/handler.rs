//! Middleware builder, handler, provider, and the recovery sweep.

use super::context::{DirtyValueBundle, DurableValueBundle, KeyedStateContext};
use super::error::{
    BoxedFactoryError, BuildContextResult, KeyedStateMiddlewareError, MiddlewareError,
    RecoveryError,
};
use super::registry::{CollectionDef, CollectionDefRegistry};
use crate::Key;
use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::deduplication::dedup_uuid_for_message;
use crate::consumer::middleware::defer::segment::compute_segment_id;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::oracle::CommitOracle;
use crate::state::pending::{PendingIndexScanner, PendingIndexStore};
use crate::state::value::{DurableWalStore, ValueKind};
use crate::state::{
    CollectionId, CollectionKindId, CollectionRef, CommitDecision, CommitMode, DirtyStoreFactory,
    DirtyStoreProvider, DurableState, EventRef, EventScopeId, StateKey, StateName, StoreOutcome,
    TimerEventRef,
};
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use crate::{EventIdentity, Partition, Topic};
use futures::StreamExt;
use std::collections::HashMap;
use std::error::Error;
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
    version: Option<Arc<str>>,
    default_ttl: Option<CompactDuration>,
    default_commit_mode: CommitMode,
    recovery_delay: CompactDuration,
    defs: HashMap<StateName, CollectionDef>,
    registry: Option<Arc<CollectionDefRegistry>>,
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
            version: None,
            default_ttl: None,
            default_commit_mode: CommitMode::Wal,
            recovery_delay: CompactDuration::new(DEFAULT_RECOVERY_DELAY_SECS),
            defs: HashMap::new(),
            registry: None,
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

    /// Sets the deduplication hash version used to derive a message's
    /// dedup id during recovery.
    ///
    /// This **must** equal the `version` configured on the deduplication
    /// middleware (see
    /// [`DeduplicationConfiguration::version`](crate::consumer::middleware::deduplication::DeduplicationConfiguration)):
    /// recovery looks a message's committed state up by the dedup id the
    /// deduplication writer produced, and the version is one of the hashed
    /// inputs. A composer wires both from the same source — see
    /// [`DEFAULT_IDEMPOTENCE_VERSION`](crate::consumer::middleware::deduplication::DEFAULT_IDEMPOTENCE_VERSION).
    /// The field is required so a forgotten wiring fails loud at
    /// [`Self::build`] rather than silently hashing a different version.
    #[must_use]
    pub fn version(mut self, version: Arc<str>) -> Self {
        self.version = Some(version);
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

    /// Supplies an explicit, pre-built registry shared with the durable
    /// bundle's recovery wrapper.
    ///
    /// Production wiring builds **one** [`Arc<CollectionDefRegistry>`] and
    /// passes it both here and to
    /// [`RecoveringValueStore`](crate::state::recovering::RecoveringValueStore)
    /// as the [`CollectionTtl`](crate::state::recovering::CollectionTtl)
    /// resolver, so the get-side (first-touch) and sweep-side recovery paths
    /// bind the **same** per-collection TTL. When set, [`Self::build`] uses
    /// this registry verbatim and ignores the per-builder
    /// [`Self::collection_def`] / [`Self::default_ttl`] /
    /// [`Self::default_commit_mode`] inputs; when unset, `build` derives a
    /// fresh registry from those inputs.
    #[must_use]
    pub fn registry(mut self, registry: Arc<CollectionDefRegistry>) -> Self {
        self.registry = Some(registry);
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
        let version = self
            .version
            .ok_or(KeyedStateMiddlewareBuildError::Missing("version"))?;
        let registry = match self.registry {
            Some(registry) => registry,
            None => Arc::new(CollectionDefRegistry {
                defs: self.defs,
                default_ttl: self.default_ttl,
                default_commit_mode: self.default_commit_mode,
            }),
        };
        Ok(KeyedStateMiddleware {
            durable,
            scanner,
            oracle,
            factory,
            consumer_group,
            version,
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
    version: Arc<str>,
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
            version: self.version.clone(),
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
    pub(super) version: Arc<str>,
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

    /// Shared tail of `on_message` / `on_timer`: resolve the per-collection
    /// seals for the wrapped context and package them into
    /// [`KeyedStateOutput::Inner`] for the apply hooks. When any collection
    /// sealed, arms the `StateRecovery` backstop timer.
    async fn finalize_inner<C>(
        &self,
        wrapped: KeyedStateContext<C, D, P::Store>,
        context: C,
        inner_output: T::Output,
    ) -> Result<KeyedStateOutput<T::Output>, MiddlewareError<T, D, Sc, O, P::Store>>
    where
        C: EventContext + Clone + Send + Sync + 'static,
    {
        let event = wrapped.event;
        let sealed_collections = wrapped
            .resolve_per_collection()
            .await
            .map_err(KeyedStateMiddlewareError::Transaction)?;
        let sealed_event = if sealed_collections.is_empty() {
            None
        } else {
            let now = CompactDateTime::now().map_err(KeyedStateMiddlewareError::DateTime)?;
            let fire = now
                .add_duration(self.recovery_delay)
                .map_err(KeyedStateMiddlewareError::DateTime)?;
            context
                .schedule(fire, TimerType::StateRecovery)
                .await
                .map_err(|e| KeyedStateMiddlewareError::Timer(Box::new(e)))?;
            Some(event)
        };
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
    ) -> Result<(), MiddlewareError<T, D, Sc, O, P::Store>>
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

    pub(super) fn derive_dedup_id_for_message(&self, message: &ConsumerMessage<T::Payload>) -> Uuid
    where
        T::Payload: EventIdentity,
    {
        let topic = message.topic();
        dedup_uuid_for_message(
            self.version.as_ref(),
            self.consumer_group.as_ref(),
            &topic,
            message.partition(),
            message,
        )
    }
}

impl<T, D, Sc, O, P> FallibleHandler for KeyedStateHandler<T, D, Sc, O, P>
where
    T: FallibleHandler,
    T::Payload: EventIdentity,
    D: DurableValueBundle + PendingIndexStore<Error = <D as DurableWalStore<ValueKind>>::Error>,
    Sc: PendingIndexScanner,
    O: CommitOracle,
    P: DirtyStoreProvider<ValueKind>,
    P::Store: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    type Error = MiddlewareError<T, D, Sc, O, P::Store>;
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
        let wrapped = self.build_context(context.clone(), key, event)?;

        let inner_output = self
            .inner
            .on_message(wrapped.clone(), message, demand_type)
            .await
            .map_err(KeyedStateMiddlewareError::Inner)?;

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
                .await?;
            return Ok(KeyedStateOutput::Recovery);
        }

        let event = EventRef::Timer(TimerEventRef::new(
            trigger.timer_type,
            trigger.time,
            trigger.tag,
        ));
        let key = trigger.key.clone();
        let wrapped = self.build_context(context.clone(), key, event)?;

        let inner_output = self
            .inner
            .on_timer(wrapped.clone(), trigger, demand_type)
            .await
            .map_err(KeyedStateMiddlewareError::Inner)?;

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
                let mut all_resolved = true;
                for collection_ref in &sealed_collections {
                    if let Err(error) = self.durable.apply_sealed(collection_ref, event).await {
                        warn!(error = ?error, "apply_sealed failed in after_commit");
                        all_resolved = false;
                    }
                }
                // Only clear the one-shot recovery backstop if every sealed
                // collection actually resolved. On any apply failure we leave
                // the timer armed so the `StateRecovery` sweep retries —
                // otherwise a committed write could be silently lost once the
                // sealed WAL row's TTL expires.
                if all_resolved {
                    clear_recovery_timer(&context).await;
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
                let mut all_resolved = true;
                for collection_ref in &sealed_collections {
                    if let Err(error) = self.durable.rollback_sealed(collection_ref, event).await {
                        warn!(error = ?error, "rollback_sealed failed in after_abort");
                        all_resolved = false;
                    }
                }
                // Symmetric with `after_commit`: leave the recovery backstop
                // armed if any rollback failed so the sweep retries.
                if all_resolved {
                    clear_recovery_timer(&context).await;
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

/// Per-partition provider for [`KeyedStateMiddleware`].
pub struct KeyedStateProvider<T, D, Sc, O, F> {
    inner_provider: T,
    durable: D,
    scanner: Sc,
    oracle: O,
    factory: F,
    consumer_group: Arc<str>,
    version: Arc<str>,
    registry: Arc<CollectionDefRegistry>,
    recovery_delay: CompactDuration,
}

impl<T, D, Sc, O, F> FallibleHandlerProvider for KeyedStateProvider<T, D, Sc, O, F>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler,
    <T::Handler as FallibleHandler>::Payload: EventIdentity,
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
            version: self.version.clone(),
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
    P: Send + Sync + 'static + EventIdentity,
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
            version: self.version.clone(),
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
) -> Result<(), RecoveryError<<D as DurableWalStore<ValueKind>>::Error, Sc::Error, O::Error>>
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
                resolve_sealed(durable, oracle, &collection_ref, wal.event())
                    .await
                    .map_err(|e| match e {
                        ResolveSealedError::Durable(e) => RecoveryError::Durable(e),
                        ResolveSealedError::Oracle(e) => RecoveryError::Oracle(e),
                    })?;
            }
        }
    }
    context
        .clear_scheduled(TimerType::StateRecovery)
        .await
        .map_err(|e| RecoveryError::Timer(Box::new(e)))?;
    Ok(())
}

/// Resolves a single sealed WAL: consult `oracle` for `event`, then apply or
/// roll back the sealed ops on `durable`.
///
/// This is the shared inner decision of every recovery path —
/// [`RecoveringValueStore`](crate::state::recovering::RecoveringValueStore)'s
/// first-touch (`get`) and recover-before-overwrite (`seal`) recovery, and
/// the [`recover_pending_entries`] timer sweep. Each caller maps
/// [`ResolveSealedError`] into its own error enum and keeps its surrounding
/// logic (the sweep's `Idle` stale-row `delete_pending`, the get-side
/// post-resolution re-read) at the callsite.
///
/// # Errors
///
/// Returns [`ResolveSealedError::Oracle`] if the oracle read fails, or
/// [`ResolveSealedError::Durable`] if `apply_sealed` / `rollback_sealed`
/// fails.
pub(crate) async fn resolve_sealed<D, O>(
    durable: &D,
    oracle: &O,
    collection: &CollectionRef<ValueKind>,
    event: EventRef,
) -> Result<StoreOutcome, ResolveSealedError<<D as DurableWalStore<ValueKind>>::Error, O::Error>>
where
    D: DurableWalStore<ValueKind>,
    O: CommitOracle,
{
    let decision = oracle
        .resolve(collection.id(), event)
        .await
        .map_err(ResolveSealedError::Oracle)?;
    match decision {
        CommitDecision::Committed => durable.apply_sealed(collection, event).await,
        CommitDecision::NotCommitted => durable.rollback_sealed(collection, event).await,
    }
    .map_err(ResolveSealedError::Durable)
}

/// Error raised by [`resolve_sealed`].
///
/// Kept distinct from [`RecoveryError`] and
/// [`RecoveringValueStoreError`](crate::state::recovering::RecoveringValueStoreError)
/// so the shared helper carries no caller-specific variants; each callsite
/// maps it into its own enum.
#[derive(Debug, Error)]
pub(crate) enum ResolveSealedError<DurableErr, OracleErr>
where
    DurableErr: Error + 'static,
    OracleErr: Error + 'static,
{
    /// The durable apply / rollback failed.
    #[error("keyed-state durable store failed")]
    Durable(#[source] DurableErr),

    /// The commit oracle failed.
    #[error("keyed-state commit oracle failed")]
    Oracle(#[source] OracleErr),
}
