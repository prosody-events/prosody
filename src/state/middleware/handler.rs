//! Middleware builder, handler, provider, and the recovery sweep.

use super::context::{
    ContextParts, DirtyValueBundle, DurableValueBundle, KeyedStateContext, MessageScope, TimerScope,
};
use super::descriptor_identity::{DescriptorIdentityStore, LazyDescriptorIdentity};
use super::error::{BoxedFactoryError, KeyedStateMiddlewareError, MiddlewareError, RecoveryError};
use super::registry::{CollectionDef, CollectionDefRegistry, RegisterStateError};
use crate::Key;
use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::deduplication::dedup_uuid_for_message;
use crate::consumer::middleware::defer::message::MessageLoader;
use crate::consumer::middleware::defer::segment::compute_segment_id;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::descriptor::{DescriptorIdentity, NoLoader, StructuralIdentity};
use crate::state::oracle::CommitOracle;
use crate::state::pending::{PendingIndexScanner, PendingIndexStore};
use crate::state::value::{DurableWalStore, ValueKind};
use crate::state::{
    CollectionId, CollectionKindId, CollectionRef, CommitDecision, CommitMode, DirtyStoreProvider,
    DurableState, EventRef, EventScopeId, StateBackend, StateBackendFactory, StateKey,
    StoreOutcome, TimerEventRef,
};
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use crate::{EventIdentity, Partition, Topic};
use futures::StreamExt;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

/// Configurable defaults for [`KeyedStateMiddleware`].
const DEFAULT_RECOVERY_DELAY_SECS: u32 = 30;

/// The durable Value bundle a backend factory mints.
type DurableOf<B> = <B as StateBackendFactory>::Durable;

/// The commit oracle a backend factory mints.
type OracleOf<B> = <B as StateBackendFactory>::Oracle;

/// The per-event dirty store behind a backend factory's provider.
type DirtyStoreOf<B> =
    <<B as StateBackendFactory>::DirtyProvider as DirtyStoreProvider<ValueKind>>::Store;

/// The fully-typed middleware error for a backend + scanner pairing.
type HandlerError<T, B, Sc> = MiddlewareError<T, DurableOf<B>, Sc, OracleOf<B>, DirtyStoreOf<B>>;

/// The wrapped context (or fully-typed error) `build_context` produces.
type BuiltContext<C, T, B, Sc, L, Scope> =
    Result<KeyedStateContext<C, DurableOf<B>, DirtyStoreOf<B>, L, Scope>, HandlerError<T, B, Sc>>;

/// Builder for [`KeyedStateMiddleware`].
///
/// Captures the per-partition backend factory, the scanner, the
/// middleware-wide default TTL, the recovery delay, and any descriptor
/// registrations before producing the middleware. The loader slot defaults
/// to [`NoLoader`]; consumers with Kafka-message collections supply a real
/// loader via [`Self::loader`].
pub struct KeyedStateMiddlewareBuilder<B, Sc, P, L = NoLoader> {
    backend: Option<B>,
    scanner: Option<Sc>,
    loader: L,
    consumer_group: Option<Arc<str>>,
    version: Option<Arc<str>>,
    default_ttl: Option<CompactDuration>,
    default_commit_mode: CommitMode,
    recovery_delay: CompactDuration,
    registrations: Vec<(&'static str, StructuralIdentity, CollectionDef)>,
    registry: Option<Arc<CollectionDefRegistry>>,
    _payload: PhantomData<fn() -> P>,
}

impl<B, Sc, P> Default for KeyedStateMiddlewareBuilder<B, Sc, P> {
    fn default() -> Self {
        Self {
            backend: None,
            scanner: None,
            loader: NoLoader,
            consumer_group: None,
            version: None,
            default_ttl: None,
            default_commit_mode: CommitMode::Wal,
            recovery_delay: CompactDuration::new(DEFAULT_RECOVERY_DELAY_SECS),
            registrations: Vec::new(),
            registry: None,
            _payload: PhantomData,
        }
    }
}

impl<B, Sc, P, L> KeyedStateMiddlewareBuilder<B, Sc, P, L> {
    /// Sets the per-partition backend factory (durable bundle + commit
    /// oracle + dirty-workspace provider).
    ///
    /// For in-memory tests pass a
    /// [`SharedStateBackend`](crate::state::SharedStateBackend); production
    /// uses the partition-scoped factories in
    /// [`production`](crate::state::production).
    #[must_use]
    pub fn backend(mut self, backend: B) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Sets the pending-index scanner.
    #[must_use]
    pub fn scanner(mut self, scanner: Sc) -> Self {
        self.scanner = Some(scanner);
        self
    }

    /// Sets the message loader Kafka-message collections resolve through,
    /// replacing the [`NoLoader`] default.
    #[must_use]
    pub fn loader<L2>(self, loader: L2) -> KeyedStateMiddlewareBuilder<B, Sc, P, L2> {
        KeyedStateMiddlewareBuilder {
            backend: self.backend,
            scanner: self.scanner,
            loader,
            consumer_group: self.consumer_group,
            version: self.version,
            default_ttl: self.default_ttl,
            default_commit_mode: self.default_commit_mode,
            recovery_delay: self.recovery_delay,
            registrations: self.registrations,
            registry: self.registry,
            _payload: PhantomData,
        }
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
    /// not registered via [`Self::state`] (defaults to
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

    /// Registers `descriptor`'s collection with operational settings `def`.
    ///
    /// Name validation and identity-conflict rejection run at
    /// [`Self::build`], the fallible boundary.
    #[must_use]
    pub fn state<DESC>(mut self, descriptor: &DESC, def: CollectionDef) -> Self
    where
        DESC: DescriptorIdentity,
    {
        self.registrations
            .push((descriptor.name(), descriptor.structural_identity(), def));
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
    /// [`Self::state`] / [`Self::default_ttl`] /
    /// [`Self::default_commit_mode`] inputs; when unset, `build` derives a
    /// fresh registry from those inputs.
    #[must_use]
    pub fn registry(mut self, registry: Arc<CollectionDefRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Shared `build` body: field validation + registry assembly. The
    /// public `build` impls layer the loader-presence check on top.
    fn build_any(
        self,
    ) -> Result<KeyedStateMiddleware<B, Sc, P, L>, KeyedStateMiddlewareBuildError> {
        let backend = self
            .backend
            .ok_or(KeyedStateMiddlewareBuildError::Missing("backend"))?;
        let scanner = self
            .scanner
            .ok_or(KeyedStateMiddlewareBuildError::Missing("scanner"))?;
        let consumer_group = self
            .consumer_group
            .ok_or(KeyedStateMiddlewareBuildError::Missing("consumer_group"))?;
        let version = self
            .version
            .ok_or(KeyedStateMiddlewareBuildError::Missing("version"))?;
        let registry = if let Some(registry) = self.registry {
            registry
        } else {
            let mut registry = CollectionDefRegistry::new(self.default_ttl)
                .with_default_commit_mode(self.default_commit_mode);
            for (name, identity, def) in self.registrations {
                registry.register_identity(name, identity, def)?;
            }
            Arc::new(registry)
        };
        Ok(KeyedStateMiddleware {
            backend,
            scanner,
            loader: self.loader,
            consumer_group,
            version,
            registry,
            recovery_delay: self.recovery_delay,
            _payload: PhantomData,
        })
    }
}

impl<B, Sc, P> KeyedStateMiddlewareBuilder<B, Sc, P, NoLoader> {
    /// Builds the middleware without a message loader.
    ///
    /// # Errors
    ///
    /// Returns [`KeyedStateMiddlewareBuildError::Missing`] if a required
    /// field was not supplied, or
    /// [`KeyedStateMiddlewareBuildError::KafkaCellWithoutLoader`] if a
    /// Kafka-message collection is registered — its handle cannot resolve
    /// messages without a loader. (The primary gate is the type system:
    /// `ctx.state(KAFKA_DESC)` does not compile against a [`NoLoader`]
    /// context; this check is the belt-and-braces for registries built at
    /// runtime.)
    pub fn build(
        self,
    ) -> Result<KeyedStateMiddleware<B, Sc, P, NoLoader>, KeyedStateMiddlewareBuildError> {
        let middleware = self.build_any()?;
        if middleware.registry.has_kafka_message_cells() {
            return Err(KeyedStateMiddlewareBuildError::KafkaCellWithoutLoader);
        }
        Ok(middleware)
    }
}

impl<B, Sc, P, L> KeyedStateMiddlewareBuilder<B, Sc, P, L>
where
    L: MessageLoader,
{
    /// Builds the middleware with a real message loader.
    ///
    /// # Errors
    ///
    /// Returns [`KeyedStateMiddlewareBuildError::Missing`] if a required
    /// field was not supplied.
    pub fn build(
        self,
    ) -> Result<KeyedStateMiddleware<B, Sc, P, L>, KeyedStateMiddlewareBuildError> {
        self.build_any()
    }
}

/// Errors raised by [`KeyedStateMiddlewareBuilder::build`].
#[derive(Debug, Error)]
pub enum KeyedStateMiddlewareBuildError {
    /// A required builder field was not supplied.
    #[error("keyed-state middleware builder missing required field: {0}")]
    Missing(&'static str),

    /// A descriptor registration was invalid or conflicted.
    #[error(transparent)]
    Register(#[from] RegisterStateError),

    /// A Kafka-message collection was registered but no message loader was
    /// supplied.
    #[error(
        "a Kafka-message collection is registered but the middleware has no message loader; \
         supply one via the builder's `loader` method"
    )]
    KafkaCellWithoutLoader,
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
pub struct KeyedStateMiddleware<B, Sc, P, L = NoLoader> {
    backend: B,
    scanner: Sc,
    loader: L,
    consumer_group: Arc<str>,
    version: Arc<str>,
    registry: Arc<CollectionDefRegistry>,
    recovery_delay: CompactDuration,
    _payload: PhantomData<fn() -> P>,
}

impl<B, Sc, P, L> Clone for KeyedStateMiddleware<B, Sc, P, L>
where
    B: Clone,
    Sc: Clone,
    L: Clone,
{
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            scanner: self.scanner.clone(),
            loader: self.loader.clone(),
            consumer_group: self.consumer_group.clone(),
            version: self.version.clone(),
            registry: self.registry.clone(),
            recovery_delay: self.recovery_delay,
            _payload: PhantomData,
        }
    }
}

impl<B, Sc, P, L> fmt::Debug for KeyedStateMiddleware<B, Sc, P, L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyedStateMiddleware")
            .field("consumer_group", &self.consumer_group)
            .field("recovery_delay", &self.recovery_delay)
            .finish_non_exhaustive()
    }
}

impl<B, Sc, P> KeyedStateMiddleware<B, Sc, P> {
    /// Returns a fresh builder for the middleware (loader slot defaults to
    /// [`NoLoader`]).
    #[must_use]
    pub fn builder() -> KeyedStateMiddlewareBuilder<B, Sc, P> {
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

/// Per-partition backend parts minted at assignment time: the durable
/// bundle, the oracle, the dirty provider, and the lazy durable-identity
/// validator built over the durable.
pub(super) struct PartitionBackend<B>
where
    B: StateBackendFactory,
{
    pub(super) durable: B::Durable,
    pub(super) oracle: B::Oracle,
    pub(super) dirty: B::DirtyProvider,
    pub(super) identity: LazyDescriptorIdentity<B::Durable>,
}

/// Per-partition keyed-state handler produced by [`KeyedStateMiddleware`].
///
/// `backend` is `Ok` when the factory succeeded at partition assignment
/// time; `Err` carries a type-erased clone of the factory error so every
/// subsequent dispatch can surface it via
/// [`KeyedStateMiddlewareError::Factory`].
pub struct KeyedStateHandler<T, B, Sc, L>
where
    B: StateBackendFactory,
{
    pub(super) inner: T,
    pub(super) backend: Result<PartitionBackend<B>, BoxedFactoryError>,
    pub(super) scanner: Sc,
    pub(super) loader: L,
    pub(super) consumer_group: Arc<str>,
    pub(super) version: Arc<str>,
    pub(super) registry: Arc<CollectionDefRegistry>,
    pub(super) segment_id: SegmentId,
    pub(super) recovery_delay: CompactDuration,
}

impl<T, B, Sc, L> KeyedStateHandler<T, B, Sc, L>
where
    T: FallibleHandler,
    B: StateBackendFactory,
    B::Durable: DurableValueBundle
        + PendingIndexStore<Error = <B::Durable as DurableWalStore<ValueKind>>::Error>
        + DescriptorIdentityStore<Error = <B::Durable as DurableWalStore<ValueKind>>::Error>,
    DirtyStoreOf<B>: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
    Sc: PendingIndexScanner,
    L: Clone + Send + Sync + 'static,
{
    /// Returns the partition backend, surfacing the captured factory error
    /// when assignment-time minting failed.
    fn backend(&self) -> Result<&PartitionBackend<B>, HandlerError<T, B, Sc>> {
        self.backend
            .as_ref()
            .map_err(|e| KeyedStateMiddlewareError::Factory(e.clone()))
    }

    fn build_context<C, Scope>(
        &self,
        inner: C,
        key: Key,
        event: EventRef,
        scope: Scope,
    ) -> BuiltContext<C, T, B, Sc, L, Scope>
    where
        C: EventContext + Clone + Send + Sync,
    {
        let backend = self.backend()?;
        let dirty = backend.dirty.for_scope(EventScopeId::fresh());
        Ok(KeyedStateContext::new(ContextParts {
            inner,
            durable: backend.durable.clone(),
            dirty,
            loader: self.loader.clone(),
            scope,
            registry: self.registry.clone(),
            state_key: StateKey::new(self.segment_id, key),
            event,
        }))
    }

    /// Shared tail of `on_message` / `on_timer`: resolve the per-collection
    /// seals for the wrapped context and package them into
    /// [`KeyedStateOutput::Inner`] for the apply hooks. When any collection
    /// sealed, arms the `StateRecovery` backstop timer.
    async fn finalize_inner<C, Scope>(
        &self,
        wrapped: KeyedStateContext<C, DurableOf<B>, DirtyStoreOf<B>, L, Scope>,
        context: C,
        inner_output: T::Output,
    ) -> Result<KeyedStateOutput<T::Output>, HandlerError<T, B, Sc>>
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
    ) -> Result<(), HandlerError<T, B, Sc>>
    where
        C: EventContext,
    {
        let backend = self.backend()?;
        let state_key = StateKey::new(self.segment_id, key);
        recover_pending_entries(
            context,
            &backend.durable,
            &self.scanner,
            &backend.oracle,
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

impl<T, B, Sc, L> FallibleHandler for KeyedStateHandler<T, B, Sc, L>
where
    T: FallibleHandler,
    T::Payload: EventIdentity,
    B: StateBackendFactory,
    B::Durable: DurableValueBundle
        + PendingIndexStore<Error = <B::Durable as DurableWalStore<ValueKind>>::Error>
        + DescriptorIdentityStore<Error = <B::Durable as DurableWalStore<ValueKind>>::Error>,
    DirtyStoreOf<B>: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
    Sc: PendingIndexScanner,
    L: Clone + Send + Sync + 'static,
{
    type Error = HandlerError<T, B, Sc>;
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
        // Invariant: no state op executes under an unvalidated identity.
        self.backend()?
            .identity
            .ensure()
            .await
            .map_err(KeyedStateMiddlewareError::Identity)?;

        // The dedup id is derived for every message — even when no
        // descriptors are registered — because the EventRef must exist
        // before we know whether the handler touches state. One hash per
        // message is a deliberate cost of the always-on layer.
        let dedup_id = self.derive_dedup_id_for_message(&message);
        let event = EventRef::Message { dedup_id };
        let key = message.key().clone();
        let scope = MessageScope::new(message.topic(), message.partition(), message.offset());
        let wrapped = self.build_context(context.clone(), key, event, scope)?;

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
        // Invariant: no state op executes under an unvalidated identity —
        // including the recovery sweep below.
        self.backend()?
            .identity
            .ensure()
            .await
            .map_err(KeyedStateMiddlewareError::Identity)?;

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
        let wrapped = self.build_context(context.clone(), key, event, TimerScope)?;

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
                // A sealed output cannot exist without a minted backend.
                let Ok(backend) = &self.backend else { return };
                let mut all_resolved = true;
                for collection_ref in &sealed_collections {
                    if let Err(error) = backend.durable.apply_sealed(collection_ref, event).await {
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
                // A sealed output cannot exist without a minted backend.
                let Ok(backend) = &self.backend else { return };
                let mut all_resolved = true;
                for collection_ref in &sealed_collections {
                    if let Err(error) = backend.durable.rollback_sealed(collection_ref, event).await
                    {
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
pub struct KeyedStateProvider<T, B, Sc, L> {
    inner_provider: T,
    backend: B,
    scanner: Sc,
    loader: L,
    consumer_group: Arc<str>,
    version: Arc<str>,
    registry: Arc<CollectionDefRegistry>,
    recovery_delay: CompactDuration,
}

impl<T, B, Sc, L> FallibleHandlerProvider for KeyedStateProvider<T, B, Sc, L>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler,
    <T::Handler as FallibleHandler>::Payload: EventIdentity,
    B: StateBackendFactory,
    B::Durable: DurableValueBundle
        + PendingIndexStore<Error = <B::Durable as DurableWalStore<ValueKind>>::Error>
        + DescriptorIdentityStore<Error = <B::Durable as DurableWalStore<ValueKind>>::Error>,
    DirtyStoreOf<B>: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
    Sc: PendingIndexScanner,
    L: Clone + Send + Sync + 'static,
{
    type Handler = KeyedStateHandler<T::Handler, B, Sc, L>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        let inner = self.inner_provider.handler_for_partition(topic, partition);
        let segment_id = compute_segment_id(topic, partition, &self.consumer_group);
        let backend = self
            .backend
            .for_partition(topic, partition)
            .map(
                |StateBackend {
                     durable,
                     oracle,
                     dirty,
                 }| {
                    // Constructed synchronously (no I/O): the durable identity
                    // rows are validated lazily on the partition's first
                    // dispatch.
                    let identity = LazyDescriptorIdentity::new(
                        durable.clone(),
                        self.registry.clone(),
                        segment_id,
                    );
                    PartitionBackend {
                        durable,
                        oracle,
                        dirty,
                        identity,
                    }
                },
            )
            .map_err(|err| {
                let boxed = BoxedFactoryError::new(&err);
                tracing::error!(
                    topic = ?topic,
                    partition,
                    error = %err,
                    "keyed-state backend factory failed; \
                     every dispatch on this partition will surface the failure"
                );
                boxed
            });
        KeyedStateHandler {
            inner,
            backend,
            scanner: self.scanner.clone(),
            loader: self.loader.clone(),
            consumer_group: self.consumer_group.clone(),
            version: self.version.clone(),
            registry: self.registry.clone(),
            segment_id,
            recovery_delay: self.recovery_delay,
        }
    }
}

impl<B, Sc, P, L> HandlerMiddleware<P> for KeyedStateMiddleware<B, Sc, P, L>
where
    B: StateBackendFactory,
    B::Durable: DurableValueBundle
        + PendingIndexStore<Error = <B::Durable as DurableWalStore<ValueKind>>::Error>
        + DescriptorIdentityStore<Error = <B::Durable as DurableWalStore<ValueKind>>::Error>,
    DirtyStoreOf<B>: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
    Sc: PendingIndexScanner,
    P: Send + Sync + 'static + EventIdentity,
    L: Clone + Send + Sync + 'static,
{
    type Provider<T>
        = KeyedStateProvider<T, B, Sc, L>
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
            backend: self.backend.clone(),
            scanner: self.scanner.clone(),
            loader: self.loader.clone(),
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
