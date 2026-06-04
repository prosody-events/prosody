//! Per-partition keyed-state manager: a peer of the timer manager.
//!
//! The partition loop acquires one [`StateManager`] per assignment through
//! a [`PartitionStateProvider`] (mirroring
//! [`TriggerStoreProvider`](crate::timers::store::TriggerStoreProvider)),
//! then mints one [`StateSession`] per event from it. The manager owns the
//! partition-lifetime pieces — the durable bundle (including the layered
//! fjall cache), the commit oracle, the dirty-workspace provider, the
//! pending-index scanner, and the message loader — while each session gets
//! `Arc`-clones plus a fresh per-event dirty scope.
//!
//! Acquisition is **eager**: descriptor identities are validated against
//! the segment's durable rows before the manager exists, so no session can
//! operate under an unvalidated identity. The partition loop retries
//! failed acquisitions until shutdown, the same pattern as timer-manager
//! initialization.
//!
//! [`NoState`] is the provider/manager for consumers without keyed state
//! (the plain consumer): sessions are [`UnavailableState`], recovery is a
//! no-op, and `StateRecovery` triggers are not intercepted.

use crate::consumer::Keyed;
use crate::consumer::event_context::BoxEventContextError;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::deduplication::dedup_uuid_for_message;
use crate::consumer::middleware::defer::message::MessageLoader;
use crate::consumer::middleware::defer::segment::compute_segment_id;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::descriptor_identity::{
    DescriptorIdentityError, DescriptorIdentityStore, acquire_descriptor_identities,
};
use crate::state::oracle::CommitOracle;
use crate::state::pending::{PendingIndexScanner, PendingIndexStore};
use crate::state::registry::CollectionDefRegistry;
use crate::state::session::{
    DirtyValueBundle, DurableValueBundle, SessionParts, StateSession, TerminationWatch,
    UnavailableState, ValueStateSession,
};
use crate::state::value::{DurableWalStore, ValueKind};
use crate::state::{
    CollectionId, CollectionKindId, CollectionRef, CommitDecision, DirtyStoreProvider,
    DurableState, EventRef, StateBackend, StateBackendFactory, StateKey, StoreOutcome,
    TimerEventRef,
};
use crate::timers::duration::CompactDuration;
use crate::timers::store::{SegmentId, TriggerStore};
use crate::timers::{TimerManager, TimerType, Trigger};
use crate::{EventIdentity, Key, Partition, Topic};
use futures::StreamExt;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;

/// The per-event dirty store behind a backend factory's provider.
type DirtyStoreOf<B> =
    <<B as StateBackendFactory>::DirtyProvider as DirtyStoreProvider<ValueKind>>::Store;

/// Per-partition keyed-state manager minted by a
/// [`PartitionStateProvider`].
///
/// Mints one session per event and runs the `StateRecovery` sweep. The
/// payload type is the loader's; the [`EventIdentity`] bound the dedup-id
/// derivation needs discharges on this trait's impls, never on the
/// partition loop.
pub trait PartitionStateManager: Clone + Send + Sync + 'static {
    /// The consumer's message payload type.
    type Payload: Send + Sync + 'static;

    /// Session type minted per event.
    type Session: StateSession<Payload = Self::Payload>;

    /// Error raised by the recovery sweep.
    type RecoveryError: Error + Send + Sync + 'static;

    /// Mints the session for a message event, deriving its
    /// [`EventRef::Message`] dedup id with the canonical writer derivation.
    fn session_for_message(
        &self,
        message: &ConsumerMessage<Self::Payload>,
        termination: TerminationWatch,
    ) -> Self::Session;

    /// Mints the session for a timer event under its
    /// [`EventRef::Timer`] reference.
    fn session_for_timer(&self, trigger: &Trigger, termination: TerminationWatch) -> Self::Session;

    /// Runs the `StateRecovery` sweep for `key` and clears the
    /// `StateRecovery` timer once the pending partition is drained.
    ///
    /// # Errors
    ///
    /// Returns [`Self::RecoveryError`] when the sweep or the timer clear
    /// fails; the caller aborts the firing trigger so the sweep re-runs.
    fn recover<T>(
        &self,
        key: Key,
        timers: &TimerManager<T>,
    ) -> impl Future<Output = Result<(), Self::RecoveryError>> + Send
    where
        T: TriggerStore;

    /// Whether the partition loop must intercept `StateRecovery` triggers
    /// and route them to [`Self::recover`]. `false` only for [`NoState`].
    fn intercepts_recovery(&self) -> bool;
}

/// Process-wide factory for per-partition [`PartitionStateManager`]s,
/// the keyed-state analog of
/// [`TriggerStoreProvider`](crate::timers::store::TriggerStoreProvider).
pub trait PartitionStateProvider<P>: Clone + Send + Sync + 'static {
    /// Manager minted per partition assignment.
    type Manager: PartitionStateManager<Payload = P>;

    /// Error raised when a partition's manager cannot be acquired.
    type AcquireError: ClassifyError + Error + Send + Sync + 'static;

    /// Acquires the manager for `(topic, partition)`, eagerly validating
    /// descriptor identities against the segment's durable rows.
    ///
    /// # Errors
    ///
    /// Returns [`Self::AcquireError`] when the backend cannot be minted or
    /// identity validation fails; the partition loop retries until
    /// shutdown.
    fn acquire(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> impl Future<Output = Result<Self::Manager, Self::AcquireError>> + Send;
}

/// Stateless provider/manager for consumers without keyed state.
///
/// Sessions are [`UnavailableState`], recovery is a no-op, and the
/// partition loop does not intercept `StateRecovery` triggers (none can
/// exist — nothing schedules them).
pub struct NoState<P>(PhantomData<fn() -> P>);

impl<P> NoState<P> {
    /// Creates the stateless provider.
    #[must_use]
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<P> Default for NoState<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P> Clone for NoState<P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P> Copy for NoState<P> {}

impl<P> fmt::Debug for NoState<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NoState")
    }
}

impl<P> PartitionStateManager for NoState<P>
where
    P: Send + Sync + 'static,
{
    type Payload = P;
    type RecoveryError = Infallible;
    type Session = UnavailableState<P>;

    fn session_for_message(
        &self,
        _message: &ConsumerMessage<P>,
        _termination: TerminationWatch,
    ) -> Self::Session {
        UnavailableState::new()
    }

    fn session_for_timer(
        &self,
        _trigger: &Trigger,
        _termination: TerminationWatch,
    ) -> Self::Session {
        UnavailableState::new()
    }

    async fn recover<T>(&self, _key: Key, _timers: &TimerManager<T>) -> Result<(), Infallible>
    where
        T: TriggerStore,
    {
        Ok(())
    }

    fn intercepts_recovery(&self) -> bool {
        false
    }
}

impl<P> PartitionStateProvider<P> for NoState<P>
where
    P: Send + Sync + 'static,
{
    type AcquireError = Infallible;
    type Manager = Self;

    async fn acquire(&self, _topic: Topic, _partition: Partition) -> Result<Self, Infallible> {
        Ok(*self)
    }
}

struct StateManagerInner<D, O, DP, Sc, L> {
    durable: D,
    oracle: O,
    dirty: DP,
    scanner: Sc,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    segment_id: SegmentId,
    recovery_delay: CompactDuration,
    version: Arc<str>,
    consumer_group: Arc<str>,
}

/// The real per-partition state manager: owns the partition-lifetime
/// durable bundle (incl. the layered cache), oracle, dirty provider,
/// scanner, and loader; mints per-event [`ValueStateSession`]s.
pub struct StateManager<D, O, DP, Sc, L> {
    inner: Arc<StateManagerInner<D, O, DP, Sc, L>>,
}

impl<D, O, DP, Sc, L> Clone for StateManager<D, O, DP, Sc, L> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<D, O, DP, Sc, L> fmt::Debug for StateManager<D, O, DP, Sc, L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateManager")
            .field("segment_id", &self.inner.segment_id)
            .field("consumer_group", &self.inner.consumer_group)
            .finish_non_exhaustive()
    }
}

impl<D, O, DP, Sc, L> StateManager<D, O, DP, Sc, L>
where
    D: Clone,
    DP: DirtyStoreProvider<ValueKind>,
    L: Clone,
{
    fn session(
        &self,
        key: Key,
        event: EventRef,
        termination: TerminationWatch,
    ) -> ValueStateSession<D, DP, L> {
        ValueStateSession::new(SessionParts {
            durable: self.inner.durable.clone(),
            dirty: self.inner.dirty.clone(),
            loader: self.inner.loader.clone(),
            registry: self.inner.registry.clone(),
            state_key: StateKey::new(self.inner.segment_id, key),
            event,
            recovery_delay: self.inner.recovery_delay,
            termination,
        })
    }
}

impl<D, O, DP, Sc, L> PartitionStateManager for StateManager<D, O, DP, Sc, L>
where
    D: DurableValueBundle + PendingIndexStore<Error = <D as DurableWalStore<ValueKind>>::Error>,
    O: CommitOracle,
    DP: DirtyStoreProvider<ValueKind>,
    DP::Store: DirtyValueBundle + Send + Sync,
    Sc: PendingIndexScanner,
    L: MessageLoader + 'static,
    L::Payload: EventIdentity,
{
    type Payload = L::Payload;
    type RecoveryError =
        RecoveryError<<D as DurableWalStore<ValueKind>>::Error, Sc::Error, O::Error>;
    type Session = ValueStateSession<D, DP, L>;

    fn session_for_message(
        &self,
        message: &ConsumerMessage<L::Payload>,
        termination: TerminationWatch,
    ) -> Self::Session {
        // The dedup id is derived for every message — even when no
        // descriptors are registered — because the EventRef must exist
        // before we know whether the handler touches state. One hash per
        // message is a deliberate cost of the always-on layer.
        let topic = message.topic();
        let dedup_id = dedup_uuid_for_message(
            self.inner.version.as_ref(),
            self.inner.consumer_group.as_ref(),
            &topic,
            message.partition(),
            message,
        );
        self.session(
            message.key().clone(),
            EventRef::Message { dedup_id },
            termination,
        )
    }

    fn session_for_timer(&self, trigger: &Trigger, termination: TerminationWatch) -> Self::Session {
        let event = EventRef::Timer(TimerEventRef::new(
            trigger.timer_type,
            trigger.time,
            trigger.tag,
        ));
        self.session(trigger.key.clone(), event, termination)
    }

    async fn recover<T>(
        &self,
        key: Key,
        timers: &TimerManager<T>,
    ) -> Result<(), Self::RecoveryError>
    where
        T: TriggerStore,
    {
        let state_key = StateKey::new(self.inner.segment_id, key.clone());
        sweep_pending(
            &self.inner.durable,
            &self.inner.scanner,
            &self.inner.oracle,
            &self.inner.registry,
            state_key,
        )
        .await?;
        timers
            .unschedule_all(&key, TimerType::StateRecovery)
            .await
            .map_err(|e| RecoveryError::Timer(Box::new(e)))?;
        Ok(())
    }

    fn intercepts_recovery(&self) -> bool {
        true
    }
}

/// Process-wide [`PartitionStateProvider`] over a
/// [`StateBackendFactory`]: acquisition mints the partition's backend and
/// eagerly validates descriptor identities.
pub struct StateManagerProvider<B, Sc, L> {
    backend: B,
    scanner: Sc,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    consumer_group: Arc<str>,
    version: Arc<str>,
    recovery_delay: CompactDuration,
}

impl<B, Sc, L> StateManagerProvider<B, Sc, L> {
    /// Creates the provider.
    ///
    /// `version` **must** equal the deduplication middleware's hash
    /// version: recovery looks a message's committed state up by the dedup
    /// id the deduplication writer produced, and the version is one of the
    /// hashed inputs.
    #[must_use]
    pub fn new(
        backend: B,
        scanner: Sc,
        loader: L,
        registry: Arc<CollectionDefRegistry>,
        consumer_group: Arc<str>,
        version: Arc<str>,
        recovery_delay: CompactDuration,
    ) -> Self {
        Self {
            backend,
            scanner,
            loader,
            registry,
            consumer_group,
            version,
            recovery_delay,
        }
    }
}

impl<B, Sc, L> Clone for StateManagerProvider<B, Sc, L>
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
            registry: self.registry.clone(),
            consumer_group: self.consumer_group.clone(),
            version: self.version.clone(),
            recovery_delay: self.recovery_delay,
        }
    }
}

impl<B, Sc, L> fmt::Debug for StateManagerProvider<B, Sc, L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateManagerProvider")
            .field("consumer_group", &self.consumer_group)
            .field("recovery_delay", &self.recovery_delay)
            .finish_non_exhaustive()
    }
}

impl<B, Sc, L, P> PartitionStateProvider<P> for StateManagerProvider<B, Sc, L>
where
    B: StateBackendFactory,
    B::Durable: DurableValueBundle
        + PendingIndexStore<Error = <B::Durable as DurableWalStore<ValueKind>>::Error>
        + DescriptorIdentityStore<Error = <B::Durable as DurableWalStore<ValueKind>>::Error>,
    DirtyStoreOf<B>: DirtyValueBundle + Send + Sync,
    Sc: PendingIndexScanner,
    L: MessageLoader<Payload = P> + 'static,
    P: EventIdentity + Send + Sync + 'static,
{
    type AcquireError =
        StateAcquireError<B::Error, <B::Durable as DurableWalStore<ValueKind>>::Error>;
    type Manager = StateManager<B::Durable, B::Oracle, B::DirtyProvider, Sc, L>;

    async fn acquire(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<Self::Manager, Self::AcquireError> {
        let segment_id = compute_segment_id(topic, partition, &self.consumer_group);
        let StateBackend {
            durable,
            oracle,
            dirty,
        } = self
            .backend
            .for_partition(topic, partition)
            .map_err(StateAcquireError::Factory)?;
        // Invariant: no state op executes under an unvalidated identity —
        // the manager does not exist until the segment's durable identity
        // rows match the registered descriptors.
        acquire_descriptor_identities(&durable, &self.registry, segment_id)
            .await
            .map_err(StateAcquireError::Identity)?;
        Ok(StateManager {
            inner: Arc::new(StateManagerInner {
                durable,
                oracle,
                dirty,
                scanner: self.scanner.clone(),
                loader: self.loader.clone(),
                registry: self.registry.clone(),
                segment_id,
                recovery_delay: self.recovery_delay,
                version: self.version.clone(),
                consumer_group: self.consumer_group.clone(),
            }),
        })
    }
}

/// Sweeps the `(segment, key)` pending partition.
///
/// For each Value entry it reads the durable partition and either cleans a
/// stale pending row over an idle partition
/// ([`PendingIndexStore::delete_pending`]) or resolves a sealed WAL
/// against the oracle via [`resolve_sealed`]. Non-Value kinds are logged
/// at WARN and skipped; future kinds plug in by extending the dispatch
/// match. Clearing the `StateRecovery` timer is the caller's last step —
/// the sweep itself is timer-agnostic.
pub(crate) async fn sweep_pending<D, Sc, O>(
    durable: &D,
    scanner: &Sc,
    oracle: &O,
    registry: &CollectionDefRegistry,
    state_key: StateKey,
) -> Result<(), RecoveryError<<D as DurableWalStore<ValueKind>>::Error, Sc::Error, O::Error>>
where
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
    Ok(())
}

/// Resolves a single sealed WAL: consult `oracle` for `event`, then apply or
/// roll back the sealed ops on `durable`.
///
/// This is the shared inner decision of every recovery path —
/// [`RecoveringValueStore`](crate::state::recovering::RecoveringValueStore)'s
/// first-touch (`get`) and recover-before-overwrite (`seal`) recovery, and
/// the [`sweep_pending`] timer sweep. Each caller maps
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

/// Error raised when a [`StateManagerProvider`] cannot acquire a
/// partition's manager.
#[derive(Debug, Error)]
pub enum StateAcquireError<FactoryErr, StoreErr>
where
    FactoryErr: ClassifyError + Error + Send + Sync + 'static,
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
{
    /// The backend factory failed to mint the partition's backend.
    #[error("keyed-state backend factory failed at partition acquisition")]
    Factory(#[source] FactoryErr),

    /// Durable descriptor-identity validation failed. A mismatch is
    /// Permanent and recurs until the deployed descriptors match the
    /// segment's frozen identity; a store failure retries on the next
    /// acquisition attempt.
    #[error("keyed-state descriptor identity acquisition failed")]
    Identity(#[source] DescriptorIdentityError<StoreErr>),
}

impl<FactoryErr, StoreErr> ClassifyError for StateAcquireError<FactoryErr, StoreErr>
where
    FactoryErr: ClassifyError + Error + Send + Sync + 'static,
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Factory(e) => e.classify_error(),
            Self::Identity(e) => e.classify_error(),
        }
    }
}

/// Errors raised by the state-recovery sweep.
///
/// The sweep only ever fails while scanning the pending index, reading or
/// mutating durable state, consulting the oracle, or clearing the recovery
/// timer. It never runs a handler, drives a transaction, or computes a
/// fire time, so those variants are absent.
#[derive(Debug, Error)]
pub enum RecoveryError<DurableErr, ScannerErr, OracleErr>
where
    DurableErr: Error + 'static,
    ScannerErr: Error + 'static,
    OracleErr: Error + 'static,
{
    /// A durable Value store operation failed.
    #[error("keyed-state durable store failed")]
    Durable(#[source] DurableErr),

    /// A scanner pull failed.
    #[error("keyed-state pending scanner failed")]
    Scanner(#[source] ScannerErr),

    /// The commit oracle failed.
    #[error("keyed-state commit oracle failed")]
    Oracle(#[source] OracleErr),

    /// Clearing the recovery timer failed (type-erased timer error).
    #[error("keyed-state recovery timer failed: {0:#}")]
    Timer(BoxEventContextError),
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

#[cfg(test)]
mod tests;
