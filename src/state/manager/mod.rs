//! Per-partition keyed-state manager: a peer of the timer manager.
//!
//! The partition loop acquires one [`StateManager`] per assignment through
//! a [`PartitionStateProvider`] (mirroring
//! [`TriggerStoreProvider`](crate::timers::store::TriggerStoreProvider)),
//! then mints one [`StateSession`] per event from it. The manager owns the
//! partition-lifetime pieces — the [`PartitionStateStore`] (cell store +
//! oracle + committed-value cache + status map) and the message loader —
//! while each session gets `Arc`-clones plus its own in-memory per-event
//! dirty store. The recovery sweep shares the *same*
//! [`PartitionStateStore`], so the status marks sessions set are visible to
//! it.
//!
//! Acquisition is **eager**: descriptor identities are validated against
//! the segment's durable rows before the manager exists, so no session can
//! operate under an unvalidated identity. The partition loop retries
//! failed acquisitions until shutdown, the same pattern as timer-manager
//! initialization.
//!
//! State is **always wired** — there is no no-state mode. The manager is
//! Kafka-agnostic: it mints a session for an already-resolved [`EventRef`],
//! never from a transport message. The partition loop builds the
//! [`EventRef`] (deriving a message's dedup id with the deduplication
//! writer's canonical derivation) and hands it in.

#[cfg(test)]
mod tests;

use crate::consumer::event_context::BoxEventContextError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::segment::partition_segment_id;
use crate::state::descriptor_identity::{
    DescriptorIdentityError, DescriptorIdentityStore, DurableNames, acquire_descriptor_identities,
};
use crate::state::oracle::CommitOracle;
use crate::state::partition_store::{CommittedCache, PartitionStateStore, PartitionStoreError};
use crate::state::registry::CollectionDefRegistry;
use crate::state::session::{
    ArmedKeys, KeyedStateSession, SessionParts, StateSession, TerminationWatch,
};
use crate::state::store::CellStore;
use crate::state::value::ValueKind;
use crate::state::{
    CollectionId, CollectionKind, CollectionRef, EventRef, STATE_FANOUT_CONCURRENCY, StateBackend,
    StateBackendFactory, StateKey, StateName, StateType,
};
use crate::timers::duration::CompactDuration;
use crate::timers::store::TriggerStore;
use crate::timers::{TimerManager, TimerType};
use crate::{Key, Partition, SegmentId, Topic};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tokio::task::coop::cooperative;

/// The Value lane's durable cell-store error of a [`StateBackend`] bundle.
type ValueCellErr<B> = <<B as StateBackend>::ValueCell as CellStore<ValueKind>>::Error;

/// The shared commit oracle's error of a [`StateBackend`] bundle.
type OracleErr<B> = <<B as StateBackend>::Oracle as CommitOracle>::Error;

/// The shared descriptor-identity store's error of a [`StateBackend`] bundle.
type IdentityErr<B> = <<B as StateBackend>::Identity as DescriptorIdentityStore>::Error;

/// Per-partition keyed-state manager minted by a
/// [`PartitionStateProvider`].
///
/// Mints one session per event from an already-resolved [`EventRef`] and
/// runs the `StateRecovery` sweep. The manager is Kafka-agnostic: building
/// the `EventRef` (including a message's dedup id) is the partition loop's
/// job.
pub trait PartitionStateManager: Clone + Send + Sync + 'static {
    /// Session type minted per event.
    type Session: StateSession;

    /// Error raised by the recovery sweep. Classified so the partition loop
    /// can drop a permanently-failing trigger (committing it) rather than
    /// refiring it forever — first-touch / the next sweep still recover the
    /// staged cell.
    type RecoveryError: ClassifyError + Error + Send + Sync + 'static;

    /// Mints the session for `event` on `key`.
    fn session(&self, key: Key, event: EventRef, termination: TerminationWatch) -> Self::Session;

    /// Runs the `StateRecovery` sweep for `key` and clears the
    /// `StateRecovery` timer once every provisional cell on the key resolves.
    ///
    /// # Errors
    ///
    /// Returns [`Self::RecoveryError`] when the sweep or the timer clear
    /// fails. The caller classifies: Transient/Terminal aborts the firing
    /// trigger so the sweep re-runs; Permanent commits it to stop the
    /// refire loop (first-touch and the key's next commit still recover).
    fn recover<T>(
        &self,
        key: Key,
        timers: &TimerManager<T>,
    ) -> impl Future<Output = Result<(), Self::RecoveryError>> + Send
    where
        T: TriggerStore;
}

/// Process-wide factory for per-partition [`PartitionStateManager`]s,
/// the keyed-state analog of
/// [`TriggerStoreProvider`](crate::timers::store::TriggerStoreProvider).
pub trait PartitionStateProvider: Clone + Send + Sync + 'static {
    /// Manager minted per partition assignment.
    type Manager: PartitionStateManager;

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

struct StateManagerInner<B, L>
where
    B: StateBackend,
{
    store: PartitionStateStore<ValueKind, B::ValueCell, B::Oracle, B::ValueCache>,
    oracle: B::Oracle,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    /// Every collection name durable on the segment at acquisition time,
    /// partitioned by kind — the union of the registered descriptors and any
    /// stored identity row whose descriptor was since removed. The recovery
    /// sweep enumerates each kind's bucket ([`DurableNames::names`]) against
    /// *that kind's* lane so deregistered crash residue is still swept
    /// (invariant 5), rather than the live registry alone. As kinds land, each
    /// gets its bucket swept against its own lane and combined with logical-and
    /// for the no-strand invariant — no change to this field.
    names: DurableNames,
    segment_id: SegmentId,
    recovery_delay: CompactDuration,
    /// Keys with a standing `StateRecovery` backstop. Sessions read/set it to
    /// amortize re-arming; `recover` removes the key when the sweep fires.
    armed: ArmedKeys,
}

/// The real per-partition state manager: owns the partition-lifetime
/// [`PartitionStateStore`], oracle, and loader; mints per-event
/// [`KeyedStateSession`]s sharing that store. Parameterized by the one
/// [`StateBackend`] bundle `B` and the loader `L`.
pub struct StateManager<B, L>
where
    B: StateBackend,
{
    inner: Arc<StateManagerInner<B, L>>,
}

impl<B, L> Clone for StateManager<B, L>
where
    B: StateBackend,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B, L> fmt::Debug for StateManager<B, L>
where
    B: StateBackend,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateManager")
            .field("segment_id", &self.inner.segment_id)
            .finish_non_exhaustive()
    }
}

impl<B, L> PartitionStateManager for StateManager<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    type RecoveryError = RecoveryError<ValueCellErr<B>, OracleErr<B>>;
    type Session = KeyedStateSession<B, L>;

    fn session(&self, key: Key, event: EventRef, termination: TerminationWatch) -> Self::Session {
        KeyedStateSession::new(SessionParts {
            store: self.inner.store.clone(),
            oracle: self.inner.oracle.clone(),
            loader: self.inner.loader.clone(),
            registry: self.inner.registry.clone(),
            state_key: StateKey::new(self.inner.segment_id, key),
            event,
            recovery_delay: self.inner.recovery_delay,
            armed: self.inner.armed.clone(),
            termination,
        })
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
        // The backstop fired, so it no longer stands: clear the armed flag so
        // the key's next stateful commit re-arms. Per-key serialization means
        // no commit on this key runs while the sweep does, so this cannot race
        // a `mark_backstop_armed`. If the sweep then aborts and the trigger
        // refires, the next commit's re-arm (`clear_and_schedule`, a singleton
        // overwrite) simply replaces the refiring timer — still one backstop.
        self.inner.armed.remove_async(&key).await;
        // Sweep each kind's name bucket against its own lane and AND the
        // results: the single backstop unschedules only when every kind on the
        // key resolved (the no-strand invariant). Value is the only production
        // kind today, so this is a single-operand AND; a future kind adds
        // `&& sweep_partition(map_store, names.names(MapKind::ID), …)` — keep the
        // `all_resolved` binding as the AND accumulator so that growth is local.
        let value_resolved = sweep_partition(
            &self.inner.store,
            self.inner.names.names(ValueKind::ID),
            &self.inner.registry,
            &state_key,
        )
        .await?;
        let all_resolved = value_resolved;
        // Only clear the backstop when every cell ended resolved (the
        // no-strand invariant): a skipped Permanent cell leaves the timer
        // armed for first-touch / a later sweep.
        if all_resolved {
            timers
                .unschedule_all(&key, TimerType::StateRecovery)
                .await
                .map_err(|e| RecoveryError::Timer(Box::new(e)))?;
        }
        Ok(())
    }
}

/// Process-wide [`PartitionStateProvider`] over a
/// [`StateBackendFactory`]: acquisition mints the partition's backend,
/// eagerly validates descriptor identities, and composes the
/// [`PartitionStateStore`].
pub struct StateManagerProvider<F, L> {
    backend: F,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    consumer_group: Arc<str>,
    recovery_delay: CompactDuration,
}

impl<F, L> StateManagerProvider<F, L> {
    /// Creates the provider.
    ///
    /// `consumer_group` derives the partition's segment id for **state-cell
    /// identity** via the crate-internal
    /// `segment::partition_segment_id` — the *same*
    /// derivation the defer stores use, so a partition's defer and state rows
    /// share one id for operational lookup. Timers currently derive their
    /// segment id with a separate legacy formula
    /// ([`Segment::for_partition`](crate::timers::store::Segment::for_partition),
    /// `NAMESPACE_URL`) pending a follow-up migration onto this id.
    /// The tables remain independent: the commit oracle resolves a timer
    /// `EventRef` by `(key, timer_type, time)` against the per-partition
    /// trigger store, never by the state segment id — don't join them in code.
    #[must_use]
    pub fn new(
        backend: F,
        loader: L,
        registry: Arc<CollectionDefRegistry>,
        consumer_group: Arc<str>,
        recovery_delay: CompactDuration,
    ) -> Self {
        Self {
            backend,
            loader,
            registry,
            consumer_group,
            recovery_delay,
        }
    }
}

impl<F, L> Clone for StateManagerProvider<F, L>
where
    F: Clone,
    L: Clone,
{
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            loader: self.loader.clone(),
            registry: self.registry.clone(),
            consumer_group: self.consumer_group.clone(),
            recovery_delay: self.recovery_delay,
        }
    }
}

impl<F, L> fmt::Debug for StateManagerProvider<F, L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateManagerProvider")
            .field("consumer_group", &self.consumer_group)
            .field("recovery_delay", &self.recovery_delay)
            .finish_non_exhaustive()
    }
}

impl<F, L> PartitionStateProvider for StateManagerProvider<F, L>
where
    F: StateBackendFactory,
    L: Clone + Send + Sync + 'static,
{
    type AcquireError = StateAcquireError<F::Error, IdentityErr<F::Backend>>;
    type Manager = StateManager<F::Backend, L>;

    async fn acquire(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<Self::Manager, Self::AcquireError> {
        let segment_id = partition_segment_id(topic, partition, &self.consumer_group);
        let backend = self
            .backend
            .for_partition(topic, partition)
            .map_err(StateAcquireError::Factory)?;
        let oracle = backend.oracle();
        // Invariant: no state op executes under an unvalidated identity —
        // the manager does not exist until the segment's durable identity
        // rows match the registered descriptors. Identity lives on the shared
        // control-plane store, decoupled from any kind's data store.
        let identity = backend.identity();
        let names = acquire_descriptor_identities(&identity, &self.registry, segment_id)
            .await
            .map_err(StateAcquireError::Identity)?;
        let store = PartitionStateStore::new(
            backend.value_cell(),
            oracle.clone(),
            backend.value_cache(),
            self.registry.clone(),
        );
        Ok(StateManager {
            inner: Arc::new(StateManagerInner {
                store,
                oracle,
                loader: self.loader.clone(),
                registry: self.registry.clone(),
                names,
                segment_id,
                recovery_delay: self.recovery_delay,
                armed: Arc::default(),
            }),
        })
    }
}

/// Sweeps every durable collection on `(segment, key)`, resolving any
/// provisional cell through the oracle.
///
/// `names` is the segment's durable name set retained from acquisition (the
/// union of registered descriptors and stored identity rows), so a
/// deregistered collection's crash residue is still swept rather than
/// stranded until its TTL (invariant 5, sweep-covers-everything). TTLs come
/// from `registry`, which falls back to the middleware-wide default for a
/// name no longer registered.
///
/// Returns `true` iff every collection ended fully resolved — the caller
/// unschedules the backstop only then (the no-strand invariant). The status
/// map inside the store lets a collection the eager promote already resolved
/// skip its read; a never-touched name streams no provisional cell and
/// resolves trivially. A per-cell Permanent failure is logged and skipped
/// inside [`PartitionStateStore::sweep_collection`], leaving that collection
/// unresolved (`false`) for first-touch or a later sweep; a transient/terminal
/// failure propagates so the trigger aborts and the sweep refires.
///
/// # Errors
///
/// Returns [`RecoveryError`] on a transient/terminal backend or oracle
/// failure.
pub(crate) async fn sweep_partition<K, S, O, C>(
    store: &PartitionStateStore<K, S, O, C>,
    names: &[StateName],
    registry: &CollectionDefRegistry,
    state_key: &StateKey,
) -> Result<bool, RecoveryError<S::Error, O::Error>>
where
    K: CollectionKind,
    S: CellStore<K>,
    O: CommitOracle,
    C: CommittedCache<K>,
{
    // Each name is its own Cassandra partition, so the per-collection sweeps
    // fan out concurrently. `try_fold` reduces to one `bool` and short-circuits
    // on a transient/terminal error (propagated via `?`), exactly as the
    // sequential `?` did; per-cell Permanent failures are logged and skipped
    // inside `sweep_collection` (returning `false`). `cooperative` wraps each
    // sweep so the fan-out yields to the runtime every ~128 collections rather
    // than draining the whole batch in one poll.
    let all_resolved = stream::iter(names.iter().cloned())
        .map(|name| {
            cooperative(async move {
                let ttl = registry.ttl_for(&name);
                let id = CollectionId::<K>::new(state_key.clone(), StateType::Application, name);
                store.sweep_collection(&CollectionRef::new(id, ttl)).await
            })
        })
        .buffer_unordered(STATE_FANOUT_CONCURRENCY)
        .try_fold(true, |all, resolved| async move { Ok(all && resolved) })
        .await?;
    Ok(all_resolved)
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
/// The sweep only ever fails while resolving provisional cells (the durable
/// cell store or the commit oracle) or clearing the recovery timer. It never
/// runs a handler, drives a transaction, or computes a fire time, so those
/// variants are absent.
#[derive(Debug, Error)]
pub enum RecoveryError<StoreErr, OracleErr>
where
    StoreErr: Error + 'static,
    OracleErr: Error + 'static,
{
    /// A durable cell store operation failed.
    #[error("keyed-state cell store failed")]
    Store(#[source] StoreErr),

    /// The commit oracle failed.
    #[error("keyed-state commit oracle failed")]
    Oracle(#[source] OracleErr),

    /// Clearing the recovery timer failed (type-erased timer error).
    #[error("keyed-state recovery timer failed: {0:#}")]
    Timer(BoxEventContextError),
}

/// Each variant delegates to its inner error's classification, so the
/// partition loop can decide whether a failed sweep is worth refiring
/// (transient) or should be dropped and left to first-touch / the next sweep
/// (permanent). The boxed `Timer` error classifies through its
/// [`EventContextError`](crate::consumer::event_context::EventContextError)
/// supertrait.
impl<StoreErr, OracleErr> ClassifyError for RecoveryError<StoreErr, OracleErr>
where
    StoreErr: ClassifyError + Error + 'static,
    OracleErr: ClassifyError + Error + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Store(e) => e.classify_error(),
            Self::Oracle(e) => e.classify_error(),
            Self::Timer(e) => e.classify_error(),
        }
    }
}

impl<StoreErr, OracleErr> From<PartitionStoreError<StoreErr, OracleErr>>
    for RecoveryError<StoreErr, OracleErr>
where
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
    OracleErr: ClassifyError + Error + Send + Sync + 'static,
{
    fn from(error: PartitionStoreError<StoreErr, OracleErr>) -> Self {
        match error {
            PartitionStoreError::Store(e) => Self::Store(e),
            PartitionStoreError::Oracle(e) => Self::Oracle(e),
        }
    }
}
