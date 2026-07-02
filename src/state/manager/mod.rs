//! Per-partition keyed-state manager: a peer of the timer manager.
//!
//! The partition loop acquires one [`StateManager`] per assignment through
//! a [`PartitionStateProvider`] (mirroring
//! [`TriggerStoreProvider`](crate::timers::store::TriggerStoreProvider)),
//! then mints one [`KeyedStateSession`] per event from it. The manager owns the
//! partition-lifetime pieces — the uniform cell store
//! ([`StateBackend::Cell`]), the commit oracle, the shared dirty workspace, and
//! the message loader — while each session gets `Arc`-clones and wraps the cell
//! store in its own per-event [`Overlay`](crate::state::overlay::Overlay). The
//! recovery sweep resolves provisional cells through the *same* cell store and
//! oracle.
//!
//! Acquisition is **eager**: descriptor identities are validated against the
//! group-global identity table before the manager exists, so no session can
//! operate under an unvalidated identity. The validation is hoisted to a
//! process-level latch (the identity table is group-global, so it runs once,
//! not per partition). The partition loop retries failed acquisitions until
//! shutdown, the same pattern as timer-manager initialization.
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
    DescriptorIdentityError, DescriptorIdentityStore, acquire_descriptor_identities,
};
use crate::state::dirty::DirtyStore;
use crate::state::oracle::CommitOracle;
use crate::state::registry::CollectionDefRegistry;
use crate::state::resolve::{ResolveCellError, sweep_provisional};
use crate::state::session::{
    ArmedKeys, CellSession, KeyedStateSession, SessionParts, TerminationWatch,
};
use crate::state::store::CellStore;
use crate::state::{
    CollectionId, CollectionRef, EventRef, STATE_FANOUT_CONCURRENCY, StateBackend,
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
use tokio::sync::OnceCell;
use tokio::task::coop::cooperative;

/// The uniform cell store's error of a [`StateBackend`] bundle (it already
/// wraps oracle failures, since `get`/`scan` resolve inside the store).
type CellErr<B> = <<B as StateBackend>::Cell as CellStore>::Error;

/// The shared commit oracle's error of a [`StateBackend`] bundle.
type OracleErr<B> = <<B as StateBackend>::Oracle as CommitOracle>::Error;

/// The shared descriptor-identity store's error of a [`StateBackend`] bundle.
type IdentityErr<B> = <<B as StateBackend>::Identity as DescriptorIdentityStore>::Error;

/// The owned, per-event lifetime of a keyed-state session.
///
/// `PartitionStateManager::session` mints exactly one of these per event. It
/// is the single owner of the event's state lifetime: non-`Clone` and
/// `#[must_use]`, so a mint site cannot drop it on the floor, and its `Drop`
/// clears the event's buffered dirty cells on **every** exit path — success,
/// error, abandon, or panic unwind. That makes "clear the buffer when the
/// event ends" structural rather than a scattered manual call a new code path
/// could forget.
///
/// The session itself stays a freely-cloned `Arc`-backed handle
/// ([`CellSession`]): the
/// [`EventContext`](crate::consumer::event_context::EventContext),
/// the descriptor handles held across `.await`, and the `'static` FFI erasure
/// all require `Clone + 'static`. So this scope owns one
/// [`handle`](Self::handle) the rest of the framework shares; the runtime
/// `invalidate` / per-op termination guards on that handle remain the
/// stand-in for the compile-time "no write after the event ends" guarantee a
/// full borrowed-handle refactor would give — they are load-bearing for the
/// `Clone + 'static` surface and stay.
///
/// # Residual (honest boundary)
///
/// `#[must_use]` + non-`Clone` + `Drop` guarantee the clear on every exit from
/// a held scope. They do **not** prevent a deliberate `let _ = session(...)`
/// or moving a `'static` handle into a task that outlives the scope; either
/// degrades to a one-event forward-leak of buffered cells (not corruption),
/// guarded by `#[must_use]`, the `let _guard = …` convention, and the session
/// lifecycle property test.
#[must_use]
pub struct EventStateScope<S>(S)
where
    S: CellSession;

impl<S> EventStateScope<S>
where
    S: CellSession,
{
    /// Wraps the minted session as the event's owned scope.
    pub fn new(session: S) -> Self {
        Self(session)
    }

    /// A cheap (`Arc`) clone of the session handle for the context and handlers
    /// to share. The scope keeps owning the lifetime; this hands out a view.
    pub fn handle(&self) -> S {
        self.0.clone()
    }
}

impl<S> Drop for EventStateScope<S>
where
    S: CellSession,
{
    fn drop(&mut self) {
        self.0.discard_dirty();
    }
}

/// Per-partition keyed-state manager minted by a
/// [`PartitionStateProvider`].
///
/// Mints one session per event from an already-resolved [`EventRef`] and
/// runs the `StateRecovery` sweep. The manager is Kafka-agnostic: building
/// the `EventRef` (including a message's dedup id) is the partition loop's
/// job.
pub trait PartitionStateManager: Clone + Send + Sync + 'static {
    /// Session type minted per event.
    type Session: CellSession;

    /// Error raised by the recovery sweep. Classified so the partition loop
    /// can drop a permanently-failing trigger (committing it) rather than
    /// refiring it forever — first-touch / the next sweep still recover the
    /// staged cell.
    type RecoveryError: ClassifyError + Error + Send + Sync + 'static;

    /// Mints the per-event session scope for `event` on `key`.
    ///
    /// Returns an [`EventStateScope`]: the single owned, non-`Clone` value
    /// whose lifetime models the event's. Build the context from its
    /// [`handle`](EventStateScope::handle) and keep the scope on the stack
    /// through dispatch; its `Drop` clears the event's dirty buffer on every
    /// exit path.
    fn session(
        &self,
        key: Key,
        event: EventRef,
        termination: TerminationWatch,
    ) -> EventStateScope<Self::Session>;

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
    /// descriptor identities against the group-global identity table.
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
    cell: B::Cell,
    /// Per-partition shared dirty workspace; each session's [`Overlay`] shares
    /// it and clears its own key's sub-range at settle.
    ///
    /// [`Overlay`]: crate::state::overlay::Overlay
    dirty: Arc<DirtyStore>,
    oracle: B::Oracle,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    segment_id: SegmentId,
    recovery_delay: CompactDuration,
    /// Keys mapped to the fire time of their standing `StateRecovery` backstop.
    /// Sessions read it to re-arm only when a newer commit's fire is sooner
    /// (the `recovery_within` tightening); `recover` removes the key when
    /// the sweep fires.
    armed: ArmedKeys,
}

/// The real per-partition state manager: owns the partition-lifetime cell
/// store, oracle, dirty workspace, and loader; mints per-event
/// [`KeyedStateSession`]s sharing them. Parameterized by the one
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
    type RecoveryError = RecoveryError<CellErr<B>, OracleErr<B>>;
    type Session = KeyedStateSession<B, L>;

    fn session(
        &self,
        key: Key,
        event: EventRef,
        termination: TerminationWatch,
    ) -> EventStateScope<Self::Session> {
        EventStateScope::new(KeyedStateSession::new(SessionParts {
            cell: self.inner.cell.clone(),
            dirty: self.inner.dirty.clone(),
            oracle: self.inner.oracle.clone(),
            loader: self.inner.loader.clone(),
            registry: self.inner.registry.clone(),
            state_key: StateKey::new(self.inner.segment_id, key),
            event,
            recovery_delay: self.inner.recovery_delay,
            armed: self.inner.armed.clone(),
            termination,
        }))
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
        // Sweep every registered collection on the key through the one uniform
        // cell store, ANDing the results: the single backstop unschedules only
        // when every collection resolved (the no-strand invariant). Names come
        // from the in-process registry (the authoritative declared set) — a
        // collection whose descriptor was removed is dormant, not swept.
        let collections: Vec<(StateType, StateName)> = self
            .inner
            .registry
            .collections()
            .map(|(state_type, name)| (state_type, name.clone()))
            .collect();
        let all_resolved = sweep_partition(
            &self.inner.cell,
            &self.inner.oracle,
            collections,
            &self.inner.registry,
            &state_key,
        )
        .await?;
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
/// [`StateBackendFactory`]: acquisition mints the partition's backend and
/// eagerly validates descriptor identities.
pub struct StateManagerProvider<F, L> {
    backend: F,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    consumer_group: Arc<str>,
    recovery_delay: CompactDuration,
    /// Process-level latch for descriptor-identity validation. The identity
    /// table is group-global, so validating the registry against it is a
    /// once-per-process concern, not per-partition. Shared across provider
    /// clones (an `Arc`), `get_or_try_init` runs the validation once on
    /// success and re-runs on `Err` — preserving retry-until-shutdown while
    /// the invariant *no state op runs under an unvalidated identity* holds.
    validated: Arc<OnceCell<()>>,
}

impl<F, L> StateManagerProvider<F, L> {
    /// Creates the provider.
    ///
    /// `consumer_group` derives the partition's segment id for **state-cell
    /// identity** via the crate-internal
    /// `segment::partition_segment_id` — the *same*
    /// derivation the defer stores use, so a partition's defer and state rows
    /// share one id for operational lookup. It is *also* the `group_id`
    /// partition key of the group-global descriptor-identity table, validated
    /// once per process at the first acquire. Timers currently derive their
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
            validated: Arc::new(OnceCell::new()),
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
            validated: self.validated.clone(),
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
        // the manager does not exist until the registered descriptors match
        // the group's frozen identity rows. The identity table is group-global,
        // so validation is a once-per-process latch (`get_or_try_init` coalesces
        // concurrent first-acquires and re-runs on a transient `Err`); any
        // partition's identity handle is equivalent. Identity lives on the
        // shared control-plane store, decoupled from any kind's data store.
        let identity = backend.identity();
        self.validated
            .get_or_try_init(|| {
                acquire_descriptor_identities(&identity, &self.registry, &self.consumer_group)
            })
            .await
            .map_err(StateAcquireError::Identity)?;
        Ok(StateManager {
            inner: Arc::new(StateManagerInner {
                cell: backend.cell(),
                dirty: Arc::new(DirtyStore::new()),
                oracle,
                loader: self.loader.clone(),
                registry: self.registry.clone(),
                segment_id,
                recovery_delay: self.recovery_delay,
                armed: Arc::default(),
            }),
        })
    }
}

/// Sweeps the given registered collections on `(segment, key)`, resolving any
/// provisional cell through the oracle.
///
/// `collections` is the kind's registered `(state_type, name)` set sourced from
/// the in-process registry (the authoritative declared set). A collection whose
/// descriptor was removed is **not** swept — its residue is dormant until the
/// declaration returns and the key is accessed (an accepted non-concern). TTLs
/// come from `registry`, falling back to the middleware-wide default.
///
/// Returns `true` iff every collection ended fully resolved — the caller
/// unschedules the backstop only then (the no-strand invariant). A
/// never-touched name streams no provisional cell and resolves trivially. A
/// per-cell Permanent failure is logged and skipped inside
/// [`sweep_provisional`](crate::state::resolve), leaving that collection
/// unresolved (`false`) for first-touch or a later sweep; a transient/terminal
/// failure propagates so the trigger aborts and the sweep refires.
///
/// # Errors
///
/// Returns [`RecoveryError`] on a transient/terminal backend or oracle
/// failure.
pub(crate) async fn sweep_partition<S, O>(
    cell: &S,
    oracle: &O,
    collections: impl IntoIterator<Item = (StateType, StateName)>,
    registry: &CollectionDefRegistry,
    state_key: &StateKey,
) -> Result<bool, ResolveCellError<S::Error, O::Error>>
where
    S: CellStore,
    O: CommitOracle,
{
    // Each name is its own Cassandra partition, so the per-collection sweeps
    // fan out concurrently. `try_fold` reduces to one `bool` and short-circuits
    // on a transient/terminal error (propagated via `?`); per-cell Permanent
    // failures are logged and skipped inside `sweep_provisional` (returning
    // `false`). `cooperative` wraps each sweep so the fan-out yields to the
    // runtime every ~128 collections rather than draining in one poll.
    let all_resolved = stream::iter(collections)
        .map(|(state_type, name)| {
            cooperative(async move {
                let ttl = registry.ttl_for(state_type, &name);
                let id = CollectionId::new(state_key.clone(), state_type, name);
                sweep_provisional(cell, oracle, &CollectionRef::new(id, ttl)).await
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

impl<StoreErr, OracleErr> From<ResolveCellError<StoreErr, OracleErr>>
    for RecoveryError<StoreErr, OracleErr>
where
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
    OracleErr: ClassifyError + Error + Send + Sync + 'static,
{
    fn from(error: ResolveCellError<StoreErr, OracleErr>) -> Self {
        match error {
            ResolveCellError::Store(e) => Self::Store(e),
            ResolveCellError::Oracle(e) => Self::Oracle(e),
        }
    }
}
