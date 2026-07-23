//! Per-partition keyed-state manager: a peer of the timer manager.
//!
//! The partition loop acquires one [`StateManager`] per assignment through
//! a [`PartitionStateProvider`] (mirroring
//! [`TriggerStoreProvider`](crate::timers::store::TriggerStoreProvider)),
//! then mints one [`KeyedStateSession`] per event from it. The manager owns the
//! partition-lifetime parts — the uniform cell store
//! (`StateBackend::Cell`), the commit oracle, the shared dirty workspace, and
//! the message loader — while each session gets `Arc`-clones and wraps the cell
//! store in its own per-event `Overlay`. The
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

use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::segment::partition_segment_id;
use crate::state::descriptor_identity::{
    DescriptorIdentityError, DescriptorIdentityStore, acquire_descriptor_identities,
};
use crate::state::dirty::DirtyStore;
use crate::state::first_write::{FirstWritePublisher, PublisherTemplate};
use crate::state::oracle::CommitOracle;
use crate::state::registry::CollectionDefRegistry;
use crate::state::resolve::{ResolveCellError, sweep_provisional};
use crate::state::session::{CellWrite, KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::CellStore;
use crate::state::{
    CollectionId, CollectionRef, EventRef, STATE_FANOUT_CONCURRENCY, StateBackend,
    StateBackendFactory, StateKey, StateName, StateType,
};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::TriggerStore;
use crate::timers::{TimerManager, TimerRequest, TimerType};
use crate::{Key, Partition, SegmentId, Topic};
use ahash::RandomState;
use futures::stream::{self, StreamExt, TryStreamExt};
use scc::HashMap as ConcurrentHashMap;
use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{OnceCell, watch};
use tokio::task::coop::cooperative;
use tokio::time::sleep;
use tracing::{Span, error};

/// Delay between retries of a transient failure while rescheduling a fresh
/// `StateRecovery` backstop after a failed sweep. Mirrors the durability-step
/// retry cadence in [`crate::consumer::middleware`].
const RESCHEDULE_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Floor on a rescheduled backstop's fire delay, so a zero (or sub-second)
/// `recovery_within` cannot turn a persistently failing sweep into a hot
/// refire loop. Matches [`RESCHEDULE_RETRY_DELAY`].
const RESCHEDULE_FLOOR: CompactDuration = CompactDuration::new(1);

/// The shared descriptor-identity store's error of a [`StateBackend`] bundle.
type IdentityErr<B> = <<B as StateBackend>::Identity as DescriptorIdentityStore>::Error;

/// Shared per-partition map from a key to the fire time of its standing
/// `StateRecovery` backstop.
///
/// A lock-free [`scc::HashMap`](ConcurrentHashMap) (not a `Mutex<HashMap>`):
/// the durability boundary touches it on every stateful commit, concurrently
/// across the partition's keys, so a single mutex would serialize unrelated
/// keys. The stored fire lets `arm_backstop` re-arm only when a newly-staged
/// commit's fire is *sooner* than the standing one (the tightening the
/// per-collection `recovery_within` bound needs); the map is still
/// self-draining ([`PartitionStateManager::recover`] removes the key when the
/// sweep fires), so it never grows without bound. Minted empty per acquisition:
/// an absent key means *unknown*, not *unarmed* — `arm_backstop` seeds it from
/// the durable trigger store on a key's first arm, so a prior epoch's standing
/// backstop is never loosened.
pub(crate) type ArmedKeys = Arc<ConcurrentHashMap<Key, CompactDateTime, RandomState>>;

/// The owned, per-event lifetime of a keyed-state session.
///
/// `PartitionStateManager::session` mints exactly one of these per event. It
/// is the single owner of the event's state lifetime: non-`Clone` and
/// `#[must_use]`, so a mint site cannot drop it on the floor, and its `Drop`
/// runs on **every** exit path — success, error, abandon, task cancellation,
/// or panic unwind — where it both flips the session terminated (the sealed
/// lifecycle's `terminate`) and clears the event's buffered dirty cells (its
/// `discard_dirty`). That makes "the session
/// outlives its event" unrepresentable: a handle leaked into a task that
/// outlives this scope finds `is_terminated() == true` and its ops error, and
/// "clear the buffer when the event ends" is structural rather than a
/// scattered manual call a new code path could forget. On the success path
/// `finalize` has already drained the buffer (the sealed lifecycle's
/// `discard_dirty` doc owns that invariant), so the Drop clear is a no-op
/// there: the scope is the failure-path backstop.
///
/// The session itself stays a freely-cloned `Arc`-backed handle
/// ([`CellWrite`]): the
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
/// Because `Drop` is sync it is **ungated**, so on the dropped-future path
/// (task cancellation, where no panic-unwind catch runs) it cannot revoke an
/// *already-admitted but parked* mutator: a detached `set` that acquired the
/// gate and parked before writing, when the dispatch future is dropped, lands
/// its write after this ungated `discard_dirty` — a one-event forward-leak of
/// buffered cells (not corruption). It is benign because future-drop is
/// partition teardown/rebalance: the shared dirty workspace dies with the
/// partition and no next same-key event reads it. The panic path takes the
/// gate-held catch in `partition::process_event`, which serializes after any
/// admitted permit and so *does* give the admitted-mutator no-residue
/// guarantee. The `terminate` flip still fences the detached task's
/// *subsequent* ops on both paths.
#[must_use]
pub struct EventStateScope<S>(S)
where
    S: CellWrite;

impl<S> EventStateScope<S>
where
    S: CellWrite,
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
    S: CellWrite,
{
    fn drop(&mut self) {
        // Flip termination first, then discard: a dispatch future dropped
        // mid-flight (task cancellation) runs no other teardown, so this sync
        // Drop is the sole fence for a handle the dropped future leaked into a
        // detached task — its subsequent ops then error `Terminated`. The
        // ungated-vs-panic-path residue boundary is documented once on the
        // type (`# Residual`).
        self.0.terminate();
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
///
/// Kept as a trait for type-parameter compression and pattern symmetry with
/// [`TriggerStoreProvider`](crate::timers::store::TriggerStoreProvider): the
/// associated `Session` type lets callers name only the manager, not its
/// backend and loader. It is deliberately not collapsed onto its single
/// production impl [`StateManager`].
pub trait PartitionStateManager: Clone + Send + Sync + 'static {
    /// Session type minted per event.
    type Session: CellWrite;

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

    /// Runs the `StateRecovery` sweep for `key` and decides what the fired
    /// trigger's commit guard should do.
    ///
    /// **Never aborts the trigger except on shutdown** (retry forever; abort
    /// only on shutdown). A fully resolved sweep, and a sweep that skips a
    /// per-cell *permanent* failure, both return [`SweepResolution::Commit`]:
    /// the fired trigger commits and nothing is rescheduled (a permanent cell
    /// never resolves, so rescheduling would only spin a refire loop —
    /// first-touch and the key's next commit recover it). A *transient* or
    /// *terminal* store failure reschedules a fresh backstop
    /// (`clear_and_schedule` at the `recovery_delay` floor tightened by the
    /// registered collections' `recovery_within` bounds, retried until it
    /// lands or shutdown) so a future sweep retries, then commits. Only when
    /// shutdown interrupts a reschedule before it lands does this return
    /// [`SweepResolution::Abort`], so the trigger refires and re-sweeps on the
    /// next partition acquisition.
    fn recover<T>(
        &self,
        key: Key,
        timers: &TimerManager<T>,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> impl Future<Output = SweepResolution> + Send
    where
        T: TriggerStore;
}

/// What the fired `StateRecovery` trigger's commit guard should do once
/// [`PartitionStateManager::recover`] returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SweepResolution {
    /// Commit the fired trigger — the sweep made progress (resolved, a
    /// permanent per-cell skip, or a fresh backstop rescheduled).
    Commit,

    /// Abort the fired trigger — shutdown interrupted a reschedule, so let the
    /// trigger refire (and re-sweep) on the next partition acquisition.
    Abort,
}

/// Process-wide factory for per-partition [`PartitionStateManager`]s,
/// the keyed-state analog of
/// [`TriggerStoreProvider`](crate::timers::store::TriggerStoreProvider).
///
/// `T` is the partition's trigger-store handle, threaded through to
/// `StateBackendFactory::for_partition` (which owns the handle-passing
/// rationale).
///
/// Like [`PartitionStateManager`], kept as a trait for type-parameter
/// compression and symmetry with `TriggerStoreProvider`, not collapsed onto its
/// single production impl [`StateManagerProvider`].
pub trait PartitionStateProvider<T>: Clone + Send + Sync + 'static {
    /// Manager minted per partition assignment.
    type Manager: PartitionStateManager;

    /// Error raised when a partition's manager cannot be acquired.
    type AcquireError: ClassifyError + Error + Send + Sync + 'static;

    /// Acquires the manager for `(topic, partition)`, eagerly validating
    /// descriptor identities against the group-global identity table.
    /// `triggers` is the partition's trigger-store handle, forwarded to the
    /// backend factory for the commit oracle's timer half.
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
        triggers: T,
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
    /// the sweep fires. Semantics of an absent key are owned by [`ArmedKeys`]:
    /// unknown, not unarmed.
    armed: ArmedKeys,
    /// The first-write publisher bound to this partition's topic, or `None`
    /// when nothing is published. Cloned into every session this manager mints.
    publisher: Option<FirstWritePublisher>,
}

/// The real per-partition state manager: owns the partition-lifetime cell
/// store, oracle, dirty workspace, and loader; mints per-event
/// [`KeyedStateSession`]s sharing them. Parameterized by the one
/// `StateBackend` bundle `B` and the loader `L`.
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

impl<B, L> PartitionStateManager for StateManager<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
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
            publisher: self.inner.publisher.clone(),
        }))
    }

    async fn recover<T>(
        &self,
        key: Key,
        timers: &TimerManager<T>,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> SweepResolution
    where
        T: TriggerStore,
    {
        let state_key = StateKey::new(self.inner.segment_id, key.clone());
        // Sweep↔debounce ordering (finding F2): clear the armed flag BEFORE
        // reading the provisional set, so the key's next stateful commit (or the
        // reschedule below) re-arms a fresh backstop. Per-key serialization — a
        // key's message and timer events run through one `KeyManager` queue —
        // means no `arm_backstop` / `mark_backstop_armed` on this key runs while
        // this sweep does, so this clear cannot race a re-arm. The boundary never
        // point-clears another event's backstop: only this fired sweep clears,
        // and only its own.
        self.inner.armed.remove_async(&key).await;
        match sweep_partition(
            &self.inner.cell,
            &self.inner.oracle,
            &self.inner.registry,
            &state_key,
        )
        .await
        {
            // Resolved, or a per-cell Permanent skip: commit and reschedule
            // nothing (see the trait doc for why).
            Ok(()) => SweepResolution::Commit,
            // Whole-sweep permanent failure: commit to stop refiring; first-touch
            // still recovers.
            Err(error) if error.classify_error() == ErrorCategory::Permanent => {
                error!(
                    key = ?key,
                    "keyed-state recovery sweep failed permanently: {error:#}; \
                     committing trigger (first-touch still recovers)"
                );
                SweepResolution::Commit
            }
            // Transient/terminal failure: reschedule a fresh backstop, then
            // commit (never abort — see the trait doc).
            Err(error) => {
                error!(
                    key = ?key,
                    "keyed-state recovery sweep failed: {error:#}; rescheduling a fresh backstop"
                );
                self.reschedule_backstop(&key, timers, shutdown).await
            }
        }
    }
}

impl<B, L> StateManager<B, L>
where
    B: StateBackend,
{
    /// Reschedules a fresh `StateRecovery` backstop after a failed sweep,
    /// retrying every non-shutdown failure until it lands.
    ///
    /// The fire delay is the `recovery_delay` floor tightened by the smallest
    /// `recovery_within` among the registered collections — the same
    /// tightening `finalize` folds onto the receipt's recovery delay, so a
    /// transient sweep failure does not stretch a tightly bounded
    /// collection's convergence out to the full floor. Which
    /// collections still hold provisional cells is unknown here (the sweep
    /// failed), so folding the whole registered set is the conservative
    /// direction: it can only fire sooner, and an early sweep that finds
    /// nothing resolves trivially. Floored at [`RESCHEDULE_FLOOR`].
    ///
    /// A rescheduled backstop is durable in the trigger store — it survives
    /// shutdown and fires on reacquisition — so once it lands this returns
    /// [`SweepResolution::Commit`] even if shutdown is now in progress. It
    /// returns [`SweepResolution::Abort`] only when shutdown interrupts
    /// *before* the backstop lands; then the fired trigger refires and
    /// re-sweeps on the next acquisition, so the cell is never orphaned.
    async fn reschedule_backstop<T>(
        &self,
        key: &Key,
        timers: &TimerManager<T>,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> SweepResolution
    where
        T: TriggerStore,
    {
        let delay = self
            .inner
            .registry
            .collections()
            .filter_map(|(state_type, name)| {
                self.inner.registry.recovery_within_for(state_type, name)
            })
            .fold(self.inner.recovery_delay, CompactDuration::min)
            .max(RESCHEDULE_FLOOR);
        loop {
            if *shutdown.borrow() >= ShutdownPhase::Cancelling {
                return SweepResolution::Abort;
            }
            let fire = match CompactDateTime::now().and_then(|now| now.add_duration(delay)) {
                Ok(fire) => fire,
                Err(error) => {
                    error!(error = %error, "failed to compute StateRecovery reschedule time; retrying");
                    sleep(RESCHEDULE_RETRY_DELAY).await;
                    continue;
                }
            };
            let request =
                TimerRequest::new(key.clone(), fire, TimerType::StateRecovery, Span::current());
            match timers.clear_and_schedule(request).await {
                Ok(()) => {
                    // The rescheduled backstop is now the standing one; record
                    // its fire so the arm-if-sooner path on the key's next commit
                    // sees it (mirrors `mark_backstop_armed`).
                    self.inner.armed.upsert_async(key.clone(), fire).await;
                    return SweepResolution::Commit;
                }
                Err(error) => {
                    error!(error = %error, "failed to reschedule StateRecovery backstop; retrying");
                    sleep(RESCHEDULE_RETRY_DELAY).await;
                }
            }
        }
    }
}

/// Process-wide [`PartitionStateProvider`] over a
/// `StateBackendFactory`: acquisition mints the partition's backend and
/// eagerly validates descriptor identities.
#[derive(Clone)]
pub struct StateManagerProvider<F, L> {
    backend: F,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    consumer_group: Arc<str>,
    recovery_delay: CompactDuration,
    /// The provider-wide first-write publication template, or `None` when no
    /// collection is published / no subsystem is configured. `acquire` binds
    /// it to the partition's topic; the shared memo dedups across every session
    /// this provider mints (see [`PublisherTemplate`]).
    publisher_template: Option<PublisherTemplate>,
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
    pub(crate) fn new(
        backend: F,
        loader: L,
        registry: Arc<CollectionDefRegistry>,
        consumer_group: Arc<str>,
        recovery_delay: CompactDuration,
        publisher_template: Option<PublisherTemplate>,
    ) -> Self {
        Self {
            backend,
            loader,
            registry,
            consumer_group,
            recovery_delay,
            publisher_template,
            validated: Arc::new(OnceCell::new()),
        }
    }
}

impl<F, L, T> PartitionStateProvider<T> for StateManagerProvider<F, L>
where
    F: StateBackendFactory<T>,
    L: Clone + Send + Sync + 'static,
    T: Send,
{
    type AcquireError = StateAcquireError<F::Error, IdentityErr<F::Backend>>;
    type Manager = StateManager<F::Backend, L>;

    async fn acquire(
        &self,
        topic: Topic,
        partition: Partition,
        triggers: T,
    ) -> Result<Self::Manager, Self::AcquireError> {
        let segment_id = partition_segment_id(topic, partition, &self.consumer_group);
        let backend = self
            .backend
            .for_partition(topic, partition, triggers)
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
                publisher: self.publisher_template.as_ref().map(|t| t.bind(topic)),
            }),
        })
    }
}

/// Sweeps every registered collection on `(segment, key)`, resolving any
/// provisional cell through the oracle. The swept set comes from `registry` —
/// the in-process authoritative declared set; a collection whose descriptor was
/// removed is dormant, not swept (an accepted non-concern). Each collection's
/// TTL comes from `registry`.
///
/// A never-touched name streams no provisional cell and resolves trivially. A
/// per-cell Permanent failure is logged and skipped inside
/// [`sweep_provisional`](crate::state::resolve), left for first-touch or a
/// later sweep; a transient/terminal failure propagates via `Err` for
/// [`PartitionStateManager::recover`] to reschedule against.
async fn sweep_partition<S, O>(
    cell: &S,
    oracle: &O,
    registry: &CollectionDefRegistry,
    state_key: &StateKey,
) -> Result<(), ResolveCellError<S::Error, O::Error>>
where
    S: CellStore,
    O: CommitOracle,
{
    // Own the `(state_type, name)` set up front: streaming the borrowed
    // `registry.collections()` items straight into the concurrent fan-out
    // trips a higher-ranked-lifetime bound on the per-item closure, and the
    // owned set is small (the registered collection count).
    let collections: Vec<(StateType, StateName)> = registry
        .collections()
        .map(|(state_type, name)| (state_type, name.clone()))
        .collect();
    // Each name is its own Cassandra partition, so the per-collection sweeps
    // fan out concurrently. `try_for_each` short-circuits on a
    // transient/terminal error (propagated via `?`); per-cell Permanent
    // failures are logged and skipped inside `sweep_provisional`. `cooperative`
    // wraps each sweep so the fan-out yields to the runtime every ~128
    // collections rather than draining in one poll.
    stream::iter(collections)
        .map(|(state_type, name)| {
            cooperative(async move {
                let ttl = registry.ttl_for(state_type, &name);
                let id = CollectionId::new(state_key.clone(), state_type, name);
                sweep_provisional(cell, oracle, &CollectionRef::new(id, ttl)).await
            })
        })
        .buffer_unordered(STATE_FANOUT_CONCURRENCY)
        .try_for_each(|_resolved| async move { Ok(()) })
        .await
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
