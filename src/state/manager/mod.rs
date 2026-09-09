//! The partition's keyed-state manager.
//!
//! Acquisition validates descriptor identities and publishes assignment routing
//! before it returns a manager. Admission resolves durable residue and retires
//! committed sources before the key can dispatch. Each dispatched event
//! receives a session over the shared cell store, dedup store, registry,
//! loader, and dirty workspace.
//!
//! Kafka partition ownership provides exclusive access. The partition loop
//! derives each `EventRef` and supplies it to the manager.

#[cfg(test)]
mod tests;

use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::segment::partition_segment_id;
use crate::state::CommitDecision;
use crate::state::backend::AdmissionChecks;
use crate::state::descriptor_identity::{
    DescriptorIdentityError, DescriptorIdentityStore, acquire_descriptor_identities,
};
use crate::state::dirty::DirtyStore;
use crate::state::marker::{CommittedMarker, EventMarker, MarkerState, MarkerVersion};
use crate::state::publisher::{AssignmentPublisher, NoPublisher};
use crate::state::registry::CollectionDefRegistry;
use crate::state::resolve::resolve_event_marker;
use crate::state::session::{EventSession, KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::CellStore;
use crate::state::{
    CollectionId, CollectionRef, EventRef, StateBackend, StateBackendFactory, StateKey, StateName,
    StateType,
};
#[cfg(test)]
use crate::state::{PartitionBackend, memory::MemoryDescriptorIdentityStore};
use crate::timers::TimerManager;
use crate::timers::duration::CompactDuration;
use crate::timers::store::TriggerStore;
use crate::{Key, Partition, SegmentId, Topic};
use futures::stream::{self, StreamExt, TryStreamExt};
use smallvec::SmallVec;
use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{OnceCell, watch};
use tokio::task::coop::cooperative;
use tokio::time::sleep;
use tracing::error;

/// The identity store error of a backend.
type IdentityErr<B> = <<B as StateBackend>::Identity as DescriptorIdentityStore>::Error;

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
/// ([`EventSession`]): the
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
    S: EventSession;

impl<S> EventStateScope<S>
where
    S: EventSession,
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
    S: EventSession,
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
/// admits each key before dispatch. The manager is Kafka-agnostic: building
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
    type Session: EventSession;

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

    /// Resolves residue and retires committed sources before dispatch.
    /// Committed residue promotes in every discovered collection, registered or
    /// not. Uncommitted registered residue aborts with the registry TTL.
    /// Unregistered version 2 residue remains untouched; version 1 residue
    /// loses its Staged row. Fresh guarantees that no registered collection
    /// has unresolved residue.
    fn admit<T>(
        &self,
        key: Key,
        timers: &TimerManager<T>,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> impl Future<Output = Admission> + Send
    where
        T: TriggerStore;
}

/// Admission decides whether an event can dispatch.
/// Poisoned records message dedup evidence best-effort, commits the source, and
/// logs the key at error level. It leaves the key unchecked and invokes neither
/// a handler nor an apply hook.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    /// The key can dispatch an event.
    Fresh,
    /// A permanent store rejection prevents dispatch.
    Poisoned,
    /// Shutdown stopped admission.
    Abandoned,
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
    /// backend factory for partition acquisition.
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
    dedup: B::Dedup,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    segment_id: SegmentId,
    dedup_ttl: CompactDuration,
    checks: B::Checks,
}

/// The real per-partition state manager: owns the partition-lifetime
/// cell store, dedup store, dirty workspace, and loader; mints per-event
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
            dedup: self.inner.dedup.clone(),
            loader: self.inner.loader.clone(),
            registry: self.inner.registry.clone(),
            state_key: StateKey::new(self.inner.segment_id, key),
            event,
            dedup_ttl: self.inner.dedup_ttl,
            checks: self.inner.checks.clone(),
            termination,
        }))
    }

    async fn admit<T>(
        &self,
        key: Key,
        timers: &TimerManager<T>,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> Admission
    where
        T: TriggerStore,
    {
        match admission_step(shutdown, || self.inner.checks.contains(&key)).await {
            Ok(true) => return Admission::Fresh,
            Ok(false) => {}
            Err(admission) => return admission,
        }
        if let Err(admission) = self.admit_unchecked(&key, timers, shutdown).await {
            return admission;
        }
        match admission_step(shutdown, || self.inner.checks.mark(&key)).await {
            Ok(()) => Admission::Fresh,
            Err(admission) => admission,
        }
    }
}

impl<B, L> StateManager<B, L>
where
    B: StateBackend,
{
    async fn admit_unchecked<T: TriggerStore>(
        &self,
        key: &Key,
        timers: &TimerManager<T>,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> Result<(), Admission> {
        let registry = &self.inner.registry;
        let state_key = StateKey::new(self.inner.segment_id, key.clone());
        let mut pending: SmallVec<[(StateType, StateName); 8]> = registry
            .collections()
            .map(|(kind, name)| (kind, name.clone()))
            .collect();
        let mut states: SmallVec<[(CollectionRef, MarkerState); 8]> =
            SmallVec::with_capacity(pending.len());

        while !pending.is_empty() {
            let count = pending.len();
            let loaded = stream::iter(pending.drain(..))
                .map(|(kind, name)| {
                    let ttl = registry.ttl_for(kind, &name);
                    let id = CollectionId::new(state_key.clone(), kind, name);
                    cooperative(async move {
                        let state =
                            admission_step(shutdown, || self.inner.cell.marker_state(&id)).await?;
                        let collection = CollectionRef::new(id, ttl);
                        Ok::<_, Admission>((collection, state))
                    })
                })
                .buffer_unordered(count)
                .try_collect::<SmallVec<[_; 8]>>()
                .await?;
            states.extend(loaded);

            for (_, state) in &states {
                let touched = state.staged.iter().flat_map(EventMarker::touched).chain(
                    state
                        .committed
                        .iter()
                        .flat_map(|marker| marker.touched.iter()),
                );
                for (kind, name) in touched {
                    if !states.iter().any(|(collection, _)| {
                        collection.id().state_type() == *kind && collection.id().name() == name
                    }) && !pending.contains(&(*kind, name.clone()))
                    {
                        pending.push((*kind, name.clone()));
                    }
                }
            }
        }

        let mut committed: SmallVec<[CommittedMarker; 8]> = SmallVec::with_capacity(states.len());
        for (_, state) in &states {
            if let Some(marker) = &state.committed {
                committed.push(marker.clone());
            }
        }

        for (collection, state) in &states {
            let Some(marker) = &state.staged else {
                continue;
            };

            let is_committed = if marker.version() == MarkerVersion::V1 {
                self.legacy_committed(key, marker.event(), timers, shutdown)
                    .await?
            } else {
                committed.iter().any(|entry| entry.certifies(marker))
            };
            if is_committed && !committed.iter().any(|entry| entry.event == marker.event()) {
                committed.push(CommittedMarker::from(marker));
            }

            if !is_committed
                && !registry.collections().any(|(kind, name)| {
                    kind == collection.id().state_type() && name == collection.id().name()
                })
            {
                if marker.version() == MarkerVersion::V1 {
                    admission_step(shutdown, || {
                        self.inner.cell.abort_provisional(collection, &[])
                    })
                    .await?;
                }
                continue;
            }

            let decision = if is_committed {
                CommitDecision::Committed
            } else {
                CommitDecision::NotCommitted
            };
            admission_step(shutdown, || {
                resolve_event_marker(&self.inner.cell, collection, marker, decision)
            })
            .await?;
        }

        for marker in committed {
            if let Some(dedup) = marker.dedup
                && !admission_step(shutdown, || self.inner.dedup.exists(dedup)).await?
            {
                admission_step(shutdown, || self.inner.dedup.insert(dedup)).await?;
            }
            if let EventRef::Timer(timer) = marker.event {
                admission_step(shutdown, || timers.retire_committed(key, timer)).await?;
            }
        }

        Ok(())
    }

    /// Reads the old commit point for residue staged before the V4 layout.
    /// This rule remains necessary while an idle key can retain a version 1
    /// payload without expiry.
    async fn legacy_committed<T: TriggerStore>(
        &self,
        key: &Key,
        event: EventRef,
        timers: &TimerManager<T>,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> Result<bool, Admission> {
        match event {
            EventRef::Message { dedup_id } => {
                admission_step(shutdown, || self.inner.dedup.exists(dedup_id)).await
            }
            EventRef::Timer(timer) => {
                let tag = admission_step(shutdown, || {
                    timers.current_timer_tag(key, timer.time, timer.timer_type)
                })
                .await?;
                Ok(tag != Some(timer.tag))
            }
        }
    }
}

/// Retries store failures until success, permanent rejection, or shutdown.
async fn admission_step<R, E, F, Fut>(
    shutdown: &watch::Receiver<ShutdownPhase>,
    mut step: F,
) -> Result<R, Admission>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, E>>,
    E: ClassifyError + Error,
{
    loop {
        if *shutdown.borrow() >= ShutdownPhase::Cancelling {
            return Err(Admission::Abandoned);
        }
        match step().await {
            Ok(value) => return Ok(value),
            Err(error) if error.classify_error() == ErrorCategory::Permanent => {
                error!(%error, "keyed-state admission failed permanently");
                return Err(Admission::Poisoned);
            }
            Err(error) => {
                error!(%error, "keyed-state admission failed; retry");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Process-wide [`PartitionStateProvider`] over a
/// `StateBackendFactory`: acquisition publishes routing and validates
/// descriptor identities before it mints the manager.
#[derive(Clone)]
pub struct StateManagerProvider<F, L, P = NoPublisher> {
    backend: F,
    loader: L,
    publisher: P,
    registry: Arc<CollectionDefRegistry>,
    consumer_group: Arc<str>,
    dedup_ttl: CompactDuration,
    /// Process-level latch for descriptor-identity validation. The identity
    /// table is group-global, so validating the registry against it is a
    /// once-per-process concern, not per-partition. Shared across provider
    /// clones (an `Arc`), `get_or_try_init` runs the validation once on
    /// success and re-runs on `Err` — preserving retry-until-shutdown while
    /// the invariant *no state op runs under an unvalidated identity* holds.
    validated: Arc<OnceCell<()>>,
}

impl<F, L, P> StateManagerProvider<F, L, P> {
    /// Creates the provider.
    ///
    /// `publisher` uses the assignment that this provider receives. It does
    /// not observe Kafka or track assignments itself.
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
    /// Timer retirement and legacy admission use the partition's trigger store.
    /// They address timers by key, type, and time, never by the state segment
    /// id.
    #[must_use]
    pub(crate) fn new(
        backend: F,
        loader: L,
        publisher: P,
        registry: Arc<CollectionDefRegistry>,
        consumer_group: Arc<str>,
        dedup_ttl: CompactDuration,
    ) -> Self {
        Self {
            backend,
            loader,
            publisher,
            registry,
            consumer_group,
            dedup_ttl,
            validated: Arc::new(OnceCell::new()),
        }
    }
}

impl<F, L, P, T> PartitionStateProvider<T> for StateManagerProvider<F, L, P>
where
    F: StateBackendFactory<T>,
    L: Clone + Send + Sync + 'static,
    P: AssignmentPublisher,
    T: Send,
{
    type AcquireError = StateAcquireError<F::Error, IdentityErr<F::Backend>, P::Error>;
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
        let dedup = backend.dedup();
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
        self.publisher
            .publish_if_owner(topic, partition)
            .await
            .map_err(StateAcquireError::Publication)?;
        Ok(StateManager {
            inner: Arc::new(StateManagerInner {
                cell: backend.cell(),
                dirty: Arc::new(DirtyStore::new()),
                dedup,
                loader: self.loader.clone(),
                registry: self.registry.clone(),
                segment_id,
                dedup_ttl: self.dedup_ttl,
                checks: backend.checks(),
            }),
        })
    }
}

/// Error raised when a [`StateManagerProvider`] cannot acquire a
/// partition's manager.
#[derive(Debug, Error)]
pub enum StateAcquireError<FactoryErr, StoreErr, PublicationErr>
where
    FactoryErr: ClassifyError + Error + Send + Sync + 'static,
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
    PublicationErr: ClassifyError + Error + Send + Sync + 'static,
{
    /// Routing-set publication failed on its owning assignment.
    #[error("keyed-state publication failed at partition acquisition")]
    Publication(#[source] PublicationErr),

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

impl<FactoryErr, StoreErr, PublicationErr> ClassifyError
    for StateAcquireError<FactoryErr, StoreErr, PublicationErr>
where
    FactoryErr: ClassifyError + Error + Send + Sync + 'static,
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
    PublicationErr: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Publication(e) => e.classify_error(),
            Self::Factory(e) => e.classify_error(),
            Self::Identity(e) => e.classify_error(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_manager<S: CellStore, D: DeduplicationStore, L, K: AdmissionChecks>(
    cell: S,
    dedup: D,
    registry: Arc<CollectionDefRegistry>,
    segment_id: SegmentId,
    checks: K,
    loader: L,
) -> StateManager<PartitionBackend<D, MemoryDescriptorIdentityStore, S, K>, L> {
    StateManager {
        inner: Arc::new(StateManagerInner {
            cell,
            dirty: Arc::new(DirtyStore::new()),
            dedup,
            loader,
            registry,
            segment_id,
            dedup_ttl: CompactDuration::new(30),
            checks,
        }),
    }
}
