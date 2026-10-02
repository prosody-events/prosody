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

mod admission;
mod provider;

use admission::admission_step;
pub use provider::{StateAcquireError, StateManagerProvider};
#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::consumer::partition::ShutdownPhase;
use crate::error::ClassifyError;
use crate::state::backend::AdmissionChecks;
use crate::state::descriptor_identity::DescriptorIdentityStore;
use crate::state::dirty::DirtyStore;
use crate::state::registry::CollectionDefRegistry;
use crate::state::session::{EventSession, KeyedStateSession, SessionParts, TerminationWatch};
#[cfg(test)]
use crate::state::store::CellStore;
use crate::state::{EventRef, StateBackend, StateKey};
#[cfg(test)]
use crate::state::{PartitionBackend, memory::MemoryDescriptorIdentityStore};
use crate::timers::TimerManager;
use crate::timers::duration::CompactDuration;
use crate::timers::store::TriggerStore;
use crate::{Key, Partition, SegmentId, Topic};
use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;

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

/// A [`PartitionStateProvider`] creates this keyed-state manager for one
/// partition. It admits each key before dispatch and creates one session per
/// event. The partition loop supplies each [`EventRef`], including the message
/// dedup id; the manager does not depend on Kafka.
///
/// The trait retains type-parameter compression and symmetry with
/// [`TriggerStoreProvider`](crate::timers::store::TriggerStoreProvider).
/// Its associated `Session` lets callers name the manager without its backend
/// and loader, even with one production implementation, [`StateManager`].
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

    /// Repairs the key's durable state before its first dispatch.
    ///
    /// An interrupted settle leaves a Staged row and provisional cells in each
    /// collection it touched. Admission reads the marker rows of every
    /// discovered collection and decides each Staged row. A Committed row with
    /// the same stage id certifies a version 2 row. The old commit point
    /// certifies a version 1 row.
    ///
    /// - A certified row promotes, registered or not.
    /// - An uncertified row in a registered collection aborts with the registry
    ///   TTL.
    /// - An uncertified version 2 row in an unregistered collection stays
    ///   untouched.
    /// - Admission deletes an uncertified version 1 row in an unregistered
    ///   collection.
    ///
    /// Admission then retires each committed source: the message dedup id or
    /// the timer trigger. A permanent store rejection skips its step and does
    /// not block dispatch. Shutdown returns [`Admission::Abandoned`].
    fn admit<'a, T>(
        &'a self,
        key: Key,
        timers: &'a TimerManager<T>,
        shutdown: &'a watch::Receiver<ShutdownPhase>,
    ) -> impl Future<Output = Admission> + Send + use<'a, Self, T>
    where
        T: TriggerStore;
}

/// Admission decides whether an event can dispatch.
/// Permanent store rejections receive local repair and do not prevent dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    /// The key can dispatch an event.
    Fresh,
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

    /// Acquires the manager for `(topic, partition)` and validates descriptor
    /// identities against the group-wide identity table.
    /// Passes the partition's `triggers` handle to the backend factory.
    ///
    /// # Errors
    ///
    /// Returns [`Self::AcquireError`] if backend creation or identity
    /// validation fails. The partition loop retries until shutdown.
    fn acquire(
        &self,
        topic: Topic,
        partition: Partition,
        triggers: T,
    ) -> impl Future<Output = Result<Self::Manager, Self::AcquireError>> + Send + use<'_, Self, T>;
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

/// Owns a partition's cell store, dedup store, dirty workspace, and loader.
/// Creates per-event [`KeyedStateSession`]s that share these resources.
/// `B` selects the `StateBackend` bundle; `L` selects the loader.
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
        let cancelled = || *shutdown.borrow() >= ShutdownPhase::Cancelling;
        match admission_step(cancelled, &key, "checks.contains", None, || {
            self.inner.checks.contains(&key)
        })
        .await
        {
            Ok(Some(true)) => return Admission::Fresh,
            Ok(Some(false) | None) => {}
            Err(admission) => return admission,
        }
        if let Err(admission) = self.admit_unchecked(&key, timers, shutdown).await {
            return admission;
        }
        match admission_step(cancelled, &key, "checks.mark", None, || {
            self.inner.checks.mark(&key)
        })
        .await
        {
            Ok(_) => Admission::Fresh,
            Err(admission) => admission,
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
