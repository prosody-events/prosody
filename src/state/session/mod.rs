//! Per-event keyed-state sessions.
//!
//! A [`StateSession`] is the per-event view over a partition's keyed-state
//! stores: byte-cell reads and writes buffer in a per-event dirty workspace,
//! and the framework drives the stage/promote lifecycle through a sealed
//! supertrait that downstream crates can neither implement nor call.
//!
//! [`ValueStateSession`] is the sole implementation — the real session,
//! minted per event by the partition's state manager. It holds an `Arc`-clone
//! of the partition-lifetime [`PartitionStateStore`] (cell store + oracle +
//! committed-value cache + status map) plus a per-event dirty workspace scope.
//! Clones share the per-event buffer, so repeated descriptor binds of the same
//! collection accumulate into one write. Keyed state is always wired, so there
//! is no stateless stub outside tests.
//!
//! # Lifecycle
//!
//! The framework's per-event sequence over a session, driven by the
//! durability boundary (`crate::consumer::middleware`'s blanket
//! `EventHandler` impl) in straight-line code:
//!
//! 1. Handler ops buffer into the dirty scope through the byte-cell methods;
//!    the deduplication middleware buffers the message's commit marker into the
//!    session via `register_marker` during unwind.
//! 2. On the final handler success, `finalize` stages every touched
//!    `ReadCommitted` collection as a provisional cell (recording the staged
//!    set inside the session) and writes `ReadUncommitted` collections as
//!    resolved values.
//! 3. Strictly after the stage, `flush_marker` writes the registered dedup
//!    marker through the commit oracle — so a present marker always certifies a
//!    durable stage. After the offset/trigger commit, `commit_apply` promotes
//!    the staged cells (O(1) per cell); `rollback_aborted` rolls them back when
//!    the event aborts.
//! 4. At attempt boundaries (retry, defer), `reset` discards the dirty scope,
//!    the touched set, the staged set, and the registered marker so the next
//!    attempt starts clean. A defer-swallow therefore flushes no marker, so the
//!    deferred reload is not deduped — with no flag and no outcome inspection.

use crate::Key;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::partition::ShutdownPhase;
#[cfg(test)]
use crate::loader::MemoryLoader;
use crate::state::cell::ProvisionalWrite;
use crate::state::descriptor::{CellKind, DescriptorIdentity, StateDescriptor, StructuralIdentity};
use crate::state::oracle::CommitOracle;
use crate::state::partition_store::{CommittedCache, PartitionStateStore};
use crate::state::registry::CollectionDefRegistry;
use crate::state::store::CellStore;
use crate::state::value::{PendingOpSource, ValueKind, ValueStore, fold_value_ops};
use crate::state::{
    CollectionId, CollectionKindId, CollectionRef, CommitMode, DirtyStoreProvider, EventRef,
    EventScopeId, Read, StateKey, StateName, StateType, StoreOutcome,
};
use crate::timers::duration::CompactDuration;
use ahash::RandomState;
use bytes::Bytes;
use parking_lot::Mutex as SyncMutex;
use scc::HashSet as ConcurrentHashSet;
use sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::warn;
use uuid::Uuid;

/// Shared per-partition set of keys with a standing `StateRecovery` backstop.
///
/// A lock-free [`scc::HashSet`](ConcurrentHashSet) (not a `Mutex<HashSet>`):
/// the durability boundary touches it on every stateful commit, concurrently
/// across the partition's keys, so a single mutex would serialize unrelated
/// keys.
pub(crate) type ArmedKeys = Arc<ConcurrentHashSet<Key, RandomState>>;

/// Bundle bound for the per-event dirty Value store a session composes
/// with.
///
/// Pulled out so impl blocks downstream can reference the bound without
/// repeating the individual trait constraints. Both
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

/// Per-event keyed-state session: typed descriptor handles operate over
/// its byte cells, the framework drives its (crate-sealed) lifecycle.
///
/// Sessions are cheap `Arc`-backed clones; every clone shares the same
/// per-event dirty workspace and staged set, so repeated descriptor binds of
/// one collection accumulate into one write. The trait is sealed — downstream
/// crates name it in bounds but cannot implement it or call the lifecycle
/// methods.
pub trait StateSession: StateLifecycle + Clone + Send + Sync + 'static {
    /// Opaque per-session capability slot. The keyed-state machinery never
    /// interprets it; a
    /// [`CellResolver`](crate::state::descriptor::CellResolver)
    /// living *outside* `src/state` reads it from the session at resolve time
    /// (the consumer pins it to its message loader). Kept fully opaque here so
    /// nothing in `src/state` couples to how cells are resolved.
    type Loader: Clone + Send + Sync + 'static;

    /// Returns the session's capability slot for a resolver to read.
    fn loader(&self) -> &Self::Loader;

    /// Validates that the named keyed-state collection is registered with
    /// the asserted structural identity, returning the canonical
    /// [`StateName`].
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session,
    /// [`StateAccessError::Unregistered`] for an unknown name, or
    /// [`StateAccessError::IdentityMismatch`] when the registered identity
    /// differs from the asserted one.
    fn verify_state_registration(
        &self,
        name: &'static str,
        identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError>;

    /// Reads the current visible cell bytes of a collection within this
    /// event's transaction.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn state_cell(
        &self,
        name: &StateName,
    ) -> impl Future<Output = Result<Option<Bytes>, StateAccessError>> + Send;

    /// Buffers a set of a collection's cell bytes within this event's
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn set_state_cell(
        &self,
        name: &StateName,
        cell: Bytes,
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

    /// Buffers a clear of a collection within this event's transaction.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn clear_state_cell(
        &self,
        name: &StateName,
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

    /// Writes a collection's buffered op directly to committed state and
    /// clears the buffer — the mid-handler write-through escape hatch.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn flush_state_cell(
        &self,
        name: &StateName,
    ) -> impl Future<Output = Result<StoreOutcome, StateAccessError>> + Send;

    /// Returns `true` once the partition is shutting down or the event has
    /// been cancelled. Descriptor handles guard every operation on this.
    fn is_terminated(&self) -> bool;
}

/// Crate-sealed lifecycle half of [`StateSession`].
///
/// The module is `pub(crate)`, so downstream crates can name
/// [`StateSession`] in bounds but can neither implement it nor reach the
/// lifecycle: staging, promoting, and resetting are framework-only moves.
pub(crate) mod sealed {
    use super::{CompactDuration, Future, StateAccessError, Uuid};

    /// Whether `finalize` staged any provisional cells.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum FinalizeOutcome {
        /// At least one `ReadCommitted` collection staged a provisional cell;
        /// the caller must arm the `StateRecovery` backstop timer and promote
        /// after the commit.
        Staged,

        /// Nothing staged: no collection was dirtied, or every dirty
        /// collection was `ReadUncommitted` and written resolved during
        /// `finalize`.
        Clean,
    }

    /// Result of resolving the recorded staged set in `commit_apply` /
    /// `rollback_aborted`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ApplyOutcome {
        /// No staged set was recorded for this event — nothing to resolve.
        NothingStaged,

        /// Every staged cell resolved (promoted to committed, or rolled back
        /// to its committed base).
        Resolved,

        /// At least one resolution failed; the per-key `StateRecovery`
        /// backstop (always left armed by the durability boundary) lets the
        /// sweep retry. The boundary logs this outcome.
        Incomplete,
    }

    /// Framework-only lifecycle over a per-event session.
    pub trait StateLifecycle {
        /// Resolves every touched collection by its commit mode:
        /// `ReadCommitted` collections stage a provisional cell (the staged
        /// set is recorded inside the session), `ReadUncommitted` collections
        /// write a resolved value.
        ///
        /// # Errors
        ///
        /// Returns a type-erased store error when staging or writing fails;
        /// nothing is recorded in that case.
        fn finalize(
            &self,
        ) -> impl Future<Output = Result<FinalizeOutcome, StateAccessError>> + Send;

        /// Promotes the recorded staged set after the event committed.
        /// Best-effort: individual failures are logged and reported via
        /// [`ApplyOutcome::Incomplete`].
        fn commit_apply(&self) -> impl Future<Output = ApplyOutcome> + Send;

        /// Rolls the recorded staged set back after the event aborted.
        /// Best-effort, symmetric with [`Self::commit_apply`].
        fn rollback_aborted(&self) -> impl Future<Output = ApplyOutcome> + Send;

        /// Buffers the message commit marker (`dedup_id`) for this event.
        ///
        /// Infallible and last-wins: the deduplication middleware calls it
        /// during unwind on a handler `Ok` or permanent error. The marker
        /// rides the session — not derived from the session's `EventRef` —
        /// because on a deferred-message reload the marker is the reloaded
        /// *message's* dedup id while the session stages under the *timer's*
        /// `EventRef`. The boundary flushes it strictly after the stage;
        /// [`Self::reset`] discards it at attempt boundaries.
        fn register_marker(&self, dedup_id: Uuid);

        /// Writes the registered marker through the commit oracle, clearing
        /// the slot only on success so the boundary can retry a transient
        /// failure. A no-op when no marker is registered (returns `Ok`).
        ///
        /// # Errors
        ///
        /// Returns a type-erased store error (carrying its classification)
        /// when the oracle write fails.
        fn flush_marker(&self) -> impl Future<Output = Result<(), StateAccessError>> + Send;

        /// Discards the per-event dirty scope, touched set, recorded staged
        /// set, and registered marker so the next attempt starts clean.
        /// Called at attempt boundaries (retry, defer-swallow).
        fn reset(&self);

        /// Delay between staging and the `StateRecovery` sweep, used by
        /// the durability boundary to arm the backstop timer.
        fn recovery_fire_delay(&self) -> CompactDuration;

        /// Whether a `StateRecovery` backstop is already standing for this
        /// session's key (a not-yet-fired timer the durability boundary armed
        /// for an earlier commit).
        ///
        /// When `true`, the boundary skips re-arming: the standing timer
        /// already covers this commit's staged cells, so rapid commits issue
        /// at most one timer write per backstop generation. Per-key
        /// serialization (one handler or sweep per key at a time) makes this
        /// race-free — the sweep that consumes the backstop cannot run while a
        /// commit is deciding whether to re-arm.
        fn backstop_armed(&self) -> impl Future<Output = bool> + Send;

        /// Records that a `StateRecovery` backstop is now standing for this
        /// session's key. Called by the boundary strictly after a successful
        /// arm; cleared when the sweep fires (the manager's `recover`).
        fn mark_backstop_armed(&self) -> impl Future<Output = ()> + Send;
    }
}

/// Clones of the partition's termination signals, captured when a session
/// is minted so descriptor handles can guard operations without holding a
/// context.
#[derive(Clone, Debug)]
pub struct TerminationWatch {
    shutdown: watch::Receiver<ShutdownPhase>,
    cancel: watch::Receiver<bool>,
}

impl TerminationWatch {
    /// Captures the partition shutdown phase and per-event cancellation
    /// receivers.
    #[must_use]
    pub fn new(shutdown: watch::Receiver<ShutdownPhase>, cancel: watch::Receiver<bool>) -> Self {
        Self { shutdown, cancel }
    }

    /// `true` once the partition is `Cancelling` (or later) or the event
    /// has been cancelled.
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        *self.shutdown.borrow() >= ShutdownPhase::Cancelling || *self.cancel.borrow()
    }
}

/// The provisional cells `finalize` staged for the promote / rollback hooks.
///
/// Each entry pairs the collection ref (carrying its TTL) with the staged
/// write, whose `data` is the value to promote to and whose `prev` is the
/// committed base to roll back to. Only `ReadCommitted` collections appear —
/// `ReadUncommitted` writes resolve at stage time with nothing to resolve.
struct StagedSet {
    entries: Vec<(CollectionRef<ValueKind>, ProvisionalWrite)>,
}

/// Construction parameters for [`ValueStateSession`], bundled so the
/// constructor stays readable.
pub struct SessionParts<S, O, C, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    /// Partition-lifetime cell store + oracle + cache + status map.
    pub store: PartitionStateStore<ValueKind, S, O, C>,

    /// Partition-lifetime commit oracle; the marker flush writes the
    /// message commit row through it. Same instance baked into `store`.
    pub oracle: O,

    /// Partition-lifetime dirty-workspace provider; `reset` mints fresh
    /// scopes from it.
    pub dirty: P,

    /// Opaque per-session capability slot a [`CellResolver`] reads at
    /// resolve time (the consumer pins it to its message loader).
    ///
    /// [`CellResolver`]: crate::state::descriptor::CellResolver
    pub loader: L,

    /// Registered collection definitions and middleware-wide defaults.
    pub registry: Arc<CollectionDefRegistry>,

    /// Segment-qualified key this session's collections live under.
    pub state_key: StateKey,

    /// The event whose stages this session owns.
    pub event: EventRef,

    /// Delay between staging and the `StateRecovery` sweep.
    pub recovery_delay: CompactDuration,

    /// Per-partition set of keys with a standing `StateRecovery` backstop,
    /// shared with the manager's `recover` so a fired sweep clears the flag.
    /// Lets the durability boundary skip re-arming while a backstop stands.
    pub armed: ArmedKeys,

    /// Termination signals captured at mint.
    pub termination: TerminationWatch,
}

struct SessionInner<S, O, C, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    store: PartitionStateStore<ValueKind, S, O, C>,
    oracle: O,
    dirty_provider: P,
    /// Current per-event dirty workspace; swapped for a fresh scope on
    /// `reset`.
    dirty: SyncMutex<P::Store>,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    event: EventRef,
    recovery_delay: CompactDuration,
    /// Shared per-partition set of keys with a standing `StateRecovery`
    /// backstop. Read/written for this session's own key only; the manager's
    /// `recover` removes the key when the sweep fires.
    armed: ArmedKeys,
    termination: TerminationWatch,
    /// Collections written this event, the finalize work-list.
    touched: SyncMutex<HashSet<StateName>>,
    staged: SyncMutex<Option<StagedSet>>,
    /// The registered message commit marker, flushed strictly after the
    /// stage and cleared by `reset`. `None` until the deduplication
    /// middleware registers it (or always, on the timer arm).
    marker: SyncMutex<Option<Uuid>>,
}

/// The real per-event session over a partition's cell store.
///
/// One session is minted per event by the partition's state manager;
/// clones share the per-event dirty scope, touched set, recorded staged set,
/// and registered marker. `S` is the cell store, `O` the commit oracle, `C`
/// the committed-value cache, `P` the per-partition dirty-workspace provider,
/// `L` the message loader.
pub struct ValueStateSession<S, O, C, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    inner: Arc<SessionInner<S, O, C, P, L>>,
}

impl<S, O, C, P, L> Clone for ValueStateSession<S, O, C, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S, O, C, P, L> fmt::Debug for ValueStateSession<S, O, C, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueStateSession")
            .field("state_key", &self.inner.state_key)
            .field("event", &self.inner.event)
            .finish_non_exhaustive()
    }
}

impl<S, O, C, P, L> ValueStateSession<S, O, C, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    /// Creates a session for one event, opening a fresh dirty scope.
    #[must_use]
    pub fn new(parts: SessionParts<S, O, C, P, L>) -> Self {
        let SessionParts {
            store,
            oracle,
            dirty,
            loader,
            registry,
            state_key,
            event,
            recovery_delay,
            armed,
            termination,
        } = parts;
        let scope = dirty.for_scope(EventScopeId::fresh());
        Self {
            inner: Arc::new(SessionInner {
                store,
                oracle,
                dirty_provider: dirty,
                dirty: SyncMutex::new(scope),
                loader,
                registry,
                state_key,
                event,
                recovery_delay,
                armed,
                termination,
                touched: SyncMutex::new(HashSet::new()),
                staged: SyncMutex::new(None),
                marker: SyncMutex::new(None),
            }),
        }
    }

    fn collection_id_for(&self, name: &StateName) -> CollectionId<ValueKind> {
        CollectionId::new(
            self.inner.state_key.clone(),
            StateType::Application,
            name.clone(),
        )
    }

    fn collection_ref_for(&self, name: &StateName) -> CollectionRef<ValueKind> {
        CollectionRef::new(
            self.collection_id_for(name),
            self.inner.registry.ttl_for(name),
        )
    }
}

impl<S, O, C, P, L> ValueStateSession<S, O, C, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
    P::Store: Clone,
{
    /// Clones the current per-event dirty scope so its async ops run without
    /// holding the swap mutex across an await.
    fn dirty_scope(&self) -> P::Store {
        self.inner.dirty.lock().clone()
    }
}

impl<S, O, C, P, L> StateSession for ValueStateSession<S, O, C, P, L>
where
    S: CellStore<ValueKind>,
    O: CommitOracle,
    C: CommittedCache<ValueKind>,
    P: DirtyStoreProvider<ValueKind>,
    P::Store: DirtyValueBundle + Send + Sync,
    L: Clone + Send + Sync + 'static,
{
    type Loader = L;

    fn loader(&self) -> &L {
        &self.inner.loader
    }

    fn verify_state_registration(
        &self,
        name: &'static str,
        identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        let Some((state_name, registered)) = self.inner.registry.lookup(name) else {
            return Err(StateAccessError::Unregistered { name });
        };
        if registered.identity != *identity {
            return Err(StateAccessError::IdentityMismatch {
                stored: registered.identity.clone(),
                asserted: identity.clone(),
            });
        }
        Ok(state_name.clone())
    }

    async fn state_cell(&self, name: &StateName) -> Result<Option<Bytes>, StateAccessError> {
        let id = self.collection_id_for(name);
        let dirty = self.dirty_scope();
        match dirty
            .get(&id)
            .await
            .map_err(|e| StateAccessError::store(&e))?
        {
            Read::Present(payload) => Ok(Some(payload)),
            Read::Absent => Ok(None),
            Read::Unknown => self
                .inner
                .store
                .committed_value(&id, &(), self.inner.event)
                .await
                .map_err(|e| StateAccessError::store(&e)),
        }
    }

    async fn set_state_cell(&self, name: &StateName, cell: Bytes) -> Result<(), StateAccessError> {
        let id = self.collection_id_for(name);
        self.dirty_scope()
            .set(&id, cell)
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        self.inner.touched.lock().insert(name.clone());
        Ok(())
    }

    async fn clear_state_cell(&self, name: &StateName) -> Result<(), StateAccessError> {
        let id = self.collection_id_for(name);
        self.dirty_scope()
            .clear(&id)
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        self.inner.touched.lock().insert(name.clone());
        Ok(())
    }

    async fn flush_state_cell(&self, name: &StateName) -> Result<StoreOutcome, StateAccessError> {
        let id = self.collection_id_for(name);
        let dirty = self.dirty_scope();
        let Some(pending) = dirty
            .pending_ops(&id)
            .map_err(|e| StateAccessError::store(&e))?
        else {
            return Ok(StoreOutcome::NoOp);
        };
        let ops: Vec<_> = pending.ops.collect();
        let value = fold_value_ops(None, ops.iter());
        let collection_ref = self.collection_ref_for(name);
        self.inner
            .store
            .write_resolved(&collection_ref, &(), value.as_ref())
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        dirty
            .clear_pending_ops(&id)
            .map_err(|e| StateAccessError::store(&e))?;
        Ok(StoreOutcome::Applied)
    }

    fn is_terminated(&self) -> bool {
        self.inner.termination.is_terminated()
    }
}

impl<S, O, C, P, L> StateLifecycle for ValueStateSession<S, O, C, P, L>
where
    S: CellStore<ValueKind>,
    O: CommitOracle,
    C: CommittedCache<ValueKind>,
    P: DirtyStoreProvider<ValueKind>,
    P::Store: DirtyValueBundle + Send + Sync,
    L: Send + Sync + 'static,
{
    /// Stages dirty `ReadCommitted` collections as provisional cells and
    /// writes `ReadUncommitted` ones resolved, recording the staged set.
    ///
    /// **Idempotent under retry**: the durability boundary retries a transient
    /// failure in place without resetting the session. Re-reading the same
    /// buffered ops and re-staging is idempotent — a provisional write
    /// overwrites the cell with the same `(data, prev, event)` triple, and
    /// re-reading `prev` returns the same committed base (the cell is now
    /// `Provisional` owned by this event, so `committed` reads its `prev`).
    /// The staged set is replaced only after every write succeeds.
    async fn finalize(&self) -> Result<FinalizeOutcome, StateAccessError> {
        let names: Vec<StateName> = self.inner.touched.lock().iter().cloned().collect();
        let dirty = self.dirty_scope();
        let mut staged = Vec::new();
        for name in names {
            let id = self.collection_id_for(&name);
            let Some(pending) = dirty
                .pending_ops(&id)
                .map_err(|e| StateAccessError::store(&e))?
            else {
                // Touched (e.g. via `get`) but never written — nothing to stage.
                continue;
            };
            let ops: Vec<_> = pending.ops.collect();
            let value = fold_value_ops(None, ops.iter());
            let collection_ref = self.collection_ref_for(&name);
            match self.inner.registry.commit_mode_for(&name) {
                CommitMode::ReadCommitted => {
                    let prev = self
                        .inner
                        .store
                        .committed(&id, &(), self.inner.event)
                        .await
                        .map_err(|e| StateAccessError::store(&e))?;
                    let write = ProvisionalWrite::new(value, prev, self.inner.event);
                    self.inner
                        .store
                        .write_provisional(&collection_ref, &(), &write)
                        .await
                        .map_err(|e| StateAccessError::store(&e))?;
                    staged.push((collection_ref, write));
                }
                CommitMode::ReadUncommitted => {
                    self.inner
                        .store
                        .write_resolved(&collection_ref, &(), value.as_ref())
                        .await
                        .map_err(|e| StateAccessError::store(&e))?;
                }
            }
        }
        if staged.is_empty() {
            Ok(FinalizeOutcome::Clean)
        } else {
            *self.inner.staged.lock() = Some(StagedSet { entries: staged });
            Ok(FinalizeOutcome::Staged)
        }
    }

    async fn commit_apply(&self) -> ApplyOutcome {
        let Some(set) = self.inner.staged.lock().take() else {
            return ApplyOutcome::NothingStaged;
        };
        let mut all_resolved = true;
        for (collection_ref, write) in &set.entries {
            if let Err(error) = self
                .inner
                .store
                .promote(collection_ref, &(), write.data())
                .await
            {
                warn!(error = ?error, "cell promote failed; leaving provisional for the sweep");
                all_resolved = false;
            }
        }
        if all_resolved {
            ApplyOutcome::Resolved
        } else {
            ApplyOutcome::Incomplete
        }
    }

    async fn rollback_aborted(&self) -> ApplyOutcome {
        let Some(set) = self.inner.staged.lock().take() else {
            return ApplyOutcome::NothingStaged;
        };
        let mut all_resolved = true;
        for (collection_ref, write) in &set.entries {
            if let Err(error) = self
                .inner
                .store
                .rollback_provisional(collection_ref, &(), write.prev())
                .await
            {
                warn!(error = ?error, "cell rollback failed; leaving provisional for the sweep");
                all_resolved = false;
            }
        }
        if all_resolved {
            ApplyOutcome::Resolved
        } else {
            ApplyOutcome::Incomplete
        }
    }

    fn register_marker(&self, dedup_id: Uuid) {
        *self.inner.marker.lock() = Some(dedup_id);
    }

    async fn flush_marker(&self) -> Result<(), StateAccessError> {
        // Read without taking: the slot clears only after the oracle write
        // succeeds, so a transient failure leaves the marker for the
        // boundary to retry.
        let Some(dedup_id) = *self.inner.marker.lock() else {
            return Ok(());
        };
        self.inner
            .oracle
            .record_message(dedup_id)
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        *self.inner.marker.lock() = None;
        Ok(())
    }

    fn reset(&self) {
        // Swap in a fresh dirty scope first so no cleared op can observe the
        // old workspace through a racing re-open.
        *self.inner.dirty.lock() = self.inner.dirty_provider.for_scope(EventScopeId::fresh());
        self.inner.touched.lock().clear();
        *self.inner.staged.lock() = None;
        *self.inner.marker.lock() = None;
    }

    fn recovery_fire_delay(&self) -> CompactDuration {
        self.inner.recovery_delay
    }

    async fn backstop_armed(&self) -> bool {
        self.inner
            .armed
            .contains_async(&self.inner.state_key.key)
            .await
    }

    async fn mark_backstop_armed(&self) {
        // `insert_async` returns `Err(key)` if already present — harmless; the
        // flag is idempotent.
        let _ = self
            .inner
            .armed
            .insert_async(self.inner.state_key.key.clone())
            .await;
    }
}

/// Crate-private descriptor the framework uses to reach a session's
/// staged lifecycle through the one public [`EventContext::state`] method.
///
/// `bind` skips registration (the lifecycle is registration-independent)
/// and returns a [`LifecycleView`] unconditionally. Downstream crates can
/// name neither this type nor the view, so the lifecycle stays
/// framework-only even though it travels through a public method.
///
/// [`EventContext::state`]: crate::consumer::event_context::EventContext::state
#[derive(Clone, Copy, Debug)]
pub(crate) struct LifecycleAccess;

impl DescriptorIdentity for LifecycleAccess {
    fn name(&self) -> &'static str {
        // Never registered, never persisted: `bind` ignores the identity.
        "\u{0}lifecycle"
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Value,
            cell_kind: CellKind::Codec,
            codec_id: None,
        }
    }
}

impl StateDescriptor for LifecycleAccess {
    type Handle<S: StateSession> = LifecycleView<S>;

    fn bind<S: StateSession>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError> {
        Ok(LifecycleView {
            session: session.clone(),
        })
    }
}

/// Crate-private view over a session's staged lifecycle, returned by
/// binding [`LifecycleAccess`].
pub(crate) struct LifecycleView<S> {
    session: S,
}

impl<S> LifecycleView<S>
where
    S: StateSession,
{
    /// See [`StateLifecycle::finalize`].
    ///
    /// # Errors
    ///
    /// Returns a type-erased store error when staging fails.
    pub(crate) async fn finalize(&self) -> Result<FinalizeOutcome, StateAccessError> {
        self.session.finalize().await
    }

    /// See [`StateLifecycle::commit_apply`].
    pub(crate) async fn commit_apply(&self) -> ApplyOutcome {
        self.session.commit_apply().await
    }

    /// See [`StateLifecycle::rollback_aborted`].
    pub(crate) async fn rollback_aborted(&self) -> ApplyOutcome {
        self.session.rollback_aborted().await
    }

    /// See [`StateLifecycle::register_marker`].
    pub(crate) fn register_marker(&self, dedup_id: Uuid) {
        self.session.register_marker(dedup_id);
    }

    /// See [`StateLifecycle::flush_marker`].
    ///
    /// # Errors
    ///
    /// Returns a type-erased store error when the oracle write fails.
    pub(crate) async fn flush_marker(&self) -> Result<(), StateAccessError> {
        self.session.flush_marker().await
    }

    /// See [`StateLifecycle::reset`].
    pub(crate) fn reset(&self) {
        self.session.reset();
    }

    /// See [`StateLifecycle::recovery_fire_delay`].
    pub(crate) fn recovery_fire_delay(&self) -> CompactDuration {
        self.session.recovery_fire_delay()
    }

    /// See [`StateLifecycle::backstop_armed`].
    pub(crate) async fn backstop_armed(&self) -> bool {
        self.session.backstop_armed().await
    }

    /// See [`StateLifecycle::mark_backstop_armed`].
    pub(crate) async fn mark_backstop_armed(&self) {
        self.session.mark_backstop_armed().await;
    }
}

/// Test-only stateless [`StateSession`]: every state op reports
/// [`StateAccessError::Unavailable`] and the lifecycle is inert.
///
/// Production always wires real keyed state, so this stub exists only for
/// tests — middleware and handler contexts that never touch state, and the
/// descriptor-bind error-surface tests. Its loader slot is a never-consulted
/// [`MemoryLoader`] purely so the session satisfies the consumer's loader
/// context bound; no op ever reaches it.
///
/// [`MemoryLoader`]: crate::loader::MemoryLoader
#[cfg(test)]
#[derive(Clone)]
pub struct UnavailableState<P> {
    loader: MemoryLoader<P>,
    /// Records every `register_marker` call so tests can prove the boundary
    /// routes the marker through the session even on a stateless stub. The
    /// stub's `flush_marker` stays inert (no oracle), returning `Ok`.
    markers: Arc<SyncMutex<Vec<Uuid>>>,
}

#[cfg(test)]
impl<P> UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    /// Creates the stateless stub.
    #[must_use]
    pub fn new() -> Self {
        Self {
            loader: MemoryLoader::new(),
            markers: Arc::new(SyncMutex::new(Vec::new())),
        }
    }

    /// The markers registered against this stub (test observability).
    pub(crate) fn registered_markers(&self) -> Vec<Uuid> {
        self.markers.lock().clone()
    }
}

#[cfg(test)]
impl<P> Default for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl<P> fmt::Debug for UnavailableState<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UnavailableState")
    }
}

#[cfg(test)]
impl<P> StateSession for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Loader = MemoryLoader<P>;

    fn loader(&self) -> &Self::Loader {
        &self.loader
    }

    fn verify_state_registration(
        &self,
        _name: &'static str,
        _identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn state_cell(&self, _name: &StateName) -> Result<Option<Bytes>, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn set_state_cell(
        &self,
        _name: &StateName,
        _cell: Bytes,
    ) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn clear_state_cell(&self, _name: &StateName) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn flush_state_cell(&self, _name: &StateName) -> Result<StoreOutcome, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    fn is_terminated(&self) -> bool {
        true
    }
}

#[cfg(test)]
impl<P> StateLifecycle for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    async fn finalize(&self) -> Result<FinalizeOutcome, StateAccessError> {
        Ok(FinalizeOutcome::Clean)
    }

    async fn commit_apply(&self) -> ApplyOutcome {
        ApplyOutcome::NothingStaged
    }

    async fn rollback_aborted(&self) -> ApplyOutcome {
        ApplyOutcome::NothingStaged
    }

    fn register_marker(&self, dedup_id: Uuid) {
        self.markers.lock().push(dedup_id);
    }

    async fn flush_marker(&self) -> Result<(), StateAccessError> {
        Ok(())
    }

    fn reset(&self) {
        self.markers.lock().clear();
    }

    fn recovery_fire_delay(&self) -> CompactDuration {
        CompactDuration::MIN
    }

    async fn backstop_armed(&self) -> bool {
        false
    }

    async fn mark_backstop_armed(&self) {}
}
