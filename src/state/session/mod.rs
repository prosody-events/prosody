//! Per-event keyed-state sessions.
//!
//! A session is the per-event view over a partition's keyed-state cell store:
//! byte-cell reads and writes buffer in a per-event dirty overlay, and the
//! framework drives the stage/promote lifecycle through a sealed supertrait
//! that downstream crates can neither implement nor call.
//!
//! [`KeyedStateSession`] is the sole implementation — the real session, minted
//! per event by the partition's state manager. It holds **one uniform
//! [`Overlay`]** (the per-event [`DirtyStore`] over the partition's committed
//! cell store) plus the cross-event singletons (the commit oracle, the
//! registered marker, the armed backstop, the event, the registry, …). Clones
//! share the per-event state, so repeated descriptor binds of one collection
//! accumulate into one write.
//!
//! # The read / mutate / lifecycle split
//!
//! The session surface is three traits, so a read-only consumer (a cross-group
//! reader) is structurally non-mutating:
//!
//! - [`CellRead`] — the read-only half: `get`/`scan` +
//!   `loader`/`is_terminated`/ `verify_state_registration`. Crate-sealed via
//!   `sealed::ReadSessionMarker`.
//! - [`CellSession`] (`= CellRead + StateLifecycle`) — adds the buffering
//!   mutators `set`/`clear`/`flush`. [`EventContext::State`] bounds this.
//! - `sealed::StateLifecycle` — the sealed, manager-driven lifecycle
//!   (`finalize`/`commit_apply`/`rollback_aborted`/marker/reset).
//!
//! Collection handles gate their methods by the session bound: reads need only
//! `CellRead`, mutators need `CellSession`, so a handle minted over a
//! `CellRead`-only session *cannot name* a mutator.
//!
//! # Lifecycle
//!
//! The framework's per-event sequence, driven by the durability boundary
//! (`crate::consumer::middleware`'s blanket `EventHandler` impl) in
//! straight-line code:
//!
//! 1. Handler ops buffer into the dirty overlay; the deduplication middleware
//!    buffers the message's commit marker via `register_marker` during unwind.
//! 2. On the final handler success, `finalize` groups the one dirty map by
//!    collection and stages each in one same-partition batch — `ReadCommitted`
//!    collections stage provisional cells, `ReadUncommitted` ones write
//!    resolved values.
//! 3. Strictly after the stage, `flush_marker` writes the registered dedup
//!    marker through the commit oracle. After the offset/trigger commit,
//!    `commit_apply` promotes the staged cells; `rollback_aborted` rolls them
//!    back.
//! 4. At attempt boundaries (retry, defer), `reset` discards the dirty overlay
//!    and staged set plus the registered marker.

use crate::Key;
use crate::consumer::event_context::{EventContext, StateAccessError};
use crate::consumer::partition::ShutdownPhase;
use crate::error::ClassifyError;
#[cfg(test)]
use crate::loader::MemoryLoader;
use crate::state::cell::ProvisionalWrite;
use crate::state::cell_key::{CellKey, Coordinate, Scan};
use crate::state::descriptor::{
    DescriptorIdentity, Registered, StateDescriptor, StructuralIdentity,
};
use crate::state::dirty::{DirtyStore, DirtyVal};
use crate::state::identity::{CollectionId, CollectionRef};
use crate::state::oracle::CommitOracle;
use crate::state::overlay::Overlay;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::CellStore;
use crate::state::{
    CollectionKindId, CommitMode, EventRef, STATE_FANOUT_CONCURRENCY, StateBackend, StateKey,
    StateName, StateType, StoreOutcome,
};
use crate::timers::duration::CompactDuration;
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex as SyncMutex;
use scc::HashSet as ConcurrentHashSet;
use sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::coop::cooperative;
use tracing::warn;
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// Shared per-partition set of keys with a standing `StateRecovery` backstop.
///
/// A lock-free [`scc::HashSet`](ConcurrentHashSet) (not a `Mutex<HashSet>`):
/// the durability boundary touches it on every stateful commit, concurrently
/// across the partition's keys, so a single mutex would serialize unrelated
/// keys.
pub(crate) type ArmedKeys = Arc<ConcurrentHashSet<Key, RandomState>>;

/// The provisional cells `finalize` staged, grouped by collection (its ref
/// carries the TTL). Only `ReadCommitted` collections appear; `ReadUncommitted`
/// writes resolve at stage time with nothing to settle. Each `(cell, write)`'s
/// `data` is the value to promote to, `prev` the committed base to roll back
/// to.
type StagedSet = Vec<(CollectionRef, Vec<(CellKey, ProvisionalWrite)>)>;

/// The read-only half of a per-event session: visible committed-value reads
/// (`get`/`scan`), the loader slot, the termination flag, and the registration
/// check. The only surface a cross-group reader implements, so a reader-minted
/// collection handle carries no mutator (the `ReadOnlyHandleCannotMutate`
/// invariant, enforced at the handle's method-impl bound).
///
/// `get`/`scan` describe the session's **visible committed bytes** for a cell —
/// the writer's [`KeyedStateSession`] realises that through the dirty overlay +
/// oracle resolution; a reader realises the same contract through a pure
/// committed projection. Neither realisation leaks into this contract.
pub trait CellRead: sealed::ReadSessionMarker + Clone + Send + Sync + 'static {
    /// Opaque per-session capability slot. The keyed-state machinery never
    /// interprets it; a
    /// [`CellResolver`](crate::state::descriptor::CellResolver)
    /// living outside `src/state` reads it from the session at resolve time.
    type Loader: Clone + Send + Sync + 'static;

    /// Error a `scan` stream may yield (the cell store's error).
    type ScanError: ClassifyError + Error + Send + Sync + 'static;

    /// Returns the session's capability slot for a resolver to read.
    fn loader(&self) -> &Self::Loader;

    /// Returns `true` once the partition is shutting down or the event has been
    /// cancelled. Descriptor handles guard every operation on this.
    fn is_terminated(&self) -> bool;

    /// Validates that the keyed-state collection named `(state_type, name)` is
    /// registered with the asserted structural identity, returning the
    /// canonical [`StateName`].
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
        state_type: StateType,
        identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError>;

    /// Reads a cell's currently visible committed value within this event's
    /// transaction (cleared/absent → `None`).
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn get(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> impl Future<Output = Result<Option<Bytes>, StateAccessError>> + Send;

    /// The single-section, start-anchored, bidirectional range primitive: a
    /// lazy stream of the visible committed cells in `coordinate` byte order.
    fn scan<'a>(
        &'a self,
        state_type: StateType,
        name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::ScanError>> + Send + 'a;
}

/// The read/buffer/mutate session collections write through. Adds the buffering
/// mutators to [`CellRead`]; the framework reaches the lifecycle through the
/// sealed `StateLifecycle` supertrait. Everything readable is inherited from
/// [`CellRead`].
pub trait CellSession: CellRead + StateLifecycle {
    /// Buffers a set of the cell's bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn set(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
        value: &[u8],
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

    /// Buffers a clear of the cell.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn clear(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

    /// Writes the cell's buffered outcome straight to committed state — the
    /// mid-handler write-through escape hatch.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn flush(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> impl Future<Output = Result<StoreOutcome, StateAccessError>> + Send;
}

/// Crate-sealed lifecycle half of [`CellSession`].
///
/// The module is `pub(crate)`, so downstream crates can name [`CellSession`] in
/// bounds but can neither implement it nor reach the lifecycle: staging,
/// promoting, and resetting are framework-only moves.
pub(crate) mod sealed {
    use super::{CompactDuration, Future, StateAccessError, Uuid};

    /// Crate-private marker keeping [`super::CellRead`] consistent with the
    /// framework's seal-everything hygiene: only the in-crate
    /// [`KeyedStateSession`](super::KeyedStateSession) (and the future reader
    /// session) implement it, so `CellRead` cannot be implemented downstream.
    /// Hygiene, not correctness — a downstream `CellRead` impl could reach no
    /// mutator anyway (those need the sealed [`StateLifecycle`]).
    pub trait ReadSessionMarker {}

    /// Whether `finalize` staged any provisional cells.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum FinalizeOutcome {
        /// At least one `ReadCommitted` collection staged a provisional cell;
        /// the caller must arm the `StateRecovery` backstop and promote after
        /// the commit.
        Staged,

        /// Nothing staged: no collection was dirtied, or every dirty collection
        /// was `ReadUncommitted` and written resolved during `finalize`.
        Clean,
    }

    /// Result of resolving the recorded staged set in `commit_apply` /
    /// `rollback_aborted`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ApplyOutcome {
        /// No staged set was recorded for this event — nothing to resolve.
        NothingStaged,

        /// Every staged cell resolved (promoted, or rolled back to its base).
        Resolved,

        /// At least one resolution failed; the per-key `StateRecovery` backstop
        /// (always left armed by the durability boundary) lets the sweep retry.
        Incomplete,
    }

    /// Framework-only lifecycle over a per-event session.
    pub trait StateLifecycle {
        /// Resolves every touched collection by its commit mode:
        /// `ReadCommitted` collections stage a provisional cell (the
        /// staged set is recorded), `ReadUncommitted` collections write
        /// a resolved value. Stages all collections before returning,
        /// so a stage error returns before the textually-later marker
        /// flush.
        ///
        /// # Errors
        ///
        /// Returns a type-erased store error when staging fails; nothing is
        /// recorded in that case.
        fn finalize(
            &self,
        ) -> impl Future<Output = Result<FinalizeOutcome, StateAccessError>> + Send;

        /// Promotes the recorded staged set after the event committed.
        /// Best-effort: individual failures are logged and folded into
        /// [`ApplyOutcome::Incomplete`].
        fn commit_apply(&self) -> impl Future<Output = ApplyOutcome> + Send;

        /// Rolls the recorded staged set back after the event aborted.
        /// Best-effort, symmetric with [`Self::commit_apply`].
        fn rollback_aborted(&self) -> impl Future<Output = ApplyOutcome> + Send;

        /// Buffers the message commit marker (`dedup_id`) for this event.
        ///
        /// Infallible and last-wins. The marker rides the session — not derived
        /// from the session's `EventRef` — because on a deferred-message reload
        /// the marker is the reloaded *message's* dedup id while the session
        /// stages under the *timer's* `EventRef`. The boundary flushes it
        /// strictly after the stage; [`Self::reset`] discards it.
        fn register_marker(&self, dedup_id: Uuid);

        /// Writes the registered marker through the commit oracle, clearing the
        /// slot only on success so the boundary can retry a transient failure.
        /// A no-op when no marker is registered (returns `Ok`).
        ///
        /// # Errors
        ///
        /// Returns a type-erased store error when the oracle write fails.
        fn flush_marker(&self) -> impl Future<Output = Result<(), StateAccessError>> + Send;

        /// Discards the per-event dirty overlay and staged set, plus the
        /// registered marker, so the next attempt starts clean.
        fn reset(&self);

        /// Delay between staging and the `StateRecovery` sweep.
        fn recovery_fire_delay(&self) -> CompactDuration;

        /// Whether a `StateRecovery` backstop is already standing for this
        /// session's key.
        fn backstop_armed(&self) -> impl Future<Output = bool> + Send;

        /// Records that a `StateRecovery` backstop is now standing for this
        /// session's key.
        fn mark_backstop_armed(&self) -> impl Future<Output = ()> + Send;
    }
}

/// Clones of the partition's termination signals, captured when a session is
/// minted so descriptor handles can guard operations without holding a context.
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

    /// `true` once the partition is `Cancelling` (or later) or the event has
    /// been cancelled.
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        *self.shutdown.borrow() >= ShutdownPhase::Cancelling || *self.cancel.borrow()
    }
}

/// How `resolve_staged` settles a staged set once the event's outcome is known.
#[derive(Clone, Copy, Debug)]
enum Resolve {
    /// The event committed: each staged cell's `data` becomes committed, via an
    /// O(1) promote that nulls `event`/`prev`.
    Promote,

    /// The event aborted: each cell's committed base `prev` is written back as
    /// the resolved value.
    Rollback,
}

/// Construction parameters for [`KeyedStateSession`], bundled so the
/// constructor stays readable.
pub struct SessionParts<B, L>
where
    B: StateBackend,
{
    /// The partition's uniform committed cell store (the session wraps it in a
    /// per-event [`Overlay`]).
    pub cell: B::Cell,

    /// Per-partition shared dirty workspace; this event's `key` sub-range is
    /// cleared at each attempt/settle boundary.
    pub dirty: Arc<DirtyStore>,

    /// Partition-lifetime commit oracle; the marker flush writes the message
    /// commit row through it. The same instance is baked into `cell`.
    pub oracle: B::Oracle,

    /// Opaque per-session capability slot a [`CellResolver`] reads at resolve
    /// time.
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

    /// Per-partition set of keys with a standing `StateRecovery` backstop.
    pub armed: ArmedKeys,

    /// Termination signals captured at mint.
    pub termination: TerminationWatch,
}

struct SessionInner<B, L>
where
    B: StateBackend,
{
    overlay: Overlay<B::Cell>,
    oracle: B::Oracle,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    event: EventRef,
    recovery_delay: CompactDuration,
    armed: ArmedKeys,
    termination: TerminationWatch,
    /// The recorded staged set the lifecycle promotes or rolls back. `None`
    /// until `finalize` stages a `ReadCommitted` collection.
    staged: SyncMutex<Option<StagedSet>>,
    /// The registered message commit marker, flushed strictly after the stage
    /// and cleared by `reset`.
    marker: SyncMutex<Option<Uuid>>,
}

/// The real per-event session over a partition's cell store.
///
/// One session is minted per event by the partition's state manager; clones
/// share the per-event overlay and singletons. `B` is the per-partition
/// [`StateBackend`] bundle; `L` is the message loader.
pub struct KeyedStateSession<B, L>
where
    B: StateBackend,
{
    inner: Arc<SessionInner<B, L>>,
}

impl<B, L> Clone for KeyedStateSession<B, L>
where
    B: StateBackend,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B, L> fmt::Debug for KeyedStateSession<B, L>
where
    B: StateBackend,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyedStateSession")
            .field("state_key", &self.inner.state_key)
            .field("event", &self.inner.event)
            .finish_non_exhaustive()
    }
}

impl<B, L> KeyedStateSession<B, L>
where
    B: StateBackend,
{
    /// Creates a session for one event, wrapping the partition's cell store in
    /// a per-event [`Overlay`] over the shared dirty workspace.
    #[must_use]
    pub fn new(parts: SessionParts<B, L>) -> Self {
        let SessionParts {
            cell,
            dirty,
            oracle,
            loader,
            registry,
            state_key,
            event,
            recovery_delay,
            armed,
            termination,
        } = parts;
        Self {
            inner: Arc::new(SessionInner {
                overlay: Overlay::new(dirty, cell, event),
                oracle,
                loader,
                registry,
                state_key,
                event,
                recovery_delay,
                armed,
                termination,
                staged: SyncMutex::new(None),
                marker: SyncMutex::new(None),
            }),
        }
    }

    /// The collection id for `(state_type, name)` under this session's key.
    fn id_for(&self, state_type: StateType, name: &StateName) -> CollectionId {
        CollectionId::new(self.inner.state_key.clone(), state_type, name.clone())
    }

    /// The collection ref (id + registry TTL) for `(state_type, name)`.
    fn ref_for(&self, state_type: StateType, name: &StateName) -> CollectionRef {
        let id = self.id_for(state_type, name);
        let ttl = self.inner.registry.ttl_for(state_type, name);
        CollectionRef::new(id, ttl)
    }

    /// Resolves the recorded staged set after the event's outcome is known —
    /// [`Resolve::Promote`] on commit, [`Resolve::Rollback`] on abort.
    /// Best-effort: drives every per-collection resolution to completion
    /// regardless of siblings, reporting [`ApplyOutcome::Incomplete`] if any
    /// failed (the backstop, always left armed, lets the sweep retry).
    async fn resolve_staged(&self, how: Resolve) -> ApplyOutcome
    where
        L: Send + Sync + 'static,
    {
        let Some(set) = self.inner.staged.lock().take() else {
            return ApplyOutcome::NothingStaged;
        };
        let lower = self.inner.overlay.lower();
        let all_resolved = stream::iter(set)
            .map(|(collection_ref, writes)| {
                cooperative(async move {
                    let result = match how {
                        Resolve::Promote => {
                            let keys: Vec<CellKey> =
                                writes.iter().map(|(cell, _)| cell.clone()).collect();
                            lower.mark_resolved(&collection_ref, &keys).await
                        }
                        Resolve::Rollback => {
                            let cells: Vec<(CellKey, Option<Bytes>)> = writes
                                .iter()
                                .map(|(cell, write)| (cell.clone(), write.prev().cloned()))
                                .collect();
                            lower.write_resolved(&collection_ref, &cells).await
                        }
                    };
                    match result {
                        Ok(()) => true,
                        Err(error) => {
                            warn!(error = ?error, "cell resolution failed; leaving provisional for the sweep");
                            false
                        }
                    }
                })
            })
            .buffer_unordered(STATE_FANOUT_CONCURRENCY)
            .fold(true, |all, ok| async move { all && ok })
            .await;
        if all_resolved {
            ApplyOutcome::Resolved
        } else {
            ApplyOutcome::Incomplete
        }
    }
}

impl<B, L> sealed::ReadSessionMarker for KeyedStateSession<B, L> where B: StateBackend {}

impl<B, L> CellRead for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    type Loader = L;
    type ScanError = <B::Cell as CellStore>::Error;

    fn loader(&self) -> &L {
        &self.inner.loader
    }

    fn is_terminated(&self) -> bool {
        self.inner.termination.is_terminated()
    }

    fn verify_state_registration(
        &self,
        name: &'static str,
        state_type: StateType,
        identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        let Some((state_name, registered)) = self.inner.registry.lookup(state_type, name) else {
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

    async fn get(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        let id = self.id_for(state_type, name);
        let committed = self
            .inner
            .overlay
            .get(&id, cell, self.inner.event)
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        Ok(committed.into_inner())
    }

    fn scan<'a>(
        &'a self,
        state_type: StateType,
        name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::ScanError>> + Send + 'a {
        let id = self.id_for(state_type, name);
        let event = self.inner.event;
        // Own the scan's coordinates so the stream borrows nothing from the
        // caller's `Scan<'_>`.
        let section = scan.section;
        let start: Bound<Coordinate> = scan.start.cloned();
        let end: Bound<Coordinate> = scan.end.cloned();
        let dir = scan.dir;
        let limit = scan.limit;
        let overlay = self.inner.overlay.clone();
        try_stream! {
            let scan = Scan {
                section,
                start: start.as_ref(),
                dir,
                end: end.as_ref(),
                limit,
            };
            let inner = overlay.scan_cells(&id, scan, event);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item?;
            }
        }
    }
}

impl<B, L> CellSession for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    async fn set(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
        value: &[u8],
    ) -> Result<(), StateAccessError> {
        let id = self.id_for(state_type, name);
        self.inner.overlay.buffer_set(&id, cell, value);
        Ok(())
    }

    async fn clear(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> Result<(), StateAccessError> {
        let id = self.id_for(state_type, name);
        self.inner.overlay.buffer_clear(&id, cell);
        Ok(())
    }

    async fn flush(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> Result<StoreOutcome, StateAccessError> {
        let id = self.id_for(state_type, name);
        let Some(value) = self.inner.overlay.dirty().lookup(&id, cell) else {
            return Ok(StoreOutcome::NoOp);
        };
        let collection_ref = self.ref_for(state_type, name);
        let data = dirty_data(value);
        self.inner
            .overlay
            .lower()
            .write_resolved(&collection_ref, &[(cell.clone(), data)])
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        self.inner.overlay.dirty().remove(&id, cell);
        Ok(StoreOutcome::Applied)
    }
}

impl<B, L> StateLifecycle for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    async fn finalize(&self) -> Result<FinalizeOutcome, StateAccessError> {
        let touched = self
            .inner
            .overlay
            .dirty()
            .touched(&self.inner.state_key.key);
        let event = self.inner.event;
        let registry = &self.inner.registry;
        let lower = self.inner.overlay.lower();
        let state_key = &self.inner.state_key;
        let staged: StagedSet = stream::iter(touched)
            .map(|((state_type, name), cells)| {
                let id = CollectionId::new(state_key.clone(), state_type, name);
                // `cooperative` adds a per-collection coop-budget checkpoint so a
                // key touching many collections does not drain the batch in one
                // poll; `buffer_unordered` keeps full concurrency.
                cooperative(stage_collection(lower, registry, event, id, cells))
            })
            .buffer_unordered(STATE_FANOUT_CONCURRENCY)
            .try_filter_map(|opt| async move { Ok(opt) })
            .try_collect()
            .await?;
        if staged.is_empty() {
            Ok(FinalizeOutcome::Clean)
        } else {
            *self.inner.staged.lock() = Some(staged);
            Ok(FinalizeOutcome::Staged)
        }
    }

    async fn commit_apply(&self) -> ApplyOutcome {
        self.resolve_staged(Resolve::Promote).await
    }

    async fn rollback_aborted(&self) -> ApplyOutcome {
        self.resolve_staged(Resolve::Rollback).await
    }

    fn register_marker(&self, dedup_id: Uuid) {
        *self.inner.marker.lock() = Some(dedup_id);
    }

    async fn flush_marker(&self) -> Result<(), StateAccessError> {
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
        // Per-key serialization means no handler op is in flight here.
        self.inner
            .overlay
            .dirty()
            .clear_event(&self.inner.state_key.key);
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
        let _ = self
            .inner
            .armed
            .insert_async(self.inner.state_key.key.clone())
            .await;
    }
}

/// Stages one collection's touched cells in a single batch, returning the
/// staged writes for the lifecycle to promote / roll back (or `None` for a
/// `ReadUncommitted` collection, which resolves at stage time). Free function
/// so no `self` borrow crosses the concurrent fan-out.
async fn stage_collection<S>(
    lower: &S,
    registry: &CollectionDefRegistry,
    event: EventRef,
    id: CollectionId,
    cells: impl IntoIterator<Item = (CellKey, DirtyVal)>,
) -> Result<Option<(CollectionRef, Vec<(CellKey, ProvisionalWrite)>)>, StateAccessError>
where
    S: CellStore,
{
    let collection_ref =
        CollectionRef::new(id.clone(), registry.ttl_for(id.state_type(), id.name()));
    match registry.commit_mode_for(id.state_type(), id.name()) {
        CommitMode::ReadCommitted => {
            let mut writes = Vec::new();
            for (cell, value) in cells {
                // The own-event committed read returns this event's `prev` while
                // its provisional cell stands, so a retry re-stages over the
                // same base (idempotent).
                let prev = lower
                    .get(&id, &cell, event)
                    .await
                    .map_err(|e| StateAccessError::store(&e))?;
                writes.push((cell, ProvisionalWrite::new(dirty_data(value), prev, event)));
            }
            lower
                .write_provisional(&collection_ref, &writes)
                .await
                .map_err(|e| StateAccessError::store(&e))?;
            Ok(Some((collection_ref, writes)))
        }
        CommitMode::ReadUncommitted => {
            let resolved: Vec<(CellKey, Option<Bytes>)> = cells
                .into_iter()
                .map(|(cell, value)| (cell, dirty_data(value)))
                .collect();
            lower
                .write_resolved(&collection_ref, &resolved)
                .await
                .map_err(|e| StateAccessError::store(&e))?;
            Ok(None)
        }
    }
}

/// The committed bytes a buffered outcome stages to (`Set` → its bytes,
/// `Cleared` → absence).
fn dirty_data(value: DirtyVal) -> Option<Bytes> {
    match value {
        DirtyVal::Set(bytes) => Some(bytes),
        DirtyVal::Cleared => None,
    }
}

/// Crate-private descriptor the framework uses to reach a session's staged
/// lifecycle through the one public [`EventContext::state`] method.
///
/// [`EventContext::state`]: crate::consumer::event_context::EventContext::state
#[derive(Clone, Copy, Debug)]
pub(crate) struct LifecycleAccess;

impl DescriptorIdentity for LifecycleAccess {
    fn name(&self) -> &'static str {
        "\u{0}lifecycle"
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Value,
            codec_id: "\u{0}framework-lifecycle",
            resolver_id: None,
            key_codec_id: None,
        }
    }
}

impl StateDescriptor for LifecycleAccess {
    type Handle<S: CellRead> = LifecycleView<S>;

    fn bind<S: CellRead>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError> {
        Ok(LifecycleView {
            session: session.clone(),
        })
    }

    /// No-op: the lifecycle handle carries no operational settings, so it
    /// keeps the default [`collection_def`](StateDescriptor::collection_def)
    /// and the inherited fluent setters are unreachable no-ops.
    fn with_collection_def(self, _def: CollectionDef) -> Self {
        self
    }
}

/// Crate-private view over a session's staged lifecycle, returned by binding
/// [`LifecycleAccess`]. Its methods are gated on [`CellSession`] (the writer
/// session), so relaxing `bind` to [`CellRead`] does not leak the lifecycle.
pub(crate) struct LifecycleView<S> {
    session: S,
}

impl<S> LifecycleView<S>
where
    S: CellSession,
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

/// Crate-private extension giving every [`EventContext`] one-call access to its
/// session's staged lifecycle through the public [`EventContext::state`]
/// method.
pub(crate) trait LifecycleAccessExt: EventContext {
    /// Binds the session's lifecycle view.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError`] only when the context is terminated;
    /// [`LifecycleAccess`] is otherwise registration-independent.
    fn lifecycle(&self) -> Result<LifecycleView<Self::State>, StateAccessError> {
        self.state(Registered::new(LifecycleAccess))
    }
}

impl<C: EventContext> LifecycleAccessExt for C {}

/// Test-only stateless session: every state op reports
/// [`StateAccessError::Unavailable`] and the lifecycle is inert.
#[cfg(test)]
#[derive(Clone)]
pub struct UnavailableState<P> {
    loader: MemoryLoader<P>,
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
impl<P> sealed::ReadSessionMarker for UnavailableState<P> {}

#[cfg(test)]
impl<P> CellRead for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Loader = MemoryLoader<P>;
    type ScanError = StateAccessError;

    fn loader(&self) -> &Self::Loader {
        &self.loader
    }

    fn is_terminated(&self) -> bool {
        true
    }

    fn verify_state_registration(
        &self,
        _name: &'static str,
        _state_type: StateType,
        _identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn get(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    fn scan<'a>(
        &'a self,
        _state_type: StateType,
        _name: &'a StateName,
        _scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::ScanError>> + Send + 'a {
        stream::once(async { Err(StateAccessError::Unavailable) })
    }
}

#[cfg(test)]
impl<P> CellSession for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    async fn set(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
        _value: &[u8],
    ) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn clear(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
    ) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn flush(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
    ) -> Result<StoreOutcome, StateAccessError> {
        Err(StateAccessError::Unavailable)
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
