//! Per-event keyed-state sessions.
//!
//! A session is the per-event view over a partition's keyed-state cell store:
//! byte-cell reads and writes buffer in a per-event dirty overlay, and the
//! framework drives the stage/promote lifecycle through a sealed supertrait
//! that downstream crates can neither implement nor call.
//!
//! [`KeyedStateSession`] is the sole implementation — the real session, minted
//! per event by the partition's state manager. It holds **one uniform
//! `Overlay`** (the per-event `DirtyStore` over the partition's committed
//! cell store) plus the cross-event singletons (the commit oracle, the
//! registered marker, the armed backstop, the event, the registry, …). Clones
//! share the per-event state, so repeated descriptor binds of one collection
//! accumulate into one write.
//!
//! # The session / lifecycle split
//!
//! The session surface is two traits:
//!
//! - [`CellSession`] — the read/buffer/mutate surface handlers reach through
//!   collection handles: `get`/`scan` + the buffering mutators
//!   `set`/`clear`/`clear_section` and the mid-handler `checkpoint`, plus
//!   `loader`/`is_terminated`/`verify_state_registration`.
//!   [`EventContext::State`] bounds this.
//! - `sealed::StateLifecycle` — the sealed, manager-driven lifecycle
//!   (`finalize`/`commit_apply`/`rollback_aborted`/marker/reset), a
//!   `pub(crate)` supertrait of [`CellSession`] that seals it: downstream
//!   crates can name `CellSession` in bounds but can neither implement it nor
//!   reach the lifecycle.
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

use crate::consumer::event_context::EventContext;
use crate::consumer::partition::ShutdownPhase;
use crate::state::access::StateAccessError;
use crate::state::cell::ProvisionalWrite;
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor::{
    DescriptorIdentity, Registered, StateDescriptor, StructuralIdentity,
};
use crate::state::dirty::{ClearedSections, DirtyStore, DirtyVal, ResolvedCells};
use crate::state::identity::{CollectionId, CollectionRef};
use crate::state::manager::ArmedKeys;
use crate::state::oracle::CommitOracle;
use crate::state::overlay::Overlay;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::CellStore;
use crate::state::{
    CollectionKindId, CommitMode, EventRef, SHARD_FANOUT_CONCURRENCY, STATE_FANOUT_CONCURRENCY,
    StateBackend, StateKey, StateName, StateType, StoreOutcome,
};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex as SyncMutex;
use sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::coop::cooperative;
use tracing::warn;
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// The per-event staged record: the provisional cells `finalize` staged,
/// grouped by collection (its ref carries the TTL), that the lifecycle
/// promotes or rolls back and that each collection's event marker lists.
///
/// Built exactly once per collection, at `finalize`, from the post-checkpoint
/// dirty buffer — checkpointed cells are already committed and never
/// marker-listed, so the record lists exactly the cells recovery owns. Only a
/// retry attempt re-running `finalize` rebuilds it, overwriting the same-event
/// marker idempotently. Only `ReadCommitted` collections appear;
/// `ReadUncommitted` writes resolve at stage time with nothing to settle. Each
/// `(cell, write)`'s `data` is the value to promote to, `prev` the committed
/// base to roll back to; a clears-only collection appears with an empty write
/// set — the entry that arms the recovery backstop.
type StagedSet = Vec<(CollectionRef, Vec<(CellKey, ProvisionalWrite)>)>;

/// The per-event session collections read, buffer, and mutate through.
///
/// `get`/`scan` describe the session's **visible committed bytes** for a cell —
/// [`KeyedStateSession`] realises that through the dirty overlay + oracle
/// resolution — and `set`/`clear`/`clear_section` buffer this event's
/// mutations (`checkpoint` writes them through mid-handler). The
/// framework reaches the manager-driven lifecycle through the sealed
/// `StateLifecycle` supertrait, which is what seals `CellSession`: downstream
/// crates can name it in bounds (e.g. [`EventContext::State`]) but can neither
/// implement it nor reach the lifecycle.
pub trait CellSession: StateLifecycle + Clone + Send + Sync + 'static {
    /// Opaque per-session capability slot. The keyed-state machinery never
    /// interprets it; a
    /// [`CellResolver`](crate::state::descriptor::CellResolver)
    /// living outside `src/state` reads it from the session at resolve time.
    type Loader: Clone + Send + Sync + 'static;

    /// Returns the session's capability slot for a resolver to read.
    fn loader(&self) -> &Self::Loader;

    /// Returns `true` once the partition is shutting down or the event has been
    /// cancelled. Descriptor handles guard every operation on this.
    fn is_terminated(&self) -> bool;

    /// Whether the collection named `(state_type, name)` carries a TTL — the
    /// query the Map bound refresh consults to keep its bound cells' TTL
    /// renewed on every `set`, so the bounds provably outlive every entry.
    /// No default impl: a silent `false` would disable the refresh for a
    /// real session.
    fn collection_has_ttl(&self, state_type: StateType, name: &StateName) -> bool;

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
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a;

    /// Buffers a set of the cell's bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session.
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
    /// Returns [`StateAccessError::Unavailable`] on a stateless session.
    fn clear(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

    /// Buffers an in-RAM dirty clear marker for one section of the
    /// collection: within this event the section reads as "deleted at this
    /// program point" — `get` answers absence, `scan` yields only cells set
    /// after the clear — and later `set`s repopulate it. Committed, the
    /// section holds exactly the survivors; aborted, it is untouched.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session.
    fn clear_section(
        &self,
        state_type: StateType,
        name: &StateName,
        section: Section,
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

    /// Durably commits the collection's buffered changes mid-handler, so they
    /// survive a restart after failure. This is why it exists: a complex or
    /// large handler (a materialization handler fanning one message into
    /// thousands of writes) checkpoints incremental progress and resumes from
    /// it on retry or redelivery instead of starting from scratch. Handler
    /// idempotence across the resume is the contract.
    ///
    /// Every currently-buffered op of the collection is written straight to
    /// committed state ([`write_resolved`]) in one same-partition batch and
    /// dropped from the dirty buffer, so multi-cell kinds checkpoint data and
    /// bookkeeping as a unit (a Map's entries and bound ratchets, a Deque's
    /// entries and window bounds). Valid in **both** commit modes. The
    /// guarantee is **at-least-once**: a checkpointed write is durable and
    /// visible immediately — never provisional, never listed in any event
    /// marker, never rolled back. Ops buffered *after* the checkpoint ride
    /// the collection's normal stage→settle path; reads already see buffered
    /// writes without checkpointing.
    ///
    /// Every collection handle exposes `checkpoint()` — Value, Map, and Deque
    /// all do, and future collection kinds must too; the handles' docs link
    /// back here.
    ///
    /// Returns [`StoreOutcome::Applied`] when buffered ops were written, or
    /// [`StoreOutcome::NoOp`] when nothing was buffered.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails (the
    /// buffer is left intact, so the ops still ride the normal commit path).
    ///
    /// [`write_resolved`]: crate::state::store::CellStore::write_resolved
    fn checkpoint(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> impl Future<Output = Result<StoreOutcome, StateAccessError>> + Send;
}

/// Crate-sealed lifecycle half of [`CellSession`].
///
/// The module is `pub(crate)`, so downstream crates can name [`CellSession`] in
/// bounds but can neither implement it nor reach the lifecycle: staging,
/// promoting, and resetting are framework-only moves.
pub(crate) mod sealed {
    use super::{CompactDateTime, CompactDuration, Future, StateAccessError, Uuid};

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

        /// At least one resolution failed. Recovery is guaranteed without any
        /// point-clear: the durability boundary never unschedules the per-key
        /// `StateRecovery` backstop (only the sweep's own fire clears it), so
        /// the standing backstop fires and the sweep retries; a
        /// transient sweep failure reschedules a fresh backstop, a
        /// permanent per-cell skip is left to first-touch and the key's
        /// next commit. The backstop aborts only on shutdown.
        Incomplete,
    }

    /// Framework-only lifecycle over a per-event session.
    pub trait StateLifecycle {
        /// Resolves every touched collection by its commit mode:
        /// `ReadCommitted` collections stage a provisional cell (the
        /// staged set is recorded), `ReadUncommitted` collections write
        /// a resolved value. Stages all collections before returning,
        /// so a stage error returns before the textually-later marker
        /// flush; a staging failure is a type-erased store error with
        /// nothing recorded.
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
        fn flush_marker(&self) -> impl Future<Output = Result<(), StateAccessError>> + Send;

        /// Discards the per-event dirty overlay and staged set, plus the
        /// registered marker, so the next attempt starts clean.
        fn reset(&self);

        /// Discards just this event's buffered dirty cells — the per-event
        /// lifecycle clear that
        /// [`EventStateScope`](crate::state::manager::EventStateScope)'s `Drop`
        /// runs on every exit path.
        ///
        /// The dirty workspace is partition-lifetime (manager-owned, shared by
        /// every session clone), so it must be cleared explicitly per event;
        /// the staged set and marker live on the per-event session and die with
        /// it, so they are not cleared here. Safe to run after dispatch
        /// returns: promote (`commit_apply` → `resolve_staged`) reads
        /// the recorded staged set, never dirty, so the buffer is dead
        /// once `finalize` has run.
        fn discard_dirty(&self);

        /// Delay between staging and the `StateRecovery` sweep: the
        /// `recovery_delay` floor tightened by the smallest `recovery_within`
        /// among the staged collections.
        fn recovery_fire_delay(&self) -> CompactDuration;

        /// The fire time of the `StateRecovery` backstop recorded as standing
        /// for this session's key, or `None` when none has been recorded this
        /// acquisition. `None` means *unknown*, not *unarmed*: the durable
        /// trigger store may still hold a prior epoch's backstop, which
        /// `arm_backstop` consults (and records here) before deciding.
        /// `arm_backstop` re-arms only when its new fire is sooner.
        fn backstop_armed(&self) -> impl Future<Output = Option<CompactDateTime>> + Send;

        /// Records that a `StateRecovery` backstop firing at `fire` now stands
        /// for this session's key (overwriting any earlier standing fire).
        fn mark_backstop_armed(&self, fire: CompactDateTime) -> impl Future<Output = ()> + Send;
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

/// Construction parameters for [`KeyedStateSession`], bundled so the
/// constructor stays readable.
pub struct SessionParts<B, L>
where
    B: StateBackend,
{
    /// The partition's uniform committed cell store (the session wraps it in a
    /// per-event `Overlay`).
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
    pub(crate) registry: Arc<CollectionDefRegistry>,

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
    /// a per-event `Overlay` over the shared dirty workspace.
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
                overlay: Overlay::new(dirty, cell),
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

    /// Resolves the recorded staged set after the event's outcome is known:
    /// `committed` ⇒ promote each cell's `data`, otherwise roll each back to
    /// its `prev`. The cell store's
    /// [`commit_provisional`](CellStore::commit_provisional) /
    /// [`abort_provisional`](CellStore::abort_provisional) carry the projection
    /// (so the write-through cache can publish it); the default impls degrade
    /// to the raw promote / `write_resolved(prev)`.
    ///
    /// Best-effort: drives every per-collection resolution to completion
    /// regardless of siblings, reporting [`ApplyOutcome::Incomplete`] if any
    /// failed (the backstop, always left armed, lets the sweep retry).
    async fn resolve_staged(&self, committed: bool) -> ApplyOutcome
    where
        L: Send + Sync + 'static,
    {
        let Some(staged) = self.inner.staged.lock().take() else {
            return ApplyOutcome::NothingStaged;
        };
        let lower = self.inner.overlay.lower();
        let all_resolved = stream::iter(staged)
            .map(|(collection_ref, writes)| {
                cooperative(async move {
                    let result = if committed {
                        // No section clears this phase (no producer); the
                        // marker delete is owned by the settle verb.
                        lower
                            .commit_provisional(&collection_ref, &writes, &[])
                            .await
                    } else {
                        lower.abort_provisional(&collection_ref, &writes).await
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

impl<B, L> CellSession for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    type Loader = L;

    fn loader(&self) -> &L {
        &self.inner.loader
    }

    fn is_terminated(&self) -> bool {
        self.inner.termination.is_terminated()
    }

    fn collection_has_ttl(&self, state_type: StateType, name: &StateName) -> bool {
        self.inner.registry.ttl_for(state_type, name).is_some()
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
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        let id = self.id_for(state_type, name);
        let event = self.inner.event;
        // `id` is local to the generator, so `scan_cells` unifies its lifetime
        // with an owned overlay; the caller's `Copy` `Scan<'a>` rides in
        // directly (it is covariant, so it coerces to that shorter scope).
        let overlay = self.inner.overlay.clone();
        try_stream! {
            let inner = overlay.scan_cells(&id, scan, event);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item.map_err(|e| StateAccessError::store(&e))?;
            }
        }
    }

    async fn set(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
        value: &[u8],
    ) -> Result<(), StateAccessError> {
        let id = self.id_for(state_type, name);
        self.inner.overlay.dirty().set(&id, cell, value);
        Ok(())
    }

    async fn clear(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> Result<(), StateAccessError> {
        let id = self.id_for(state_type, name);
        self.inner.overlay.dirty().clear(&id, cell);
        Ok(())
    }

    async fn clear_section(
        &self,
        state_type: StateType,
        name: &StateName,
        section: Section,
    ) -> Result<(), StateAccessError> {
        let id = self.id_for(state_type, name);
        self.inner.overlay.dirty().clear_section(&id, section);
        Ok(())
    }

    async fn checkpoint(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Result<StoreOutcome, StateAccessError> {
        let id = self.id_for(state_type, name);
        let resolved = self.inner.overlay.dirty().collection_snapshot(&id);
        if resolved.is_empty() {
            return Ok(StoreOutcome::NoOp);
        }
        let ttl = self.inner.registry.ttl_for(state_type, name);
        let collection_ref = CollectionRef::new(id.clone(), ttl);
        self.inner
            .overlay
            .lower()
            .write_resolved(&collection_ref, &resolved, &[])
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        // Drain only after the write landed: a store failure leaves the
        // buffer intact, so the ops still ride the normal commit path. The
        // drain also drops the collection's dirty clear markers — sound
        // because a cleared section's erase was expanded to per-cell clears
        // in the same write.
        self.inner.overlay.dirty().remove_collection(&id);
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
            .map(|((state_type, name), cleared, cells)| {
                let id = CollectionId::new(state_key.clone(), state_type, name);
                // `cooperative` adds a per-collection coop-budget yield point
                // so a key touching many collections does not drain the batch
                // in one poll; `buffer_unordered` keeps full concurrency.
                cooperative(stage_collection(lower, registry, event, id, cleared, cells))
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
        self.resolve_staged(true).await
    }

    async fn rollback_aborted(&self) -> ApplyOutcome {
        self.resolve_staged(false).await
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

    fn discard_dirty(&self) {
        // Per-key serialization means no handler op is in flight here.
        self.inner
            .overlay
            .dirty()
            .clear_event(&self.inner.state_key.key);
    }

    fn reset(&self) {
        self.discard_dirty();
        *self.inner.staged.lock() = None;
        *self.inner.marker.lock() = None;
    }

    fn recovery_fire_delay(&self) -> CompactDuration {
        // Tighten the always-on floor by the smallest `recovery_within` among
        // the collections this event staged. Runs after `finalize` records the
        // staged set, so a `Clean` (or not-yet-staged) event reads `None` and
        // keeps the floor. `recovery_within` on a `ReadUncommitted` collection
        // is inert: such collections never stage a provisional cell, so they
        // never appear here. The fold is sync (`recovery_within_for` is sync),
        // so no await runs under the `staged` lock.
        let floor = self.inner.recovery_delay;
        self.inner.staged.lock().as_ref().map_or(floor, |staged| {
            staged
                .iter()
                .filter_map(|(collection, _)| {
                    let id = collection.id();
                    self.inner
                        .registry
                        .recovery_within_for(id.state_type(), id.name())
                })
                .fold(floor, CompactDuration::min)
        })
    }

    async fn backstop_armed(&self) -> Option<CompactDateTime> {
        self.inner
            .armed
            .read_async(&self.inner.state_key.key, |_, &fire| fire)
            .await
    }

    async fn mark_backstop_armed(&self, fire: CompactDateTime) {
        self.inner
            .armed
            .upsert_async(self.inner.state_key.key.clone(), fire)
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
    cleared: ClearedSections,
    cells: impl IntoIterator<Item = (CellKey, DirtyVal)>,
) -> Result<Option<(CollectionRef, Vec<(CellKey, ProvisionalWrite)>)>, StateAccessError>
where
    S: CellStore,
{
    let collection_ref =
        CollectionRef::new(id.clone(), registry.ttl_for(id.state_type(), id.name()));
    match registry.commit_mode_for(id.state_type(), id.name()) {
        CommitMode::ReadCommitted => {
            let id = &id;
            // Read each touched cell's committed base concurrently: the
            // own-event committed read returns this event's `prev` while its
            // provisional cell stands, so a retry re-stages over the same base
            // (idempotent). `cooperative` adds a per-cell coop-budget
            // checkpoint; `buffer_unordered` keeps full concurrency. Reordering
            // is irrelevant — the cells are distinct coordinates landing in one
            // same-partition batch. These reads are all within this one
            // collection (one shard), so they fan out under the within-partition
            // `SHARD_FANOUT_CONCURRENCY`, not the cross-partition bound.
            let writes: Vec<(CellKey, ProvisionalWrite)> = stream::iter(cells)
                .map(|(cell, value)| {
                    let data = value.into_data();
                    cooperative(async move {
                        let prev = lower
                            .get(id, &cell, event)
                            .await
                            .map_err(|e| StateAccessError::store(&e))?;
                        Ok((cell, ProvisionalWrite::new(data, prev, event)))
                    })
                })
                .buffer_unordered(SHARD_FANOUT_CONCURRENCY)
                .try_collect()
                .await?;
            if writes.is_empty() {
                // Backstop-arming rule: a clears-only collection stages
                // nothing durable (its erase was expanded to per-cell clears
                // at the descriptor layer), but it still records an empty
                // staged-set entry so `finalize` reports
                // [`FinalizeOutcome::Staged`] and the boundary arms the
                // `StateRecovery` backstop.
                return Ok((!cleared.is_empty()).then(|| (collection_ref, Vec::new())));
            }
            // `finalize` builds the staged record exactly once per collection
            // from the post-checkpoint dirty buffer, so `staged` IS this
            // stage's writes; only a retry attempt re-running `finalize`
            // re-stages (an idempotent same-event marker overwrite). `clears`
            // stay empty: a cleared section's erase rides the per-cell clears
            // above.
            lower
                .write_provisional(&collection_ref, &writes, &[], &writes)
                .await
                .map_err(|e| StateAccessError::store(&e))?;
            Ok(Some((collection_ref, writes)))
        }
        CommitMode::ReadUncommitted => {
            let resolved: ResolvedCells = cells
                .into_iter()
                .map(|(cell, value)| (cell, value.into_data()))
                .collect();
            if resolved.is_empty() {
                // A marker-only `ReadUncommitted` collection is a no-op: RU
                // never stages, so there is nothing for recovery to arm.
                return Ok(None);
            }
            lower
                .write_resolved(&collection_ref, &resolved, &[])
                .await
                .map_err(|e| StateAccessError::store(&e))?;
            Ok(None)
        }
    }
}

/// Crate-private descriptor the framework uses to reach a session through the
/// one public [`EventContext::state`] method — the sole state surface wrapper
/// contexts forward. Binding it yields the session itself (`Handle<S> = S`), so
/// the durability boundary calls the sealed [`StateLifecycle`] methods on the
/// session directly; a second required context method would burden every
/// wrapper and expose the raw session to handlers.
///
/// [`EventContext::state`]: crate::consumer::event_context::EventContext::state
#[derive(Clone, Copy, Debug)]
pub(crate) struct LifecycleAccess;

impl DescriptorIdentity for LifecycleAccess {
    /// Inert: [`LifecycleAccess::bind`](StateDescriptor::bind) returns the
    /// session verbatim without validating registration, and `LifecycleAccess`
    /// is never registered, so neither `name` nor `structural_identity` is ever
    /// consulted. They exist only to satisfy the [`StateDescriptor`]
    /// supertrait.
    fn name(&self) -> &'static str {
        "\u{0}lifecycle"
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Value,
            format_id: "\u{0}framework-lifecycle",
            resolver_id: None,
            key_format_id: "\u{0}framework-lifecycle",
        }
    }
}

impl StateDescriptor for LifecycleAccess {
    type Handle<S: CellSession> = S;

    /// Returns the session itself — the lifecycle tunnel binds no typed handle
    /// and validates no registration; the boundary drives the sealed
    /// [`StateLifecycle`] on the returned session.
    fn bind<S: CellSession>(self, session: &S) -> Result<S, StateAccessError> {
        Ok(session.clone())
    }

    /// No-op: the lifecycle tunnel carries no operational settings, so it keeps
    /// the default [`collection_def`](StateDescriptor::collection_def) and the
    /// inherited fluent setters are unreachable no-ops.
    fn with_collection_def(self, _def: CollectionDef) -> Self {
        self
    }
}

/// Crate-private extension giving every [`EventContext`] one-call access to its
/// per-event session through the public [`EventContext::state`] method. The
/// returned session exposes the sealed [`StateLifecycle`] the durability
/// boundary drives.
pub(crate) trait LifecycleAccessExt: EventContext {
    /// Binds the event's session through the lifecycle tunnel. Fails with
    /// [`StateAccessError`] only when the context is terminated;
    /// [`LifecycleAccess`] is otherwise registration-independent.
    fn lifecycle(&self) -> Result<Self::State, StateAccessError> {
        self.state(Registered::new(LifecycleAccess))
    }
}

impl<C: EventContext> LifecycleAccessExt for C {}
