//! Per-event keyed-state sessions.
//!
//! A [`StateSession`] is the per-event view over a partition's keyed-state
//! stores: byte-cell reads and writes buffer in per-event dirty workspaces, and
//! the framework drives the stage/promote lifecycle through a sealed supertrait
//! that downstream crates can neither implement nor call.
//!
//! [`KeyedStateSession`] is the sole implementation — the real session, minted
//! per event by the partition's state manager. It composes **one `Lane` per
//! collection kind** (`Lanes`): each lane owns that kind's
//! partition-lifetime [`PartitionStateStore`] plus a per-event dirty workspace,
//! and the session owns the cross-kind **singletons** (the commit oracle, the
//! registered marker, the armed backstop, the event, the registry, …). Clones
//! share the per-event state, so repeated descriptor binds of one collection
//! accumulate into one write. Keyed state is always wired, so there is no
//! stateless stub outside tests.
//!
//! # The kind seam
//!
//! [`StateSession`] is the **kind-agnostic** core (registration, loader,
//! termination, the sealed lifecycle). The only per-kind surface is
//! [`CellAccess<K>`], implemented once per lane: every kind's handle — Value's
//! `StateHandle`, future Map/Deque handles — bounds its op methods on
//! `CellAccess<K>` for its own `K` (Value's `CellAddr` is `()`). Adding a kind
//! is therefore additive: a new lane field (which fails every exhaustive
//! lifecycle destructure until wired), a `CellAccess<K>` impl, a `+
//! CellAccess<K>` bound on `EventContext::State`, and a sweep name set — the
//! marker, backstop, and `settle`/`abandon` machinery are untouched. The
//! generic `Lane` body is exercised on a non-Value kind by the direct
//! `Lane<CounterKind>` property test in the `lane` module's test submodule, so
//! the machinery the Value lane shares stays covered without a test kind in
//! production types.
//!
//! # Lifecycle
//!
//! The framework's per-event sequence, driven by the durability boundary
//! (`crate::consumer::middleware`'s blanket `EventHandler` impl) in
//! straight-line code:
//!
//! 1. Handler ops buffer into each kind's dirty scope through the cell methods;
//!    the deduplication middleware buffers the message's commit marker via
//!    `register_marker` during unwind.
//! 2. On the final handler success, `finalize` stages **every** lane (awaiting
//!    all via `try_join!`, so a lane error returns before the marker flush) —
//!    `ReadCommitted` collections stage provisional cells, `ReadUncommitted`
//!    ones write resolved values.
//! 3. Strictly after the stage, `flush_marker` writes the registered dedup
//!    marker through the commit oracle. After the offset/trigger commit,
//!    `commit_apply` promotes every lane's staged cells; `rollback_aborted`
//!    rolls them back. Both drive **all** lanes to completion (`join!`, not
//!    `try_join!`), so a failure in one kind never strands another.
//! 4. At attempt boundaries (retry, defer), `reset` discards every lane's dirty
//!    scope and staged set plus the registered marker.

use crate::Key;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::partition::ShutdownPhase;
#[cfg(test)]
use crate::loader::MemoryLoader;
use crate::state::descriptor::{CellKind, DescriptorIdentity, StateDescriptor, StructuralIdentity};
use crate::state::identity::{CollectionId, CollectionKind, CollectionRef};
use crate::state::oracle::CommitOracle;
use crate::state::partition_store::PartitionStateStore;
use crate::state::registry::CollectionDefRegistry;
use crate::state::value::ValueKind;
use crate::state::{
    CollectionKindId, EventRef, StateBackend, StateKey, StateName, StateType, StoreOutcome,
};
use crate::timers::duration::CompactDuration;
use ahash::RandomState;
use bytes::Bytes;
use lane::{Lane, Resolve};
use parking_lot::Mutex as SyncMutex;
use scc::HashSet as ConcurrentHashSet;
use sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::{join, try_join};
use uuid::Uuid;

mod lane;

#[cfg(test)]
mod tests;

/// Shared per-partition set of keys with a standing `StateRecovery` backstop.
///
/// A lock-free [`scc::HashSet`](ConcurrentHashSet) (not a `Mutex<HashSet>`):
/// the durability boundary touches it on every stateful commit, concurrently
/// across the partition's keys, so a single mutex would serialize unrelated
/// keys.
pub(crate) type ArmedKeys = Arc<ConcurrentHashSet<Key, RandomState>>;

/// Per-event keyed-state session: typed descriptor handles operate over its
/// byte cells, the framework drives its (crate-sealed) lifecycle.
///
/// Sessions are cheap `Arc`-backed clones; every clone shares the same
/// per-event lanes and singletons, so repeated descriptor binds of one
/// collection accumulate into one write. The trait is sealed — downstream
/// crates name it in bounds but cannot implement it or call the lifecycle
/// methods.
///
/// This is the **kind-agnostic** core: registration, the loader slot,
/// termination, and the sealed lifecycle. Per-kind cell access — the only
/// per-kind surface — lives on [`CellAccess<K>`], one impl per lane, so adding
/// a kind never touches this trait.
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

    /// Returns `true` once the partition is shutting down or the event has
    /// been cancelled. Descriptor handles guard every operation on this.
    fn is_terminated(&self) -> bool;
}

/// The uniform **addressed** point-cell access surface for one collection kind.
///
/// A handle for kind `K` (Value, Map, Deque, a sketch) bounds its op methods on
/// `S: CellAccess<K>` — exactly as [`StateHandle::get`] surfaces
/// `R: CellResolver<S>` — and translates its operations (`push`, `get_entry`,
/// `increment`, …) into these point-cell ops over the kind's
/// [`CellAddr`](CollectionKind::CellAddr) plus any header-cell maintenance. The
/// session impls one per lane, forwarding to that lane; the kind semantics
/// ([`combine`](CollectionKind::combine)/[`apply`](CollectionKind::apply)) live
/// in the kind, not here.
///
/// Multi-cell **scans** (Map `iterate`, Deque `range`) are deliberately *not*
/// expressible here — a future bulk-read surface adds them when scannable kinds
/// land.
///
/// [`StateHandle::get`]: crate::state::descriptor::StateHandle::get
pub trait CellAccess<K>: StateSession
where
    K: CollectionKind,
{
    /// Reads the cell's current visible value within this event's transaction.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn read_cell(
        &self,
        name: &StateName,
        addr: &K::CellAddr,
    ) -> impl Future<Output = Result<Option<Bytes>, StateAccessError>> + Send;

    /// Buffers a set of the cell's bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn set_cell(
        &self,
        name: &StateName,
        addr: &K::CellAddr,
        cell: &[u8],
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

    /// Buffers a clear of the cell.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn clear_cell(
        &self,
        name: &StateName,
        addr: &K::CellAddr,
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

    /// Writes the cell's buffered op straight to committed state — the
    /// mid-handler write-through escape hatch.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn flush_cell(
        &self,
        name: &StateName,
        addr: &K::CellAddr,
    ) -> impl Future<Output = Result<StoreOutcome, StateAccessError>> + Send;
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
        /// Resolves every touched collection across every kind by its commit
        /// mode: `ReadCommitted` collections stage a provisional cell (the
        /// staged set is recorded per lane), `ReadUncommitted` collections
        /// write a resolved value. Awaits **all** lanes before returning, so a
        /// lane error returns before the textually-later marker flush.
        ///
        /// # Errors
        ///
        /// Returns a type-erased store error when staging or writing fails;
        /// nothing is recorded for the failing lane in that case.
        fn finalize(
            &self,
        ) -> impl Future<Output = Result<FinalizeOutcome, StateAccessError>> + Send;

        /// Promotes every lane's recorded staged set after the event committed.
        /// Best-effort across lanes: individual failures are logged and folded
        /// into [`ApplyOutcome::Incomplete`], never cancelling a sibling lane.
        fn commit_apply(&self) -> impl Future<Output = ApplyOutcome> + Send;

        /// Rolls every lane's recorded staged set back after the event aborted.
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

        /// Discards every lane's per-event dirty scope and staged set, plus the
        /// registered marker, so the next attempt starts clean.
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

/// Folds the per-lane [`ApplyOutcome`]s of `commit_apply`/`rollback_aborted`
/// into one: `Incomplete` dominates (so the single backstop stays armed if any
/// kind failed), then `Resolved`, then `NothingStaged`.
fn merge_apply(outcomes: impl IntoIterator<Item = ApplyOutcome>) -> ApplyOutcome {
    outcomes
        .into_iter()
        .fold(ApplyOutcome::NothingStaged, |acc, next| match (acc, next) {
            (ApplyOutcome::Incomplete, _) | (_, ApplyOutcome::Incomplete) => {
                ApplyOutcome::Incomplete
            }
            (ApplyOutcome::Resolved, _) | (_, ApplyOutcome::Resolved) => ApplyOutcome::Resolved,
            _ => ApplyOutcome::NothingStaged,
        })
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

/// The session's per-kind lanes, projected from the one backend bundle `B`. The
/// destructure at every lifecycle fan-out site is **exhaustive** (no `..`), so
/// adding a kind's lane fails to compile until it is wired into
/// `finalize`/`commit_apply`/`rollback_aborted`/`reset` —
/// invalid-states-unrepresentable applied to the composition itself.
///
/// Today only the Value lane exists, so the fan-out sites are single-element
/// `try_join!`/`merge_apply([…])`; that shape is preserved deliberately (with a
/// comment at each site) so a future kind ANDs its lane in alongside Value
/// rather than a reader collapsing the array-of-one back to a scalar.
struct Lanes<B>
where
    B: StateBackend,
{
    value: Lane<ValueKind, B::ValueCell, B::Oracle, B::ValueCache>,
}

/// Construction parameters for [`KeyedStateSession`], bundled so the
/// constructor stays readable.
pub struct SessionParts<B, L>
where
    B: StateBackend,
{
    /// Partition-lifetime Value cell store + oracle + cache + status map.
    pub store: PartitionStateStore<ValueKind, B::ValueCell, B::Oracle, B::ValueCache>,

    /// Partition-lifetime commit oracle; the marker flush writes the
    /// message commit row through it. Same instance baked into `store`.
    pub oracle: B::Oracle,

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

struct SessionInner<B, L>
where
    B: StateBackend,
{
    lanes: Lanes<B>,
    oracle: B::Oracle,
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
    /// The registered message commit marker, flushed strictly after the stage
    /// and cleared by `reset`. A session-level singleton (never per lane), so
    /// "one marker, after every stage" is structural. `None` until the
    /// deduplication middleware registers it (or always, on the timer arm).
    marker: SyncMutex<Option<Uuid>>,
}

/// The real per-event session over a partition's cell stores.
///
/// One session is minted per event by the partition's state manager; clones
/// share the per-event lanes and singletons. `B` is the per-partition
/// [`StateBackend`] bundle (every lane's cell store + cache and the one shared
/// oracle, projected behind the single parameter); `L` is the message loader.
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
    /// Creates a session for one event, opening a fresh dirty scope per lane.
    #[must_use]
    pub fn new(parts: SessionParts<B, L>) -> Self {
        let SessionParts {
            store,
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
                lanes: Lanes {
                    value: Lane::new(store),
                },
                oracle,
                loader,
                registry,
                state_key,
                event,
                recovery_delay,
                armed,
                termination,
                marker: SyncMutex::new(None),
            }),
        }
    }

    /// The collection id for `name` under this session's key, for any kind
    /// `K` (the kind is carried only at the type level, so no bound is needed).
    fn id_for<K>(&self, name: &StateName) -> CollectionId<K> {
        CollectionId::new(
            self.inner.state_key.clone(),
            StateType::Application,
            name.clone(),
        )
    }

    /// The collection ref (id + registry TTL) for `name` under kind `K`.
    fn ref_for<K>(&self, name: &StateName) -> CollectionRef<K> {
        CollectionRef::new(self.id_for(name), self.inner.registry.ttl_for(name))
    }
}

/// Value access surface (`CellAddr = ()`), forwarding to the value lane — the
/// "one `CellAccess<K>` impl per lane" pattern, here for the Value lane.
impl<B, L> CellAccess<ValueKind> for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    async fn read_cell(
        &self,
        name: &StateName,
        addr: &(),
    ) -> Result<Option<Bytes>, StateAccessError> {
        let id = self.id_for(name);
        self.inner
            .lanes
            .value
            .read_cell(&id, addr, self.inner.event)
            .await
    }

    async fn set_cell(
        &self,
        name: &StateName,
        addr: &(),
        cell: &[u8],
    ) -> Result<(), StateAccessError> {
        let id = self.id_for(name);
        self.inner.lanes.value.set_cell(&id, addr, cell).await;
        Ok(())
    }

    async fn clear_cell(&self, name: &StateName, addr: &()) -> Result<(), StateAccessError> {
        let id = self.id_for(name);
        self.inner.lanes.value.clear_cell(&id, addr).await;
        Ok(())
    }

    async fn flush_cell(
        &self,
        name: &StateName,
        addr: &(),
    ) -> Result<StoreOutcome, StateAccessError> {
        let collection_ref = self.ref_for(name);
        self.inner
            .lanes
            .value
            .flush_cell(&collection_ref, addr)
            .await
    }
}

impl<B, L> StateSession for KeyedStateSession<B, L>
where
    B: StateBackend,
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

    fn is_terminated(&self) -> bool {
        self.inner.termination.is_terminated()
    }
}

impl<B, L> KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Send + Sync + 'static,
{
    /// Resolves every lane's staged set the same way and folds the per-lane
    /// outcomes — the shared body of `commit_apply`/`rollback_aborted`.
    ///
    /// `join!` (not `try_join!`): best-effort, drive every lane to completion,
    /// never cancel a sibling and strand its cells. `merge_apply` folds the
    /// outcomes so `Incomplete` dominates and the one backstop stays armed if
    /// any kind failed. The destructure is exhaustive (no `..`), so a new lane
    /// must be wired here too.
    async fn apply_all(&self, how: Resolve) -> ApplyOutcome {
        let Lanes { value } = &self.inner.lanes;
        // Single-element `join!`/array kept deliberately: a future kind adds its
        // `resolve` to the `join!` and its outcome to the array, and
        // `merge_apply` folds them. Do not collapse `[v]`/`join!` to a scalar.
        let (v,) = join!(value.resolve(how));
        merge_apply([v])
    }
}

impl<B, L> StateLifecycle for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Send + Sync + 'static,
{
    async fn finalize(&self) -> Result<FinalizeOutcome, StateAccessError> {
        // Exhaustive destructure (no `..`): a new lane field fails to compile
        // here until wired. `try_join!` awaits *all* lanes and returns a lane
        // error via `?` — before the marker flush in `settle` — so "every kind
        // staged" strictly precedes the single marker (invariant 1).
        let Lanes { value } = &self.inner.lanes;
        let event = self.inner.event;
        let registry = &self.inner.registry;
        // Single-element `try_join!`/tuple kept deliberately: a future kind adds
        // its `stage` to the `try_join!` and `|| kind_staged` to the fold below.
        // Do not collapse the tuple to a bare `await`.
        let (value_staged,) = try_join!(value.stage(event, registry))?;
        let staged = value_staged;
        Ok(if staged {
            FinalizeOutcome::Staged
        } else {
            FinalizeOutcome::Clean
        })
    }

    async fn commit_apply(&self) -> ApplyOutcome {
        self.apply_all(Resolve::Promote).await
    }

    async fn rollback_aborted(&self) -> ApplyOutcome {
        self.apply_all(Resolve::Rollback).await
    }

    fn register_marker(&self, dedup_id: Uuid) {
        *self.inner.marker.lock() = Some(dedup_id);
    }

    async fn flush_marker(&self) -> Result<(), StateAccessError> {
        // Read without taking: the slot clears only after the oracle write
        // succeeds, so a transient failure leaves the marker for the boundary
        // to retry.
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
        // Per-key serialization means no handler op is in flight here, so there
        // is no racing observer to guard against. Exhaustive destructure (no
        // `..`) kept deliberately: a future kind's lane must `reset()` here too.
        let Lanes { value } = &self.inner.lanes;
        value.reset();
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

    fn is_terminated(&self) -> bool {
        true
    }
}

/// The stub refuses Value cell access too, so a context carrying it satisfies
/// the `CellAccess<ValueKind>` bound on `EventContext::State` while every op
/// still reports [`StateAccessError::Unavailable`]. (`bind` fails earlier at
/// `verify_state_registration`, so these are belt-and-suspenders.)
#[cfg(test)]
impl<P> CellAccess<ValueKind> for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    async fn read_cell(
        &self,
        _name: &StateName,
        _addr: &(),
    ) -> Result<Option<Bytes>, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn set_cell(
        &self,
        _name: &StateName,
        _addr: &(),
        _cell: &[u8],
    ) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn clear_cell(&self, _name: &StateName, _addr: &()) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn flush_cell(
        &self,
        _name: &StateName,
        _addr: &(),
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
