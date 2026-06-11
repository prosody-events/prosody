//! Per-event keyed-state sessions.
//!
//! A [`StateSession`] is the per-event view over a partition's keyed-state
//! stores: byte-cell reads and writes join one shared per-event transaction
//! map, and the framework drives the seal/apply lifecycle through a sealed
//! supertrait that downstream crates can neither implement nor call.
//!
//! [`ValueStateSession`] is the sole implementation — the real session,
//! minted per event by the partition's state manager. It holds the
//! partition-lifetime durable bundle plus a per-event dirty workspace scope;
//! clones share the transaction map, so repeated descriptor binds of the same
//! collection accumulate into one transaction. Keyed state is always wired, so
//! there is no stateless stub.
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
//! 2. On the final handler success, `finalize` seals every dirty `Wal`
//!    collection (recording the sealed set inside the session) and
//!    direct-applies `Direct` collections.
//! 3. Strictly after the seal, `flush_marker` writes the registered dedup
//!    marker through the commit oracle — so a present marker always certifies a
//!    durable seal. Then `commit_apply` / `rollback_aborted` resolve the
//!    recorded sealed set against the durable store.
//! 4. At attempt boundaries (retry, defer), `reset` discards the dirty scope,
//!    the transaction map, the sealed set, and the registered marker so the
//!    next attempt starts clean. A defer-swallow therefore flushes no marker,
//!    so the deferred reload is not deduped — with no flag and no outcome
//!    inspection.

use crate::consumer::event_context::StateAccessError;
use crate::consumer::partition::ShutdownPhase;
#[cfg(test)]
use crate::loader::MemoryLoader;
use crate::state::descriptor::{CellKind, DescriptorIdentity, StateDescriptor, StructuralIdentity};
use crate::state::oracle::CommitOracle;
use crate::state::registry::CollectionDefRegistry;
use crate::state::value::{
    DirectApplyStore, DurableWalStore, PendingOpSource, TransactionValueStore, ValueKind,
    ValueStore,
};
use crate::state::{
    CollectionId, CollectionKindId, CollectionRef, CommitMode, DirtyStoreProvider, EventRef,
    EventScopeId, Read, StateKey, StateName, StateType, StoreOutcome,
};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use parking_lot::Mutex as SyncMutex;
use sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::watch;
use tracing::warn;
use uuid::Uuid;

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

/// Bundle bound for the durable Value store a session composes with.
pub trait DurableValueBundle:
    ValueStore<Error = <Self as DurableWalStore<ValueKind>>::Error>
    + DurableWalStore<ValueKind>
    + DirectApplyStore<ValueKind, Error = <Self as DurableWalStore<ValueKind>>::Error>
    + fmt::Debug
    + Clone
    + Send
    + Sync
    + 'static
{
}

impl<T> DurableValueBundle for T where
    T: ValueStore<Error = <T as DurableWalStore<ValueKind>>::Error>
        + DurableWalStore<ValueKind>
        + DirectApplyStore<ValueKind, Error = <T as DurableWalStore<ValueKind>>::Error>
        + fmt::Debug
        + Clone
        + Send
        + Sync
        + 'static
{
}

/// Per-event keyed-state session: typed descriptor handles operate over
/// its byte cells, the framework drives its (crate-sealed) lifecycle.
///
/// Sessions are cheap `Arc`-backed clones; every clone shares the same
/// per-event transaction map, so repeated descriptor binds of one
/// collection accumulate into one transaction. The trait is sealed —
/// downstream crates name it in bounds but cannot implement it or call
/// the lifecycle methods.
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

    /// Drains a collection's buffered ops directly to authoritative state
    /// and returns its transaction to `Clean`.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails
    /// (including an illegal flush after seal).
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
/// lifecycle: sealing, applying, and resetting are framework-only moves.
pub(crate) mod sealed {
    use super::{Future, StateAccessError, Uuid};
    use crate::timers::duration::CompactDuration;

    /// Whether `finalize` produced sealed collections.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum FinalizeOutcome {
        /// At least one `Wal` collection sealed; the caller must arm the
        /// `StateRecovery` backstop timer.
        Sealed,

        /// Nothing sealed: no collection was dirtied, or every dirty
        /// collection was `Direct` and applied during `finalize`.
        Clean,
    }

    /// Result of resolving the recorded sealed set in `commit_apply` /
    /// `rollback_aborted`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ApplyOutcome {
        /// No sealed set was recorded for this event — nothing to resolve.
        NothingSealed,

        /// Every recorded sealed collection resolved (applied to
        /// authoritative state and its WAL cleared).
        Resolved,

        /// At least one resolution failed; the per-key `StateRecovery`
        /// backstop (always left armed by the durability boundary) lets the
        /// sweep retry. The boundary logs this outcome.
        Incomplete,
    }

    /// Framework-only lifecycle over a per-event session.
    pub trait StateLifecycle {
        /// Resolves every touched collection by its commit mode: `Wal`
        /// collections seal (the sealed set is recorded inside the
        /// session), `Direct` collections apply.
        ///
        /// # Errors
        ///
        /// Returns a type-erased store error when sealing or applying
        /// fails; nothing is recorded in that case.
        fn finalize(
            &self,
        ) -> impl Future<Output = Result<FinalizeOutcome, StateAccessError>> + Send;

        /// Applies the recorded sealed set after the event committed.
        /// Best-effort: individual failures are logged and reported via
        /// [`ApplyOutcome::Incomplete`].
        fn commit_apply(&self) -> impl Future<Output = ApplyOutcome> + Send;

        /// Rolls the recorded sealed set back after the event aborted.
        /// Best-effort, symmetric with `commit_apply`.
        fn rollback_aborted(&self) -> impl Future<Output = ApplyOutcome> + Send;

        /// Buffers the message commit marker (`dedup_id`) for this event.
        ///
        /// Infallible and last-wins: the deduplication middleware calls it
        /// during unwind on a handler `Ok` or permanent error. The marker
        /// rides the session — not derived from the session's `EventRef` —
        /// because on a deferred-message reload the marker is the reloaded
        /// *message's* dedup id while the session seals under the *timer's*
        /// `EventRef`. The boundary flushes it strictly after the seal;
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

        /// Discards the per-event dirty scope, transaction map, recorded
        /// sealed set, and registered marker so the next attempt starts
        /// clean. Called at attempt boundaries (retry, defer-swallow).
        fn reset(&self);

        /// Delay between sealing and the `StateRecovery` sweep, used by
        /// the durability boundary to arm the backstop timer.
        fn recovery_fire_delay(&self) -> CompactDuration;
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

type ValueTx<D, S> = Arc<AsyncMutex<TransactionValueStore<D, S>>>;

/// The sealed set `finalize` records for the apply hooks.
///
/// The owning event is not stored here — it is always `self.inner.event`,
/// read at apply time.
struct SealedSet {
    collections: Vec<CollectionRef<ValueKind>>,
}

/// Which durable resolution the apply hooks drive the recorded sealed set
/// through after the event's durability marker settles.
#[derive(Clone, Copy, Debug)]
enum Resolution {
    /// The event committed: apply each sealed collection.
    Commit,

    /// The event aborted: roll each sealed collection back.
    Rollback,
}

/// Construction parameters for [`ValueStateSession`], bundled so the
/// constructor stays readable.
pub struct SessionParts<D, O, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    /// Partition-lifetime durable Value bundle.
    pub durable: D,

    /// Partition-lifetime commit oracle; the marker flush writes the
    /// message commit row through it.
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

    /// The event whose seals this session owns.
    pub event: EventRef,

    /// Delay between sealing and the `StateRecovery` sweep.
    pub recovery_delay: CompactDuration,

    /// Termination signals captured at mint.
    pub termination: TerminationWatch,
}

struct SessionInner<D, O, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    durable: D,
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
    termination: TerminationWatch,
    transactions: SyncMutex<HashMap<StateName, ValueTx<D, P::Store>>>,
    sealed: SyncMutex<Option<SealedSet>>,
    /// The registered message commit marker, flushed strictly after the
    /// seal and cleared by `reset`. `None` until the deduplication
    /// middleware registers it (or always, on the timer arm).
    marker: SyncMutex<Option<Uuid>>,
}

/// The real per-event session over a partition's Value stores.
///
/// One session is minted per event by the partition's state manager;
/// clones share the per-event transaction map, dirty scope, recorded
/// sealed set, and registered marker. `D` is the durable Value bundle, `O`
/// the commit oracle, `P` the per-partition dirty-workspace provider, `L`
/// the message loader.
pub struct ValueStateSession<D, O, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    inner: Arc<SessionInner<D, O, P, L>>,
}

impl<D, O, P, L> Clone for ValueStateSession<D, O, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<D, O, P, L> fmt::Debug for ValueStateSession<D, O, P, L>
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

impl<D, O, P, L> ValueStateSession<D, O, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    /// Creates a session for one event, opening a fresh dirty scope.
    #[must_use]
    pub fn new(parts: SessionParts<D, O, P, L>) -> Self {
        let SessionParts {
            durable,
            oracle,
            dirty,
            loader,
            registry,
            state_key,
            event,
            recovery_delay,
            termination,
        } = parts;
        let scope = dirty.for_scope(EventScopeId::fresh());
        Self {
            inner: Arc::new(SessionInner {
                durable,
                oracle,
                dirty_provider: dirty,
                dirty: SyncMutex::new(scope),
                loader,
                registry,
                state_key,
                event,
                recovery_delay,
                termination,
                transactions: SyncMutex::new(HashMap::new()),
                sealed: SyncMutex::new(None),
                marker: SyncMutex::new(None),
            }),
        }
    }

    /// The event this session's seals belong to (test observability).
    #[cfg(test)]
    pub(crate) fn event(&self) -> EventRef {
        self.inner.event
    }

    /// The currently registered marker (test observability).
    #[cfg(test)]
    pub(crate) fn registered_marker(&self) -> Option<Uuid> {
        *self.inner.marker.lock()
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

impl<D, O, P, L> ValueStateSession<D, O, P, L>
where
    D: Clone,
    P: DirtyStoreProvider<ValueKind>,
    P::Store: Clone,
{
    fn open_transaction(&self, name: &StateName) -> ValueTx<D, P::Store> {
        let mut txs = self.inner.transactions.lock();
        if let Some(existing) = txs.get(name) {
            return existing.clone();
        }

        let collection = self.collection_ref_for(name);
        let mode = self.inner.registry.commit_mode_for(name);
        let tx = TransactionValueStore::new(
            self.inner.durable.clone(),
            self.inner.dirty.lock().clone(),
            collection,
            self.inner.event,
            mode,
        );
        let handle = Arc::new(AsyncMutex::new(tx));
        txs.insert(name.clone(), handle.clone());
        handle
    }
}

impl<D, O, P, L> StateSession for ValueStateSession<D, O, P, L>
where
    D: DurableValueBundle,
    O: CommitOracle,
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
        let tx = self.open_transaction(name);
        let guard = tx.lock().await;
        let read = guard
            .get(&self.collection_id_for(name))
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        Ok(match read {
            Read::Present(payload) => Some(payload),
            Read::Absent | Read::Unknown => None,
        })
    }

    async fn set_state_cell(&self, name: &StateName, cell: Bytes) -> Result<(), StateAccessError> {
        let tx = self.open_transaction(name);
        let guard = tx.lock().await;
        guard
            .set(&self.collection_id_for(name), cell)
            .await
            .map_err(|e| StateAccessError::store(&e))
    }

    async fn clear_state_cell(&self, name: &StateName) -> Result<(), StateAccessError> {
        let tx = self.open_transaction(name);
        let guard = tx.lock().await;
        guard
            .clear(&self.collection_id_for(name))
            .await
            .map_err(|e| StateAccessError::store(&e))
    }

    async fn flush_state_cell(&self, name: &StateName) -> Result<StoreOutcome, StateAccessError> {
        let tx = self.open_transaction(name);
        let mut guard = tx.lock().await;
        guard.flush().await.map_err(|e| StateAccessError::store(&e))
    }

    fn is_terminated(&self) -> bool {
        self.inner.termination.is_terminated()
    }
}

impl<D, O, P, L> ValueStateSession<D, O, P, L>
where
    D: DurableValueBundle,
    P: DirtyStoreProvider<ValueKind>,
{
    /// Drives the recorded sealed set through one durable resolution after
    /// the event's durability marker settles. Best-effort: individual
    /// failures are logged and surface as [`ApplyOutcome::Incomplete`] so
    /// the caller leaves the `StateRecovery` timer armed for the sweep.
    async fn resolve_sealed(&self, resolution: Resolution) -> ApplyOutcome {
        let Some(set) = self.inner.sealed.lock().take() else {
            return ApplyOutcome::NothingSealed;
        };
        let event = self.inner.event;
        let mut all_resolved = true;
        for collection_ref in &set.collections {
            let result = match resolution {
                Resolution::Commit => self.inner.durable.apply_sealed(collection_ref, event).await,
                Resolution::Rollback => {
                    self.inner
                        .durable
                        .rollback_sealed(collection_ref, event)
                        .await
                }
            };
            if let Err(error) = result {
                warn!(error = ?error, ?resolution, "sealed resolution failed");
                all_resolved = false;
            }
        }
        if all_resolved {
            ApplyOutcome::Resolved
        } else {
            ApplyOutcome::Incomplete
        }
    }
}

impl<D, O, P, L> StateLifecycle for ValueStateSession<D, O, P, L>
where
    D: DurableValueBundle,
    O: CommitOracle,
    P: DirtyStoreProvider<ValueKind>,
    P::Store: DirtyValueBundle + Send + Sync,
    L: Send + Sync + 'static,
{
    /// Seals dirty `Wal` collections and direct-applies `Direct` ones,
    /// recording the sealed set. **Idempotent under retry**: the durability
    /// boundary retries a transient seal failure in place, so a partial seal
    /// (collection A sealed, B failed) must not poison the re-run. An
    /// already-`Sealed` `Wal` collection re-collects into the sealed set
    /// rather than erroring `AlreadySealed`, and a `Finished` `Direct`
    /// collection (applied on a prior attempt) is tolerated.
    async fn finalize(&self) -> Result<FinalizeOutcome, StateAccessError> {
        use crate::state::value::TransactionValueStoreError;

        // Snapshot every touched collection under one lock, then drop the
        // sync guard before awaiting — the per-event map only grows, so
        // every name already maps to its open transaction.
        let transactions: Vec<(StateName, ValueTx<D, P::Store>)> = {
            let txs = self.inner.transactions.lock();
            txs.iter().map(|(n, tx)| (n.clone(), tx.clone())).collect()
        };
        let mut sealed = Vec::new();
        for (name, tx) in transactions {
            let mut guard = tx.lock().await;
            match self.inner.registry.commit_mode_for(&name) {
                CommitMode::Wal => match guard.seal().await {
                    Ok(sealed_collection) => sealed.push(sealed_collection.collection().clone()),
                    Err(TransactionValueStoreError::NoPendingOps) => {
                        // Touched the collection (e.g. via `get`) but
                        // never mutated it — nothing to seal.
                    }
                    // A prior attempt of this same finalize already sealed
                    // this collection; re-collect it so the retry records the
                    // complete sealed set instead of failing forever.
                    Err(TransactionValueStoreError::AlreadySealed) => {
                        sealed.push(self.collection_ref_for(&name));
                    }
                    Err(err) => return Err(StateAccessError::store(&err)),
                },
                CommitMode::Direct => match guard.direct_apply().await {
                    Ok(_)
                    | Err(
                        TransactionValueStoreError::NoPendingOps
                        | TransactionValueStoreError::Finished,
                    ) => {}
                    Err(err) => return Err(StateAccessError::store(&err)),
                },
            }
        }
        if sealed.is_empty() {
            Ok(FinalizeOutcome::Clean)
        } else {
            *self.inner.sealed.lock() = Some(SealedSet {
                collections: sealed,
            });
            Ok(FinalizeOutcome::Sealed)
        }
    }

    async fn commit_apply(&self) -> ApplyOutcome {
        self.resolve_sealed(Resolution::Commit).await
    }

    async fn rollback_aborted(&self) -> ApplyOutcome {
        self.resolve_sealed(Resolution::Rollback).await
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
        // Swap in a fresh dirty scope first so no cleared transaction can
        // observe the old workspace through a racing re-open.
        *self.inner.dirty.lock() = self.inner.dirty_provider.for_scope(EventScopeId::fresh());
        self.inner.transactions.lock().clear();
        *self.inner.sealed.lock() = None;
        *self.inner.marker.lock() = None;
    }

    fn recovery_fire_delay(&self) -> CompactDuration {
        self.inner.recovery_delay
    }
}

/// Crate-private descriptor the framework uses to reach a session's
/// sealed lifecycle through the one public [`EventContext::state`] method.
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
            schema_label: None,
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

/// Crate-private view over a session's sealed lifecycle, returned by
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
    /// Returns a type-erased store error when sealing or applying fails.
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
        ApplyOutcome::NothingSealed
    }

    async fn rollback_aborted(&self) -> ApplyOutcome {
        ApplyOutcome::NothingSealed
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
}

#[cfg(test)]
mod tests;
