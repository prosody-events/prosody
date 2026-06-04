//! Per-event keyed-state sessions.
//!
//! A [`StateSession`] is the per-event view over a partition's keyed-state
//! stores: byte-cell reads and writes join one shared per-event transaction
//! map, and the framework drives the seal/apply lifecycle through a sealed
//! supertrait that downstream crates can neither implement nor call.
//!
//! Two implementations ship:
//!
//! * [`ValueStateSession`] — the real session, minted per event by the
//!   partition's state manager. Holds the partition-lifetime durable bundle
//!   plus a per-event dirty workspace scope; clones share the transaction map,
//!   so repeated descriptor binds of the same collection accumulate into one
//!   transaction.
//! * [`UnavailableState`] — the stateless stub used where keyed state is not
//!   wired (the plain consumer, most handler tests). Every operation reports
//!   [`StateAccessError::Unavailable`]; the lifecycle is a no-op.
//!
//! # Lifecycle
//!
//! The framework's per-event sequence over a session:
//!
//! 1. Handler ops buffer into the dirty scope through the byte-cell methods.
//! 2. On handler success, `finalize` seals every dirty `Wal` collection
//!    (recording the sealed set inside the session) and direct-applies `Direct`
//!    collections.
//! 3. After the durability marker resolves, `commit_apply` / `rollback_aborted`
//!    resolve the recorded sealed set against the durable store.
//! 4. At attempt boundaries (retry, defer), `reset` discards the dirty scope,
//!    the transaction map, and the sealed set so the next attempt starts clean.

use crate::consumer::event_context::StateAccessError;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::defer::message::MessageLoader;
use crate::consumer::partition::ShutdownPhase;
use crate::state::descriptor::{KafkaMessageRef, StructuralIdentity};
use crate::state::middleware::CollectionDefRegistry;
use crate::state::value::{
    DirectApplyStore, DurableWalStore, PendingOpSource, TransactionValueStore, ValueKind,
    ValueStore,
};
use crate::state::{
    CollectionId, CollectionRef, CommitMode, DirtyStoreProvider, EventRef, EventScopeId, Read,
    StateKey, StateName, StateType, StoreOutcome,
};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use parking_lot::Mutex as SyncMutex;
use sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::watch;
use tracing::warn;

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
    /// The consumer's message payload type, used by Kafka-message handles
    /// to return fully typed [`ConsumerMessage`]s.
    type Payload: Send + Sync + 'static;

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

    /// Loads the full consumer message referenced by a Kafka-message state
    /// cell, decoded by the consumer's own codec.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Load`] when the loader fails (Permanent for a
    /// deleted or compacted-away offset).
    fn load_message(
        &self,
        message_ref: KafkaMessageRef,
    ) -> impl Future<Output = Result<ConsumerMessage<Self::Payload>, StateAccessError>> + Send;

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
    use super::{Future, StateAccessError};
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

        /// Every recorded sealed collection resolved; the caller may clear
        /// the `StateRecovery` backstop timer.
        Resolved,

        /// At least one resolution failed; the caller must leave the
        /// `StateRecovery` timer armed so the sweep retries.
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

        /// Discards the per-event dirty scope, transaction map, and
        /// recorded sealed set so the next attempt starts clean. Called at
        /// attempt boundaries (retry, defer-swallow).
        fn reset(&self);

        /// Delay between sealing and the `StateRecovery` sweep, used by
        /// the lifecycle middleware to arm the backstop timer.
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
struct SealedSet {
    event: EventRef,
    collections: Vec<CollectionRef<ValueKind>>,
}

/// Construction parameters for [`ValueStateSession`], bundled so the
/// constructor stays readable.
pub struct SessionParts<D, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    /// Partition-lifetime durable Value bundle.
    pub durable: D,

    /// Partition-lifetime dirty-workspace provider; `reset` mints fresh
    /// scopes from it.
    pub dirty: P,

    /// Message loader Kafka-message collections resolve through.
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

struct SessionInner<D, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    durable: D,
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
}

/// The real per-event session over a partition's Value stores.
///
/// One session is minted per event by the partition's state manager;
/// clones share the per-event transaction map, dirty scope, and recorded
/// sealed set. `D` is the durable Value bundle, `P` the per-partition
/// dirty-workspace provider, `L` the message loader.
pub struct ValueStateSession<D, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    inner: Arc<SessionInner<D, P, L>>,
}

impl<D, P, L> Clone for ValueStateSession<D, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<D, P, L> fmt::Debug for ValueStateSession<D, P, L>
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

impl<D, P, L> ValueStateSession<D, P, L>
where
    P: DirtyStoreProvider<ValueKind>,
{
    /// Creates a session for one event, opening a fresh dirty scope.
    #[must_use]
    pub fn new(parts: SessionParts<D, P, L>) -> Self {
        let SessionParts {
            durable,
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

impl<D, P, L> ValueStateSession<D, P, L>
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

impl<D, P, L> StateSession for ValueStateSession<D, P, L>
where
    D: DurableValueBundle,
    P: DirtyStoreProvider<ValueKind>,
    P::Store: DirtyValueBundle + Send + Sync,
    L: MessageLoader + 'static,
{
    type Payload = L::Payload;

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

    async fn load_message(
        &self,
        message_ref: KafkaMessageRef,
    ) -> Result<ConsumerMessage<L::Payload>, StateAccessError> {
        self.inner
            .loader
            .load_message(message_ref.topic, message_ref.partition, message_ref.offset)
            .await
            .map_err(|e| StateAccessError::load(&e))
    }

    fn is_terminated(&self) -> bool {
        self.inner.termination.is_terminated()
    }
}

impl<D, P, L> StateLifecycle for ValueStateSession<D, P, L>
where
    D: DurableValueBundle,
    P: DirtyStoreProvider<ValueKind>,
    P::Store: DirtyValueBundle + Send + Sync,
    L: Send + Sync + 'static,
{
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
                    Err(err) => return Err(StateAccessError::store(&err)),
                },
                CommitMode::Direct => match guard.direct_apply().await {
                    Ok(_) | Err(TransactionValueStoreError::NoPendingOps) => {}
                    Err(err) => return Err(StateAccessError::store(&err)),
                },
            }
        }
        if sealed.is_empty() {
            Ok(FinalizeOutcome::Clean)
        } else {
            *self.inner.sealed.lock() = Some(SealedSet {
                event: self.inner.event,
                collections: sealed,
            });
            Ok(FinalizeOutcome::Sealed)
        }
    }

    async fn commit_apply(&self) -> ApplyOutcome {
        let Some(set) = self.inner.sealed.lock().take() else {
            return ApplyOutcome::NothingSealed;
        };
        let mut all_resolved = true;
        for collection_ref in &set.collections {
            if let Err(error) = self
                .inner
                .durable
                .apply_sealed(collection_ref, set.event)
                .await
            {
                warn!(error = ?error, "apply_sealed failed after commit");
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
        let Some(set) = self.inner.sealed.lock().take() else {
            return ApplyOutcome::NothingSealed;
        };
        let mut all_resolved = true;
        for collection_ref in &set.collections {
            if let Err(error) = self
                .inner
                .durable
                .rollback_sealed(collection_ref, set.event)
                .await
            {
                warn!(error = ?error, "rollback_sealed failed after abort");
                all_resolved = false;
            }
        }
        if all_resolved {
            ApplyOutcome::Resolved
        } else {
            ApplyOutcome::Incomplete
        }
    }

    fn reset(&self) {
        // Swap in a fresh dirty scope first so no cleared transaction can
        // observe the old workspace through a racing re-open.
        *self.inner.dirty.lock() = self.inner.dirty_provider.for_scope(EventScopeId::fresh());
        self.inner.transactions.lock().clear();
        *self.inner.sealed.lock() = None;
    }

    fn recovery_fire_delay(&self) -> CompactDuration {
        self.inner.recovery_delay
    }
}

/// Stateless [`StateSession`]: every operation reports
/// [`StateAccessError::Unavailable`] and the lifecycle is a no-op.
///
/// Used wherever keyed state is not wired — the plain consumer's partition
/// loop and handler tests that never touch state.
pub struct UnavailableState<P>(PhantomData<fn() -> P>);

impl<P> UnavailableState<P> {
    /// Creates the stateless session.
    #[must_use]
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<P> Default for UnavailableState<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P> Clone for UnavailableState<P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P> Copy for UnavailableState<P> {}

impl<P> fmt::Debug for UnavailableState<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UnavailableState")
    }
}

impl<P> StateSession for UnavailableState<P>
where
    P: Send + Sync + 'static,
{
    type Payload = P;

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

    async fn load_message(
        &self,
        _message_ref: KafkaMessageRef,
    ) -> Result<ConsumerMessage<P>, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    fn is_terminated(&self) -> bool {
        true
    }
}

impl<P> StateLifecycle for UnavailableState<P>
where
    P: Send + Sync + 'static,
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

    fn reset(&self) {}

    fn recovery_fire_delay(&self) -> CompactDuration {
        CompactDuration::MIN
    }
}

#[cfg(test)]
mod tests;
