//! The per-event keyed-state session and its operations.

use super::sealed::{MutatePermit, OpPermit, SessionGate};
use super::stage::write_direct;
use super::{AttemptEpoch, SessionInner, SessionParts};
use crate::state::access::StateAccessError;
use crate::state::cell::{Committed, Projection};
use crate::state::cell_key::{CellKey, CellRef, Scan, Section};
use crate::state::descriptor::StructuralIdentity;
use crate::state::identity::{CollectionId, CollectionRef};
use crate::state::overlay::Overlay;
use crate::state::registry::CollectionDef;
use crate::state::store::{CellBuffer, CellRead, ReadBatch};
use crate::state::{StateBackend, StateName, StateType, StoreOutcome};
use async_stream::try_stream;
use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use parking_lot::{Mutex as SyncMutex, RwLock};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

/// The real per-event session over a partition's cell store.
///
/// One session is minted per event by the partition's state manager; clones
/// share the per-event dirty overlay and reload-marker override plus the
/// cross-event singletons. `B` is the per-partition `StateBackend` bundle;
/// `L` is the message loader.
pub struct KeyedStateSession<B, L>
where
    B: StateBackend,
{
    pub(super) inner: Arc<SessionInner<B, L>>,
    /// This clone's pinned attempt epoch, copied verbatim by [`Clone`] and
    /// living OUTSIDE the shared `inner` so a leaked clone (or a clone of a
    /// clone) keeps its stale pin and can never re-pin itself. Only the
    /// crate-internal [`StateLifecycle::repin`]
    /// mints a clone re-pinned to the live epoch.
    pub(super) pinned: AttemptEpoch,
}

impl<B, L> Clone for KeyedStateSession<B, L>
where
    B: StateBackend,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            pinned: self.pinned,
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
            dedup,
            loader,
            registry,
            state_key,
            event,
            dedup_ttl,
            checks,
            termination,
        } = parts;
        Self {
            inner: Arc::new(SessionInner {
                stage_id: OnceLock::new(),
                overlay: Overlay::new(dirty, cell),
                dedup,
                loader,
                registry,
                state_key,
                event,
                dedup_ttl,
                checks,
                termination,
                reload_marker: SyncMutex::new(None),
                terminated: AtomicBool::new(false),
                gate: SessionGate::new(),
                epoch: RwLock::new(AttemptEpoch::INITIAL),
            }),
            // A freshly-minted session is attempt 1: `pinned == *epoch.read()`.
            pinned: AttemptEpoch::INITIAL,
        }
    }

    /// The collection id for `(state_type, name)` under this session's key.
    fn id_for(&self, state_type: StateType, name: &StateName) -> CollectionId {
        CollectionId::new(self.inner.state_key.clone(), state_type, name.clone())
    }

    /// The session's opaque capability slot — see
    /// [`StateSession::Loader`](crate::state::collection::StateSession::Loader).
    pub(in crate::state) fn message_loader(&self) -> &L {
        &self.inner.loader
    }

    /// The session's live attempt epoch. A one-line copy-out: the leaf
    /// `RwLock` read guard is dropped before returning, so it is never held
    /// across an `.await`.
    pub(super) fn current_epoch(&self) -> AttemptEpoch {
        *self.inner.epoch.read()
    }

    /// Bumps the live attempt epoch to the next value. The **only** epoch
    /// writer, with exactly one call site: inside
    /// [`StateLifecycle::reset`], under the
    /// held gate permit. Lock ordering is always gate → epoch (the gate is an
    /// async tokio mutex, this is a `parking_lot` leaf), so there is no
    /// sync lock-order cycle; the write guard is dropped before returning and
    /// never crosses an `.await`.
    pub(super) fn bump_epoch(&self) {
        let mut epoch = self.inner.epoch.write();
        *epoch = epoch.next();
    }

    /// Acquires the session operation gate for one command — the owner
    /// engine's read state. See [`SessionGate`].
    pub(in crate::state) async fn permit(&self) -> OpPermit<'_> {
        self.inner.gate.read().await
    }

    /// Whether this handle/session clone's pinned epoch still equals the live
    /// session epoch. `false` once a later attempt boundary
    /// ([`StateLifecycle::reset`]) bumped it — the pin half of the owner
    /// engine's live guard, of the mutator admission order, and of
    /// [`Self::rollback`]'s `NoOp` guard.
    pub(in crate::state) fn attempt_current(&self) -> bool {
        self.pinned == self.current_epoch()
    }

    /// The registry's operational settings for the name, defaults included. A
    /// collection binding captures it once, so every configuration query
    /// inside a scoped operation answers from one snapshot.
    pub(in crate::state) fn collection_def(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> CollectionDef {
        self.inner.registry.def_for(state_type, name)
    }

    /// Returns `true` once the partition is shutting down or the event has been
    /// cancelled. The owner engine guards every command on this.
    pub(in crate::state) fn is_terminated(&self) -> bool {
        self.inner.termination.is_terminated() || self.inner.terminated.load(Ordering::Relaxed)
    }

    /// Validates that the keyed-state collection named `(state_type, name)` is
    /// registered with the asserted structural identity, returning the
    /// canonical [`StateName`].
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unregistered`] for an unknown name, or
    /// [`StateAccessError::IdentityMismatch`] when the registered identity
    /// differs from the asserted one.
    pub(in crate::state) fn verify_state_registration(
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

    /// Reads one projected cell through the event overlay.
    ///
    /// # Errors
    ///
    /// Returns the store error.
    pub(in crate::state) async fn get<P: Projection>(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: CellRef<'_>,
    ) -> Result<Option<P::Payload>, StateAccessError>
    where
        B::Cell: CellRead<P>,
    {
        let id = self.id_for(state_type, name);
        let committed = self
            .inner
            .overlay
            .get::<P>(&id, cell)
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        Ok(committed.into_inner())
    }

    /// Reads one projected answer for each coordinate in input order.
    ///
    /// # Errors
    ///
    /// Returns the store error.
    pub(in crate::state) async fn get_many<P: Projection>(
        &self,
        state_type: StateType,
        name: &StateName,
        section: Section,
        batch: &ReadBatch<'_>,
    ) -> Result<CellBuffer<Option<P::Payload>>, StateAccessError>
    where
        B::Cell: CellRead<P>,
    {
        let id = self.id_for(state_type, name);
        let committed = self
            .inner
            .overlay
            .get_many::<P>(&id, section, batch)
            .await?;
        Ok(committed.into_iter().map(Committed::into_inner).collect())
    }

    /// The single-section, start-anchored, bidirectional range primitive: a
    /// lazy stream of the visible committed cells in `coordinate` byte order.
    pub(in crate::state) fn scan<'a, P: Projection>(
        &'a self,
        state_type: StateType,
        name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), StateAccessError>> + Send + use<'a, B, L, P>
    where
        B::Cell: CellRead<P>,
    {
        let id = self.id_for(state_type, name);
        // `id` is local to the generator, so `scan` unifies its lifetime
        // with an owned overlay; the caller's `Copy` `Scan<'a>` rides in
        // directly (it is covariant, so it coerces to that shorter scope).
        let overlay = self.inner.overlay.clone();
        try_stream! {
            let inner = overlay.scan::<P>(&id, scan);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item.map_err(|e| StateAccessError::store(&e))?;
            }
        }
    }

    /// Acquires the operation gate for a mutator, then applies the one total
    /// admission order under the held permit before returning the witness:
    ///
    /// 1. **pin** — a stale attempt (this handle outlived its dispatch and the
    ///    epoch was bumped) errors [`StateAccessError::Terminated`].
    /// 2. **closed** — the settle boundary already closed the session, so an
    ///    own-event mutation past the settle window errors
    ///    [`StateAccessError::SessionClosed`].
    /// 3. **termination** — shutdown or cancellation errors
    ///    [`StateAccessError::Terminated`].
    ///
    /// The pin check runs first. So a mutation under a still-current pin
    /// classifies `SessionClosed` rather than `Terminated`, even under
    /// shutdown or cancellation.
    ///
    /// The permit is held across all three checks. The epoch bump needs the
    /// gate exclusively, so the pin stays stable between the check and the
    /// return.
    ///
    /// # Errors
    ///
    /// [`StateAccessError::Terminated`] on a stale attempt or a
    /// shutting-down/cancelled session, or [`StateAccessError::SessionClosed`]
    /// once the settle boundary has closed the session.
    pub(in crate::state) async fn mutate_permit(
        &self,
    ) -> Result<MutatePermit<'_>, StateAccessError> {
        let permit = self.permit().await;
        self.check_write_admission(&permit)?;
        Ok(MutatePermit::witness(permit))
    }

    /// The one total write-admission order, applied under a held `permit` — the
    /// single definition [`Self::mutate_permit`] runs to mint its witness and
    /// the owner engine re-runs before each staged mutation. See
    /// [`Self::mutate_permit`] for the order and why it is that order.
    ///
    /// # Errors
    ///
    /// As [`Self::mutate_permit`].
    pub(in crate::state) fn check_write_admission(
        &self,
        permit: &OpPermit<'_>,
    ) -> Result<(), StateAccessError> {
        if !self.attempt_current() {
            return Err(StateAccessError::Terminated);
        }
        if permit.is_closed() {
            return Err(StateAccessError::SessionClosed);
        }
        if self.is_terminated() {
            return Err(StateAccessError::Terminated);
        }
        Ok(())
    }

    /// Stages one already-encoded mutation into this event's dirty overlay —
    /// the synchronous, infallible sink a scoped collection invocation replays
    /// its journal through. `Some(bytes)` stages a write, `None` an absence.
    ///
    /// `permit` is the admission witness: the replay runs under the write hold
    /// the invocation took, and a `&MutatePermit` derefs to one, so "staged
    /// without the gate" does not compile. It is never inspected.
    pub(in crate::state) fn stage_cell(
        &self,
        _permit: &OpPermit<'_>,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
        value: Option<Bytes>,
    ) {
        let id = self.id_for(state_type, name);
        let dirty = self.inner.overlay.dirty();
        match value {
            Some(bytes) => dirty.set_owned(&id, cell, bytes),
            None => dirty.clear(&id, cell),
        }
    }

    /// Stages one section's dirty clear marker — the sink a whole-layout reset
    /// replays through, one entry per declared section. Within this event the
    /// section reads as "deleted at this program point": a read answers
    /// absence, a scan yields only cells staged after the clear, and later
    /// writes repopulate it. Same admission witness as [`Self::stage_cell`].
    pub(in crate::state) fn stage_section_clear(
        &self,
        _permit: &OpPermit<'_>,
        state_type: StateType,
        name: &StateName,
        section: Section,
    ) {
        let id = self.id_for(state_type, name);
        self.inner.overlay.dirty().clear_section(&id, section);
    }

    /// Durably commits the collection's buffered changes mid-handler — the
    /// engine command behind every handle's `commit()`. The contract lives on
    /// the [`collection`](crate::state::collection) module's mid-handler
    /// durability section.
    ///
    /// Returns [`StoreOutcome::Applied`] when buffered ops were written, or
    /// [`StoreOutcome::NoOp`] when nothing was buffered.
    ///
    /// `permit` is the admission witness: the whole sequence runs under the
    /// caller's hold, so it never re-enters the non-reentrant gate. It is never
    /// inspected. [`Self::rollback`] takes no witness, because it acquires the
    /// gate itself.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Store`] when the underlying store fails (the
    /// buffer is left intact, so the ops still ride the normal commit path).
    pub(in crate::state) async fn commit(
        &self,
        _permit: &OpPermit<'_>,
        state_type: StateType,
        name: &StateName,
    ) -> Result<StoreOutcome, StateAccessError> {
        let id = self.id_for(state_type, name);
        let dirty = self.inner.overlay.dirty();
        let ttl = self.inner.registry.ttl_for(state_type, name);
        let collection_ref = CollectionRef::new(id.clone(), ttl);
        let written = write_direct(
            self.inner.overlay.lower(),
            &collection_ref,
            &dirty.cleared_sections(&id),
            dirty.collection_snapshot(&id),
        )
        .await?;
        if !written {
            return Ok(StoreOutcome::NoOp);
        }
        // Drain only after the write landed: a store failure leaves the
        // buffer intact, so the ops still ride the normal commit path. The
        // drain also drops the collection's dirty clear markers — sound
        // because the clears were applied durably in the same write.
        dirty.remove_collection(&id);
        Ok(StoreOutcome::Applied)
    }

    /// Discards the collection's buffered uncommitted ops mid-handler — the
    /// engine command behind every handle's `rollback()`, and
    /// [`Self::commit`]'s discard twin. The contract lives on the
    /// [`collection`](crate::state::collection) module's mid-handler durability
    /// section.
    ///
    /// Returns [`StoreOutcome::Applied`] when buffered ops were discarded, or
    /// [`StoreOutcome::NoOp`] on an empty, closed, stale-pinned, or terminated
    /// session.
    pub(in crate::state) async fn rollback(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> StoreOutcome {
        // The gate acquire for rollback lives HERE, not in the engine (every
        // handle path is a single delegating call): the drain must serialize
        // with commit()'s snapshot→write→drain (KV4), and rollback's
        // infallible contract needs the gate's phase — a CLOSED session (the
        // settle boundary already snapshotted it) discards nothing, expressed
        // as a NoOp because the signature cannot surface `SessionClosed`.
        let permit = self.permit().await;
        // The infallible signature reports every admission refusal as `NoOp`.
        if self.check_write_admission(&permit).is_err() {
            return StoreOutcome::NoOp;
        }
        let id = self.id_for(state_type, name);
        let dirty = self.inner.overlay.dirty();
        // Peek-then-drain is race-free under the held gate permit: no other
        // session op can interleave between the probe and the drain.
        if !dirty.collection_dirty(&id) {
            return StoreOutcome::NoOp;
        }
        dirty.remove_collection(&id);
        StoreOutcome::Applied
    }
}
