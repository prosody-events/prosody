//! The owner engine: collection operations over a per-event session.
//!
//! Read admission is the session gate's read permit and write admission its
//! mutate permit, so a scoped operation holds exactly the hold the old
//! per-method acquire took. Reads keep the existing storage-tier order — event
//! dirty overlay, then the committed cache, then the cold path — and an
//! operation neither owns nor warms that cache: creating or dropping one has no
//! effect on cache warmth.

use super::{Mutation, MutationJournal, StateSession, WritableStateSession, ensure_live, sealed};
use crate::state::access::StateAccessError;
use crate::state::cell_key::CellKey;
use crate::state::session::{CellRead, CellWrite, KeyedStateSession};
use crate::state::{StateBackend, StateName, StateType, StoreOutcome};
use bytes::Bytes;

/// The engine every per-event session binds.
///
/// `pub` is forced: it is the value of the sealed `Session::Engine` associated
/// type, which is itself `pub`. The module's `pub(crate)` visibility is the
/// seal — nothing re-exports the type, and every method it carries lives on a
/// trait a downstream crate cannot name.
pub struct OwnerEngine;

/// Bridge: the owner engine still drives the session's cell-command surface
/// (`get`, `mutate_permit`, `commit`, `rollback`, and the synchronous overlay
/// staging). The commands die with that surface, once Map and Deque also run
/// through the engine and the capability traits collapse into
/// [`StateSession`].
impl<S: CellRead> sealed::ReadEngine<S> for OwnerEngine {
    type ReadInner<'a>
        = S::Permit<'a>
    where
        S: 'a;

    async fn begin_read(session: &S) -> S::Permit<'_> {
        session.permit().await
    }

    /// The `_inner` permit is the admission witness the read demands; the
    /// session call does not take it, but the returned future captures the
    /// borrow, so the gate stays held while the read runs.
    async fn read_point(
        session: &S,
        _inner: &mut S::Permit<'_>,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        ensure_live(session)?;
        session.get(state_type, name, cell).await
    }
}

impl<S: CellWrite> sealed::WriteEngine<S> for OwnerEngine {
    type WriteInner<'a>
        = S::MutatePermit<'a>
    where
        S: 'a;

    async fn begin_write(session: &S) -> Result<S::MutatePermit<'_>, StateAccessError> {
        session.mutate_permit().await
    }

    /// The same total admission order [`CellWrite::mutate_permit`] applied at
    /// the start of the invocation, re-applied under the still-held permit.
    fn validate_write(session: &S, inner: &S::MutatePermit<'_>) -> Result<(), StateAccessError> {
        if !session.attempt_current() {
            return Err(StateAccessError::Terminated);
        }
        if inner.is_closed() {
            return Err(StateAccessError::SessionClosed);
        }
        if session.is_terminated() {
            return Err(StateAccessError::Terminated);
        }
        Ok(())
    }

    fn apply(
        session: &S,
        state_type: StateType,
        name: &StateName,
        inner: &S::MutatePermit<'_>,
        journal: MutationJournal,
    ) {
        // The mutate permit derefs to the read permit the staging sink demands
        // as its admission witness.
        let permit = &**inner;
        for mutation in journal {
            match mutation {
                Mutation::Set { cell, bytes } => {
                    session.stage_cell(permit, state_type, name, &cell, Some(bytes));
                }
                Mutation::Clear { cell } => {
                    session.stage_cell(permit, state_type, name, &cell, None);
                }
            }
        }
    }

    async fn commit(
        session: &S,
        state_type: StateType,
        name: &StateName,
    ) -> Result<StoreOutcome, StateAccessError> {
        let _permit = session.mutate_permit().await?;
        ensure_live(session)?;
        session.commit(state_type, name).await
    }

    async fn rollback(session: &S, state_type: StateType, name: &StateName) -> StoreOutcome {
        // Unwitnessed by design: the session owns rollback's gate acquire, so
        // taking a permit here would re-enter the non-reentrant gate.
        session.rollback(state_type, name).await
    }
}

impl<B, L> sealed::Session for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    type Engine = OwnerEngine;
}

impl<B, L> sealed::WritableSession for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
}

impl<B, L> StateSession for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    type Loader = L;

    fn loader(&self) -> &L {
        self.message_loader()
    }
}

impl<B, L> WritableStateSession for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
}
