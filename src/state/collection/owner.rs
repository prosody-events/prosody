//! The owner engine: collection operations over a per-event session.
//!
//! Read admission is the session gate's read permit and write admission its
//! mutate permit, so a scoped operation holds exactly one hold for its whole
//! body. Reads keep the existing storage-tier order — event dirty overlay, then
//! the committed cache, then the cold path — and an operation neither owns nor
//! warms that cache: creating or dropping one has no effect on cache warmth.
//!
//! The engine is the only caller of the session's crate-private commands, which
//! is what keeps every cell reachable through a scoped operation alone.

use super::{Mutation, MutationJournal, StateSession, WritableStateSession, sealed};
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor::StructuralIdentity;
use crate::state::registry::CollectionDef;
use crate::state::session::{KeyedStateSession, MutatePermit, OpPermit};
use crate::state::store::{CellBuffer, CoordinateBatch, PresenceBatch};
use crate::state::{StateBackend, StateName, StateType, StoreOutcome};
use bytes::Bytes;
use futures::stream::Stream;

/// The engine every per-event session binds.
///
/// `pub` is forced: it is the value of the sealed `Session::Engine` associated
/// type, which is itself `pub`. The module's `pub(crate)` visibility is the
/// seal — nothing re-exports the type, and every method it carries lives on a
/// trait a downstream crate cannot name.
pub struct OwnerEngine;

impl<B, L> sealed::ReadEngine<KeyedStateSession<B, L>> for OwnerEngine
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    /// The owner keeps nothing across a stream continuation: a chunk
    /// reacquires the gate from the session, which is what keeps the gate off
    /// the yield path.
    type Plan = ();
    type ReadInner<'a>
        = OpPermit<'a>
    where
        KeyedStateSession<B, L>: 'a;

    /// Validates the name against the partition's collection registry, and the
    /// asserted structural identity against the registered one.
    fn verify_registration(
        session: &KeyedStateSession<B, L>,
        name: &'static str,
        state_type: StateType,
        identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        session.verify_state_registration(name, state_type, identity)
    }

    /// The registry's definition for the name, defaults included.
    fn collection_def(
        session: &KeyedStateSession<B, L>,
        state_type: StateType,
        name: &StateName,
    ) -> CollectionDef {
        session.collection_def(state_type, name)
    }

    async fn begin_read(session: &KeyedStateSession<B, L>) -> OpPermit<'_> {
        session.permit().await
    }

    /// The `_inner` permit is the admission witness the read demands; the
    /// session call does not take it, but the returned future captures the
    /// borrow, so the gate stays held while the read runs.
    async fn read_point(
        session: &KeyedStateSession<B, L>,
        _inner: &mut Self::ReadInner<'_>,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        ensure_live(session)?;
        session.get(state_type, name, cell).await
    }

    /// The batch twin of [`Self::read_point`], with the same witness and the
    /// same guard.
    async fn read_batch(
        session: &KeyedStateSession<B, L>,
        _inner: &mut Self::ReadInner<'_>,
        state_type: StateType,
        name: &StateName,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        ensure_live(session)?;
        session.get_many(state_type, name, section, batch).await
    }

    async fn read_presence_batch(
        session: &KeyedStateSession<B, L>,
        _inner: &mut Self::ReadInner<'_>,
        state_type: StateType,
        name: &StateName,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<PresenceBatch, StateAccessError> {
        ensure_live(session)?;
        session
            .contains_many(state_type, name, section, batch)
            .await
    }

    fn capture(_inner: &OpPermit<'_>) {}

    /// Reacquires the gate for one continuation — the same acquire
    /// [`Self::begin_read`] performs, which is what makes a coordinate stream
    /// free of a gate hold across its yields.
    async fn resume<'a>(session: &'a KeyedStateSession<B, L>, (): &()) -> OpPermit<'a> {
        session.permit().await
    }

    fn page<'a>(
        session: &'a KeyedStateSession<B, L>,
        (): &'a (),
        state_type: StateType,
        name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        // Unwitnessed by design: a range pages gate-free, taking the gate only
        // for the planning command that preceded it.
        session.scan(state_type, name, scan)
    }

    fn page_keys<'a>(
        session: &'a KeyedStateSession<B, L>,
        (): &'a (),
        state_type: StateType,
        name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<CellKey, StateAccessError>> + Send + 'a {
        session.scan_keys(state_type, name, scan)
    }

    fn fence(session: &KeyedStateSession<B, L>) -> Result<(), StateAccessError> {
        ensure_live(session)
    }
}

impl<B, L> sealed::WriteEngine<KeyedStateSession<B, L>> for OwnerEngine
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    type WriteInner<'a>
        = MutatePermit<'a>
    where
        KeyedStateSession<B, L>: 'a;

    async fn begin_write(
        session: &KeyedStateSession<B, L>,
    ) -> Result<MutatePermit<'_>, StateAccessError> {
        session.mutate_permit().await
    }

    /// The same total admission order the session's `mutate_permit` applied at
    /// the start of the invocation, re-applied under the still-held permit.
    fn validate_write(
        session: &KeyedStateSession<B, L>,
        inner: &MutatePermit<'_>,
    ) -> Result<(), StateAccessError> {
        session.check_write_admission(inner)
    }

    fn apply(
        session: &KeyedStateSession<B, L>,
        state_type: StateType,
        name: &StateName,
        inner: &MutatePermit<'_>,
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
                Mutation::Reset { sections } => {
                    for section in sections {
                        session.stage_section_clear(permit, state_type, name, *section);
                    }
                }
            }
        }
    }

    async fn commit(
        session: &KeyedStateSession<B, L>,
        state_type: StateType,
        name: &StateName,
    ) -> Result<StoreOutcome, StateAccessError> {
        let permit = session.mutate_permit().await?;
        session.commit(&permit, state_type, name).await
    }

    async fn rollback(
        session: &KeyedStateSession<B, L>,
        state_type: StateType,
        name: &StateName,
    ) -> StoreOutcome {
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

/// Guards every owner command: a session whose partition is shutting down,
/// whose event is cancelled, or whose pinned attempt epoch no longer matches
/// the live one (a handle or stream leaked past its dispatch attempt) refuses
/// with [`StateAccessError::Terminated`]. The published reader has no such
/// state, so its engine needs no counterpart.
///
/// # Errors
///
/// [`StateAccessError::Terminated`], as above.
fn ensure_live<B, L>(session: &KeyedStateSession<B, L>) -> Result<(), StateAccessError>
where
    B: StateBackend,
{
    if session.is_terminated() || !session.attempt_current() {
        return Err(StateAccessError::Terminated);
    }
    Ok(())
}
