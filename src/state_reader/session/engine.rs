//! The published-reader engine: collection operations over one `StateReader`
//! call.
//!
//! Its whole invocation state is that call's own source selection, seeded from
//! and published back to the session-shared [`PinnedSource`]. Each invocation
//! is internally coherent: it answers from one source. After a selection is
//! published, every later invocation adopts it. Two invocations that both start
//! unselected can each probe and can each answer from a different source,
//! because only one of them wins the publication. An invocation that never
//! selected a source falls back to the shared cell before it will probe. Two
//! operations on one reader otherwise share nothing and overlap freely.
//!
//! There is deliberately **no** [`WriteEngine`](sealed::WriteEngine) impl here:
//! the write scope exists only for a session whose engine has one, so a reader
//! collection has no mutation to refuse. What the engine can reach is likewise
//! a matter of type, not of runtime checks: [`ReaderBackend`] offers a
//! committed cell source and a loader, and no oracle or writable store, so
//! gaining either behavior takes a dependency change.

use super::{PinnedSource, ReadSession};
use crate::codec::Codec;
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::collection::{StateSession, sealed};
use crate::state::descriptor::StructuralIdentity;
use crate::state::registry::{CollectionDef, MAX_KEYSET_LIMIT};
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state::{StateName, StateType};
use crate::state_reader::backend::ReaderBackend;
use bytes::Bytes;
use futures::stream::Stream;

/// The engine every published-reader session binds. `pub` for the same reason
/// [`OwnerEngine`](crate::state::collection::owner::OwnerEngine) is: it is the
/// value of the sealed `Session::Engine` associated type.
pub struct ReaderEngine;

impl<C: Codec, B: ReaderBackend<C>> sealed::Session for ReadSession<C, B> {
    type Engine = ReaderEngine;
}

impl<C: Codec, B: ReaderBackend<C>> StateSession for ReadSession<C, B> {
    type Loader = B::Loader;

    fn loader(&self) -> &B::Loader {
        self.context.backend.loader()
    }
}

impl<C: Codec, B: ReaderBackend<C>> sealed::ReadEngine<ReadSession<C, B>> for ReaderEngine {
    /// A managed stream carries the invocation's selection, so a continuation
    /// that captured one addresses exactly the source the planning command
    /// chose. A plan captured before anything was selected falls back to the
    /// session-shared selection at the moment it runs, and probes only when
    /// that is empty too.
    type Plan = Option<PinnedSource>;
    /// Operation-local source selection: the session's already-published
    /// selection, or `None` until a command first finds stored data.
    type ReadInner<'a> = Option<PinnedSource>;

    /// Identity was validated at acquisition against every source. The
    /// descriptor is the source of both this session's collection and the
    /// handle's asserted identity, so a self-check is redundant.
    fn verify_registration(
        session: &ReadSession<C, B>,
        _name: &'static str,
        _state_type: StateType,
        _identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        Ok(session.context.name.clone())
    }

    /// The reader's own descriptor settings, except the keyset bound: the
    /// persisted keyset records its own overflow, and the global validated
    /// ceiling safely admits every tracked keyset regardless of the reader's
    /// local operational setting — so binding can never change a reader
    /// stream's tracked-versus-scan arm.
    fn collection_def(
        session: &ReadSession<C, B>,
        _state_type: StateType,
        _name: &StateName,
    ) -> CollectionDef {
        CollectionDef {
            keyset_limit: MAX_KEYSET_LIMIT,
            ..session.context.def.collection
        }
    }

    async fn begin_read(session: &ReadSession<C, B>) -> Self::ReadInner<'_> {
        // Admission is not a concept here: the invocation starts from whatever
        // the session already selected and performs no I/O until its first
        // command.
        session.pin.get().cloned()
    }

    async fn read_point(
        session: &ReadSession<C, B>,
        inner: &mut Self::ReadInner<'_>,
        _state_type: StateType,
        _name: &StateName,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        let unselected = inner.is_none();
        let result = session.point_read(inner, cell).await;
        if unselected {
            publish(session, inner.as_ref());
        }
        result
    }

    async fn read_batch(
        session: &ReadSession<C, B>,
        inner: &mut Self::ReadInner<'_>,
        _state_type: StateType,
        _name: &StateName,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        let unselected = inner.is_none();
        let result = session.batch_read(inner, section, batch).await;
        if unselected {
            publish(session, inner.as_ref());
        }
        result
    }

    fn capture(inner: &Self::ReadInner<'_>) -> Self::Plan {
        inner.clone()
    }

    async fn resume<'a>(_session: &'a ReadSession<C, B>, plan: &Self::Plan) -> Self::ReadInner<'a> {
        plan.clone()
    }

    fn page<'a>(
        session: &'a ReadSession<C, B>,
        plan: &'a Self::Plan,
        _state_type: StateType,
        _name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        session.scan_from(plan.as_ref(), scan)
    }

    /// Vacuous: a published reader has no attempt, no cancellation, and no
    /// teardown to leak past.
    fn fence(_session: &ReadSession<C, B>) -> Result<(), StateAccessError> {
        Ok(())
    }
}

/// Publishes an invocation's first selection to the session-shared cell, so a
/// later invocation on the same session addresses the same source. Discarding
/// the `set` result is safe: one operation's reads are sequential, so no other
/// selection can have landed in between.
fn publish<C: Codec, B: ReaderBackend<C>>(
    session: &ReadSession<C, B>,
    selection: Option<&PinnedSource>,
) {
    if let Some(pin) = selection {
        let _ = session.pin.set(pin.clone());
    }
}
