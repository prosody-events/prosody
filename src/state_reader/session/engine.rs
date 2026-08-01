//! The published-reader engine: collection operations over one `StateReader`
//! call.
//!
//! Its whole invocation state is that call's own source selection, seeded from
//! and published back to the session-shared [`PinnedSource`] so the invocation
//! and the `CellRead` bridge paths never disagree about which source answers.
//! Two operations on one reader otherwise share nothing and overlap freely.
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
use crate::state::cell_key::CellKey;
use crate::state::collection::{StateSession, sealed};
use crate::state::{StateName, StateType};
use crate::state_reader::backend::ReaderBackend;
use bytes::Bytes;

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
    /// Operation-local source selection: the session's already-published
    /// selection, or `None` until a command first finds stored data.
    type ReadInner<'a> = Option<PinnedSource>;

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
        if unselected && let Some(pin) = inner.as_ref() {
            // The invocation just made the session's first selection: publish
            // it so the bridge paths address the same source. Discarding the
            // result is safe — one operation's reads are sequential, so no
            // other selection can have landed in between.
            let _ = session.pin.set(pin.clone());
        }
        result
    }
}
