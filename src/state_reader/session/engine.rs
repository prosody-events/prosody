//! The published-reader engine: collection operations over one `StateReader`
//! call.
//!
//! Its whole invocation state is that call's own source selection, so two
//! operations on one reader share nothing and overlap freely. There is
//! deliberately **no** write engine here: a reader collection has no write
//! scope to refuse, because the bound that would produce one is unsatisfiable.
//! Reader commands reach only published committed state through the reader
//! store and cache — the engine is constructed without an oracle or writable
//! store, so gaining either behavior requires a type-level dependency change
//! rather than a missed runtime check.

use super::{PinnedSource, ReadSession};
use crate::codec::Codec;
use crate::state::access::StateAccessError;
use crate::state::cell_key::CellKey;
use crate::state::collection::{StateSession, sealed};
use crate::state::{StateName, StateType};
use crate::state_reader::backend::ReaderBackend;
use bytes::Bytes;

/// The engine every published-reader session binds.
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
    /// Operation-local source selection: `None` until a command first finds
    /// stored data, then selected for the rest of that invocation.
    type ReadInner<'a> = Option<PinnedSource>;

    async fn begin_read(_session: &ReadSession<C, B>) -> Self::ReadInner<'_> {
        // Admission is not a concept here: the invocation starts with no
        // selection and performs no I/O until its first command.
        None
    }

    async fn read_point(
        session: &ReadSession<C, B>,
        inner: &mut Self::ReadInner<'_>,
        _state_type: StateType,
        _name: &StateName,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        session.point_read(inner, cell).await
    }
}
