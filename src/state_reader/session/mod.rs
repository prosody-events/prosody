//! One committed read operation over a fixed publication snapshot.
//!
//! A [`ReadSession`] has no mutation interface.
//! This enforces `ReadOnlyHandleCannotMutate`.
//! [`CommittedCellSource`] supplies both projections through collection
//! evidence.
//!
//! `SingleSourceCoherence` requires each session to select at most one source.
//! Each engine invocation borrows its selection exclusively and publishes the
//! result to the session. A captured selection takes precedence over the shared
//! selection. A read probes only when neither selection exists.
//!
//! A probe reads all sources concurrently and resolves results in source order.
//! The first source with data supplies the pin. Data takes precedence over
//! errors from other sources. If no source returns data, any source error
//! becomes the result. A point probe checks one cell. A batch probe checks all
//! requested coordinates. A range probe retains the selected source's stream
//! after its first row.
//!
//! Every later read uses the pinned source, even after absence or an error.
//! The probe does not run again after selection.

use crate::Key;
use crate::codec::Codec;
use crate::segment::partition_segment_id;
use crate::state::access::StateAccessError;
use crate::state::cell::Projection;
use crate::state::cell_key::{CellKey, CellRef, Scan, Section};
use crate::state::identity::{CollectionId, StateKey};
use crate::state::store::{CellBuffer, ReadBatch, ensure_aligned};
use crate::state_reader::backend::{CommittedCellSource, ReaderBackend};
use crate::state_reader::cache::CacheLookup;
use crate::state_reader::partition_for_key;
use crate::state_reader::source::{Source, ValidatedPublications};
use futures::stream::{FuturesOrdered, Stream, StreamExt};
use smallvec::smallvec;
use std::borrow::{Borrow, Cow};
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::task::coop::cooperative;

mod context;
pub(crate) mod engine;

pub(crate) use context::{ReaderCollectionDef, ReaderContext};

/// A per-operation read-only session over a collection's validated publication
/// snapshot. Its engine carries no write half, so a handle built from one
/// cannot express a mutation (`ReadOnlyHandleCannotMutate`).
///
/// It is public because it appears in the `FromSession` bounds on
/// [`StateReader`](super::StateReader)'s read methods, mirroring the owner's
/// public `KeyedStateSession`. Its fields and constructor stay crate-internal,
/// so a downstream crate can name it in a bound but can neither build one nor
/// reach a cell through it.
pub struct ReadSession<C: Codec, B> {
    context: ReaderContext<C, B>,
    snapshot: Arc<ValidatedPublications>,
    key: Key,
    /// The session-shared selection, so every invocation after the first
    /// data-bearing probe addresses one source. The engine seeds each
    /// invocation from it and publishes the first selection back.
    ///
    /// Operation-local selection does not replace this cell. No plan links two
    /// invocations on one session, and the second invocation must address the
    /// first selection with no second probe
    /// (`one_session_selects_its_source_once`).
    pin: Arc<OnceLock<PinnedSource>>,
}

/// One operation's selected publication source: the stable [`Source`] plus
/// that source's computed [`CollectionId`].
///
/// `pub` is forced: it is the reader engine's `ReadInner` associated type, and
/// the sealed engine trait is itself `pub`. Nothing re-exports it and the
/// module is private, so it stays unreachable from outside the crate.
#[derive(Clone)]
pub struct PinnedSource {
    source: Source,
    collection: CollectionId,
}

impl<C: Codec, B> Clone for ReadSession<C, B> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            snapshot: self.snapshot.clone(),
            key: self.key.clone(),
            pin: self.pin.clone(),
        }
    }
}

impl<C: Codec, B: ReaderBackend<C>> ReadSession<C, B> {
    /// Builds a session for one operation over `snapshot`, with a fresh pin.
    pub(crate) fn new(
        context: ReaderContext<C, B>,
        snapshot: Arc<ValidatedPublications>,
        key: Key,
    ) -> Self {
        Self {
            context,
            snapshot,
            key,
            pin: Arc::new(OnceLock::new()),
        }
    }

    /// Computes the backing [`CollectionId`] for `source`. The key routes to a
    /// partition via `partition_for_key`, then to a segment via
    /// `partition_segment_id`. The key is non-empty by construction, since
    /// empty keys are rejected at the `StateReader` boundary, so
    /// `partition_for_key` never errors here in practice.
    fn collection_id_for(&self, source: &Source) -> Result<CollectionId, StateAccessError> {
        let partition = partition_for_key(self.key.as_bytes(), source.partition_count)
            .map_err(|e| StateAccessError::store(&e))?;
        let segment = partition_segment_id(source.topic, partition, &source.group_id);
        let state_key = StateKey::new(segment, self.key.clone());
        Ok(CollectionId::new(
            state_key,
            self.context.state_type,
            self.context.name.clone(),
        ))
    }

    /// `selected` when the operation already routed this source, else a fresh
    /// routing through [`Self::collection_id_for`].
    fn resolved_id<'a>(
        &self,
        selected: Option<&'a CollectionId>,
        source: &Source,
    ) -> Result<Cow<'a, CollectionId>, StateAccessError> {
        match selected {
            Some(id) => Ok(Cow::Borrowed(id)),
            None => self.collection_id_for(source).map(Cow::Owned),
        }
    }

    fn cache_key<'a>(&'a self, source: &'a Source, cell: CellRef<'a>) -> CacheLookup<'a> {
        CacheLookup((
            source,
            self.context.state_type,
            &self.context.name,
            &self.key,
            cell,
        ))
    }

    /// One source's committed point read, cached per policy. `selected` is the
    /// operation's already-routed collection id for this source, if any, so a
    /// selected read reuses it instead of re-routing the key.
    async fn cached_point<P: Projection>(
        &self,
        selected: Option<&CollectionId>,
        source: &Source,
        cell: CellRef<'_>,
    ) -> Result<Option<P::Payload>, StateAccessError>
    where
        B::Cells: CommittedCellSource<P>,
    {
        match self.context.def.read_cache_ttl {
            None => {
                let id = self.resolved_id(selected, source)?;
                CommittedCellSource::<P>::load(self.context.backend.cells(), &id, cell)
                    .await
                    .map_err(|error| StateAccessError::store(&error))
            }
            Some(ttl) => {
                let key = self.cache_key(source, cell);
                // `collection_id_for` hashes the key and routes it to a
                // segment. It runs only inside the fill closure on a cache
                // miss, never on a hit.
                self.context
                    .cache
                    .get_cached::<P, _, _>(key, ttl, || async {
                        let id = self.resolved_id(selected, source)?;
                        CommittedCellSource::<P>::load(self.context.backend.cells(), &id, cell)
                            .await
                            .map_err(|error| StateAccessError::store(&error))
                    })
                    .await
            }
        }
    }

    /// One source's committed batch read, cached per policy (index-aligned to
    /// `batch`).
    async fn cached_batch<P: Projection>(
        &self,
        selected: Option<&CollectionId>,
        source: &Source,
        section: Section,
        batch: &ReadBatch<'_>,
    ) -> Result<CellBuffer<Option<P::Payload>>, StateAccessError>
    where
        B::Cells: CommittedCellSource<P>,
    {
        match self.context.def.read_cache_ttl {
            None => {
                let id = self.resolved_id(selected, source)?;
                let buffer = CommittedCellSource::<P>::load_many(
                    self.context.backend.cells(),
                    &id,
                    section,
                    batch,
                )
                .await
                .map_err(|error| StateAccessError::store(&error))?;
                // `CommittedCellSource` is a downstream trait, so check the
                // alignment its contract promises in every build. The cached
                // arm gets the same check inside `get_many_cached`.
                ensure_aligned(buffer.len(), batch.len())?;
                Ok(buffer)
            }
            Some(ttl) => {
                let keys = self.batch_cache_keys(source, section, batch);
                // `collection_id_for` runs only when the batch fill fires (a
                // miss), never when the batch is served entirely from the cache.
                self.context
                    .cache
                    .get_many_cached::<P, _, _>(keys, ttl, || async {
                        let id = self.resolved_id(selected, source)?;
                        CommittedCellSource::<P>::load_many(
                            self.context.backend.cells(),
                            &id,
                            section,
                            batch,
                        )
                        .await
                        .map_err(|error| StateAccessError::store(&error))
                    })
                    .await
            }
        }
    }

    /// Borrows cache keys from a bounded coordinate batch.
    fn batch_cache_keys<'a, 'buf>(
        &'a self,
        source: &'a Source,
        section: Section,
        batch: &'a ReadBatch<'buf>,
    ) -> impl ExactSizeIterator<Item = CacheLookup<'a>> + Clone + use<'a, 'buf, C, B> {
        batch.iter().map(move |coordinate| {
            self.cache_key(
                source,
                CellRef {
                    section,
                    coordinate,
                },
            )
        })
    }

    /// One operation's committed point read: address the already-selected
    /// source, or probe for one.
    async fn point_read<P: Projection>(
        &self,
        selection: &mut Option<PinnedSource>,
        cell: CellRef<'_>,
    ) -> Result<Option<P::Payload>, StateAccessError>
    where
        B::Cells: CommittedCellSource<P>,
    {
        if let Some(pin) = selection.as_ref() {
            return self
                .cached_point::<P>(Some(&pin.collection), &pin.source, cell)
                .await;
        }
        self.probe_point::<P>(selection, cell).await
    }

    /// Uses [`Self::resolve_probe`] for a point read.
    async fn probe_point<P: Projection>(
        &self,
        selection: &mut Option<PinnedSource>,
        cell: CellRef<'_>,
    ) -> Result<Option<P::Payload>, StateAccessError>
    where
        B::Cells: CommittedCellSource<P>,
    {
        self.resolve_probe(
            selection,
            |source| self.cached_point::<P>(None, source, cell),
            Option::is_some,
            || None,
        )
        .await
    }

    /// One operation's committed batch read, index-aligned to `batch`: address
    /// the already-selected source, or probe for one.
    async fn batch_read<P: Projection>(
        &self,
        selection: &mut Option<PinnedSource>,
        section: Section,
        batch: &ReadBatch<'_>,
    ) -> Result<CellBuffer<Option<P::Payload>>, StateAccessError>
    where
        B::Cells: CommittedCellSource<P>,
    {
        if let Some(pin) = selection.as_ref() {
            return self
                .cached_batch::<P>(Some(&pin.collection), &pin.source, section, batch)
                .await;
        }
        self.probe_batch::<P>(selection, section, batch).await
    }

    /// Uses [`Self::resolve_probe`] for a value batch.
    async fn probe_batch<P: Projection>(
        &self,
        selection: &mut Option<PinnedSource>,
        section: Section,
        batch: &ReadBatch<'_>,
    ) -> Result<CellBuffer<Option<P::Payload>>, StateAccessError>
    where
        B::Cells: CommittedCellSource<P>,
    {
        self.resolve_probe(
            selection,
            |source| self.cached_batch::<P>(None, source, section, batch),
            |buffer| buffer.iter().any(Option::is_some),
            || smallvec![None; batch.len()],
        )
        .await
    }

    /// Resolves one concurrent source probe in source order.
    ///
    /// The first source with data wins. Data outranks earlier errors, and the
    /// first error outranks an all-absent result. [`FuturesOrdered`] allocates
    /// one node per source, bounded by `MAX_PUBLICATION_SOURCES`. This bounded
    /// allocation is acceptable beside the source I/O.
    async fn resolve_probe<'a, T, F, Fut, A>(
        &'a self,
        selection: &mut Option<PinnedSource>,
        fetch: F,
        has_data: fn(&T) -> bool,
        absent: A,
    ) -> Result<T, StateAccessError>
    where
        F: Fn(&'a Source) -> Fut,
        Fut: Future<Output = Result<T, StateAccessError>>,
        A: FnOnce() -> T,
    {
        let mut ordered = FuturesOrdered::new();
        for source in self.snapshot.sources() {
            let read = fetch(source);
            ordered.push_back(cooperative(async move { (source, read.await) }));
        }
        let mut first_err = None;
        while let Some((source, result)) = cooperative(ordered.next()).await {
            match result {
                Ok(value) if has_data(&value) => {
                    let collection = self.collection_id_for(source)?;
                    *selection = Some(PinnedSource {
                        source: source.clone(),
                        collection,
                    });
                    return Ok(value);
                }
                Err(error) if first_err.is_none() => first_err = Some(error),
                Ok(_) | Err(_) => {}
            }
        }
        first_err.map_or_else(|| Ok(absent()), Err)
    }

    /// Streams one source's committed cells under the projection.
    fn source_scan<'a, P: Projection, I: Borrow<CollectionId> + Send>(
        &'a self,
        id: I,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), StateAccessError>> + Send + use<'a, C, B, P, I>
    where
        B::Cells: CommittedCellSource<P>,
    {
        async_stream::try_stream! {
            let inner = CommittedCellSource::<P>::scan(self.context.backend.cells(), id.borrow(), scan);
            futures::pin_mut!(inner);
            while let Some(item) = cooperative(inner.next()).await {
                yield item.map_err(|error| StateAccessError::store(&error))?;
            }
        }
    }

    /// Uses the captured source, then the shared source, or probes in source
    /// order. The first source with a row supplies the complete stream and
    /// the shared pin. An unpinned probe allocates one stream box per
    /// source, bounded by `MAX_PUBLICATION_SOURCES`. The box keeps the
    /// selected stream alive after its first row.
    fn scan_from<'a, P: Projection>(
        &'a self,
        selected: Option<&'a PinnedSource>,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), StateAccessError>> + Send + use<'a, C, B, P>
    where
        B::Cells: CommittedCellSource<P>,
    {
        async_stream::try_stream! {
            if let Some(pin) = selected.or_else(|| self.pin.get()) {
                let inner = self.source_scan::<P, _>(&pin.collection, scan);
                futures::pin_mut!(inner);
                while let Some(item) = cooperative(inner.next()).await {
                    yield item?;
                }
                return;
            }

            let mut selection = None;
            let found = self.resolve_probe(
                &mut selection,
                |source| async move {
                    let id = self.collection_id_for(source)?;
                    // Like resolve_probe nodes, stream boxes are bounded by MAX_PUBLICATION_SOURCES per operation, not per cell.
                    let mut stream = Box::pin(self.source_scan::<P, _>(id, scan));
                    let first = cooperative(stream.next()).await.transpose()?;
                    Ok(first.map(|row| (row, stream)))
                },
                Option::is_some,
                || None,
            ).await?;
            engine::publish(self, selection.as_ref());
            if let Some((first, mut stream)) = found {
                yield first;
                while let Some(item) = cooperative(stream.next()).await {
                    yield item?;
                }
            }
        }
    }
}
