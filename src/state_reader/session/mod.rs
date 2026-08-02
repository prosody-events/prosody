//! The per-operation read session that runs probe-and-pin.
//!
//! A [`ReadSession`] binds an engine with no write half, so a handle built from
//! a reader cannot express a mutation. Call this the
//! `ReadOnlyHandleCannotMutate` invariant. One session is built per
//! `StateReader` operation and captures an immutable snapshot. One logical
//! operation therefore resolves against one exact source set and pins at most
//! one source. Call this the `SingleSourceCoherence` invariant.
//!
//! Every guarantee here is structural. `ReadOnlyHandleCannotMutate` holds
//! because no mutator bound exists. Committed-only holds because the only value
//! a session can materialize is
//! [`Cell::project_committed`](crate::state::cell::Cell::project_committed).
//!
//! `SingleSourceCoherence` holds two ways. Within one invocation, the
//! `&mut Option<PinnedSource>` that every engine path threads (see [`engine`])
//! stops two overlapping unpinned reads from compiling, so a probe always
//! reaches its pin before the next read starts. A probe is concurrent across
//! *sources*, never across reads: `probe_batch` fans one batch out to every
//! source at once, and still resolves to one pin. Across invocations, the
//! session-shared pin has exactly one writer, `engine::publish`, so the second
//! invocation addresses the first selection.
//!
//! The selection reaches the read paths two ways, and the two agree. A scoped
//! collection operation carries its own invocation-local selection: the reader
//! engine seeds it from the session-shared [`PinnedSource`] and publishes the
//! first one it makes back to that shared cell. A managed stream carries the
//! selection its planning command captured. The precedence is uniform: a
//! captured selection wins, an uncaptured one defers to the shared cell, and
//! only a wholly unselected point or batch read probes.
//!
//! Probe-and-pin is the reader's source-selection strategy:
//!
//! * **Point read and `get_many`** issue the read to every source concurrently
//!   and resolve in source order with early exit. A [`FuturesOrdered`] yields
//!   in push order regardless of completion timing, so the lowest-ordered
//!   source with data always wins the pin. A fast `None` from a non-owner can
//!   never beat a slow `Some` from the owner. A source that errors is skipped
//!   and its error remembered. Data beats a skipped error. No data plus at
//!   least one error is an error.
//! * **Range page** never probes. It addresses the source the operation already
//!   selected.
//!
//! Once pinned, every later call addresses the pinned source directly, even on
//! `None` or `Err`. The probe never reruns within an operation. Determinism is
//! source-preference order, not a stable pin under transient faults: with
//! `A=Err` and `B=Some` the pin is B, and a later run with `A=Some` pins A.
//! That is an availability difference, never a committed-only violation.

use crate::Key;
use crate::codec::Codec;
use crate::segment::partition_segment_id;
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::identity::{CollectionId, StateKey};
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state_reader::backend::{CommittedCellSource, ReaderBackend};
use crate::state_reader::cache::CacheKey;
use crate::state_reader::partition_for_key;
use crate::state_reader::source::{Source, ValidatedPublications};
use bytes::Bytes;
use futures::stream::{FuturesOrdered, Stream, StreamExt};
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
    fn resolved_id(
        &self,
        selected: Option<&CollectionId>,
        source: &Source,
    ) -> Result<CollectionId, StateAccessError> {
        match selected {
            Some(id) => Ok(id.clone()),
            None => self.collection_id_for(source),
        }
    }

    fn cache_key(&self, source: &Source, cell: &CellKey) -> CacheKey {
        (
            source.clone(),
            self.context.state_type,
            self.context.name.clone(),
            self.key.clone(),
            cell.clone(),
        )
    }

    /// One source's committed point read, cached per policy. `selected` is the
    /// operation's already-routed collection id for this source, if any, so a
    /// selected read reuses it instead of re-routing the key.
    async fn cached_point(
        &self,
        selected: Option<&CollectionId>,
        source: &Source,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        match self.context.def.read_cache_ttl {
            None => {
                let id = self.resolved_id(selected, source)?;
                self.context
                    .backend
                    .cells()
                    .load(&id, cell)
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
                    .get_cached(key, ttl, || async {
                        let id = self.resolved_id(selected, source)?;
                        self.context
                            .backend
                            .cells()
                            .load(&id, cell)
                            .await
                            .map_err(|error| StateAccessError::store(&error))
                    })
                    .await
            }
        }
    }

    /// One source's committed batch read, cached per policy (index-aligned to
    /// `batch`).
    async fn cached_batch(
        &self,
        selected: Option<&CollectionId>,
        source: &Source,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        match self.context.def.read_cache_ttl {
            None => {
                let id = self.resolved_id(selected, source)?;
                let buffer = self
                    .context
                    .backend
                    .cells()
                    .load_many(&id, section, batch)
                    .await
                    .map_err(|error| StateAccessError::store(&error))?;
                // `CommittedCellSource` is a downstream trait, so check the
                // alignment its contract promises in every build. The cached
                // arm gets the same check inside `get_many_cached`.
                if buffer.len() != batch.len() {
                    return Err(StateAccessError::misaligned_batch(
                        buffer.len(),
                        batch.len(),
                    ));
                }
                Ok(buffer)
            }
            Some(ttl) => {
                // A stream chunk is up to `CELL_BATCH`, well past
                // `CELLS_INLINE`, so this buffer spills to the heap on the
                // common path. Accepted: it is one allocation, sized once from
                // the batch and bounded by it, in front of a network-bound or
                // fjall-bound store read. The keys ARE the cache lookup. A
                // wider `CELLS_INLINE` is the wrong fix, because it would
                // inflate every `CellBuffer` on every path.
                let keys: CellBuffer<CacheKey> = batch
                    .iter()
                    .map(|coordinate| {
                        self.cache_key(
                            source,
                            &CellKey {
                                section,
                                coordinate: coordinate.clone(),
                            },
                        )
                    })
                    .collect();
                // `collection_id_for` runs only when the batch fill fires (a
                // miss), never when the batch is served entirely from the cache.
                self.context
                    .cache
                    .get_many_cached(&keys, ttl, || async {
                        let id = self.resolved_id(selected, source)?;
                        self.context
                            .backend
                            .cells()
                            .load_many(&id, section, batch)
                            .await
                            .map_err(|error| StateAccessError::store(&error))
                    })
                    .await
            }
        }
    }

    /// One operation's committed point read: address the already-selected
    /// source, or probe for one.
    async fn point_read(
        &self,
        selection: &mut Option<PinnedSource>,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        if let Some(pin) = selection.as_ref() {
            return self
                .cached_point(Some(&pin.collection), &pin.source, cell)
                .await;
        }
        self.probe_point(selection, cell).await
    }

    /// Probe-and-pin for one point read: issue the read to every source
    /// concurrently and resolve in source order with early exit, selecting the
    /// first source that answers with data.
    ///
    /// A [`FuturesOrdered`] yields in push order regardless of completion
    /// timing, so the lowest-ordered source with data always wins: a fast
    /// `None` from a non-owner can never beat a slow `Some` from the owner. A
    /// source that errors is skipped and its error remembered; data beats a
    /// skipped error, and no data plus at least one error is an error.
    async fn probe_point(
        &self,
        selection: &mut Option<PinnedSource>,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        let sources = self.snapshot.sources();
        // `FuturesOrdered` heap-allocates one node per source, bounded by
        // `MAX_PUBLICATION_SOURCES`. This is a per-operation, I/O-bound
        // cross-group read, not the per-message or per-cell steady state the
        // allocation rule targets. A bounded 16-node allocation alongside the
        // store reads is acceptable here. Do not replace it with a hand-rolled
        // poll loop over a `SmallVec` to avoid the allocation.
        // Each future yields the source it read, so the selection never
        // depends on the completion order matching the source order.
        let mut ordered = FuturesOrdered::new();
        for source in sources {
            ordered.push_back(cooperative(async move {
                (source, self.cached_point(None, source, cell).await)
            }));
        }
        let mut first_err = None;
        while let Some((source, result)) = cooperative(ordered.next()).await {
            match result {
                Ok(Some(value)) => {
                    let collection = self.collection_id_for(source)?;
                    *selection = Some(PinnedSource {
                        source: source.clone(),
                        collection,
                    });
                    return Ok(Some(value));
                }
                Ok(None) => {}
                Err(error) => {
                    if first_err.is_none() {
                        first_err = Some(error);
                    }
                }
            }
        }
        match first_err {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }

    /// One operation's committed batch read, index-aligned to `batch`: address
    /// the already-selected source, or probe for one.
    async fn batch_read(
        &self,
        selection: &mut Option<PinnedSource>,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        if let Some(pin) = selection.as_ref() {
            return self
                .cached_batch(Some(&pin.collection), &pin.source, section, batch)
                .await;
        }
        self.probe_batch(selection, section, batch).await
    }

    /// Probe-and-pin for one batch read — [`Self::probe_point`]'s batch twin.
    /// A buffer holding data anywhere pins its source; among the remaining
    /// outcomes an error outranks an all-absent buffer, because absence cannot
    /// be proven through a source that failed.
    async fn probe_batch(
        &self,
        selection: &mut Option<PinnedSource>,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        let sources = self.snapshot.sources();
        // Bounded per-operation fan-out; see the ruling on the point-read
        // `FuturesOrdered` above.
        let mut ordered = FuturesOrdered::new();
        for source in sources {
            ordered.push_back(cooperative(async move {
                (
                    source,
                    self.cached_batch(None, source, section, batch).await,
                )
            }));
        }
        let mut first_err = None;
        while let Some((source, result)) = cooperative(ordered.next()).await {
            match result {
                Ok(buffer) => {
                    if buffer.iter().any(Option::is_some) {
                        let collection = self.collection_id_for(source)?;
                        *selection = Some(PinnedSource {
                            source: source.clone(),
                            collection,
                        });
                        return Ok(buffer);
                    }
                }
                Err(error) => {
                    if first_err.is_none() {
                        first_err = Some(error);
                    }
                }
            }
        }
        match first_err {
            Some(error) => Err(error),
            // Every source that answered answered all-absent, which the
            // index-aligned contract makes exactly `batch.len()` `None`s — so
            // no buffer needs keeping. This is also the empty-source arm, which
            // the non-empty snapshot forbids.
            None => Ok((0..batch.len()).map(|_| None).collect()),
        }
    }

    /// One operation's committed range page over the selected source.
    ///
    /// A captured `Some(selected)` always wins: a continuation addresses the
    /// source its planning command chose. A `None` falls back to the
    /// session-shared pin **sampled at first poll**, so a stream constructed
    /// before a sibling read pinned still addresses that selection rather than
    /// opening a second source.
    ///
    /// A range page never probes. Every range plan follows a metadata point
    /// read that already pinned, so a wholly unselected one is unconstructable
    /// through the collection API. Such a plan terminates the stream with
    /// [`StateAccessError::Unavailable`] instead of opening a source the
    /// operation did not select.
    fn scan_from<'a>(
        &'a self,
        selected: Option<&'a PinnedSource>,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        async_stream::try_stream! {
            let pin = selected
                .or_else(|| self.pin.get())
                .ok_or(StateAccessError::Unavailable)?;
            let id = pin.collection.clone();
            let inner = self.context.backend.cells().scan(&id, scan);
            futures::pin_mut!(inner);
            while let Some(item) = cooperative(inner.next()).await {
                yield item.map_err(|error| StateAccessError::store(&error))?;
            }
        }
    }
}
