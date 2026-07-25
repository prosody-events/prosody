//! The per-operation read session that runs probe-and-pin.
//!
//! A [`ReadSession`] implements [`CellRead`] only. It has no mutator bound, so
//! a handle built from a reader cannot express a mutation. Call this the
//! `ReadOnlyHandleCannotMutate` invariant. One session is built per
//! `StateReader` operation and captures an immutable snapshot. One logical
//! operation therefore resolves against one exact source set and pins at most
//! one source. Call this the `SingleSourceCoherence` invariant.
//!
//! Two of these guarantees are structural. `ReadOnlyHandleCannotMutate` holds
//! because no mutator bound exists. Committed-only holds because the only value
//! a session can materialize is
//! [`Cell::project_committed`](crate::state::cell::Cell::project_committed).
//!
//! `SingleSourceCoherence` is not purely type-enforced. It rests on a runtime
//! rule: the unpinned reads within one operation must be issued sequentially,
//! each pinning before the next begins. Every read path here obeys that rule. A
//! probe runs to a pin, then later calls address the pin, so a torn two-source
//! view cannot occur. A concurrent batched probe added later would have to keep
//! this sequential-pin behavior to preserve the invariant.
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
//! * **Scan** probes sources sequentially. The first stream to yield a cell
//!   pins. A mid-stream error after the pin terminates with `Err`, because a
//!   restart would double-yield. An error before any yield is skipped. An empty
//!   source falls through to the next.
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
use crate::state::descriptor::StructuralIdentity;
use crate::state::identity::{CollectionId, StateKey};
use crate::state::registry::CollectionDef;
use crate::state::session::CellRead;
use crate::state::session::sealed::ReadAdmission;
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state::{StateName, StateType};
use crate::state_reader::cache::{CacheKey, ReaderCache};
use crate::state_reader::loader::ReaderLoader;
use crate::state_reader::partition_for_key;
use crate::state_reader::source::{Source, ValidatedPublications};
use crate::state_reader::stores::ReaderStores;
use bytes::Bytes;
use futures::stream::{FuturesOrdered, Stream, StreamExt};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::task::coop::cooperative;

/// A collection definition whose inherited reader-cache policy is resolved.
#[derive(Clone, Copy)]
pub(crate) struct ReaderCollectionDef {
    collection: CollectionDef,
    read_cache_ttl: Option<Duration>,
}

impl ReaderCollectionDef {
    pub(crate) fn new(collection: CollectionDef, read_cache_ttl: Option<Duration>) -> Self {
        Self {
            collection,
            read_cache_ttl,
        }
    }
}

/// Everything a [`StateReader`](super::StateReader) shares with every session
/// it builds: which collection it addresses, and the infrastructure handles it
/// cloned from the bundle. A session adds only what varies per operation — the
/// snapshot, the key, and the pin.
///
/// Every field is a cheap-clone handle, so building a session per operation
/// copies no resources.
pub(crate) struct ReaderContext<C: Codec> {
    pub(super) stores: ReaderStores,
    loader: Arc<ReaderLoader<C>>,
    cache: ReaderCache,
    def: ReaderCollectionDef,
    pub(super) state_type: StateType,
    pub(super) name: StateName,
}

impl<C: Codec> ReaderContext<C> {
    pub(crate) fn new(
        stores: ReaderStores,
        loader: Arc<ReaderLoader<C>>,
        cache: ReaderCache,
        def: ReaderCollectionDef,
        state_type: StateType,
        name: StateName,
    ) -> Self {
        Self {
            stores,
            loader,
            cache,
            def,
            state_type,
            name,
        }
    }
}

/// Cloning shares handles; the manual impl keeps the bundle free of a
/// `C: Clone` bound the derive would add.
impl<C: Codec> Clone for ReaderContext<C> {
    fn clone(&self) -> Self {
        Self {
            stores: self.stores.clone(),
            loader: self.loader.clone(),
            cache: self.cache.clone(),
            def: self.def,
            state_type: self.state_type,
            name: self.name.clone(),
        }
    }
}

/// A per-operation read-only session over a collection's validated publication
/// snapshot. Implements [`CellRead`] only.
///
/// It is public because it appears in the `FromSession` bounds on
/// [`StateReader`](super::StateReader)'s read methods, mirroring the owner's
/// public `KeyedStateSession`. Its fields and constructor stay crate-internal,
/// so a downstream crate can name it in a bound but can neither build one nor
/// reach a cell through it.
pub struct ReadSession<C: Codec> {
    context: ReaderContext<C>,
    snapshot: Arc<ValidatedPublications>,
    key: Key,
    /// The pinned source, shared across the operation's handle clones so every
    /// call after the first data-bearing probe addresses one source.
    pin: Arc<OnceLock<Source>>,
}

impl<C: Codec> Clone for ReadSession<C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            snapshot: self.snapshot.clone(),
            key: self.key.clone(),
            pin: self.pin.clone(),
        }
    }
}

impl<C: Codec> ReadSession<C> {
    /// Builds a session for one operation over `snapshot`, with a fresh pin.
    pub(crate) fn new(
        context: ReaderContext<C>,
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
        let segment = partition_segment_id(source.id.topic, partition, &source.id.group_id);
        let state_key = StateKey::new(segment, self.key.clone());
        Ok(CollectionId::new(
            state_key,
            self.context.state_type,
            self.context.name.clone(),
        ))
    }

    fn cache_key(&self, source: &Source, cell: &CellKey) -> CacheKey {
        (
            source.id.clone(),
            self.context.name.clone(),
            self.key.clone(),
            cell.clone(),
        )
    }

    /// One source's committed point read, cached per policy.
    async fn cached_point(
        &self,
        source: &Source,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        match self.context.def.read_cache_ttl {
            None => {
                let id = self.collection_id_for(source)?;
                self.context.stores.read_committed(&id, cell).await
            }
            Some(ttl) => {
                let key = self.cache_key(source, cell);
                // `collection_id_for` does key murmur and segment routing. It
                // runs only inside the fill closure on a cache miss, never on a
                // hit.
                self.context
                    .cache
                    .get_cached(key, ttl, || async {
                        let id = self.collection_id_for(source)?;
                        self.context.stores.read_committed(&id, cell).await
                    })
                    .await
            }
        }
    }

    /// One source's committed batch read, cached per policy (index-aligned to
    /// `batch`).
    async fn cached_batch(
        &self,
        source: &Source,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        match self.context.def.read_cache_ttl {
            None => {
                let id = self.collection_id_for(source)?;
                self.context
                    .stores
                    .read_committed_many(&id, section, batch)
                    .await
            }
            Some(ttl) => {
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
                        let id = self.collection_id_for(source)?;
                        self.context
                            .stores
                            .read_committed_many(&id, section, batch)
                            .await
                    })
                    .await
            }
        }
    }
}

impl<C: Codec> ReadAdmission for ReadSession<C> {
    type Permit<'s> = ();

    async fn permit(&self) -> Self::Permit<'_> {}

    fn attempt_current(&self) -> bool {
        true
    }
}

impl<C: Codec> CellRead for ReadSession<C>
where
    C::Payload: Clone,
{
    type Loader = ReaderLoader<C>;

    fn loader(&self) -> &Self::Loader {
        &self.context.loader
    }

    fn is_terminated(&self) -> bool {
        false
    }

    fn collection_has_ttl(&self, _state_type: StateType, _name: &StateName) -> bool {
        self.context.def.collection.ttl.is_some()
    }

    fn collection_keyset_limit(&self, _state_type: StateType, _name: &StateName) -> usize {
        self.context.def.collection.keyset_limit
    }

    fn collection_capacity(
        &self,
        _state_type: StateType,
        _name: &StateName,
    ) -> Option<NonZeroUsize> {
        self.context.def.collection.capacity
    }

    fn verify_state_registration(
        &self,
        _name: &'static str,
        _state_type: StateType,
        _identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        // Identity was validated at acquisition against every source. The
        // descriptor is the source of both this session's collection and the
        // handle's asserted identity, so a self-check is redundant.
        Ok(self.context.name.clone())
    }

    async fn get(
        &self,
        _state_type: StateType,
        _name: &StateName,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        if let Some(source) = self.pin.get() {
            return self.cached_point(source, cell).await;
        }
        let sources = self.snapshot.sources();
        // `FuturesOrdered` heap-allocates one node per source, bounded by
        // `MAX_PUBLICATION_SOURCES`. This is a per-operation, I/O-bound
        // cross-group read, not the per-message or per-cell steady state the
        // allocation rule targets. A bounded 16-node allocation alongside the
        // store reads is acceptable here. Do not replace it with a hand-rolled
        // poll loop over a `SmallVec` to avoid the allocation.
        // Each future yields the source it read, so the pin never depends on
        // the completion order matching the source order.
        let mut ordered = FuturesOrdered::new();
        for source in sources {
            ordered.push_back(cooperative(async move {
                (source, self.cached_point(source, cell).await)
            }));
        }
        let mut first_err = None;
        while let Some((source, result)) = ordered.next().await {
            match result {
                Ok(Some(value)) => {
                    // Discarding the `set` result is safe because unpinned reads
                    // in one operation are issued sequentially (the
                    // `SingleSourceCoherence` invariant). This is the first pin,
                    // so the `OnceLock` is empty and the set succeeds.
                    let _ = self.pin.set(source.clone());
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

    async fn get_many(
        &self,
        _state_type: StateType,
        _name: &StateName,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        if let Some(source) = self.pin.get() {
            return self.cached_batch(source, section, batch).await;
        }
        let sources = self.snapshot.sources();
        // Bounded per-operation fan-out; see the ruling on the point-read
        // `FuturesOrdered` above.
        let mut ordered = FuturesOrdered::new();
        for source in sources {
            ordered.push_back(cooperative(async move {
                (source, self.cached_batch(source, section, batch).await)
            }));
        }
        let mut first_err = None;
        let mut all_none: Option<CellBuffer<Option<Bytes>>> = None;
        while let Some((source, result)) = ordered.next().await {
            match result {
                Ok(buffer) => {
                    if buffer.iter().any(Option::is_some) {
                        // First pin, so the set succeeds and discarding its
                        // result is safe; see the `get` pin above.
                        let _ = self.pin.set(source.clone());
                        return Ok(buffer);
                    }
                    if all_none.is_none() {
                        all_none = Some(buffer);
                    }
                }
                Err(error) => {
                    if first_err.is_none() {
                        first_err = Some(error);
                    }
                }
            }
        }
        match (all_none, first_err) {
            // A data-bearing buffer already returned early above. Among the
            // remaining outcomes an error outranks an all-`None` buffer, because
            // absence cannot be proven through a source that failed. This
            // mirrors the point read, where a remembered error beats the `None`
            // answers.
            (_, Some(error)) => Err(error),
            (Some(buffer), None) => Ok(buffer),
            // Unreachable: the snapshot is non-empty, so at least one source
            // answered with data, all-`None`, or an error.
            (None, None) => Ok((0..batch.len()).map(|_| None).collect()),
        }
    }

    fn scan<'a>(
        &'a self,
        _state_type: StateType,
        _name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        async_stream::try_stream! {
            if let Some(source) = self.pin.get() {
                let id = self.collection_id_for(source)?;
                let inner = self.context.stores.scan_committed(&id, scan);
                futures::pin_mut!(inner);
                while let Some(item) = inner.next().await {
                    yield item?;
                }
                return;
            }
            // Unpinned sequential probe. This is the general `CellRead::scan`
            // contract for a fresh session. The public Map and Deque reader
            // streams never reach it: each first reads its keyset or bounds cell
            // through the point fan-out (`get`), which pins before any scan is
            // issued, or yields no scan at all. This is correct behavior for an
            // unpinned scan, not dead code. Do not delete it as unreachable.
            let sources = self.snapshot.sources();
            let mut first_err = None;
            let mut pinned = false;
            for source in sources {
                let id = self.collection_id_for(source)?;
                let inner = self.context.stores.scan_committed(&id, scan);
                futures::pin_mut!(inner);
                let mut yielded_any = false;
                loop {
                    match inner.next().await {
                        Some(Ok(item)) => {
                            if !yielded_any {
                                let _ = self.pin.set(source.clone());
                                yielded_any = true;
                            }
                            yield item;
                        }
                        Some(Err(error)) => {
                            if yielded_any {
                                // Mid-stream error after the pin. Terminate,
                                // never restart, because a restart would
                                // double-yield or skip cells.
                                Err(error)?;
                            } else if first_err.is_none() {
                                first_err = Some(error);
                            }
                            break;
                        }
                        None => break,
                    }
                }
                if yielded_any {
                    pinned = true;
                    break;
                }
            }
            // Nothing pinned: propagate a remembered error, else the stream is
            // empty (every source completed empty).
            if !pinned && let Some(error) = first_err {
                Err(error)?;
            }
        }
    }
}
