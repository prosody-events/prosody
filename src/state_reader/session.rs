//! The per-operation read session realizing probe-and-pin.
//!
//! A [`ReadSession`] implements [`CellRead`] **only** — it has no mutator
//! bounds, so a reader-minted handle cannot express a mutation (the
//! `ReadOnlyHandleCannotMutate` invariant). One session is minted per
//! `StateReader` operation and captures an immutable snapshot, so one logical
//! operation resolves against one exact source set and pins at most one source
//! (the `SingleSourceCoherence` invariant).
//!
//! Probe-and-pin (see the plan's "Source selection"):
//!
//! * **Point read / `get_many`** — issue the read to every source concurrently,
//!   but resolve **in source order** with early exit: a [`FuturesOrdered`]
//!   polls all pushed futures each poll yet yields strictly in push (source)
//!   order, so a fast `None` from a non-owner never beats a slow `Some` from
//!   the owner. The lowest-ordered source with data wins and pins; a source
//!   that errors is skipped (error remembered); data beats a skipped error; no
//!   data plus at least one error is an error.
//! * **Scan** — probe sources **sequentially**; the first stream to yield a
//!   cell pins, a post-pin mid-stream error terminates with `Err` (no restart
//!   would double-yield), a pre-yield error is skipped, an empty source falls
//!   through.
//!
//! Once pinned, every later call addresses the pinned source directly — even
//! on `None`/`Err`; the probe never reruns inside an operation. Determinism is
//! **source-preference order**, not a stable pin under transient faults
//! (`A=Err,B=Some` pins B; a later run with `A=Some` pins A) — an
//! availability/staleness difference, never a committed-only violation.

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
use tokio::task::coop::cooperative;

/// A per-operation read-only session over a collection's validated publication
/// snapshot. Implements [`CellRead`] only.
///
/// Public because it appears in the `FromSession` bounds on
/// [`StateReader`](super::StateReader)'s read methods (mirroring the owner's
/// public `KeyedStateSession`); its fields and constructor stay crate-internal,
/// so a downstream crate can name it in a bound but can neither build one nor
/// reach a cell through it.
pub struct ReadSession<C: Codec> {
    snapshot: Arc<ValidatedPublications>,
    stores: ReaderStores,
    loader: Arc<ReaderLoader<C>>,
    cache: ReaderCache,
    key: Key,
    def: CollectionDef,
    name: StateName,
    /// The pinned source, shared across the operation's handle clones so every
    /// call after the first data-bearing probe addresses one source.
    pin: Arc<OnceLock<Source>>,
}

impl<C: Codec> Clone for ReadSession<C> {
    fn clone(&self) -> Self {
        Self {
            snapshot: self.snapshot.clone(),
            stores: self.stores.clone(),
            loader: self.loader.clone(),
            cache: self.cache.clone(),
            key: self.key.clone(),
            def: self.def,
            name: self.name.clone(),
            pin: self.pin.clone(),
        }
    }
}

impl<C: Codec> ReadSession<C> {
    /// Mints a session for one operation over `snapshot`, with a fresh pin.
    pub(crate) fn new(
        snapshot: Arc<ValidatedPublications>,
        stores: ReaderStores,
        loader: Arc<ReaderLoader<C>>,
        cache: ReaderCache,
        key: Key,
        def: CollectionDef,
        name: StateName,
    ) -> Self {
        Self {
            snapshot,
            stores,
            loader,
            cache,
            key,
            def,
            name,
            pin: Arc::new(OnceLock::new()),
        }
    }

    /// Computes the backing collection id for `source`: `partition_for_key` →
    /// `partition_segment_id(topic, partition, group)` → [`CollectionId`]. The
    /// key is non-empty by construction (rejected at the `StateReader`
    /// boundary), so `partition_for_key` never errors here in practice.
    fn collection_id_for(&self, source: &Source) -> Result<CollectionId, StateAccessError> {
        let partition = partition_for_key(self.key.as_bytes(), source.partition_count)
            .map_err(|e| StateAccessError::store(&e))?;
        let segment = partition_segment_id(source.id.topic, partition, &source.id.group_id);
        let state_key = StateKey::new(segment, self.key.clone());
        Ok(CollectionId::new(
            state_key,
            StateType::Application,
            self.name.clone(),
        ))
    }

    fn cache_key(&self, source: &Source, cell: &CellKey) -> CacheKey {
        (
            source.id.clone(),
            self.name.clone(),
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
        match self.def.read_cache.ttl() {
            None => {
                let id = self.collection_id_for(source)?;
                self.stores.read_committed(&id, cell).await
            }
            Some(ttl) => {
                let ttl_ms = ttl.as_millis() as u64;
                let key = self.cache_key(source, cell);
                // `collection_id_for` (key murmur + segment routing) runs only
                // on a cache miss — the fill closure — never on a hit.
                self.cache
                    .get_cached(key, ttl_ms, || async {
                        let id = self.collection_id_for(source)?;
                        self.stores.read_committed(&id, cell).await
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
        match self.def.read_cache.ttl() {
            None => {
                let id = self.collection_id_for(source)?;
                self.stores.read_committed_many(&id, section, batch).await
            }
            Some(ttl) => {
                let ttl_ms = ttl.as_millis() as u64;
                let keys: Vec<CacheKey> = batch
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
                let values = self
                    .cache
                    .get_many_cached(&keys, ttl_ms, || async {
                        let id = self.collection_id_for(source)?;
                        self.stores
                            .read_committed_many(&id, section, batch)
                            .await
                            .map(|buffer| buffer.into_iter().collect::<Vec<_>>())
                    })
                    .await?;
                Ok(values.into_iter().collect())
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
        &self.loader
    }

    fn is_terminated(&self) -> bool {
        false
    }

    fn collection_has_ttl(&self, _state_type: StateType, _name: &StateName) -> bool {
        self.def.ttl.is_some()
    }

    fn collection_keyset_limit(&self, _state_type: StateType, _name: &StateName) -> usize {
        self.def.keyset_limit
    }

    fn collection_capacity(
        &self,
        _state_type: StateType,
        _name: &StateName,
    ) -> Option<NonZeroUsize> {
        self.def.capacity
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
        Ok(self.name.clone())
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
        // `FuturesOrdered` heap-allocates a node per source (≤
        // `MAX_PUBLICATION_SOURCES`). Kept over a hand-rolled poll loop on a
        // pinned `SmallVec`: this is a per-operation, I/O-bound cross-group read
        // (not the per-message/per-cell steady state the alloc rule targets), so
        // a bounded 16-node allocation alongside the store reads is not a
        // pessimization, and the ordered-with-early-exit fan-out reads far
        // clearer than the zero-alloc alternative.
        let mut ordered = FuturesOrdered::new();
        for source in sources {
            ordered.push_back(cooperative(
                async move { self.cached_point(source, cell).await },
            ));
        }
        let mut first_err = None;
        let mut idx = 0usize;
        while let Some(result) = ordered.next().await {
            match result {
                Ok(Some(value)) => {
                    let _ = self.pin.set(sources[idx].clone());
                    return Ok(Some(value));
                }
                Ok(None) => {}
                Err(error) => {
                    if first_err.is_none() {
                        first_err = Some(error);
                    }
                }
            }
            idx += 1;
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
                self.cached_batch(source, section, batch).await
            }));
        }
        let mut first_err = None;
        let mut all_none: Option<CellBuffer<Option<Bytes>>> = None;
        let mut idx = 0usize;
        while let Some(result) = ordered.next().await {
            match result {
                Ok(buffer) => {
                    if buffer.iter().any(Option::is_some) {
                        let _ = self.pin.set(sources[idx].clone());
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
            idx += 1;
        }
        match (all_none, first_err) {
            (Some(buffer), _) => Ok(buffer),
            (None, Some(error)) => Err(error),
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
                let inner = self.stores.scan_committed(&id, scan);
                futures::pin_mut!(inner);
                while let Some(item) = inner.next().await {
                    yield item?;
                }
                return;
            }
            let sources = self.snapshot.sources();
            let mut first_err = None;
            let mut pinned = false;
            for source in sources {
                let id = self.collection_id_for(source)?;
                let inner = self.stores.scan_committed(&id, scan);
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
                                // Post-pin mid-stream error: terminate, never
                                // restart (a restart would double-yield/skip).
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
