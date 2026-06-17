//! The per-partition state store: the one combinator that fronts a durable
//! [`CellStore`] with a committed-value cache, a commit oracle, and a bounded
//! status map.
//!
//! It replaces the WAL-era `Layered` + `Recovering` stack. Reads are
//! cache-first and resolve a provisional cell through the oracle before
//! serving (first-touch). Writes stage provisionally or write resolved
//! values, patching the cache only at committed points (never at stage
//! time), so the cache holds committed values only. The status map lets the
//! recovery sweep skip cells already known resolved.
//!
//! # Cache discipline (invariant: cache-holds-committed-only)
//!
//! The cache is patched at exactly three committed points —
//! [`PartitionStateStore::promote`] (post-commit),
//! [`PartitionStateStore::write_resolved`], and resolution inside
//! [`PartitionStateStore::committed_value`] /
//! [`PartitionStateStore::sweep_collection`]. It is **never** patched at stage
//! time ([`PartitionStateStore::write_provisional`]); during the provisional
//! window the cached value correctly remains the committed `prev`. Cache
//! failures log and degrade — they never become the outer error, because the
//! durable backend is authoritative.

use super::cell::{Cell, Committed, ProvisionalWrite};
use super::identity::{CollectionId, CollectionKind, CollectionRef};
use super::oracle::CommitOracle;
use super::registry::CollectionDefRegistry;
use super::resolve::{ResolveCellError, resolve_cell};
use super::store::CellStore;
use crate::error::{ClassifyError, ErrorCategory};
use bytes::Bytes;
use futures::StreamExt;
use quick_cache::sync::Cache;
use std::error::Error;
use std::future::Future;
use std::slice;
use std::sync::Arc;
use thiserror::Error;
use tracing::{error, warn};

/// Capacity of the per-partition cell-status map. Bounded so a partition with
/// many keys cannot grow it without limit; an eviction simply forces the
/// sweep to re-read that cell, never to miss it.
const STATUS_CACHE_CAPACITY: usize = 16_384;

/// Process-local cache of committed cell values.
///
/// A hit answers a read without touching the durable backend; a miss falls
/// through to [`CellStore::read_cell`]. The cache holds committed values
/// only (see the module-level discipline). It is a hint: correctness rests
/// on the backend, so every method's failure is logged and degraded, never
/// propagated.
pub trait CommittedCache<K>: Clone + Send + Sync + 'static
where
    K: CollectionKind,
{
    /// Error type for cache operations.
    type Error: Error + Send + Sync + 'static;

    /// Looks up the cached committed value: `Some` on hit, `None` on miss.
    fn get<'a>(
        &'a self,
        collection: &'a CollectionId<K>,
        addr: &'a K::CellAddr,
    ) -> impl Future<Output = Result<Option<Committed>, Self::Error>> + Send + 'a;

    /// Patches the cache to a known-committed value.
    fn put<'a>(
        &'a self,
        collection: &'a CollectionId<K>,
        addr: &'a K::CellAddr,
        value: &'a Committed,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Removes the cached entry so the next read misses.
    fn invalidate<'a>(
        &'a self,
        collection: &'a CollectionId<K>,
        addr: &'a K::CellAddr,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Whether a collection's cells are all known resolved, used only to skip
/// reads in the recovery sweep. Never the source of truth — eviction forces
/// a re-read, never a missed cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CellStatus {
    /// At least one stage write happened and has not been observed resolved.
    Provisional,

    /// Every cell of the collection is known resolved.
    Resolved,
}

/// The per-partition state store: durable backend + oracle + committed-value
/// cache + bounded status map + the shared registry (for per-collection
/// TTLs bound on resolution write-backs).
#[derive(Clone)]
pub struct PartitionStateStore<K, S, O, C>
where
    K: CollectionKind,
{
    store: S,
    oracle: O,
    cache: C,
    registry: Arc<CollectionDefRegistry>,
    status: Arc<Cache<CollectionId<K>, CellStatus>>,
}

impl<K, S, O, C> PartitionStateStore<K, S, O, C>
where
    K: CollectionKind,
    S: CellStore<K>,
    O: CommitOracle,
    C: CommittedCache<K>,
{
    /// Composes a partition state store.
    #[must_use]
    pub fn new(store: S, oracle: O, cache: C, registry: Arc<CollectionDefRegistry>) -> Self {
        Self {
            store,
            oracle,
            cache,
            registry,
            status: Arc::new(Cache::new(STATUS_CACHE_CAPACITY)),
        }
    }

    /// The durable backend, for the recovery sweep's name enumeration and
    /// the external reader's bare reads.
    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Reads the committed value of a cell for the running handler.
    ///
    /// Cache-first; a miss reads the durable cell. A resolved cell's value is
    /// committed and cached. A provisional cell owned by `own_event` is the
    /// running handler's own (provably uncommitted) write, so the committed
    /// base is its `prev` — returned without an oracle consult or a durable
    /// write (the own-event-base-is-prev invariant). Any other provisional
    /// cell is resolved through the oracle and the committed result cached.
    ///
    /// # Errors
    ///
    /// Returns [`PartitionStoreError`] when the durable read, the oracle, or
    /// the resolution write fails. Cache failures never surface.
    pub async fn committed_value(
        &self,
        collection: &CollectionId<K>,
        addr: &K::CellAddr,
        own_event: super::EventRef,
    ) -> Result<Option<Bytes>, PartitionStoreError<S::Error, O::Error>> {
        Ok(self
            .committed(collection, addr, own_event)
            .await?
            .into_inner())
    }

    /// Reads the committed value of a cell as a [`Committed`], for the running
    /// handler's finalize. Same resolution as [`Self::committed_value`] — it is
    /// the resolved-read path, so minting the returned [`Committed`] stays
    /// inside the only place that can vouch the value is committed; `finalize`
    /// pairs it as a [`ProvisionalWrite`]'s `prev` without fabricating one.
    ///
    /// # Errors
    ///
    /// Returns [`PartitionStoreError`] when the durable read, the oracle, or
    /// the resolution write fails. Cache failures never surface.
    pub async fn committed(
        &self,
        collection: &CollectionId<K>,
        addr: &K::CellAddr,
        own_event: super::EventRef,
    ) -> Result<Committed, PartitionStoreError<S::Error, O::Error>> {
        match self.cache.get(collection, addr).await {
            Ok(Some(committed)) => return Ok(committed),
            Ok(None) => {}
            Err(error) => {
                warn!(error = %error, "committed-value cache read failed; falling through");
            }
        }

        let cell = self
            .store
            .read_cell(collection, addr)
            .await
            .map_err(PartitionStoreError::Store)?;
        let committed = match cell {
            Cell::Resolved(committed) => committed,
            Cell::Provisional(provisional) if provisional.event() == own_event => {
                Committed::new(provisional.into_prev())
            }
            Cell::Provisional(provisional) => {
                let collection_ref = self.collection_ref(collection);
                resolve_cell(
                    &self.store,
                    &self.oracle,
                    &collection_ref,
                    addr,
                    provisional,
                )
                .await?
            }
        };
        self.patch_cache(collection, addr, &committed).await;
        Ok(committed)
    }

    /// Reads one cell without resolving — the external committed-only
    /// projection. One point read, no oracle, no mutation.
    ///
    /// # Errors
    ///
    /// Returns the backend error when the read fails.
    pub async fn read_cell(
        &self,
        collection: &CollectionId<K>,
        addr: &K::CellAddr,
    ) -> Result<Cell, S::Error> {
        self.store.read_cell(collection, addr).await
    }

    /// Stages a provisional write. Marks the status `Provisional` *before*
    /// the durable write (write-ahead intent) and leaves the cache untouched
    /// — the cached value is still the committed `prev`.
    ///
    /// # Errors
    ///
    /// Returns the backend error when the stage write fails.
    pub async fn write_provisional(
        &self,
        collection: &CollectionRef<K>,
        addr: &K::CellAddr,
        write: &ProvisionalWrite,
    ) -> Result<(), S::Error> {
        self.status
            .insert(collection.id().clone(), CellStatus::Provisional);
        self.store
            .write_provisional(collection, &[(addr.clone(), write.clone())])
            .await
    }

    /// Writes a resolved value directly (the `ReadUncommitted` path and the
    /// mid-handler flush). Patches the cache to the written value.
    ///
    /// # Errors
    ///
    /// Returns the backend error when the write fails.
    pub async fn write_resolved(
        &self,
        collection: &CollectionRef<K>,
        addr: &K::CellAddr,
        data: Option<&Bytes>,
    ) -> Result<(), S::Error> {
        self.store
            .write_resolved(collection, &[(addr.clone(), data.cloned())])
            .await?;
        let committed = Committed::new(data.cloned());
        self.patch_cache(collection.id(), addr, &committed).await;
        self.mark_resolved(collection.id());
        Ok(())
    }

    /// Promotes a staged cell after the event committed: nulls `event`/`prev`
    /// durably (O(1) bytes), patches the cache to `data`, and marks the
    /// collection resolved. Best-effort — on a backend failure the cell stays
    /// provisional and the lazy paths converge it.
    ///
    /// # Errors
    ///
    /// Returns the backend error when the promote write fails.
    pub async fn promote(
        &self,
        collection: &CollectionRef<K>,
        addr: &K::CellAddr,
        data: Option<&Bytes>,
    ) -> Result<(), S::Error> {
        self.store
            .mark_resolved(collection, slice::from_ref(addr))
            .await?;
        let committed = Committed::new(data.cloned());
        self.patch_cache(collection.id(), addr, &committed).await;
        self.mark_resolved(collection.id());
        Ok(())
    }

    /// Stages a collection's touched cells in **one** backend batch (the lane's
    /// bulk stage). Marks the collection `Provisional` before the durable write
    /// (write-ahead intent) and leaves the cache untouched — the cached value
    /// is still the committed `prev`. One [`CellStore::write_provisional`]
    /// call regardless of cell count, so a Map collection's hundreds of
    /// entries cost one round-trip, not hundreds.
    ///
    /// # Errors
    ///
    /// Returns the backend error when the stage write fails.
    pub(crate) async fn write_provisional_batch(
        &self,
        collection: &CollectionRef<K>,
        writes: &[(K::CellAddr, ProvisionalWrite)],
    ) -> Result<(), S::Error> {
        self.status
            .insert(collection.id().clone(), CellStatus::Provisional);
        self.store.write_provisional(collection, writes).await
    }

    /// Writes a collection's touched cells as resolved values in **one**
    /// backend batch (the `ReadUncommitted` stage and the rollback arm).
    /// Patches the cache per cell to the written value and marks the
    /// collection resolved.
    ///
    /// # Errors
    ///
    /// Returns the backend error when the write fails.
    pub(crate) async fn write_resolved_batch(
        &self,
        collection: &CollectionRef<K>,
        cells: &[(K::CellAddr, Option<Bytes>)],
    ) -> Result<(), S::Error> {
        self.store.write_resolved(collection, cells).await?;
        for (addr, data) in cells {
            self.patch_cache(collection.id(), addr, &Committed::new(data.clone()))
                .await;
        }
        self.mark_resolved(collection.id());
        Ok(())
    }

    /// Promotes a collection's staged cells after commit in **one** backend
    /// batch: nulls each cell's `event`/`prev` (O(1) bytes), patches the cache
    /// to `data`, and marks the collection resolved. One
    /// [`CellStore::mark_resolved`] call regardless of cell count.
    ///
    /// # Errors
    ///
    /// Returns the backend error when the promote write fails.
    pub(crate) async fn promote_batch(
        &self,
        collection: &CollectionRef<K>,
        cells: &[(K::CellAddr, Option<Bytes>)],
    ) -> Result<(), S::Error> {
        let addrs: Vec<K::CellAddr> = cells.iter().map(|(addr, _)| addr.clone()).collect();
        self.store.mark_resolved(collection, &addrs).await?;
        for (addr, data) in cells {
            self.patch_cache(collection.id(), addr, &Committed::new(data.clone()))
                .await;
        }
        self.mark_resolved(collection.id());
        Ok(())
    }

    /// Rolls a staged cell back to its committed base (inline abandon, before
    /// any marker flush). Writes `prev` as the resolved value and patches the
    /// cache.
    ///
    /// # Errors
    ///
    /// Returns the backend error when the write fails.
    pub async fn rollback_provisional(
        &self,
        collection: &CollectionRef<K>,
        addr: &K::CellAddr,
        prev: Option<&Bytes>,
    ) -> Result<(), S::Error> {
        self.write_resolved(collection, addr, prev).await
    }

    /// Resolves every provisional cell of a collection through the oracle
    /// (the quiescence sweep). Returns `true` iff every cell ended resolved —
    /// the caller unschedules the backstop only then (the no-strand
    /// invariant). A per-cell Permanent failure is logged and skipped,
    /// leaving the cell for first-touch or a later sweep; anything else
    /// propagates so the trigger aborts and the sweep refires.
    ///
    /// # Errors
    ///
    /// Returns [`PartitionStoreError`] on a transient/terminal backend or
    /// oracle failure, or a `provisional_cells` stream failure.
    pub async fn sweep_collection(
        &self,
        collection: &CollectionRef<K>,
    ) -> Result<bool, PartitionStoreError<S::Error, O::Error>> {
        if matches!(self.status.get(collection.id()), Some(CellStatus::Resolved)) {
            return Ok(true);
        }

        let stream = self.store.provisional_cells(collection.id());
        futures::pin_mut!(stream);
        let mut all_resolved = true;
        while let Some(item) = stream.next().await {
            let (addr, provisional) = item.map_err(PartitionStoreError::Store)?;
            match resolve_cell(&self.store, &self.oracle, collection, &addr, provisional).await {
                Ok(committed) => self.patch_cache(collection.id(), &addr, &committed).await,
                Err(error) if error.classify_error() == ErrorCategory::Permanent => {
                    error!(
                        name = collection.id().name().as_str(),
                        "skipping permanently-failing provisional cell; first-touch or the cell \
                         TTL must resolve it: {error:#}"
                    );
                    all_resolved = false;
                }
                Err(error) => return Err(error.into()),
            }
        }
        if all_resolved {
            self.mark_resolved(collection.id());
        }
        Ok(all_resolved)
    }

    /// Builds the per-collection [`CollectionRef`] with its registry TTL.
    fn collection_ref(&self, id: &CollectionId<K>) -> CollectionRef<K> {
        CollectionRef::new(
            id.clone(),
            self.registry.ttl_for(id.state_type(), id.name()),
        )
    }

    /// Patches the committed-value cache, logging and degrading on failure.
    async fn patch_cache(&self, id: &CollectionId<K>, addr: &K::CellAddr, value: &Committed) {
        if let Err(error) = self.cache.put(id, addr, value).await {
            warn!(error = %error, "committed-value cache patch failed; invalidating");
            if let Err(error) = self.cache.invalidate(id, addr).await {
                warn!(error = %error, "committed-value cache invalidation failed; entry may be stale");
            }
        }
    }

    /// Marks a collection's cells all-resolved in the status map.
    fn mark_resolved(&self, id: &CollectionId<K>) {
        self.status.insert(id.clone(), CellStatus::Resolved);
    }
}

/// Error raised by [`PartitionStateStore`] read and resolution paths.
#[derive(Debug, Error)]
pub enum PartitionStoreError<StoreErr, OracleErr>
where
    StoreErr: Error + 'static,
    OracleErr: Error + 'static,
{
    /// The durable cell store failed.
    #[error("keyed-state cell store failed")]
    Store(#[source] StoreErr),

    /// The commit oracle failed.
    #[error("keyed-state commit oracle failed")]
    Oracle(#[source] OracleErr),
}

impl<StoreErr, OracleErr> ClassifyError for PartitionStoreError<StoreErr, OracleErr>
where
    StoreErr: ClassifyError + Error + 'static,
    OracleErr: ClassifyError + Error + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Store(e) => e.classify_error(),
            Self::Oracle(e) => e.classify_error(),
        }
    }
}

impl<StoreErr, OracleErr> From<ResolveCellError<StoreErr, OracleErr>>
    for PartitionStoreError<StoreErr, OracleErr>
where
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
    OracleErr: ClassifyError + Error + Send + Sync + 'static,
{
    fn from(error: ResolveCellError<StoreErr, OracleErr>) -> Self {
        match error {
            ResolveCellError::Store(e) => Self::Store(e),
            ResolveCellError::Oracle(e) => Self::Oracle(e),
        }
    }
}

#[cfg(test)]
mod tests;
