//! In-memory keyed-state stores.

use super::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Direction, Scan};
use super::descriptor_identity::{
    DescriptorIdentityStore, DurableDescriptorIdentity, RegisterOutcome,
};
use super::marker::{EventMarker, SectionClear};
use super::oracle::CommitOracle;
use super::registry::CollectionDefRegistry;
use super::resolve::{
    ResolveCellError, Resolver, flatten_resolve, help_read_window, resolve_marker, resolve_read,
};
use super::store::{CellStore, route_abort, route_commit};
use super::{CollectionId, CollectionRef, EventRef, StateType};
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use futures::Stream;
use scc::hash_map::Entry;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::task::coop::cooperative;

/// Group-global identity key: `(group_id, state_type discriminator, name)`.
type IdentityKey = (String, i8, String);

/// The in-memory cell map: cells keyed by `(CollectionId, CellKey)`.
type CellMap = scc::HashMap<(CollectionId, CellKey), StoredCell, RandomState>;

/// The in-memory event-marker map: one standing [`EventMarker`] per collection.
type MarkerMap = scc::HashMap<CollectionId, EventMarker, RandomState>;

/// A process-wide shareable in-memory cell map.
///
/// The oracle-independent half of [`MemoryCellStore`]: the cells themselves,
/// keyed by `(CollectionId, CellKey)`, plus the standing **event markers**
/// (mirroring the Cassandra marker rows). Both are the *durable* half — the
/// marker map is this backend's whole marker slice, and it must survive the
/// `make_store()` crash exactly as Cassandra marker rows would, so it lives
/// here and not on the per-assignment [`MemoryCellStore`]. There is no seed or
/// memo because RAM *is* the memo. One instance is shared across partition
/// assignments (`Clone` shares the `Arc`), so committed state survives a
/// rebalance within the process — each partition wraps it in a fresh
/// [`MemoryCellStore`] carrying that partition's oracle.
#[derive(Clone, Debug, Default)]
pub struct MemoryCells {
    inner: Arc<CellMap>,
    markers: Arc<MarkerMap>,
}

impl MemoryCells {
    /// Creates an empty shared cell map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every stored cell key for `collection`, **regardless of variant** — the
    /// physical-shape probe for the row-absence invariant, so a lingering
    /// `Resolved(None)` entry (which must be a removal, not a tombstone) is
    /// visible as an extra member.
    #[cfg(test)]
    pub(crate) fn stored_coordinates(&self, collection: &CollectionId) -> Vec<CellKey> {
        let mut out = Vec::new();
        self.inner.iter_sync(|(id, cell), _stored| {
            if id == collection {
                out.push(cell.clone());
            }
            true
        });
        out
    }

    /// The stored cell keys currently in the `Provisional` variant — the
    /// staged-cell probe's raw view (never routed through the resolving
    /// store, which would first-touch-resolve what it reads).
    #[cfg(test)]
    pub(crate) fn provisional_coordinates(&self, collection: &CollectionId) -> Vec<CellKey> {
        let mut out = Vec::new();
        self.inner.iter_sync(|(id, cell), stored| {
            if id == collection && matches!(stored, StoredCell::Provisional { .. }) {
                out.push(cell.clone());
            }
            true
        });
        out
    }

    /// The collection's standing event marker read straight from the durable
    /// backing — the marker-shape probe, never routed through the resolving
    /// store.
    #[cfg(test)]
    pub(crate) fn standing_marker_of(&self, collection: &CollectionId) -> Option<EventMarker> {
        self.markers
            .read_sync(collection, |_, marker| marker.clone())
    }
}

/// In-memory, uniform [`CellStore`] — the in-memory (and mock-mode) backend.
///
/// The provisional-cell durable backend keyed by `(CollectionId, CellKey)`:
/// each cell is either resolved or provisional. Resolution of in-flight
/// provisional cells funnels through the composed `Resolver` — the same
/// oracle/registry the production store uses — so `get`/`scan_cells` return
/// resolved [`Committed`] cells exactly as Cassandra does.
#[derive(Clone, Debug)]
pub struct MemoryCellStore<O> {
    cells: MemoryCells,
    resolver: Resolver<O>,
    /// The in-RAM provisional-coordinate index gating the recovery sweep — see
    /// [`WarmIndex`] for its seed/re-seed semantics. `Arc` so the per-event
    /// store clones share one instance, minted fresh per store so a re-acquired
    /// partition re-seeds from the process-shared `cells`.
    warm: Arc<WarmIndex>,
}

impl<O> MemoryCellStore<O> {
    /// Wraps a shared cell map, resolving through `oracle` and binding
    /// per-collection TTLs from `registry` on resolution write-backs.
    #[must_use]
    pub(crate) fn new(cells: MemoryCells, oracle: O, registry: Arc<CollectionDefRegistry>) -> Self {
        Self {
            cells,
            resolver: Resolver::new(oracle, registry),
            warm: Arc::default(),
        }
    }
}

impl<O> MemoryCellStore<O>
where
    O: CommitOracle,
{
    /// The raw stored cell at `(collection, cell)`, defaulting a missing row to
    /// `Resolved(Committed(None))`.
    fn read_raw(&self, collection: &CollectionId, cell: &CellKey) -> Cell {
        self.map()
            .read_sync(&(collection.clone(), cell.clone()), |_, stored| {
                stored.to_cell()
            })
            .unwrap_or_else(|| Cell::Resolved(Committed::new(None)))
    }

    /// The shared cell map.
    fn map(&self) -> &CellMap {
        &self.cells.inner
    }

    /// The committed-unapplied read window, shared verbatim by `get` and
    /// `scan_cells`: resolve a standing FOREIGN clears-bearing marker
    /// (`help_read_window`) before the raw read/snapshot, so both paths serve
    /// post-clear truth. The marker map is RAM — the ~always marker-free fast
    /// path costs one map read. Both read paths MUST run identical help;
    /// funneling them through one helper makes drift structurally impossible.
    async fn read_help(
        &self,
        collection_ref: &CollectionRef,
        own: EventRef,
    ) -> Result<(), ResolveCellError<Infallible, O::Error>> {
        let marker = self.standing_marker(collection_ref.id()).await?;
        help_read_window(
            self,
            self.resolver.oracle(),
            collection_ref,
            marker.as_ref(),
            own,
        )
        .await
        .map_err(flatten_resolve)?;
        Ok(())
    }

    /// Applies one frozen section clear: removes every stored cell of the
    /// cleared section whose coordinate is not a survivor (positional
    /// exclusion — the frozen list is sorted, so a binary search decides), and
    /// clears each removed coordinate from the [`WarmIndex`]. Erasing a
    /// still-provisional foreign entry is correct (the erasure argument on
    /// [`CellStore::commit_provisional`]); removal is idempotent.
    async fn erase_clear(&self, collection: &CollectionId, clear: &SectionClear) {
        let mut removed: Vec<CellKey> = Vec::new();
        self.map().iter_sync(|(id, cell), _stored| {
            if id == collection
                && cell.section == clear.section()
                && clear.survivors().binary_search(&cell.coordinate).is_err()
            {
                removed.push(cell.clone());
            }
            true
        });
        for cell in removed {
            self.map()
                .remove_async(&(collection.clone(), cell.clone()))
                .await;
            self.warm.clear(collection, &cell).await;
        }
    }
}

impl<O> CellStore for MemoryCellStore<O>
where
    O: CommitOracle,
{
    type Error = ResolveCellError<Infallible, O::Error>;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<Committed, Self::Error> {
        let collection_ref = self.resolver.collection_ref(collection);
        self.read_help(&collection_ref, own).await?;
        let raw = self.read_raw(collection, cell);
        resolve_read(
            self,
            self.resolver.oracle(),
            &collection_ref,
            cell,
            own,
            raw,
        )
        .await
        .map_err(flatten_resolve)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        let collection_ref = self.resolver.collection_ref(collection);
        try_stream! {
            // Read-help before the snapshot (see `read_help`), so the snapshot
            // below reads post-clear truth.
            self.read_help(&collection_ref, own).await?;
            // Snapshot the matching raw cells synchronously (scc holds no
            // borrowing iterator across an await), then resolve each lazily.
            let mut raw: Vec<(CellKey, Cell)> = Vec::new();
            self.map().iter_sync(|(id, cell), stored| {
                if id == collection && cell.section == scan.section && scan.contains(&cell.coordinate) {
                    raw.push((cell.clone(), stored.to_cell()));
                }
                true
            });
            raw.sort_by(|(a, _), (b, _)| a.coordinate.cmp(&b.coordinate));
            if scan.dir == Direction::Backward {
                raw.reverse();
            }
            let limit = scan.limit;
            // The limit bounds *yielded* (present) cells, not raw rows: a cleared
            // or rolled-back-to-absent cell in range is skipped without consuming
            // a limit slot (matching the Cassandra scan's `yielded` counter).
            //
            // The resolved fast path touches no tokio leaf, so a large in-memory
            // scan would drain in one poll; a per-item `cooperative` yield point
            // fires every ~128 items.
            let mut yielded = 0usize;
            for (cell, stored) in raw {
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                let committed = cooperative(resolve_read(
                    self,
                    self.resolver.oracle(),
                    &collection_ref,
                    &cell,
                    own,
                    stored,
                ))
                .await
                .map_err(flatten_resolve)?;
                if let Some(bytes) = committed.into_inner() {
                    yield (cell, bytes);
                    yielded += 1;
                }
            }
        }
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        try_stream! {
            // The coordinates to visit. Warm (seeded): the in-RAM index — an
            // empty snapshot yields nothing and never scans the map. Cold: the
            // one-time full-map seed scan that populates the index and marks the
            // collection seeded, mirroring the Cassandra cold marker seed.
            let coords = if self.warm.is_seeded(collection).await {
                self.warm.snapshot(collection)
            } else {
                let mut coords: Vec<CellKey> = Vec::new();
                self.map().iter_sync(|(id, cell), stored| {
                    if id == collection && matches!(stored, StoredCell::Provisional { .. }) {
                        coords.push(cell.clone());
                    }
                    true
                });
                for cell in &coords {
                    self.warm.record(collection, cell).await;
                }
                self.warm.mark_seeded(collection).await;
                coords
            };
            // Point-read each coordinate; a concurrently-resolved coordinate
            // decodes `Resolved` and is dropped (over-report-safe). The reads
            // touch no tokio leaf, so a per-item `cooperative` yield point
            // releases a large recovery drain to the runtime every ~128 items.
            for cell in coords {
                if let Cell::Provisional(provisional) = self.read_raw(collection, &cell) {
                    yield cooperative(async move { (cell, provisional) }).await;
                }
            }
        }
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        Ok(match self.read_raw(collection, cell) {
            Cell::Provisional(provisional) => Some(provisional),
            Cell::Resolved(_) => None,
        })
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        // `None` ⇒ the explicit empty-stage no-op: no marker and no boundary
        // check (nothing to strand). A clears-only stage passes a marker with
        // empty `staged()` and runs the boundary like any stage.
        debug_assert!(
            marker.is_some() || writes.is_empty(),
            "a markerless stage must write nothing"
        );
        if let Some(marker) = marker {
            debug_assert!(
                writes
                    .iter()
                    .all(|(cell, _)| marker.staged().binary_search(cell).is_ok()),
                "every staged write must be listed by the event marker"
            );
            // Stage boundary: resolve any standing FOREIGN marker (a different
            // event) before overwriting it, establishing marker uniqueness per
            // collection. A resolution failure fails the stage.
            if let Some(standing) = self
                .cells
                .markers
                .read_async(collection.id(), |_, marker| marker.clone())
                .await
                && standing.event() != marker.event()
            {
                resolve_marker(self, self.resolver.oracle(), collection, &standing)
                    .await
                    .map_err(flatten_resolve)?;
            }
            // Marker-first: order-irrelevant in memory (no mid-call crash), but
            // mirrors the documented stage ordering.
            self.cells
                .markers
                .upsert_async(collection.id().clone(), marker.clone())
                .await;
        }
        for (cell, write) in writes {
            self.map()
                .upsert_async(
                    (collection.id().clone(), cell.clone()),
                    StoredCell::Provisional {
                        data: write.data().cloned(),
                        prev: write.prev().cloned(),
                        event: write.event(),
                    },
                )
                .await;
            // Record after the write lands (an in-memory upsert never fails).
            self.warm.record(collection.id(), cell).await;
        }
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // Erase the cleared sections before upserting `cells` (belt-and-braces
        // — survivors are excluded positionally anyway). `write_resolved`
        // never touches the marker map.
        for clear in clears {
            self.erase_clear(collection.id(), clear).await;
        }
        for (cell, data) in cells {
            match data {
                // Present value: upsert the resolved cell.
                Some(_) => {
                    self.map()
                        .upsert_async(
                            (collection.id().clone(), cell.clone()),
                            StoredCell::Resolved(data.clone()),
                        )
                        .await;
                }
                // Absent value: remove the entry (the row-absence invariant —
                // a missing entry already reads `Resolved(Committed(None))` via
                // `read_raw`'s default). Removing an absent key is a no-op.
                None => {
                    self.map()
                        .remove_async(&(collection.id().clone(), cell.clone()))
                        .await;
                }
            }
            // Rollback/committed-write resolves the cell; drop its provisional
            // coordinate (a no-op for a never-staged direct write).
            self.warm.clear(collection.id(), cell).await;
        }
        Ok(())
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        for cell in cells {
            if let Entry::Occupied(mut entry) = self
                .map()
                .entry_async((collection.id().clone(), cell.clone()))
                .await
                && let StoredCell::Provisional { data, .. } = entry.get()
            {
                let data = data.clone();
                *entry.get_mut() = StoredCell::Resolved(data);
            }
            // Promote resolves the cell; drop its provisional coordinate
            // (idempotent — clearing an absent coordinate is a no-op).
            self.warm.clear(collection.id(), cell).await;
        }
        Ok(())
    }

    async fn standing_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        Ok(self
            .cells
            .markers
            .read_async(collection, |_, marker| marker.clone())
            .await)
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // Route, erase, delete — all idempotent, and memory has no mid-call
        // crash, so the ordering carries no correctness weight (survivors are
        // excluded from the erase positionally either way).
        route_commit(self, collection, writes).await?;
        for clear in clears {
            self.erase_clear(collection.id(), clear).await;
        }
        // Settle owns the marker delete; removing an absent marker is a no-op
        // (idempotent settle).
        self.cells.markers.remove_async(collection.id()).await;
        Ok(())
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        route_abort(self, collection, writes).await?;
        self.cells.markers.remove_async(collection.id()).await;
        Ok(())
    }
}

/// In-memory group-global [`DescriptorIdentityStore`].
///
/// The control-plane half of in-memory keyed state, decoupled from any cell
/// data. One instance is shared process-wide across partition reassignments
/// (`Clone` shares the `Arc`), so registered identities survive a rebalance
/// within the process — the property `acquire_descriptor_identities` relies on
/// to coalesce cross-partition first-acquires.
#[derive(Clone, Debug, Default)]
pub struct MemoryDescriptorIdentityStore {
    inner: Arc<scc::HashMap<IdentityKey, DurableDescriptorIdentity, RandomState>>,
}

impl MemoryDescriptorIdentityStore {
    /// Creates an empty identity store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl DescriptorIdentityStore for MemoryDescriptorIdentityStore {
    type Error = Infallible;

    async fn read_identity(
        &self,
        group_id: &str,
        state_type: StateType,
        name: &str,
    ) -> Result<Option<DurableDescriptorIdentity>, Self::Error> {
        let key = (group_id.to_owned(), state_type.into(), name.to_owned());
        Ok(self.inner.read_async(&key, |_, row| row.clone()).await)
    }

    async fn register_identity(
        &self,
        group_id: &str,
        row: &DurableDescriptorIdentity,
    ) -> Result<RegisterOutcome, Self::Error> {
        // Atomic insert-if-absent, mirroring Cassandra's `INSERT … IF NOT
        // EXISTS`: a present key yields `Conflict(existing)` so the caller
        // validates without a re-read.
        let key = (group_id.to_owned(), row.state_type, row.name.clone());
        match self.inner.entry_async(key).await {
            Entry::Vacant(slot) => {
                slot.insert_entry(row.clone());
                Ok(RegisterOutcome::Applied)
            }
            Entry::Occupied(existing) => Ok(RegisterOutcome::Conflict(existing.get().clone())),
        }
    }
}

/// The memory backend's in-RAM provisional-coordinate index gating the recovery
/// sweep — the analog of the Cassandra path's disk-backed fjall index.
///
/// `coords` is the live provisional `(collection, cell)` set; `seeded` records
/// the collections whose one-time cold seed scan has run (an unseeded
/// collection re-scans [`MemoryCells`] before it can short-circuit). Held
/// behind an `Arc` on [`MemoryCellStore`] and minted fresh per store, so a
/// re-acquired partition is cold and re-seeds. In-RAM and infallible, so unlike
/// the fjall index its methods carry no `Result`.
///
/// The invariant: `is_seeded(c)` ⟹ `coords` holds every provisional coordinate
/// of `c`. `record` on stage, `clear` on resolve, and the cold seed's scan
/// uphold it; no failure path can leave `coords` incomplete while seeded.
#[derive(Debug, Default)]
struct WarmIndex {
    seeded: scc::HashSet<CollectionId, RandomState>,
    coords: scc::HashSet<(CollectionId, CellKey), RandomState>,
}

impl WarmIndex {
    /// Whether `collection`'s one-time cold seed scan has run.
    async fn is_seeded(&self, collection: &CollectionId) -> bool {
        self.seeded.contains_async(collection).await
    }

    /// Marks `collection` seeded once its cold seed scan completes.
    async fn mark_seeded(&self, collection: &CollectionId) {
        let _ = self.seeded.insert_async(collection.clone()).await;
    }

    /// Records `cell` as provisional in `collection` (after a stage).
    async fn record(&self, collection: &CollectionId, cell: &CellKey) {
        let _ = self
            .coords
            .insert_async((collection.clone(), cell.clone()))
            .await;
    }

    /// Clears `cell` from `collection` (after a promote/rollback).
    async fn clear(&self, collection: &CollectionId, cell: &CellKey) {
        self.coords
            .remove_async(&(collection.clone(), cell.clone()))
            .await;
    }

    /// Snapshots `collection`'s provisional coordinates — the recovery drain
    /// buffer, sized to `#provisional`. Empty ⟹ the warm sweep scans nothing.
    fn snapshot(&self, collection: &CollectionId) -> Vec<CellKey> {
        let mut out = Vec::new();
        self.coords.iter_sync(|(id, cell)| {
            if id == collection {
                out.push(cell.clone());
            }
            true
        });
        out
    }
}

/// One stored cell in [`MemoryCellStore`].
#[derive(Clone, Debug)]
enum StoredCell {
    Resolved(Option<Bytes>),
    Provisional {
        data: Option<Bytes>,
        prev: Option<Bytes>,
        event: EventRef,
    },
}

impl StoredCell {
    fn to_cell(&self) -> Cell {
        match self {
            Self::Resolved(data) => Cell::Resolved(Committed::new(data.clone())),
            Self::Provisional { data, prev, event } => {
                Cell::Provisional(ProvisionalCell::new(data.clone(), prev.clone(), *event))
            }
        }
    }
}
