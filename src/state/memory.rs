//! In-memory keyed-state stores.

use super::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use super::descriptor_identity::{
    DescriptorIdentityStore, DurableDescriptorIdentity, RegisterOutcome,
};
use super::dirty::{Edge, remove_span};
use super::marker::{EventMarker, SectionClear};
use super::oracle::CommitOracle;
use super::publication::{PublicationRows, PublicationStore, StatePublication};
use super::registry::CollectionDefRegistry;
use super::resolve::{
    ResolveCellError, Resolver, flatten_resolve, help_read_window, help_write_window, peek_read,
    resolve_marker, resolve_read,
};
use super::store::{CellBuffer, CellStore, CoordinateBatch, provisional_point_loop};
use super::{CollectionId, CollectionRef, EventRef, StateName, StateType};
use crate::Topic;
use crate::state_reader::PUBLICATION_READ_LIMIT;
use crate::subsystem::SubsystemName;
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use futures::Stream;
use scc::hash_map::Entry;
use scc::{Guard, TreeIndex};
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::convert::Infallible;
use std::future::Future;
use std::ops::RangeInclusive;
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

    /// The raw stored [`Cell`] at `(collection, cell)`, defaulting a missing
    /// row to `Resolved(Committed(None))`. This body applies no oracle. Both
    /// the owner store's [`MemoryCellStore::read_raw`] and the reader's
    /// [`Self::read_committed`] call it, so they cannot disagree on the default
    /// shape.
    fn read_committed_cell(&self, collection: &CollectionId, cell: &CellKey) -> Cell {
        self.inner
            .read_sync(&(collection.clone(), cell.clone()), |_, stored| {
                stored.to_cell()
            })
            .unwrap_or_else(|| Cell::Resolved(Committed::new(None)))
    }

    /// A committed point read that applies no oracle. This is what the
    /// standalone reader uses against the in-memory backend. It projects
    /// [`Cell::project_committed`], never an in-flight provisional value and
    /// never owner-side repair.
    pub(crate) fn read_committed(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Option<Bytes> {
        self.read_committed_cell(collection, cell)
            .project_committed()
            .cloned()
    }

    /// Batch form of [`Self::read_committed`], index-aligned to `batch`.
    /// Mirrors Cassandra's
    /// [`read_committed_many`](crate::state::cassandra::CassandraCellResources::read_committed_many).
    /// Memory has no batch statement, so it reads each coordinate from the map
    /// separately: no dedup, one `Option<Bytes>` per input position.
    pub(crate) fn read_committed_many(
        &self,
        collection: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> CellBuffer<Option<Bytes>> {
        batch
            .iter()
            .map(|coordinate| {
                self.read_committed(
                    collection,
                    &CellKey {
                        section,
                        coordinate: coordinate.clone(),
                    },
                )
            })
            .collect()
    }

    /// A committed section scan that applies no oracle. It follows the same
    /// snapshot, sort, then yield steps as [`MemoryCellStore::scan_cells`], but
    /// skips the read-help and oracle resolution, projecting
    /// [`Cell::project_committed`] for each raw cell. The per-item projection
    /// is wrapped in [`cooperative`] so a large in-memory drain yields
    /// every ~128 items. The error type is [`Infallible`]: an in-memory
    /// committed projection cannot fail.
    pub(crate) fn scan_committed<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Infallible>> + Send + 'a {
        try_stream! {
            // Snapshot the matching raw cells synchronously (scc holds no
            // borrowing iterator across an await), then project each lazily.
            let mut raw: Vec<(CellKey, Cell)> = Vec::new();
            self.inner.iter_sync(|(id, cell), stored| {
                if id == collection
                    && cell.section == scan.section
                    && scan.contains(&cell.coordinate)
                {
                    raw.push((cell.clone(), stored.to_cell()));
                }
                true
            });
            raw.sort_by(|(a, _), (b, _)| a.coordinate.cmp(&b.coordinate));
            if scan.dir == Direction::Backward {
                raw.reverse();
            }
            let limit = scan.limit;
            let mut yielded = 0usize;
            for (cell, stored) in raw {
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                if let Some(bytes) =
                    cooperative(async move { stored.project_committed().cloned() }).await
                {
                    yield (cell, bytes);
                    yielded += 1;
                }
            }
        }
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

/// In-memory, uniform `CellStore` — the in-memory (and mock-mode) backend.
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
    /// `Resolved(Committed(None))`. Delegates to
    /// [`MemoryCells::read_committed_cell`], the oracle-free body shared with
    /// the reader.
    fn read_raw(&self, collection: &CollectionId, cell: &CellKey) -> Cell {
        self.cells.read_committed_cell(collection, cell)
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

    /// The raw resolved-write apply — erase the cleared sections, then upsert
    /// present values / remove absent ones (the row-absence invariant), and
    /// drop each cell's provisional coordinate from the [`WarmIndex`]. Shared
    /// by the trait [`write_resolved`](CellStore::write_resolved) (which runs
    /// the write-help boundary first) and the settle verbs (which run while the
    /// marker being settled still stands and must NOT re-enter the boundary —
    /// re-entry would recurse on that marker's own resolution).
    async fn apply_resolved(
        &self,
        collection: &CollectionId,
        cells: &[(CellKey, Option<Bytes>)],
        clears: &[SectionClear],
    ) {
        // Erase the cleared sections before upserting `cells` (belt-and-braces
        // — survivors are excluded positionally anyway).
        for clear in clears {
            self.erase_clear(collection, clear).await;
        }
        for (cell, data) in cells {
            match data {
                // Present value: upsert the resolved cell.
                Some(_) => {
                    self.map()
                        .upsert_async(
                            (collection.clone(), cell.clone()),
                            StoredCell::Resolved(data.clone()),
                        )
                        .await;
                }
                // Absent value: remove the entry (the row-absence invariant —
                // a missing entry already reads `Resolved(Committed(None))` via
                // `read_raw`'s default). Removing an absent key is a no-op.
                None => {
                    self.map()
                        .remove_async(&(collection.clone(), cell.clone()))
                        .await;
                }
            }
            // Rollback/committed-write resolves the cell; drop its provisional
            // coordinate (a no-op for a never-staged direct write).
            self.warm.clear(collection, cell).await;
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
                let committed =
                    cooperative(peek_read(self.resolver.oracle(), &collection_ref, own, stored))
                        .await
                        .map_err(ResolveCellError::Oracle)?;
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

    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>> + Send + 'a
    {
        // No batch query of its own — the raw point-loop reference, reading each
        // distinct coordinate through `provisional_cell_at` in ascending order.
        provisional_point_loop(self, collection, section, batch)
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
        // The write-side committed-unapplied boundary (`help_write_window`):
        // resolve any standing clears-bearing marker before this blind write,
        // ordering the write after that resolution so a stale clear's replay
        // cannot erase it (modulo the concurrent-resolver residual documented on
        // `help_write_window`). Marker-free otherwise (the marker lifecycle
        // belongs to the staged verbs). The marker map is RAM — the ~always
        // marker-free fast path costs one read.
        let standing = self.standing_marker(collection.id()).await?;
        help_write_window(self, self.resolver.oracle(), collection, standing.as_ref())
            .await
            .map_err(flatten_resolve)?;
        self.apply_resolved(collection.id(), cells, clears).await;
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
        // Route present-data cells to a promote (`mark_resolved`) and
        // absent-data cells to a row-deleting raw apply (the row-absence
        // invariant), then erase the clears and delete the marker. The raw
        // `apply_resolved` never re-enters the write-help boundary — the marker
        // being settled here still stands, so a re-entry would recurse on it.
        // All steps idempotent, and memory has no mid-call crash, so the
        // ordering carries no correctness weight (survivors are excluded from
        // the erase positionally either way).
        let mut keeps: CellBuffer<CellKey> = SmallVec::with_capacity(writes.len());
        let mut absents: CellBuffer<(CellKey, Option<Bytes>)> =
            SmallVec::with_capacity(writes.len());
        for (cell, write) in writes {
            if write.data().is_some() {
                keeps.push(cell.clone());
            } else {
                absents.push((cell.clone(), None));
            }
        }
        if !keeps.is_empty() {
            self.mark_resolved(collection, &keeps).await?;
        }
        if !absents.is_empty() {
            self.apply_resolved(collection.id(), &absents, &[]).await;
        }
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
        // Write each staged cell's committed base `prev` back as resolved
        // (`prev = None` restores exact absence) via the raw apply — which,
        // unlike the trait `write_resolved`, must not re-enter the write-help
        // boundary on the marker this abort is deleting — then delete the
        // marker.
        let cells: CellBuffer<(CellKey, Option<Bytes>)> = writes
            .iter()
            .map(|(cell, write)| (cell.clone(), write.prev().cloned()))
            .collect();
        self.apply_resolved(collection.id(), &cells, &[]).await;
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

/// The in-memory [`PublicationStore`], used only for discovery.
///
/// It holds a [`scc::TreeIndex`] keyed by the full publication primary key
/// `((subsystem, state_type, name), group_id, topic)`. A `read_publications`
/// is therefore a prefix range over one `(subsystem, state_type, name)`.
/// Cloning shares the `Arc`, mirroring the sibling memory stores.
///
/// Removal path: the [`remove_group`](PublicationStore::remove_group) verb
/// drops one group's whole slice of a collection, and dropping the last handle
/// drops the whole tree (mock-mode lifetime).
/// The map is bounded by the live published `(group, topic)` set of the
/// collections in play, so there is no unbounded keyed growth. It uses no
/// `Mutex`: scc is lock-free.
#[derive(Clone, Debug, Default)]
pub struct MemoryPublicationStore {
    rows: Arc<TreeIndex<PublicationKey, StatePublication>>,
}

impl MemoryPublicationStore {
    /// Creates an empty publication store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PublicationStore for MemoryPublicationStore {
    type Error = Infallible;

    async fn upsert(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        row: &StatePublication,
    ) -> Result<(), Self::Error> {
        self.rows.upsert_sync(
            publication_key(subsystem, state_type, name, row),
            row.clone(),
        );
        Ok(())
    }

    async fn remove_group(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: &str,
    ) -> Result<(), Self::Error> {
        remove_span(
            &self.rows,
            PublicationScope::group_range(subsystem, state_type, name, group_id),
        );
        Ok(())
    }

    async fn read_publications(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> Result<PublicationRows, Self::Error> {
        let guard = Guard::new();
        let out = self
            .rows
            .range(PublicationScope::range(subsystem, state_type, name), &guard)
            .take(PUBLICATION_READ_LIMIT)
            .map(|(_key, row)| row.clone())
            .collect();
        drop(guard);
        Ok(out)
    }
}

/// One publication row's address in [`MemoryPublicationStore`]'s tree: the full
/// primary key. Every field orders deterministically. In particular [`Topic`]
/// is `Intern<str>`, whose `Ord` compares the interned `str` lexically. So the
/// derived `Ord` total order is a valid tree ordering with no extra work.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PublicationKey {
    subsystem: SubsystemName,
    state_type: StateType,
    name: StateName,
    group_id: Arc<str>,
    topic: Topic,
}

/// Bounding query for a span of [`PublicationKey`]s. Compares on
/// `(subsystem, state_type, name)` and, when `group_id` is set, on the group
/// too, ignoring everything past that. So [`range`](Self::range) spans one
/// collection's sources — the `read_publications` sub-range — and
/// [`group_range`](Self::group_range) spans one group's slice of it, whatever
/// topics that group published under. See [`Edge`] for why each bound is a
/// strict separator.
#[derive(Clone, Eq, PartialEq)]
struct PublicationScope {
    subsystem: SubsystemName,
    state_type: StateType,
    name: StateName,
    group_id: Option<Arc<str>>,
    edge: Edge,
}

impl PublicationScope {
    /// The inclusive separator pair spanning `(subsystem, state_type, name)`'s
    /// sources.
    fn range(
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> RangeInclusive<Self> {
        Self::spanning(subsystem, state_type, name, None)
    }

    /// The inclusive separator pair spanning one group's sources of
    /// `(subsystem, state_type, name)`.
    fn group_range(
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: &str,
    ) -> RangeInclusive<Self> {
        Self::spanning(subsystem, state_type, name, Some(Arc::from(group_id)))
    }

    fn spanning(
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: Option<Arc<str>>,
    ) -> RangeInclusive<Self> {
        let at = |edge, group_id| Self {
            subsystem: subsystem.clone(),
            state_type,
            name: name.clone(),
            group_id,
            edge,
        };
        at(Edge::Low, group_id.clone())..=at(Edge::High, group_id)
    }

    fn cmp_key(&self, key: &PublicationKey) -> Ordering {
        self.subsystem
            .cmp(&key.subsystem)
            .then(self.state_type.cmp(&key.state_type))
            .then(self.name.cmp(&key.name))
            .then_with(|| {
                self.group_id.as_ref().map_or(Ordering::Equal, |group| {
                    group.as_ref().cmp(key.group_id.as_ref())
                })
            })
    }
}

impl scc::Equivalent<PublicationKey> for PublicationScope {
    fn equivalent(&self, key: &PublicationKey) -> bool {
        scc::Comparable::compare(self, key) == Ordering::Equal
    }
}

impl scc::Comparable<PublicationKey> for PublicationScope {
    fn compare(&self, key: &PublicationKey) -> Ordering {
        self.cmp_key(key).then(self.edge.beyond())
    }
}

/// Builds the tree key for one publication source.
fn publication_key(
    subsystem: &SubsystemName,
    state_type: StateType,
    name: &StateName,
    row: &StatePublication,
) -> PublicationKey {
    PublicationKey {
        subsystem: subsystem.clone(),
        state_type,
        name: name.clone(),
        group_id: row.group_id.clone(),
        topic: row.topic,
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
