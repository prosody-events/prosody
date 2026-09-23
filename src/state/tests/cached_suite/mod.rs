//! Memory-backed properties and tests for the write-through K/V cache.
//!
//! The production [`Cached`] path only assembles over Cassandra in production,
//! so the backend-generic flagship exercises it solely through the
//! live-cluster arm at the 25-iteration `INTEGRATION_TESTS` count. These tests
//! put the **real** `Cached` over a memory lower store and a real fjall cache
//! (the shared test database), so the same cache code runs at full
//! `QUICKCHECK_TESTS` with no cluster.
//!
//! [`transparency::prop_cached_is_transparent`] compares cached and uncached
//! stores. Both stores must return the same result after every generated
//! operation.

use crate::state::cell::{CacheEntry, Presence, Projection, Read, Values};
use crate::state::cell_key::CellRef;
use crate::state::marker::ProvisionalStage;
use crate::state::store::{CacheBatch, CellBackend, CellRead, CommittedBatch, Durable, ReadBatch};
use crate::state::tests::support::listed;

use super::super::cached::{Cached, DELETE_RETRY_BUDGET};
use super::super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::super::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use super::super::fjall::test_db;
use super::super::fjall::{Clock, FjallCellCache};
use super::super::marker::{EventMarker, SectionClear};
use super::super::memory::{MemoryCellStore, MemoryCells};
use super::super::store::{CellBuffer, CellStore, CoordinateBatch};
use super::super::{CollectionId, CollectionRef, EventRef};
use super::cell_suite::{
    FailingCellStore, MemoryDeduplicationStore, MemoryShapeProbe, OverlayTrace, Poison,
    PoisonHandle, SECTION, ScanTrace, Trace, bytes, cell_at, run_bottom_scan_trace,
    run_crash_equivalence_trace, run_overlay_trace,
};
use super::support::{
    CountProjection, CountingCellStore, HoldingCellStore, batch_of, fresh_collection as collection,
    probe,
};
use crate::error::ErrorCategory;
use crate::state::marker::MarkerState;
use crate::state::tests::support::{admit_collection, evidence, seed_commit_evidence};
use crate::test_util::{GlobalMetrics, TEST_RUNTIME, labels};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::{Stream, StreamExt};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::ops::Bound;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

mod batch;
mod fills;
mod promote;
mod publish;
mod reads;
mod transparency;
mod ttl;
mod writes;
use batch::counting_cached;
use promote::stage_committed_marker;

/// Builds a production-shaped `Cached` over the shared fjall database (the
/// `name` warm-reuse keyspace pair) and the shared memory cells.
fn cached_over(cells: &MemoryCells, name: &str) -> Result<Cached<MemoryCellStore>> {
    let lower = MemoryCellStore::new(cells.clone());
    Ok(Cached::new(test_db::cache(name)?, lower))
}

/// A memory store that supplies a fixed row expiry for every projection.
/// An absent projection can retain a row TTL, as a provisional cell can.
/// The shared clock supplies the remaining whole seconds.
/// Other operations preserve the inner store's counters.
#[derive(Clone)]
struct TtlAwareCellStore<S> {
    inner: CountingCellStore<S>,
    clock: Clock,
    death: u64,
}

impl<S> TtlAwareCellStore<S> {
    fn new(inner: CountingCellStore<S>, clock: Clock, death: u64) -> Self {
        Self {
            inner,
            clock,
            death,
        }
    }

    fn lower_reads(&self) -> usize {
        self.inner.lower_reads()
    }

    fn reset(&self) {
        self.inner.reset();
    }

    /// The whole remaining seconds against the fixed clock — the FLOOR
    /// `TTL(data)` reports for a live row, `None` once `death` has passed.
    fn remaining(&self) -> Option<CompactDuration> {
        let now = self.clock.now_ms();
        (now < self.death).then(|| {
            CompactDuration::new(u32::try_from((self.death - now) / 1_000).unwrap_or(u32::MAX))
        })
    }
}

impl<S: CellBackend> CellBackend for TtlAwareCellStore<S> {
    type Error = S::Error;
}

impl<S: CellRead<P>, P: CountProjection> CellRead<P> for TtlAwareCellStore<S> {
    async fn read<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: CellRef<'a>,
    ) -> Result<Durable<P>, Self::Error> {
        {
            let (committed, _) = CellRead::<P>::read(&self.inner, collection, cell).await?;
            Ok((committed, self.remaining()))
        }
    }

    async fn read_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a ReadBatch<'_>,
    ) -> Result<CacheBatch<P>, Self::Error> {
        let mut cells = CellRead::<P>::read_many(&self.inner, collection, section, batch).await?;
        let remaining = self.remaining();
        for (_, ttl) in &mut cells {
            *ttl = remaining;
        }
        Ok(cells)
    }

    fn scan<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), Self::Error>> + Send + use<'a, S, P> {
        CellRead::<P>::scan(&self.inner, collection, scan)
    }
}

impl<S> CellStore for TtlAwareCellStore<S>
where
    S: CellStore,
{
    fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> impl Future<Output = Result<Option<ProvisionalCell>, Self::Error>> + Send + use<'a, S>
    {
        self.inner.provisional_cell_at(collection, cell)
    }

    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>>
    + Send
    + use<'a, S> {
        self.inner.provisional_many(collection, section, batch)
    }

    fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        stage: ProvisionalStage<'a>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, S> {
        self.inner.write_provisional(collection, stage)
    }

    fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, S> {
        self.inner.write_resolved(collection, cells, clears)
    }

    fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, S> {
        self.inner.mark_resolved(collection, cells)
    }

    async fn marker_state<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<MarkerState, Self::Error> {
        self.inner.marker_state(collection).await
    }

    fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        marker: &'a EventMarker,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, S> {
        self.inner.commit_provisional(collection, marker, writes)
    }

    fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, S> {
        self.inner.abort_provisional(collection, writes)
    }
}

/// Unified-view soundness over `Overlay<Cached<MemoryCellStore>>`: the real
/// cache (warm hits, fall-through fills, negative caching) must answer
/// **identically** to the dirty-over-committed `BTreeMap` oracle after every
/// intermixed `get`/`scan`/`set`/`clear` — the warmth-invariance differential,
/// at full `QUICKCHECK_TESTS`.
#[test]
fn prop_memory_cached_overlay_view() {
    fn property(trace: OverlayTrace) -> Result<bool> {
        let cells = MemoryCells::new();
        let lower = cached_over(&cells, "overlay")?;
        TEST_RUNTIME.block_on(run_overlay_trace(lower, trace))
    }
    QuickCheck::new().quickcheck(property as fn(OverlayTrace) -> Result<bool>);
}

/// Collects a forward scan over `start` to `end`, mapping each cell to its
/// single coordinate byte. A whole-section scan passes a `Bound::Included`
/// of a dominating sentinel (`255`) rather than an unbounded edge.
async fn scan_forward<S>(
    store: &S,
    id: &CollectionId,
    start: u8,
    end: Bound<u8>,
) -> Result<Vec<(Vec<u8>, Bytes)>>
where
    S: CellStore,
{
    let start_c = Coordinate::from_bytes(vec![start]);
    let end_c = end.map(|b| Coordinate::from_bytes(vec![b]));
    let scan = Scan {
        section: SECTION,
        start: Bound::Included(start_c.as_bytes()),
        dir: Direction::Forward,
        end: end_c.as_ref().map(Coordinate::as_bytes),
        fetch_hint: None,
    };
    let stream = CellRead::<Values>::scan(store, id, scan);
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let (key, value) = item?;
        out.push((key.coordinate.as_bytes().to_vec(), value));
    }
    Ok(out)
}

/// Crash-recovery equivalence over the **real** `Cached<MemoryCellStore>` at
/// the full alphabet of markers with clears: each resolution arm drives
/// `commit_provisional`/`abort_provisional` (the publish-on-settle path), and
/// a "crash" rebuilds the cache cold over the same warm memory cells (a fresh
/// fjall workspace — the assignment-scoped lifecycle). The committed
/// projection must converge to the model on every path — write-through
/// publish, cold restart, AND the delete legs: every committed durable clear
/// applied beneath the cache must delete its sections' entries before the gap
/// erase, with the lower fault seam (`FaultDepth::Lower` settle failures +
/// directed post-failure reads, stage faults) firing beneath the cache.
#[test]
fn prop_memory_cached_crash_equivalence() {
    fn property(trace: Trace) -> Result<bool> {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        // Each `make` yields a cold cache over the same warm memory cells +
        // dedup store, so a crash drops the cache but not durable state; the
        // runner's lower fault seam sits between the cache and the bottom
        // store. `test_db::cold_cache` reuses the `crash` keyspace pair on
        // the shared database and CLEARS it (a cheap journal marker, no
        // fsync) — modeling a fresh assignment without a keyspace creation
        // per make. Distinct v4 segments per iteration keep the shared
        // keyspace's crashes disjoint.
        let make = |handle: &PoisonHandle| {
            let lower =
                FailingCellStore::with_handle(MemoryCellStore::new(cells.clone()), handle.clone());
            Ok(Cached::new(test_db::cold_cache("crash")?, lower))
        };
        // The durable physical shape lives in the shared memory cells (fjall is
        // only the cold-on-crash cache), so the row-absence probe reads them.
        let probe = MemoryShapeProbe(cells.clone());
        TEST_RUNTIME.block_on(run_crash_equivalence_trace(
            make,
            dedup.clone(),
            trace,
            &probe,
        ))
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> Result<bool>);
}

// ---------------------------------------------------------------------------
// The transparency property
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// TTL co-expiry
// ---------------------------------------------------------------------------

// ─────────────────────────── batch reads (get_many) ────────────────────────
