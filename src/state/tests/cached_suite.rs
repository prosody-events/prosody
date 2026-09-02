//! Memory-backed properties and tests for the write-through K/V cache.
//!
//! The production [`Cached`] path only assembles over Cassandra in production,
//! so the backend-generic flagship exercises it solely through the
//! live-cluster arm at the 25-iteration `INTEGRATION_TESTS` count. These tests
//! put the **real** `Cached` over a memory lower store and a real fjall cache
//! (the shared test database), so the same cache code runs at full
//! `QUICKCHECK_TESTS` with no cluster.
//!
//! [`prop_cached_is_transparent`] compares cached and uncached stores.
//! Both stores must return the same result after every generated operation.

use super::super::cached::{Cached, DELETE_RETRY_BUDGET};
use super::super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::super::cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge, Section};
use super::super::fjall::Clock;
use super::super::fjall::test_db;
use super::super::marker::{EventMarker, SectionClear};
use super::super::memory::{MemoryCellStore, MemoryCells};
use super::super::oracle::CommitOracle;
use super::super::registry::CollectionDefRegistry;
use super::super::resolve::sweep_provisional;
use super::super::store::{CellBuffer, CellStore, CoordinateBatch};
use super::super::{CollectionId, CollectionRef, EventRef};
use super::cell_suite::{
    FailingCellStore, MemoryShapeProbe, OverlayTrace, Poison, PoisonHandle, SECTION,
    ScriptedOracle, Trace, bytes, cell_at, run_crash_equivalence_trace, run_overlay_trace,
    stage_deferred_repair_shape,
};
use super::support::{
    CountingCellStore, HoldingCellStore, batch_of, fresh_collection as collection, probe,
};
use crate::error::ErrorCategory;
use crate::test_util::{GlobalMetrics, TEST_RUNTIME, labels};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, ensure, eyre};
use futures::{Stream, StreamExt};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use uuid::Uuid;

/// Builds a production-shaped `Cached` over the shared fjall database (the
/// `name` warm-reuse keyspace pair) and the shared memory cells.
fn cached_over(
    cells: &MemoryCells,
    oracle: &ScriptedOracle,
    name: &str,
) -> Result<Cached<MemoryCellStore<ScriptedOracle>>> {
    let lower = MemoryCellStore::new(
        cells.clone(),
        oracle.clone(),
        Arc::new(CollectionDefRegistry::default()),
    );
    Ok(Cached::new(test_db::cache(name)?, lower))
}

/// A memory-backed [`CellStore`] that surfaces each present row's remaining TTL
/// the way Cassandra's `TTL(data)` does — whole seconds, FLOORED, computed
/// against the shared fixed [`Clock`] from one absolute `death` — so the
/// cache's read-fill re-stamp (`expiry_for`) is reachable over memory. The
/// plain memory store reports `None`, which makes that arithmetic structurally
/// unreachable. Every other operation delegates to an inner
/// [`CountingCellStore`], preserving its read/scan counters.
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

impl<S> CellStore for TtlAwareCellStore<S>
where
    S: CellStore,
{
    type Error = S::Error;

    fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<Committed, Self::Error>> + Send + 'a {
        self.inner.get(collection, cell, own)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        self.inner.scan_cells(collection, scan, own)
    }

    async fn get_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<(Committed, Option<CompactDuration>), Self::Error> {
        let (committed, _) = self.inner.get_for_cache(collection, cell, own).await?;
        // A present row carries a live remaining TTL; an absent row has none.
        let remaining = committed
            .get()
            .is_some()
            .then(|| self.remaining())
            .flatten();
        Ok((committed, remaining))
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        self.inner.provisional_cells(collection)
    }

    fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> impl Future<Output = Result<Option<ProvisionalCell>, Self::Error>> + Send + 'a {
        self.inner.provisional_cell_at(collection, cell)
    }

    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>> + Send + 'a
    {
        self.inner.provisional_many(collection, section, batch)
    }

    fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.write_provisional(collection, writes, marker)
    }

    fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.write_resolved(collection, cells, clears)
    }

    fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.mark_resolved(collection, cells)
    }

    fn unsettled_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Future<Output = Result<Option<EventMarker>, Self::Error>> + Send + 'a {
        self.inner.unsettled_marker(collection)
    }

    fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.commit_provisional(collection, writes, clears)
    }

    fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
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
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let lower = cached_over(&cells, &oracle, "overlay")?;
        TEST_RUNTIME.block_on(run_overlay_trace(lower, trace))
    }
    QuickCheck::new().quickcheck(property as fn(OverlayTrace) -> Result<bool>);
}

/// Collects a forward scan over `start` to `end`, mapping each cell to its
/// single coordinate byte. A whole-section scan passes a `ScanEdge::Included`
/// of a dominating sentinel (`255`) rather than an unbounded edge.
async fn scan_forward<S>(
    store: &S,
    id: &CollectionId,
    start: u8,
    end: ScanEdge<u8>,
    own: EventRef,
) -> Result<Vec<(Vec<u8>, Bytes)>>
where
    S: CellStore,
{
    let start_c = Coordinate::from_bytes(vec![start]);
    let end_c = end.map(|b| Coordinate::from_bytes(vec![b]));
    let scan = Scan {
        section: SECTION,
        start: ScanEdge::Included(&start_c),
        dir: Direction::Forward,
        end: end_c.as_ref(),
        limit: None,
    };
    let stream = store.scan_cells(id, scan, own);
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let (key, value) = item?;
        out.push((key.coordinate.as_bytes().to_vec(), value));
    }
    Ok(out)
}

/// Warm survival within an assignment: a `Cached` rebuilt over the **same**
/// fjall workspace (not a fresh assignment) is warm — its disk-backed
/// provisional-coordinate cache and committed-cell entries both survive. The
/// rebuilt cache's recovery sweep answers from the local fjall index with
/// **zero** cold `provisional_cells` sweeps (rebuilding via a single
/// `provisional_many` lower batch, never a per-coordinate point read),
/// and a warm `get` serves with zero lower reads. This is the
/// in-assignment-warm proxy the crash case (a fresh assignment) is the cold
/// complement of.
#[test]
fn warm_entries_and_index_survive_same_workspace_rebuild() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            cells,
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let id = collection("warm")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = probe(1);

        // First cache instance: warm a value and leave a provisional cell.
        let cached = Cached::new(test_db::cache("warm")?, counting.clone());
        cached
            .write_resolved(&cref, &[(cell_at(9), Some(bytes(9)))], &[])
            .await?;
        let prev = cached.get(&id, &cell_at(3), event).await?;
        let writes = [(
            cell_at(3),
            ProvisionalWrite::new(Some(bytes(5)), prev, event),
        )];
        let marker = EventMarker::frozen(event, &writes, &[]);
        cached
            .write_provisional(&cref, &writes, Some(&marker))
            .await?;

        // Prime the warm provisional-coordinate cache: the first sweep is a cold
        // seed (one `provisional_cells` call), which records the coordinate into
        // fjall and marks the collection seeded.
        counting.reset();
        let cold = drain_provisional(&cached, &id).await?;
        assert_eq!(cold, vec![3], "the cold sweep finds the provisional cell");
        assert_eq!(
            counting.recovery_sweeps(),
            1,
            "the first sweep is a cold seed"
        );

        // Rebuild `Cached` over the SAME workspace (a session clone within one
        // assignment). The disk-backed warm index + cell entries survive.
        let restarted = Cached::new(test_db::cache("warm")?, counting.clone());
        counting.reset();

        // The rebuilt sweep is WARM: zero cold `provisional_cells` sweeps, and it
        // still finds the provisional cell via a single `provisional_many` lower
        // batch (zero per-coordinate point reads).
        let warm = drain_provisional(&restarted, &id).await?;
        assert_eq!(
            warm,
            vec![3],
            "the warm sweep still finds the provisional cell"
        );
        assert_eq!(
            counting.recovery_sweeps(),
            0,
            "a warm sweep issues NO cold provisional_cells read"
        );
        assert_eq!(
            counting.warm_point_reads(),
            0,
            "the warm sweep issues no per-coordinate point read"
        );
        assert_eq!(
            counting.raw_batch_reads(),
            1,
            "the warm sweep rebuilds the one provisional coordinate via a single lower batch"
        );

        // The committed-cell entry also survives: the warm `9` serves from
        // fjall with no lower read.
        counting.reset();
        assert_eq!(
            restarted.get(&id, &cell_at(9), probe(2)).await?.get(),
            Some(&bytes(9)),
            "the warm value survives the rebuild and serves from fjall"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "a warm get after a same-workspace rebuild reads nothing"
        );
        Ok(())
    })
}

/// A warm-index read failure must degrade the recovery sweep to the cold
/// durable re-seed — never fabricate an empty (clean) sweep. Forcing
/// `index_snapshot` to fail while the collection stays *seeded* exercises the
/// exact branch that would otherwise report zero provisional cells and let the
/// backstop unschedule, stranding a live provisional cell (F2 / no-strand).
#[test]
fn warm_snapshot_failure_degrades_to_cold_reseed() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let id = collection("degrade")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = probe(1);

        let fjall = test_db::cache("degrade")?;
        let fail_snapshot = fjall.fail_index_snapshot();
        let cached = Cached::new(fjall, counting.clone());

        // Leave a provisional cell, then seed the warm index with a cold sweep
        // (records the coordinate into fjall and marks the collection seeded).
        let prev = cached.get(&id, &cell_at(3), event).await?;
        let writes = [(
            cell_at(3),
            ProvisionalWrite::new(Some(bytes(5)), prev, event),
        )];
        let marker = EventMarker::frozen(event, &writes, &[]);
        cached
            .write_provisional(&cref, &writes, Some(&marker))
            .await?;
        assert_eq!(
            drain_provisional(&cached, &id).await?,
            vec![3],
            "the cold seed finds the provisional cell"
        );

        // Force the warm coords read to fail while the collection stays seeded.
        // The sweep must fall through to a cold `provisional_cells` re-seed that
        // still finds the cell — not report an empty, clean sweep.
        counting.reset();
        fail_snapshot.store(true, Ordering::Relaxed);
        assert_eq!(
            drain_provisional(&cached, &id).await?,
            vec![3],
            "a warm snapshot failure re-seeds from the durable index, never an empty sweep"
        );
        assert_eq!(
            counting.recovery_sweeps(),
            1,
            "the failed warm read degrades to a cold provisional_cells re-seed"
        );
        fail_snapshot.store(false, Ordering::Relaxed);
        Ok(())
    })
}

/// A cold-seed `index_record` failure must leave the collection **unseeded** so
/// the next sweep re-seeds from the durable index — never latch `seeded` over
/// an incomplete on-disk coords set, which would drop the unrecorded coordinate
/// from every later warm sweep and strand it. Symmetric with
/// `write_provisional`'s unseed-on-record-failure.
#[test]
fn cold_seed_record_failure_leaves_collection_unseeded() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let id = collection("reseed")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = probe(1);

        let fjall = test_db::cache("reseed")?;
        let fail_record = fjall.fail_index_record();
        let cached = Cached::new(fjall, counting.clone());

        // Leave a provisional cell.
        let prev = cached.get(&id, &cell_at(3), event).await?;
        let writes = [(
            cell_at(3),
            ProvisionalWrite::new(Some(bytes(5)), prev, event),
        )];
        let marker = EventMarker::frozen(event, &writes, &[]);
        cached
            .write_provisional(&cref, &writes, Some(&marker))
            .await?;

        // Cold seed with a failing `index_record`: the sweep still finds the
        // cell (from the durable lower), but the failed record must not be
        // papered over by marking the collection seeded.
        counting.reset();
        fail_record.store(true, Ordering::Relaxed);
        assert_eq!(
            drain_provisional(&cached, &id).await?,
            vec![3],
            "the cold seed still yields the provisional cell"
        );
        fail_record.store(false, Ordering::Relaxed);

        // The next sweep must be COLD again (re-seed) — proving the collection
        // was left unseeded. A wrongly-latched `seeded` would take the warm path
        // over the empty (unrecorded) snapshot and yield nothing, stranding it.
        counting.reset();
        assert_eq!(
            drain_provisional(&cached, &id).await?,
            vec![3],
            "the next sweep re-seeds and finds the cell, not an empty warm snapshot"
        );
        assert_eq!(
            counting.recovery_sweeps(),
            1,
            "the collection was left unseeded, so the next sweep re-seeds cold"
        );
        Ok(())
    })
}

/// Drains a `provisional_cells` sweep into the ascending list of coordinate
/// first-bytes it yields.
async fn drain_provisional<L>(cached: &Cached<L>, id: &CollectionId) -> Result<Vec<u8>>
where
    L: CellStore,
{
    let stream = cached.provisional_cells(id);
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let (cell, _) = item.map_err(|e| eyre!("provisional sweep failed: {e:?}"))?;
        out.push(cell.coordinate.as_bytes()[0]);
    }
    out.sort_unstable();
    Ok(out)
}

/// Crash-recovery equivalence over the **real** `Cached<MemoryCellStore>` at
/// the full clears-bearing alphabet: each resolution arm drives
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
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        // Each `make` yields a cold cache over the same warm memory cells +
        // oracle, so a crash drops the cache but not durable state; the
        // runner's lower fault seam sits between the cache and the bottom
        // store. `test_db::cold_cache` reuses the `crash` keyspace pair on
        // the shared database and CLEARS it (a cheap journal marker, no
        // fsync) — modeling a fresh assignment without a keyspace creation
        // per make. Distinct v4 segments per iteration keep the shared
        // keyspace's crashes disjoint.
        let make = |handle: &PoisonHandle| {
            let lower = FailingCellStore::with_handle(
                MemoryCellStore::new(
                    cells.clone(),
                    oracle.clone(),
                    Arc::new(CollectionDefRegistry::default()),
                ),
                handle.clone(),
            );
            Ok(Cached::new(test_db::cold_cache("crash")?, lower))
        };
        // The durable physical shape lives in the shared memory cells (fjall is
        // only the cold-on-crash cache), so the row-absence probe reads them.
        let probe = MemoryShapeProbe(cells.clone());
        TEST_RUNTIME.block_on(run_crash_equivalence_trace(
            make,
            oracle.clone(),
            trace,
            &probe,
        ))
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> Result<bool>);
}

/// TTL co-expiry over the real `Cached` with a verified [`Clock`]: a value
/// written through a short-TTL collection is served from fjall while live, then
/// — after the clock advances **past its floor expiry to a sub-second instant**
/// — the expired entry reads as a miss, the get **falls through** to the lower
/// store, yields its current answer, and **re-stamps the fjall entry to
/// `floor(now) + remaining`** (the co-expiry invariant). The lower store
/// reports a live `TTL(data)`-style remaining ([`TtlAwareCellStore`]), so the
/// floored re-stamp is exercised and asserted `≤` the row's death. No sleep;
/// the clock is advanced directly.
///
/// Example test by necessity: the hit-vs-fall-through decision turns on a
/// sub-second clock crossing whose counter grain sits below the model's
/// abstraction, so the fall-through test cannot be generalized into the
/// generator.
#[test]
fn expired_entry_reads_as_miss_and_refills() -> Result<()> {
    // A sub-second instant past the floor expiry (6_000), so the re-stamp's floor
    // sheds the 500 ms remainder; the lower row dies at 30_000 (`TTL(data)`).
    const NOW_EXPIRED: u64 = 6_500;
    const ROW_DEATH: u64 = 30_000;

    TEST_RUNTIME.block_on(async {
        let now = Arc::new(AtomicU64::new(1_000));
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let lower = TtlAwareCellStore::new(
            CountingCellStore::new(MemoryCellStore::new(
                cells,
                oracle,
                Arc::new(CollectionDefRegistry::default()),
            )),
            Clock::Fixed(now.clone()),
            ROW_DEATH,
        );
        let cached = Cached::new(
            test_db::cache_with_clock("ttl", Clock::Fixed(now.clone()))?,
            lower.clone(),
        );
        let id = collection("ttl")?;
        // A 5-second TTL: write-through stamps `floor(now) + 5s`.
        let cref = CollectionRef::new(id.clone(), Some(CompactDuration::new(5)));

        // Write through coordinate 7; it is now warm and served from fjall.
        cached
            .write_resolved(&cref, &[(cell_at(7), Some(bytes(7)))], &[])
            .await?;
        lower.reset();
        assert_eq!(
            cached.get(&id, &cell_at(7), probe(1)).await?.get(),
            Some(&bytes(7)),
            "a live warm value serves from fjall"
        );
        assert_eq!(lower.lower_reads(), 0, "a live warm get reads nothing");

        // Advance the clock past the floor expiry (1_000 + 5_000 = 6_000ms).
        now.store(NOW_EXPIRED, Ordering::Relaxed);
        // Rewrite the durable value WITHOUT going through the cache, so fjall
        // still holds the stale (now-expired) `7` while the lower store holds
        // `70`. The expired get must fall through and yield `70`.
        lower
            .write_resolved(&cref, &[(cell_at(7), Some(bytes(70)))], &[])
            .await?;
        lower.reset();
        assert_eq!(
            cached.get(&id, &cell_at(7), probe(2)).await?.get(),
            Some(&bytes(70)),
            "an expired get falls through to the fresh durable value"
        );
        assert!(
            lower.lower_reads() > 0,
            "the expired get must read the lower store"
        );

        // The fall-through re-stamped the fjall entry to `floor(now) + remaining`,
        // flooring the sub-second remainder so it never overhangs the row death.
        let remaining_ms = ((ROW_DEATH - NOW_EXPIRED) / 1_000) * 1_000;
        let want_expiry = NOW_EXPIRED - NOW_EXPIRED % 1_000 + remaining_ms;
        let stamped = cached.stored_expiry(&id, &cell_at(7)).await?;
        assert_eq!(
            stamped,
            Some(want_expiry),
            "the re-stamped fjall expiry must be floor(now) + remaining"
        );
        assert!(
            stamped.is_some_and(|e| e <= ROW_DEATH),
            "the re-stamped expiry must not overhang the durable row death"
        );

        // The fall-through re-published a fresh entry; a get now serves `70`
        // from fjall again (KV5 restored).
        lower.reset();
        assert_eq!(
            cached.get(&id, &cell_at(7), probe(3)).await?.get(),
            Some(&bytes(70)),
            "the re-published value serves from fjall"
        );
        assert_eq!(lower.lower_reads(), 0, "the re-published get reads nothing");
        Ok(())
    })
}

/// `Cached::provisional_many` delegates the raw read to the lower store and
/// publishes NOTHING into the committed-value cache: it delegates exactly once
/// (one lower batch read), and a subsequent point `get` of a read coordinate
/// still incurs a lower read (a fjall miss) — proving no committed projection
/// was warmed by the raw verb.
#[test]
fn cached_provisional_many_does_not_publish() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let id = collection("cached-no-publish")?;
        let cref = CollectionRef::new(id.clone(), None);
        // Stage a provisional cell in the LOWER store directly, so fjall stays
        // untouched (a cached stage would publish the cell's `prev`).
        let event = probe(0x5EED);
        let prev = counting.get(&id, &cell_at(2), event).await?;
        let writes = [(
            cell_at(2),
            ProvisionalWrite::new(Some(bytes(20)), prev, event),
        )];
        let marker = EventMarker::frozen(event, &writes, &[]);
        counting
            .write_provisional(&cref, &writes, Some(&marker))
            .await?;

        let cached = Cached::new(test_db::cache("cached-no-publish")?, counting.clone());
        counting.reset();

        let batch = batch_of([2])?;
        let out = cached.provisional_many(&id, SECTION, &batch).await?;
        assert_eq!(out.len(), 1, "the staged provisional cell survives");
        assert_eq!(
            counting.raw_batch_reads(),
            1,
            "delegated exactly once to the lower batch"
        );

        // Nothing was published, so a point get of the read coordinate still
        // falls through to the lower store (a fjall miss).
        counting.reset();
        cached.get(&id, &cell_at(2), probe(7)).await?;
        assert!(
            counting.lower_reads() >= 1,
            "provisional_many must not warm the committed-value cache"
        );
        Ok(())
    })
}

/// Proves that a failed cache update removes the old entry.
///
/// The next read must load and cache the durable value.
#[test]
fn failed_publish_deletes_the_stale_entry() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let fjall = test_db::cache("fault")?;
        let fail = fjall.fail_puts();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(fjall, counting.clone());
        let id = collection("fault")?;
        let cref = CollectionRef::new(id.clone(), None);

        // First write publishes cleanly and warms `1`.
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(1)))], &[])
            .await?;
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(1)).await?.get(),
            Some(&bytes(1))
        );
        assert_eq!(counting.lower_reads(), 0, "the seeded entry is warm");

        // Now force every publish to fail. The lower write still succeeds
        // (durable truth is `2`), but the fjall publish fails → the stale
        // entry is deleted (failed-publish cache guard).
        fail.store(true, Ordering::Relaxed);
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(2)))], &[])
            .await?;

        // Heal the cache fault; the next get is a miss, so it falls through to
        // the durable `2` (exactly one lower read) and re-publishes — never
        // serving the stale fjall `1`.
        fail.store(false, Ordering::Relaxed);
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(2)).await?.get(),
            Some(&bytes(2)),
            "a failed publish deletes the entry, so the next read self-heals"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the healed get falls through once"
        );
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(3)).await?.get(),
            Some(&bytes(2)),
            "the fall-through re-warmed the cell"
        );
        assert_eq!(counting.lower_reads(), 0, "the re-warmed get reads nothing");
        Ok(())
    })
}

/// Proves that a failed batch update removes every old entry.
#[test]
fn failed_batch_publish_deletes_every_batch_cell() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let fjall = test_db::cache("batch-fault")?;
        let fail = fjall.fail_puts();
        let lower = MemoryCellStore::new(cells, oracle, Arc::new(CollectionDefRegistry::default()));
        let cached = Cached::new(fjall, lower);
        let id = collection("batch-fault")?;
        let cref = CollectionRef::new(id.clone(), None);

        let seed = [(cell_at(1), Some(bytes(1))), (cell_at(2), Some(bytes(2)))];
        let update = [(cell_at(1), Some(bytes(11))), (cell_at(2), Some(bytes(22)))];

        // One multi-cell write-through publishes cleanly and warms both cells.
        cached.write_resolved(&cref, &seed, &[]).await?;
        for (c, v) in [(1u8, 1u8), (2, 2)] {
            assert_eq!(
                cached
                    .get(&id, &cell_at(c), probe(u128::from(c)))
                    .await?
                    .get(),
                Some(&bytes(v))
            );
        }

        // Force the batch commit to fail. The lower write still succeeds
        // (durable truth is `11`/`22`), but the atomic batch lands nothing →
        // every coordinate in the batch is deleted (failed-publish cache guard).
        fail.store(true, Ordering::Relaxed);
        cached.write_resolved(&cref, &update, &[]).await?;

        // Heal the fault; both gets are misses, so each falls through to its
        // fresh durable value — never serving the stale batch.
        fail.store(false, Ordering::Relaxed);
        for (c, v) in [(1u8, 11u8), (2, 22)] {
            assert_eq!(
                cached
                    .get(&id, &cell_at(c), probe(u128::from(c) + 100))
                    .await?
                    .get(),
                Some(&bytes(v)),
                "a failed batch deletes every coordinate, so each read self-heals"
            );
        }
        Ok(())
    })
}

/// Proves that cache entry removal retries before it disables the cache.
///
/// The promoted value must replace the old cached value.
#[test]
fn promote_delete_retries_before_cache_disablement() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let fjall = test_db::cache("promote-delete")?;
        let fail_deletes = fjall.fail_deletes();
        let lower = MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        );
        let cached = Cached::new(fjall.clone(), lower);
        let id = collection("promote-delete")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = probe(1);

        // Committed base `1`, warm by write-through; stage `5` over it (the
        // stage publishes `prev` = 1, so the entry stays warm with 1).
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(1)))], &[])
            .await?;
        let prev = cached.get(&id, &cell_at(0), event).await?;
        let writes = [(
            cell_at(0),
            ProvisionalWrite::new(Some(bytes(5)), prev, event),
        )];
        let marker = EventMarker::frozen(event, &writes, &[]);
        cached
            .write_provisional(&cref, &writes, Some(&marker))
            .await?;

        // Make cache removal fail within the retry limit.
        fail_deletes.store(u64::try_from(DELETE_RETRY_BUDGET - 1)?, Ordering::Relaxed);
        cached.mark_resolved(&cref, &[cell_at(0)]).await?;
        assert_eq!(
            fail_deletes.load(Ordering::Relaxed),
            0,
            "the injected delete failures must have fired"
        );
        assert!(
            !fjall.is_disabled(),
            "an in-budget removal does not disable the cache"
        );
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(2)).await?.get(),
            Some(&bytes(5)),
            "a failed delete must not leave the stale pre-promote value warm"
        );
        Ok(())
    })
}

/// Proves that cache removal recovers within its retry limit.
#[test]
fn write_path_delete_recovers_within_budget() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let fjall = test_db::cache("write-delete")?;
        let fail_puts = fjall.fail_puts();
        let fail_deletes = fjall.fail_deletes();
        let lower = MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        );
        let cached = Cached::new(fjall.clone(), lower);
        let id = collection("write-delete")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Warm `1` cleanly, then write `2` with the publish AND the first
        // delete attempts failing (within the budget).
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(1)))], &[])
            .await?;
        fail_puts.store(true, Ordering::Relaxed);
        fail_deletes.store(u64::try_from(DELETE_RETRY_BUDGET - 1)?, Ordering::Relaxed);
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(2)))], &[])
            .await?;
        fail_puts.store(false, Ordering::Relaxed);

        assert_eq!(
            fail_deletes.load(Ordering::Relaxed),
            0,
            "the injected delete failures must have fired"
        );
        assert!(
            !fjall.is_disabled(),
            "a within-budget removal does not disable the cache"
        );
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(1)).await?.get(),
            Some(&bytes(2)),
            "a doubly-failed publish+delete must still evict, so the read self-heals"
        );
        Ok(())
    })
}

/// Proves that a failed durable write does not cache the new value.
///
/// The next read loads the old durable value.
#[test]
fn failed_lower_write_leaves_cache_serving_pre_write_value() -> Result<()> {
    use crate::state::StateName;

    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let handle: PoisonHandle = Arc::default();
        let cached = Cached::new(
            test_db::cache("establish_fault")?,
            FailingCellStore::with_handle(counting.clone(), handle.clone()),
        );
        let id = collection("establish-fault")?;
        let cref = CollectionRef::new(id.clone(), None);
        let name: StateName = id.name().clone();

        // Seed a base value; the write-through warms it, so a get serves
        // from fjall with zero lower reads — the arrange proof.
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(1)))], &[])
            .await?;
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(1)).await?.get(),
            Some(&bytes(1)),
            "the seeded base serves warm"
        );
        assert_eq!(counting.lower_reads(), 0, "the arrange get is warm");

        // A clears-free lower write fault: the write must surface Err. The
        // drop-safe pre-call cell delete (F1) already evicted the entry, so the
        // follow-up serves the PRE-write value via a cold fall-through (one
        // lower read) — correct, never a warm stale hit, never a phantom
        // publish of the new value — then re-warms.
        *handle.lock() = Some(Poison::WriteResolved(
            name.clone(),
            ErrorCategory::Transient,
        ));
        assert!(
            cached
                .write_resolved(&cref, &[(cell_at(0), Some(bytes(2)))], &[])
                .await
                .is_err(),
            "the armed lower write must be rejected"
        );
        *handle.lock() = None;
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(2)).await?.get(),
            Some(&bytes(1)),
            "the failed write serves the PRE-write value"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the drop-safe pre-call delete left the cell cold: one fall-through, never a phantom \
             publish of the new value"
        );
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(20)).await?.get(),
            Some(&bytes(1)),
            "the fall-through re-warmed the pre-write value"
        );
        assert_eq!(counting.lower_reads(), 0, "the re-warmed get reads nothing");

        // Fail the durable write after the cache removes the section.
        let cells = [(cell_at(3), Some(bytes(9)))];
        let clear = SectionClear::frozen_resolved(SECTION, &cells);
        *handle.lock() = Some(Poison::WriteResolved(name, ErrorCategory::Transient));
        assert!(
            cached
                .write_resolved(&cref, &cells, slice::from_ref(&clear))
                .await
                .is_err(),
            "the armed clears-bearing lower write must be rejected"
        );
        *handle.lock() = None;
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(3)).await?.get(),
            Some(&bytes(1)),
            "delete-first on a failed apply degrades to a correct slow read, never a wrong one"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the deleted section falls through exactly once"
        );
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(4)).await?.get(),
            Some(&bytes(1)),
            "the fall-through re-warmed the coordinate"
        );
        assert_eq!(counting.lower_reads(), 0, "the re-warmed get reads nothing");
        Ok(())
    })
}

/// Proves that a resolved write removes entries affected by an unsettled clear.
///
/// A later read cannot return the old cached value.
#[test]
fn blind_write_deletes_beneath_resolved_marker_window() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(test_db::cache("blind-d3")?, counting);
        let id = collection("blind-d3")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event_a = probe(1);

        // Seed a base value (no marker → boundary no-op); the write-through
        // warms it.
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(1)))], &[])
            .await?;

        // Stage a committed clears-bearing marker through the cache and leave
        // it unsettled (no settle). The cache now holds the published `prev`
        // (bytes(1)) for the staged coordinate.
        let prev = cached.get(&id, &cell_at(0), event_a).await?;
        let writes = vec![(
            cell_at(0),
            ProvisionalWrite::new(Some(bytes(2)), prev, event_a),
        )];
        let clears = vec![SectionClear::frozen(SECTION, &writes)];
        let marker = EventMarker::frozen(event_a, &writes, &clears);
        cached
            .write_provisional(&cref, &writes, Some(&marker))
            .await?;
        oracle.record_message(Uuid::from_u128(1)).await?;

        // Blind-write a different coordinate: the lower write resolves marker A
        // beneath the cache (promoting cell_at(0) to bytes(2) durably), and prior-clear
        // cache guard deletes the staged coordinate's stale warm entry.
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(9)))], &[])
            .await?;

        let read = probe(u128::MAX / 2);
        ensure!(
            cached.get(&id, &cell_at(0), read).await?.get() == Some(&bytes(2)),
            "the staged coordinate must read the beneath-resolved value, not the stale prev"
        );
        ensure!(
            cached.get(&id, &cell_at(1), read).await?.get() == Some(&bytes(9)),
            "the blind write did not read back"
        );
        Ok(())
    })
}

/// Proves that settlement removes a cached value from a cleared section.
///
/// The next read must report that the cell is absent.
#[test]
fn repair_defers_then_clear_evicts_stale_fill() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(test_db::cache("repair-defer")?, counting.clone());
        let cref = CollectionRef::new(collection("repair-defer")?, None);

        // Stage the defect shape through the lower store so x stays cold in the
        // cache; record E committed.
        let (x, s, event_e) = stage_deferred_repair_shape(&counting, &cref).await?;
        oracle.record_message(Uuid::from_u128(2)).await?;

        // The cold fill defers the repair (own-marker clear resolution declines below)
        // and caches the peek projection with no durable write.
        ensure!(
            cached.get(cref.id(), &x, event_e).await?.get() == Some(&bytes(7)),
            "the deferred fill serves the committed-base projection"
        );

        // Resolving E evicts the stale fill (section-clear cache guard) and erases x
        // durably (gap).
        sweep_provisional(&cached, &oracle, &cref)
            .await
            .map_err(|e| eyre!("sweep failed: {e:?}"))?;
        let read = probe(u128::MAX / 2);
        ensure!(
            cached.get(cref.id(), &x, read).await?.get().is_none(),
            "the committed clear must evict the stale fill and leave x absent"
        );
        ensure!(
            cached.get(cref.id(), &s, read).await?.get() == Some(&bytes(1)),
            "the survivor must promote to its committed value"
        );
        Ok(())
    })
}

/// Drop-safety (F1): `write_resolved` is the one user-droppable write path
/// (mid-handler `commit()` / `ReadUncommitted` finalize). If the future is
/// dropped between the durable write landing and the re-warming publish, the
/// written cell's OLD entry must not survive as a stale warm hit (KV1) — the
/// pre-call delete leaves it cold, so the next read falls through to the
/// durable NEW value. Reverting the pre-call delete freezes the stale `A` warm
/// and makes this test fail (the get would serve `A` with zero lower
/// reads).
#[test]
fn dropped_write_resolved_leaves_no_stale_entry() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let holding = HoldingCellStore::new(counting.clone());
        let holds = holding.holds();
        let cached = Cached::new(test_db::cache("drop_write")?, holding);
        let id = collection("drop-write")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Warm committed `A` = 1 through the cache (no charge armed, so the
        // hold passes through).
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(1)))], &[])
            .await?;
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(1)).await?.get(),
            Some(&bytes(1)),
            "A is warm"
        );
        assert_eq!(counting.lower_reads(), 0, "the arrange get is warm");

        // Arm the next `write_resolved` to park after its durable write lands,
        // then spawn a write of `B` = 2 and drop it mid-flight — the pre-call
        // delete has run and the durable write has landed, but the re-warming
        // publish never will.
        holds.write_resolved().arm(1);
        let landed_before = holds.write_resolved().landed();
        let task = tokio::spawn({
            let cached = cached.clone();
            let cref = cref.clone();
            async move {
                cached
                    .write_resolved(&cref, &[(cell_at(0), Some(bytes(2)))], &[])
                    .await
            }
        });
        holds.write_resolved().entered().await;
        assert!(
            holds.write_resolved().landed() > landed_before,
            "the durable write landed before the drop"
        );
        task.abort();
        assert!(task.await.is_err(), "the write future was dropped");

        // The cell is COLD (pre-call delete), so the next get falls through to
        // the durable NEW value `B` = 2 — never a warm stale `A`.
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(2)).await?.get(),
            Some(&bytes(2)),
            "a dropped write_resolved must not leave the stale pre-write value warm"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the dropped write left the cell cold: exactly one fall-through"
        );
        Ok(())
    })
}

/// Proves that a scan does not change a provisional cell.
///
/// A later point read or recovery sweep can repair the cell.
#[test]
fn scan_resolution_is_read_only() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let lower = MemoryCellStore::new(
            cells.clone(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        );
        let id = collection("scan-readonly")?;
        let cref = CollectionRef::new(id.clone(), None);
        let prior_event = probe(7);

        // Seed a prior event committed provisional at x: a clear (data = None) over
        // committed base `A` = 1, owned by `prior event`, which the oracle says
        // committed. The point-read repair arm would `write_resolved(x, None)`,
        // a durable delete; the scan must not.
        let writes = [(
            cell_at(4),
            ProvisionalWrite::new(None, Committed::new(Some(bytes(1))), prior_event),
        )];
        let marker = EventMarker::frozen(prior_event, &writes, &[]);
        lower
            .write_provisional(&cref, &writes, Some(&marker))
            .await?;
        oracle.record_message(Uuid::from_u128(7)).await?;
        assert_eq!(
            cells.provisional_coordinates(&id),
            vec![cell_at(4)],
            "the prior event committed provisional is seeded"
        );

        // Scan the section (a distinct own event). The cell resolves to its
        // committed view — absent, since data = None — so the scan yields
        // nothing, but must leave the durable cell provisional.
        let seen = scan_forward(&lower, &id, 0, ScanEdge::Included(255), probe(99)).await?;
        assert!(
            seen.is_empty(),
            "the committed clear resolves to absent, so the scan yields nothing"
        );
        assert_eq!(
            cells.provisional_coordinates(&id),
            vec![cell_at(4)],
            "the scan must not durably resolve the prior event provisional (read-only): a scan \
             write-back could clobber a newer commit of the same cell"
        );
        Ok(())
    })
}

/// Proves that a section clear removes only entries in that section.
#[test]
fn delete_section_removes_exactly_the_cleared_section() -> Result<()> {
    /// The cell at coordinate `c` in section 1 (the surviving sibling; the
    /// shared [`cell_at`] addresses section 0, the cleared one).
    fn sect1_cell(c: u8) -> CellKey {
        CellKey {
            section: Section::new(1),
            coordinate: Coordinate::from_bytes(vec![c]),
        }
    }

    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(test_db::cache("clear_delete")?, counting.clone());
        let id = collection("clear-delete")?;
        let other = collection("clear-delete-other")?;
        let cref = CollectionRef::new(id.clone(), None);
        let other_ref = CollectionRef::new(other.clone(), None);

        // Warm entries in section 0 AND section 1, plus a sibling collection.
        cached
            .write_resolved(
                &cref,
                &[
                    (cell_at(0), Some(bytes(10))),
                    (sect1_cell(0), Some(bytes(20))),
                ],
                &[],
            )
            .await?;
        cached
            .write_resolved(&other_ref, &[(cell_at(0), Some(bytes(30)))], &[])
            .await?;

        // A committed clear of section 0 with one survivor write.
        let survivors = [(cell_at(5), Some(bytes(50)))];
        let clear = SectionClear::frozen_resolved(SECTION, &survivors);
        cached
            .write_resolved(&cref, &survivors, slice::from_ref(&clear))
            .await?;

        // The cleared section's pre-clear entry is gone: durable truth is
        // absent, and the read pays the one cold fall-through.
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(2)).await?.get(),
            None,
            "the committed clear erased the non-survivor"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the cleared entry is cold — exactly one fall-through"
        );
        // The survivor re-warmed via the post-clear publish.
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(5), probe(3)).await?.get(),
            Some(&bytes(50)),
            "the survivor serves the post-clear value"
        );
        assert_eq!(counting.lower_reads(), 0, "the survivor is warm");
        // The sibling section and the sibling collection stay warm.
        counting.reset();
        assert_eq!(
            cached.get(&id, &sect1_cell(0), probe(4)).await?.get(),
            Some(&bytes(20)),
            "the sibling section survives the clear"
        );
        assert_eq!(
            cached.get(&other, &cell_at(0), probe(5)).await?.get(),
            Some(&bytes(30)),
            "the sibling collection survives the clear"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "nothing beyond the cleared section was evicted"
        );
        Ok(())
    })
}

/// KV2 negative caching: two gets of a never-written cell issue exactly one
/// lower read — the first falls through and publishes the Absent tag, the
/// second is a warm absent hit.
#[test]
fn absent_get_is_cached() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(test_db::cache("absent")?, counting.clone());
        let id = collection("absent")?;

        counting.reset();
        assert_eq!(cached.get(&id, &cell_at(9), probe(1)).await?.get(), None);
        assert_eq!(cached.get(&id, &cell_at(9), probe(2)).await?.get(), None);
        assert_eq!(
            counting.lower_reads(),
            1,
            "two gets of an absent cell pay exactly one durable read"
        );
        Ok(())
    })
}

/// Cell-load metrics count logical cells. They distinguish cache answers from
/// durable fallbacks without labels that contain user identities.
#[test]
fn cell_load_metrics_report_source_and_cache_result() -> Result<()> {
    let metrics = GlobalMetrics::install();
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            ScriptedOracle::default(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let fjall = test_db::cache("cell-load-metrics")?;
        let fail_puts = fjall.fail_puts();
        let cached = Cached::new(fjall, counting).with_metrics(metrics.cell_metrics());
        let id = collection("cell-load-metrics")?;

        cached.get(&id, &cell_at(1), probe(1)).await?;
        cached.get(&id, &cell_at(1), probe(2)).await?;
        let batch = batch_of(2u8..5)?;
        cached.get_many(&id, SECTION, &batch, probe(3)).await?;
        cached.get_many(&id, SECTION, &batch, probe(4)).await?;

        let points = metrics.points("prosody.state.cell.loads")?;
        assert_eq!(
            points,
            [
                (("get", "cache", "hit"), 1),
                (("get_many", "cache", "hit"), 3),
                (("get", "store", "miss"), 1),
                (("get_many", "store", "not_all_hit"), 3),
            ]
            .into_iter()
            .map(|((operation, source, result), value)| {
                (
                    labels([
                        ("prosody.state.cell.cache.result", result),
                        ("prosody.state.cell.operation.name", operation),
                        ("prosody.state.cell.load.source", source),
                    ]),
                    value,
                )
            })
            .collect::<Vec<_>>()
        );
        assert_eq!(
            metrics.points("prosody.state.cell.load.duration")?,
            points
                .into_iter()
                .map(|(attributes, _)| (attributes, 1))
                .collect::<Vec<_>>()
        );
        assert!(metrics.is_exponential_histogram("prosody.state.cell.load.duration")?);
        metrics.metrics().request_latency.record(0.01_f64, &[]);
        assert!(metrics.is_exponential_histogram("prosody.request.duration")?);

        fail_puts.store(true, Ordering::Relaxed);
        cached.get(&id, &cell_at(9), probe(5)).await?;
        assert_eq!(
            metrics.points("prosody.state.cell.cache.errors")?,
            vec![(
                labels([
                    ("prosody.state.cell.cache.phase", "fill"),
                    ("prosody.state.cell.operation.name", "get"),
                ]),
                1,
            )]
        );

        let failed_metrics = GlobalMetrics::install();
        let failed_lower = FailingCellStore::failing_get_for_cache(
            MemoryCellStore::new(
                MemoryCells::new(),
                ScriptedOracle::default(),
                Arc::new(CollectionDefRegistry::default()),
            ),
            BTreeMap::from([(8, ErrorCategory::Transient)]),
        );
        let failed = Cached::new(test_db::cache("cell-load-error-metrics")?, failed_lower)
            .with_metrics(failed_metrics.cell_metrics());
        let failed_id = collection("cell-load-error-metrics")?;
        assert!(failed.get(&failed_id, &cell_at(8), probe(6)).await.is_err());
        assert_eq!(
            failed_metrics.points("prosody.state.cell.load.duration")?,
            vec![(
                labels([
                    ("prosody.error.category", "transient"),
                    ("prosody.state.cell.cache.result", "miss"),
                    ("prosody.state.cell.load.source", "store"),
                    ("prosody.state.cell.operation.name", "get"),
                ]),
                1,
            )]
        );
        Ok(())
    })
}

/// Stages a 3-cell marker (`data` = 100+c over committed base `c`) through
/// `cached` and records its commit verdict — the shared prologue of the
/// settlement cache update tests. Returns the staged writes and the frozen
/// marker.
async fn stage_committed_marker<L>(
    cached: &Cached<L>,
    oracle: &ScriptedOracle,
    cref: &CollectionRef,
    dedup: u128,
) -> Result<Vec<(CellKey, ProvisionalWrite)>>
where
    L: CellStore,
{
    let id = cref.id();
    let event = probe(dedup);
    for c in [1u8, 2, 3] {
        cached
            .write_resolved(cref, &[(cell_at(c), Some(bytes(c)))], &[])
            .await?;
    }
    let mut writes = Vec::new();
    for c in [1u8, 2, 3] {
        let prev = cached.get(id, &cell_at(c), event).await?;
        writes.push((
            cell_at(c),
            ProvisionalWrite::new(Some(bytes(100 + c)), prev, event),
        ));
    }
    let marker = EventMarker::frozen(event, &writes, &[]);
    cached
        .write_provisional(cref, &writes, Some(&marker))
        .await?;
    // The verdict is fixed before commit_provisional ever runs (the settle
    // boundary records the marker first) — the settlement cache update
    // precondition.
    oracle.record_message(Uuid::from_u128(dedup)).await?;
    Ok(writes)
}

/// Proves that settlement caches committed data before durable promotion.
///
/// Cached cells must not return their prior values.
#[test]
fn d5_transform_installs_committed_data_precall() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        // ---- Window (a): the lower promote fails. --------------------------
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let handle: PoisonHandle = Arc::default();
        let cached = Cached::new(
            test_db::cache("d5_precall")?,
            FailingCellStore::with_handle(counting.clone(), handle.clone()),
        );
        let id = collection("d5-precall")?;
        let cref = CollectionRef::new(id.clone(), None);
        let writes = stage_committed_marker(&cached, &oracle, &cref, 1).await?;

        *handle.lock() = Some(Poison::Collection(
            id.name().clone(),
            ErrorCategory::Transient,
        ));
        let result = cached.commit_provisional(&cref, &writes, &[]).await;
        assert!(result.is_err(), "the poisoned lower promote must surface");
        *handle.lock() = None;

        // Every staged cell reads back WARM with the committed data — never
        // `prev`, and with zero lower reads (the transform ran pre-call).
        counting.reset();
        for c in [1u8, 2, 3] {
            assert_eq!(
                cached.get(&id, &cell_at(c), probe(50)).await?.get(),
                Some(&bytes(100 + c)),
                "cell {c} serves the oracle-committed data, never prev"
            );
        }
        assert_eq!(
            counting.lower_reads(),
            0,
            "the transform kept every staged cell warm through the failed promote"
        );

        // ---- Window (b): the settle future is DROPPED after the lower batch
        // landed (response withheld, then the task aborted). -----------------
        let oracle_b = ScriptedOracle::default();
        let counting_b = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle_b.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let holding = HoldingCellStore::new(counting_b.clone());
        let holds = holding.holds();
        let cached_b = Cached::new(test_db::cache("d5_drop")?, holding);
        let id_b = collection("d5-drop")?;
        let cref_b = CollectionRef::new(id_b.clone(), None);
        let writes_b = stage_committed_marker(&cached_b, &oracle_b, &cref_b, 2).await?;

        holds.commit_provisional().arm(1);
        let landed_before = holds.commit_provisional().landed();
        let task = tokio::spawn({
            let cached_b = cached_b.clone();
            let cref_b = cref_b.clone();
            let writes_b = writes_b.clone();
            async move { cached_b.commit_provisional(&cref_b, &writes_b, &[]).await }
        });
        // Wait until the lower batch LANDED and the response is withheld, then
        // drop the settle future mid-flight.
        holds.commit_provisional().entered().await;
        assert!(
            holds.commit_provisional().landed() > landed_before,
            "the lower batch landed before the drop"
        );
        task.abort();
        assert!(task.await.is_err(), "the settle future was dropped");

        counting_b.reset();
        for c in [1u8, 2, 3] {
            assert_eq!(
                cached_b.get(&id_b, &cell_at(c), probe(51)).await?.get(),
                Some(&bytes(100 + c)),
                "cell {c} serves the committed data across the dropped settle"
            );
        }
        assert_eq!(
            counting_b.lower_reads(),
            0,
            "the pre-call transform kept the cells warm across the drop"
        );

        // ---- The cold arm (marker-grain lemma): force-delete one cell's
        // entry (the transform's delete fallback shape); it pays exactly one
        // durable read and still resolves to data, while the siblings stay
        // warm and never serve prev. This sub-assert is green-is-correct for
        // the deleted cell: a deleted entry cannot serve a ghost by
        // construction. ------------------------------------------------------
        cached_b.evict_for_tests(&id_b, &[cell_at(2)]).await?;
        counting_b.reset();
        assert_eq!(
            cached_b.get(&id_b, &cell_at(2), probe(52)).await?.get(),
            Some(&bytes(102)),
            "the force-deleted cell resolves durably to the committed data"
        );
        assert_eq!(counting_b.lower_reads(), 1, "the deleted cell is cold");
        counting_b.reset();
        for c in [1u8, 3] {
            assert_eq!(
                cached_b.get(&id_b, &cell_at(c), probe(53)).await?.get(),
                Some(&bytes(100 + c)),
                "sibling {c} never serves prev beside a cold cell"
            );
        }
        assert_eq!(counting_b.lower_reads(), 0, "the siblings stay warm");
        Ok(())
    })
}

/// Proves that a failed settlement cache update removes the affected entries.
///
/// The function must return the durable-store result.
#[test]
fn d5_transform_batch_failure_degrades_to_delete() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let handle: PoisonHandle = Arc::default();
        let fjall = test_db::cache("d5_fallback")?;
        let fail_puts = fjall.fail_puts();
        let cached = Cached::new(
            fjall,
            FailingCellStore::with_handle(counting.clone(), handle.clone()),
        );
        let id = collection("d5-fallback")?;
        let cref = CollectionRef::new(id.clone(), None);
        let writes = stage_committed_marker(&cached, &oracle, &cref, 3).await?;

        // Poisoned lower + failed transform: the POISON surfaces, verbatim.
        fail_puts.store(true, Ordering::Relaxed);
        *handle.lock() = Some(Poison::Collection(
            id.name().clone(),
            ErrorCategory::Transient,
        ));
        let result = cached.commit_provisional(&cref, &writes, &[]).await;
        assert!(
            matches!(result, Err(ref e) if format!("{e}").contains("poison")),
            "the lower error returns verbatim — a fjall failure is never folded in"
        );
        *handle.lock() = None;

        // Healthy lower + still-failing transform: Ok, verbatim.
        let result = cached.commit_provisional(&cref, &writes, &[]).await;
        assert!(
            result.is_ok(),
            "a fjall transform failure never folds into the lower Ok"
        );
        fail_puts.store(false, Ordering::Relaxed);

        // The fallback delete left the cells COLD: each next get pays exactly
        // one durable read and yields the committed data (never stale prev),
        // then republishes (a cold miss, never a ghost of the deleted entry).
        counting.reset();
        for c in [1u8, 2, 3] {
            assert_eq!(
                cached.get(&id, &cell_at(c), probe(60)).await?.get(),
                Some(&bytes(100 + c)),
                "cell {c} resolves durably to the committed data"
            );
        }
        assert_eq!(
            counting.lower_reads(),
            3,
            "the fallback delete left every staged cell cold"
        );
        counting.reset();
        for c in [1u8, 2, 3] {
            assert_eq!(
                cached.get(&id, &cell_at(c), probe(61)).await?.get(),
                Some(&bytes(100 + c)),
                "cell {c} re-warmed from the cold fall-through"
            );
        }
        assert_eq!(counting.lower_reads(), 0, "the republished cells are warm");
        Ok(())
    })
}

/// Proves that repeated settlement writes the same cache values and expiry.
#[test]
fn d5_transform_retry_is_byte_equivalent() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let now = Arc::new(AtomicU64::new(1_000));
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(
            test_db::cache_with_clock("d5_retry", Clock::Fixed(now.clone()))?,
            counting.clone(),
        );
        let id = collection("d5-retry")?;
        // A TTL'd collection, so the stage stamps a finite expiry the retry
        // must REUSE (a fresh now+ttl would differ after the clock advance).
        let cref = CollectionRef::new(id.clone(), Some(CompactDuration::new(60)));
        let writes = stage_committed_marker(&cached, &oracle, &cref, 4).await?;

        cached.commit_provisional(&cref, &writes, &[]).await?;
        let first: Vec<Option<u64>> = {
            let mut out = Vec::new();
            for c in [1u8, 2, 3] {
                out.push(cached.stored_expiry(&id, &cell_at(c)).await?);
            }
            out
        };

        // Advance the clock (a fresh now+ttl restamp would now differ), then
        // run the transform again — the sweep-retry shape.
        now.store(5_500, Ordering::Relaxed);
        cached.commit_provisional(&cref, &writes, &[]).await?;

        counting.reset();
        for (i, c) in [1u8, 2, 3].into_iter().enumerate() {
            assert_eq!(
                cached.stored_expiry(&id, &cell_at(c)).await?,
                first[i],
                "cell {c}'s expiry is unchanged by the retry (stage-anchored reuse)"
            );
            assert_eq!(
                cached.get(&id, &cell_at(c), probe(70)).await?.get(),
                Some(&bytes(100 + c)),
                "cell {c}'s value is unchanged by the retry"
            );
        }
        assert_eq!(counting.lower_reads(), 0, "the retried entries stay warm");
        Ok(())
    })
}

/// The set equation: a commit whose event CLEARS the sections it repopulates
/// keeps the staged coordinates warm (S holds `data`) while every other cached
/// entry of the cleared sections is deleted (C ∖ S). Variable-length
/// coordinates across MULTIPLE sections make an exclusion-set encoding
/// mismatch (the index-key form's extra kind byte) unable to pass: a wrong
/// encoding would silently delete S ∩ C and the zero-lower-reads assert would
/// go red.
#[test]
fn d5_clear_and_repopulate_keeps_staged_cells_warm() -> Result<()> {
    fn cell(section: i8, coord: &[u8]) -> CellKey {
        CellKey {
            section: Section::new(section),
            coordinate: Coordinate::from_bytes(coord.to_vec()),
        }
    }

    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(test_db::cache("set_equation")?, counting.clone());
        let id = collection("set-equation")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = probe(5);

        // Pre-clear entries in both sections (the C ∖ S victims).
        let victims = [cell(0, &[0x01]), cell(0, &[0xAA, 0xBB]), cell(1, &[0x02])];
        for (i, victim) in victims.iter().enumerate() {
            cached
                .write_resolved(
                    &cref,
                    &[(victim.clone(), Some(bytes(u8::try_from(i)?)))],
                    &[],
                )
                .await?;
        }

        // Stage VARIABLE-LENGTH coordinates across BOTH sections, with the
        // event clearing both sections.
        let staged_cells = [
            cell(0, &[0x07]),
            cell(0, &[0x10, 0x20, 0x30]),
            cell(1, &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]),
        ];
        let mut writes = Vec::new();
        for (i, staged) in staged_cells.iter().enumerate() {
            let prev = cached.get(&id, staged, event).await?;
            writes.push((
                staged.clone(),
                ProvisionalWrite::new(Some(bytes(200 + u8::try_from(i)?)), prev, event),
            ));
        }
        let clears = [
            SectionClear::frozen(Section::new(0), &writes),
            SectionClear::frozen(Section::new(1), &writes),
        ];
        let marker = EventMarker::frozen(event, &writes, &clears);
        cached
            .write_provisional(&cref, &writes, Some(&marker))
            .await?;
        oracle.record_message(Uuid::from_u128(5)).await?;

        cached.commit_provisional(&cref, &writes, &clears).await?;

        // S: every staged cell reads back WARM with `data` — zero lower reads.
        counting.reset();
        for (i, staged) in staged_cells.iter().enumerate() {
            assert_eq!(
                cached.get(&id, staged, probe(80)).await?.get(),
                Some(&bytes(200 + u8::try_from(i)?)),
                "staged cell {i} keeps commit warmth through the clear"
            );
        }
        assert_eq!(
            counting.lower_reads(),
            0,
            "the scoped section delete excluded the staged coordinates"
        );
        // C ∖ S: the sections' other entries are gone (durably erased by the
        // clear; the cache pays the cold fall-through to absence).
        for victim in &victims {
            assert_eq!(
                cached.get(&id, victim, probe(81)).await?.get(),
                None,
                "the cleared sections' other entries are gone"
            );
        }
        Ok(())
    })
}

/// Negative caching (KV2), committed arm: an absent-base cell staged and
/// committed by a prior-event event beneath the cache resolves PRESENT on the
/// fall-through read, which publishes the present value — the second get is
/// warm.
#[test]
fn absent_fill_over_committed_foreign_provisional_publishes_present() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(test_db::cache("neg_committed")?, counting.clone());
        let id = collection("neg-committed")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Stage below the cache (the crash/prior event shape): the cache never
        // saw the stage, so the cell is a genuine miss.
        let a = probe(1);
        let writes = [(
            cell_at(4),
            ProvisionalWrite::new(Some(bytes(44)), Committed::new(None), a),
        )];
        let marker = EventMarker::frozen(a, &writes, &[]);
        counting
            .write_provisional(&cref, &writes, Some(&marker))
            .await?;
        oracle.record_message(Uuid::from_u128(1)).await?;

        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(4), probe(2)).await?.get(),
            Some(&bytes(44)),
            "the fill resolves the committed prior event provisional to present"
        );
        assert!(counting.lower_reads() >= 1, "the first get falls through");
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(4), probe(3)).await?.get(),
            Some(&bytes(44)),
            "the resolved value was published"
        );
        assert_eq!(counting.lower_reads(), 0, "the second get is warm");
        Ok(())
    })
}

/// Negative caching (KV2), aborted arm: an absent-base cell staged by a
/// prior-event event that never committed resolves ABSENT on the fall-through
/// read, which publishes the Absent tag — the second get answers `None` with
/// zero lower reads.
#[test]
fn absent_fill_over_aborted_foreign_provisional_publishes_absent() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(test_db::cache("neg_aborted")?, counting.clone());
        let id = collection("neg-aborted")?;
        let cref = CollectionRef::new(id.clone(), None);

        let a = probe(1);
        let writes = [(
            cell_at(4),
            ProvisionalWrite::new(Some(bytes(44)), Committed::new(None), a),
        )];
        let marker = EventMarker::frozen(a, &writes, &[]);
        counting
            .write_provisional(&cref, &writes, Some(&marker))
            .await?;
        // No record: the oracle resolves the event NotCommitted (aborted).

        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(4), probe(2)).await?.get(),
            None,
            "the fill resolves the aborted prior event provisional to its absent prev"
        );
        assert!(counting.lower_reads() >= 1, "the first get falls through");
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(4), probe(3)).await?.get(),
            None,
            "absence stays correct"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "the Absent tag was published — repeated absent reads are free"
        );
        Ok(())
    })
}

/// The recovery sweep never calls `scan_cells` — it rides `unsettled_marker`,
/// the warm index, and `provisional_many` batch reads — so "scans are
/// durable" (KV3) adds zero recovery cost. Falsified through the
/// counting-store seam: the exact op set is asserted, never the sweep
/// rewritten.
#[test]
fn sweep_issues_no_scan_cells() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(test_db::cache("sweep_budget")?, counting.clone());
        let id = collection("sweep-budget")?;
        let cref = CollectionRef::new(id.clone(), None);
        let writes = stage_committed_marker(&cached, &oracle, &cref, 6).await?;
        drop(writes);

        counting.reset();
        let resolved = sweep_provisional(&cached, &oracle, &cref)
            .await
            .map_err(|e| eyre!("sweep failed: {e:?}"))?;
        assert!(resolved, "the sweep resolved the staged marker");
        assert_eq!(
            counting.lower_scans(),
            0,
            "the sweep issues no scan_cells — recovery rides marker + batch reads only"
        );
        assert!(
            counting.marker_reads() >= 1,
            "the sweep rode the unsettled-marker leg"
        );
        Ok(())
    })
}

/// The KV5 fault clause's fail-N budget: a fill whose publish fails N times
/// then succeeds issues exactly N+1 durable reads, then zero.
#[test]
fn fill_publish_failure_costs_one_read_each() -> Result<()> {
    const N: usize = 3;

    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let fjall = test_db::cache("fill_budget")?;
        let fail_puts = fjall.fail_puts();
        let cached = Cached::new(fjall, counting.clone());
        let id = collection("fill-budget")?;
        let cref = CollectionRef::new(id.clone(), None);
        counting
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(7)))], &[])
            .await?;

        counting.reset();
        fail_puts.store(true, Ordering::Relaxed);
        for n in 0..N {
            assert_eq!(
                cached
                    .get(&id, &cell_at(1), probe(u128::try_from(n)?))
                    .await?
                    .get(),
                Some(&bytes(7)),
                "a degraded fill still answers correctly"
            );
        }
        fail_puts.store(false, Ordering::Relaxed);
        // The (N+1)th read heals: one more durable read, then zero.
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(90)).await?.get(),
            Some(&bytes(7))
        );
        assert_eq!(
            counting.lower_reads(),
            N + 1,
            "N failed publishes cost exactly N+1 durable reads"
        );
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(91)).await?.get(),
            Some(&bytes(7))
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "the first successful publish restores permanence (KV5)"
        );
        Ok(())
    })
}

/// The read-degrade companion: a corrupt fjall frame at a cell degrades that
/// one get to a durable read (warn-skip, never a failed get), and the fill's
/// publish overwrites the corrupt frame so the next get is warm again.
#[test]
fn fjall_read_failure_degrades_that_get() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let fjall = test_db::cache("read_degrade")?;
        let cached = Cached::new(fjall.clone(), counting.clone());
        let id = collection("read-degrade")?;
        let cref = CollectionRef::new(id.clone(), None);

        counting
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(7)))], &[])
            .await?;
        // Seed a corrupt frame (unknown tag byte) at the cell's fjall key.
        fjall
            .seed_raw_cell(&id, &cell_at(1), Bytes::from_static(&[0xFE, 0, 0]))
            .await?;

        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(1)).await?.get(),
            Some(&bytes(7)),
            "a fjall read failure degrades the get to a durable read, never fails it"
        );
        assert_eq!(counting.lower_reads(), 1, "exactly one degraded read");
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(2)).await?.get(),
            Some(&bytes(7)),
            "the fill overwrote the corrupt frame"
        );
        assert_eq!(counting.lower_reads(), 0, "the repaired entry is warm");
        Ok(())
    })
}

/// Proves that cache disablement applies to all workspace clones.
///
/// A removal failure disables clone A.
/// Clone B must then use durable storage.
/// Recovery must ignore an incomplete cache index.
#[test]
fn cache_disablement_applies_to_all_workspace_clones() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let fjall = test_db::cache("disabled")?;
        let fail_puts = fjall.fail_puts();
        let fail_deletes = fjall.fail_deletes();
        let fail_index_record = fjall.fail_index_record();
        let cached_a = Cached::new(fjall.clone(), counting.clone());
        let cached_b = cached_a.clone();
        let id = collection("disabled")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = probe(1);

        // Add a cached value and create a complete provisional index.
        cached_a
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(1)))], &[])
            .await?;
        let prev = cached_a.get(&id, &cell_at(0), event).await?;
        let early = [(
            cell_at(0),
            ProvisionalWrite::new(Some(bytes(9)), prev, event),
        )];
        let marker = EventMarker::frozen(event, &early, &[]);
        cached_a
            .write_provisional(&cref, &early, Some(&marker))
            .await?;
        let seeded = drain_provisional(&cached_a, &id).await?;
        assert_eq!(seeded, vec![0], "the first recovery created the index");

        // Make the index update and its required cleanup fail.
        fail_index_record.store(true, Ordering::Relaxed);
        fail_deletes.store(u64::try_from(DELETE_RETRY_BUDGET + 2)?, Ordering::Relaxed);
        fail_puts.store(true, Ordering::Relaxed);
        let prev1 = counting.get(&id, &cell_at(1), event).await?;
        let stage = [(
            cell_at(1),
            ProvisionalWrite::new(Some(bytes(2)), prev1, event),
        )];
        let marker2 = EventMarker::frozen(event, &stage, &[]);
        cached_a
            .write_provisional(&cref, &stage, Some(&marker2))
            .await?;
        fail_index_record.store(false, Ordering::Relaxed);
        fail_puts.store(false, Ordering::Relaxed);
        fail_deletes.store(0, Ordering::Relaxed);

        assert!(
            fjall.is_disabled(),
            "the cleanup failure disabled the cache"
        );

        // Clone B must not return the old cached value.
        counting.reset();
        assert_eq!(
            cached_b.get(&id, &cell_at(1), event).await?.get(),
            Some(&bytes(1)),
            "clone B reads the committed value from durable storage"
        );
        assert!(counting.lower_reads() >= 1, "B's get is a durable read");
        // A repeated read must also use durable storage.
        counting.reset();
        let _ = cached_b.get(&id, &cell_at(1), event).await?;
        assert!(
            counting.lower_reads() >= 1,
            "clone B does not update a disabled cache"
        );
        assert!(
            fjall.is_disabled(),
            "the cache remains disabled for the assignment"
        );

        // Add a provisional cell after cache disablement.
        // Recovery must read durable state instead of the incomplete index.
        let prev3 = counting.get(&id, &cell_at(3), event).await?;
        let post = [(
            cell_at(3),
            ProvisionalWrite::new(Some(bytes(5)), prev3, event),
        )];
        let marker3 = EventMarker::frozen(event, &post, &[]);
        cached_b
            .write_provisional(&cref, &post, Some(&marker3))
            .await?;
        let swept = drain_provisional(&cached_b, &id).await?;
        assert!(
            swept.contains(&3),
            "recovery finds the cell that the disabled cache did not index"
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// The transparency property
// ---------------------------------------------------------------------------

/// The keys a transparency trace addresses (all in [`SECTION`]).
const POOL: u8 = 5;

/// One op of a [`CacheTrace`]. The staged lifecycle is clean by construction
/// (`Stage` only when idle; `Commit`/`Abort`/`Promote` only when staged), so
/// any prefix is itself a valid trace — which is how `shrink` minimises.
#[derive(Clone, Debug)]
enum CacheOp {
    /// `write_resolved` of `(key, value)` cells; `clear` erases [`SECTION`]
    /// with the written present cells as survivors.
    Write {
        cells: Vec<(u8, Option<u8>)>,
        clear: bool,
    },
    /// `write_provisional` of present-data writes under a frozen marker;
    /// `clear` freezes a [`SECTION`] clear into it.
    Stage { writes: Vec<(u8, u8)>, clear: bool },
    /// Record the unsettled stage's verdict, then `commit_provisional`.
    Commit,
    /// `abort_provisional` of the unsettled stage.
    Abort,
    /// Raw `mark_resolved` of the unsettled stage's cells (the sweep's promote
    /// path) — leaves the marker unsettled, as the raw verb does.
    Promote,
    /// A point read of one pool key.
    Get(u8),
    /// A full-section scan.
    Scan,
    /// Advance the fixed clock by N milliseconds (sub-second-grain, so the
    /// floor arithmetic is exercised).
    Advance(u16),
    /// Toggle the fjall publish fault seam.
    FaultPuts(bool),
    /// Adds removal failures below the cache-disablement limit.
    FaultDeletes(u8),
}

/// A generated cell-op trace with an optional per-trace collection TTL.
#[derive(Clone, Debug)]
struct CacheTrace {
    ttl: Option<u32>,
    ops: Vec<CacheOp>,
}

impl Arbitrary for CacheTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let ttl = if bool::arbitrary(g) {
            None
        } else {
            Some(1 + u32::from(u8::arbitrary(g) % 8))
        };
        let len = usize::arbitrary(g) % 20;
        let mut staged = false;
        let mut ops = Vec::with_capacity(len);
        for _ in 0..len {
            let roll = u8::arbitrary(g) % 16;
            // While a stage stands, only reads, clock movement, faults, and
            // the stage's own settle are legal — per-key serialization means
            // no handler write can interleave a stage and its settle, and the
            // settlement cache update argument (the staged rows still hold the verdict's
            // data when commit_provisional runs) rests on exactly that.
            let op = match roll {
                0..=3 if !staged => {
                    let n = 1 + usize::arbitrary(g) % 3;
                    let cells = (0..n)
                        .map(|_| {
                            (
                                u8::arbitrary(g) % POOL,
                                bool::arbitrary(g).then(|| u8::arbitrary(g)),
                            )
                        })
                        .collect();
                    CacheOp::Write {
                        cells,
                        clear: u8::arbitrary(g) % 4 == 0,
                    }
                }
                0..=6 => {
                    if staged {
                        // Settle the unsettled stage.
                        staged = false;
                        match u8::arbitrary(g) % 3 {
                            0 => CacheOp::Commit,
                            1 => CacheOp::Abort,
                            _ => CacheOp::Promote,
                        }
                    } else {
                        staged = true;
                        // Distinct keys per stage (an event stages each cell
                        // at most once).
                        let mut keys: Vec<u8> = (0..POOL).collect();
                        let n = 1 + usize::arbitrary(g) % 3;
                        let mut writes = Vec::with_capacity(n);
                        for _ in 0..n {
                            let i = usize::arbitrary(g) % keys.len();
                            writes.push((keys.swap_remove(i), u8::arbitrary(g)));
                        }
                        CacheOp::Stage {
                            writes,
                            clear: u8::arbitrary(g) % 4 == 0,
                        }
                    }
                }
                7..=10 => CacheOp::Get(u8::arbitrary(g) % POOL),
                11 => CacheOp::Scan,
                12..=13 => CacheOp::Advance(u16::arbitrary(g) % 12_000),
                14 => CacheOp::FaultPuts(bool::arbitrary(g)),
                _ => CacheOp::FaultDeletes(1 + u8::arbitrary(g) % 2),
            };
            ops.push(op);
        }
        Self { ttl, ops }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let ttl = self.ttl;
        let ops = self.ops.clone();
        // A prefix of a clean-lifecycle trace is itself clean, so truncation
        // is a safe shrink; also try dropping the TTL entirely.
        let prefixes = (0..ops.len()).map(move |n| Self {
            ttl,
            ops: ops[..n].to_vec(),
        });
        let drop_ttl = self
            .ttl
            .map(|_| Self {
                ttl: None,
                ops: self.ops.clone(),
            })
            .into_iter();
        Box::new(drop_ttl.chain(prefixes))
    }
}

/// An unsettled stage's replay bookkeeping.
struct Staged {
    dedup: u128,
    writes: Vec<(CellKey, ProvisionalWrite)>,
    clears: Vec<SectionClear>,
}

/// A consumed-but-unsettled marker (left unsettled by [`CacheOp::Promote`]).
struct StaleMarker {
    staged: Vec<CellKey>,
    clears: bool,
}

/// The KV5 warm-set model: cell → the expiry its entry carries (`u64::MAX`
/// unsettled in for a fill's effectively-unreachable stamp). A cell present
/// and unexpired here MUST be a fjall hit; anything else is excluded by
/// construction.
type WarmModel = HashMap<u8, u64>;

/// The one-shot replay state of [`prop_cached_is_transparent`], carried
/// through every op and the per-op verification passes.
struct Replay {
    subject: Cached<TtlAwareCellStore<MemoryCellStore<ScriptedOracle>>>,
    twin: MemoryCellStore<ScriptedOracle>,
    oracle: ScriptedOracle,
    id: CollectionId,
    cref: CollectionRef,
    twin_ref: CollectionRef,
    now: Arc<AtomicU64>,
    ttl_ms: Option<u64>,
    clock: u64,
    stage_seq: u128,
    staged: Option<Staged>,
    stale: Option<StaleMarker>,
    fault_puts: bool,
    warm: WarmModel,
    fail_puts: Arc<AtomicBool>,
    fail_deletes: Arc<AtomicU64>,
}

impl Replay {
    /// The `own` event verification reads use: the unsettled stage's event
    /// while one stands (an own-event read short-circuits to `prev` without
    /// resolving the stage), a fixed prior event reader otherwise.
    fn reader(&self) -> EventRef {
        match &self.staged {
            Some(staged) => probe(staged.dedup),
            None => probe(u128::MAX),
        }
    }

    /// The modeled fjall expiry of a write-through landing now.
    fn write_expiry(&self) -> u64 {
        self.ttl_ms
            .map_or(u64::MAX, |ttl| (self.clock - self.clock % 1_000) + ttl)
    }

    /// Whether the warm model says `key` is a live hit at the current clock.
    fn is_warm(&self, key: u8) -> bool {
        self.warm
            .get(&key)
            .is_some_and(|&expiry| expiry == u64::MAX || self.clock < expiry)
    }

    /// Publish-through model update: the batch either warms every cell (a
    /// clean atomic publish) or — under the puts fault — lands nothing and
    /// failed-publish cache guard deletes every cell.
    fn model_publish(&mut self, keys: impl IntoIterator<Item = u8>) {
        let expiry = self.write_expiry();
        for key in keys {
            if self.fault_puts {
                self.warm.remove(&key);
            } else {
                self.warm.insert(key, expiry);
            }
        }
    }

    /// Removes model entries that marker resolution can change.
    fn model_stale_resolved(&mut self) {
        if let Some(stale) = self.stale.take() {
            if stale.clears {
                self.warm.clear();
            } else {
                for cell in &stale.staged {
                    self.warm.remove(&cell.coordinate.as_bytes()[0]);
                }
            }
        }
    }

    /// One `write_resolved` through both stores, updating the warm model.
    async fn step_write(&mut self, cells: &[(u8, Option<u8>)], clear: bool) -> Result<()> {
        let mut resolved: Vec<(CellKey, Option<Bytes>)> = Vec::new();
        let mut seen: HashSet<u8> = HashSet::new();
        for (key, value) in cells {
            // Last-writer-wins within one batch is the store's contract only
            // per distinct cell; keep cells distinct.
            if seen.insert(*key) {
                resolved.push((cell_at(*key), value.map(bytes)));
            }
        }
        let clears: Vec<SectionClear> = clear
            .then(|| SectionClear::frozen_resolved(SECTION, &resolved))
            .into_iter()
            .collect();
        self.subject
            .write_resolved(&self.cref, &resolved, &clears)
            .await
            .map_err(|e| eyre!("subject write: {e:?}"))?;
        self.twin
            .write_resolved(&self.twin_ref, &resolved, &clears)
            .await
            .map_err(|e| eyre!("twin write: {e:?}"))?;
        if !clears.is_empty() {
            // section-clear cache guard whole-section delete ran before the lower write.
            self.warm.clear();
        }
        self.model_publish(
            resolved
                .iter()
                .map(|(cell, _)| cell.coordinate.as_bytes()[0]),
        );
        Ok(())
    }

    /// One `write_provisional` through both stores, updating the warm model
    /// (including the boundary prior-clear cache guard over a stale marker).
    async fn step_stage(&mut self, writes: &[(u8, u8)], clear: bool) -> Result<()> {
        self.stage_seq += 1;
        let event = probe(self.stage_seq);
        let mut staged: Vec<(CellKey, ProvisionalWrite)> = Vec::new();
        for (key, value) in writes {
            // The committed base read, off the twin — identical to the
            // subject's by the parity just asserted, and it leaves the
            // subject's cache untouched.
            let prev = self
                .twin
                .get(&self.id, &cell_at(*key), event)
                .await
                .map_err(|e| eyre!("twin prev read: {e:?}"))?;
            staged.push((
                cell_at(*key),
                ProvisionalWrite::new(Some(bytes(*value)), prev, event),
            ));
        }
        let clears: Vec<SectionClear> = clear
            .then(|| SectionClear::frozen(SECTION, &staged))
            .into_iter()
            .collect();
        let marker = EventMarker::frozen(event, &staged, &clears);
        // The subject's stage boundary resolves a stale prior event marker
        // beneath and fires the boundary prior-clear cache guard.
        let stale_pending = self.stale.is_some();
        self.subject
            .write_provisional(&self.cref, &staged, Some(&marker))
            .await
            .map_err(|e| eyre!("subject stage: {e:?}"))?;
        self.twin
            .write_provisional(&self.twin_ref, &staged, Some(&marker))
            .await
            .map_err(|e| eyre!("twin stage: {e:?}"))?;
        if stale_pending {
            // The boundary delete is verdict-blind and unconditional on
            // clears: staged coordinates always drop; a clears-bearing stale
            // marker drops its section wholesale.
            self.model_stale_resolved();
        }
        self.model_publish(staged.iter().map(|(cell, _)| cell.coordinate.as_bytes()[0]));
        self.staged = Some(Staged {
            dedup: self.stage_seq,
            writes: staged,
            clears,
        });
        Ok(())
    }

    /// Runs one op against both stores, updating the warm model.
    async fn step(&mut self, op: &CacheOp) -> Result<()> {
        match op {
            CacheOp::Write { cells, clear } => self.step_write(cells, *clear).await?,
            CacheOp::Stage { writes, clear } => self.step_stage(writes, *clear).await?,
            CacheOp::Commit => {
                let Some(staged) = self.staged.take() else {
                    return Err(eyre!("commit without an unsettled stage"));
                };
                self.oracle
                    .record_message(Uuid::from_u128(staged.dedup))
                    .await?;
                self.subject
                    .commit_provisional(&self.cref, &staged.writes, &staged.clears)
                    .await
                    .map_err(|e| eyre!("subject commit: {e:?}"))?;
                self.twin
                    .commit_provisional(&self.twin_ref, &staged.writes, &staged.clears)
                    .await
                    .map_err(|e| eyre!("twin commit: {e:?}"))?;
                let staged_keys: Vec<u8> = staged
                    .writes
                    .iter()
                    .map(|(cell, _)| cell.coordinate.as_bytes()[0])
                    .collect();
                if !staged.clears.is_empty() {
                    // Scoped section-clear cache guard: everything but the staged coordinates goes.
                    self.warm.retain(|key, _| staged_keys.contains(key));
                }
                if self.fault_puts {
                    // The transform failed; the fallback delete landed.
                    for key in &staged_keys {
                        self.warm.remove(key);
                    }
                }
                // A successful transform keeps the staged cells warm at their
                // stage-anchored expiry — already in the model from the stage.
            }
            CacheOp::Abort => {
                let Some(staged) = self.staged.take() else {
                    return Err(eyre!("abort without an unsettled stage"));
                };
                self.subject
                    .abort_provisional(&self.cref, &staged.writes)
                    .await
                    .map_err(|e| eyre!("subject abort: {e:?}"))?;
                self.twin
                    .abort_provisional(&self.twin_ref, &staged.writes)
                    .await
                    .map_err(|e| eyre!("twin abort: {e:?}"))?;
                self.model_publish(
                    staged
                        .writes
                        .iter()
                        .map(|(cell, _)| cell.coordinate.as_bytes()[0]),
                );
            }
            CacheOp::Promote => {
                let Some(staged) = self.staged.take() else {
                    return Err(eyre!("promote without an unsettled stage"));
                };
                let cells: Vec<CellKey> =
                    staged.writes.iter().map(|(cell, _)| cell.clone()).collect();
                self.subject
                    .mark_resolved(&self.cref, &cells)
                    .await
                    .map_err(|e| eyre!("subject promote: {e:?}"))?;
                self.twin
                    .mark_resolved(&self.twin_ref, &cells)
                    .await
                    .map_err(|e| eyre!("twin promote: {e:?}"))?;
                // promotion cache guard deleted the promoted entries; the raw verb leaves the
                // marker unsettled (consumed later by a read or the next
                // stage's boundary).
                for cell in &cells {
                    self.warm.remove(&cell.coordinate.as_bytes()[0]);
                }
                self.stale = Some(StaleMarker {
                    staged: cells,
                    clears: !staged.clears.is_empty(),
                });
            }
            CacheOp::Get(key) => self.check_get(*key).await?,
            CacheOp::Scan => self.check_scan().await?,
            CacheOp::Advance(ms) => {
                self.clock += u64::from(*ms);
                self.now.store(self.clock, Ordering::Relaxed);
            }
            CacheOp::FaultPuts(on) => {
                self.fault_puts = *on;
                self.fail_puts.store(*on, Ordering::Relaxed);
            }
            CacheOp::FaultDeletes(n) => {
                self.fail_deletes.store(u64::from(*n), Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// One parity get of `key`, updating the warm model with the fill.
    async fn check_get(&mut self, key: u8) -> Result<()> {
        let own = self.reader();
        // A fall-through read of a stale clears-bearing marker fires prior-clear cache
        // guard and resolves the marker beneath; a warm hit touches neither.
        let falls_through = !self.is_warm(key);
        if falls_through
            && let Some(stale) = &self.stale
            && stale.clears
            && self.staged.is_none()
        {
            self.model_stale_resolved();
        }
        let subject = self
            .subject
            .get(&self.id, &cell_at(key), own)
            .await
            .map_err(|e| eyre!("subject get: {e:?}"))?;
        let twin = self
            .twin
            .get(&self.id, &cell_at(key), own)
            .await
            .map_err(|e| eyre!("twin get: {e:?}"))?;
        if subject.get() != twin.get() {
            return Err(eyre!(
                "get({key}) diverged: subject {:?}, twin {:?}",
                subject.get(),
                twin.get()
            ));
        }
        if falls_through {
            // The fill published (or failed to): fills stamp the remaining
            // TTL of a far-future death — effectively never.
            if self.fault_puts {
                self.warm.remove(&key);
            } else {
                self.warm.insert(key, u64::MAX);
            }
        }
        Ok(())
    }

    /// One parity full-section scan. Scans fire prior-clear cache guard on a
    /// stale clears-bearing marker and resolve it beneath, but publish
    /// nothing (KV3).
    async fn check_scan(&mut self) -> Result<()> {
        let own = self.reader();
        if let Some(stale) = &self.stale
            && stale.clears
            && self.staged.is_none()
        {
            self.model_stale_resolved();
        }
        let subject =
            scan_forward(&self.subject, &self.id, 0, ScanEdge::Included(255), own).await?;
        let twin = scan_forward(&self.twin, &self.id, 0, ScanEdge::Included(255), own).await?;
        if subject != twin {
            return Err(eyre!("scan diverged: subject {subject:?}, twin {twin:?}"));
        }
        Ok(())
    }

    /// The after-every-op verification: every pool cell's get and one
    /// full-section scan answer identically.
    async fn verify(&mut self) -> Result<()> {
        for key in 0..POOL {
            self.check_get(key).await?;
        }
        self.check_scan().await
    }
}

/// **The transparency property** — the whole contract of a transparent cache
/// in one differential: one generated cell-op trace (writes, provisional
/// stage/commit/abort, raw promotes, clears, gets, scans, clock movement,
/// TTL'd and not) through `Cached` over a memory store AND through a bare
/// memory twin sharing one scripted oracle; after **every** op, every pool
/// cell's `get` and a full-section scan answer identically. Bounded fjall
/// fault injection (`fail_puts`, an in-budget `fail_deletes` countdown) runs
/// degraded-cache paths inside the property, not beside it.
///
/// The second arm is the **KV5 budget**: after the trace (fault seams healed
/// and one model-updating verification pass run), re-getting every pool cell
/// issues zero lower reads for every cell the warm-set model holds — the
/// exceptions (expired, cleared, fault-path, marker-resolution evictions) are
/// excluded by construction as the model's removals.
#[test]
fn prop_cached_is_transparent() {
    fn property(trace: CacheTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(async move {
            const START: u64 = 1_000;
            /// Far beyond any bounded trace's clock: fills never expire.
            const DEATH: u64 = u64::MAX / 2;

            let now = Arc::new(AtomicU64::new(START));
            let oracle = ScriptedOracle::default();
            let counting = CountingCellStore::new(MemoryCellStore::new(
                MemoryCells::new(),
                oracle.clone(),
                Arc::new(CollectionDefRegistry::default()),
            ));
            let ttl_lower =
                TtlAwareCellStore::new(counting.clone(), Clock::Fixed(now.clone()), DEATH);
            let fjall = test_db::cache_with_clock("transparent", Clock::Fixed(now.clone()))?;
            let fail_puts = fjall.fail_puts();
            let fail_deletes = fjall.fail_deletes();
            let subject = Cached::new(fjall, ttl_lower.clone());
            let twin = MemoryCellStore::new(
                MemoryCells::new(),
                oracle.clone(),
                Arc::new(CollectionDefRegistry::default()),
            );
            let id = collection("transparent")?;
            let ttl = trace.ttl.map(CompactDuration::new);
            let mut replay = Replay {
                subject,
                twin,
                oracle,
                cref: CollectionRef::new(id.clone(), ttl),
                twin_ref: CollectionRef::new(id.clone(), ttl),
                id,
                now,
                ttl_ms: trace.ttl.map(|s| u64::from(s) * 1_000),
                clock: START,
                stage_seq: 0,
                staged: None,
                stale: None,
                fault_puts: false,
                warm: WarmModel::new(),
                fail_puts,
                fail_deletes,
            };

            for (index, op) in trace.ops.iter().enumerate() {
                replay
                    .step(op)
                    .await
                    .map_err(|e| eyre!("op {index} ({op:?}): {e}"))?;
                replay
                    .verify()
                    .await
                    .map_err(|e| eyre!("after op {index} ({op:?}): {e}"))?;
            }

            // The KV5 budget arm: heal the seams, run one model-updating
            // verification pass (re-warming what the faults left cold and
            // consuming any stale marker), then assert every warm-model cell
            // re-gets with zero lower reads.
            replay.fail_puts.store(false, Ordering::Relaxed);
            replay.fail_deletes.store(0, Ordering::Relaxed);
            replay.fault_puts = false;
            replay.verify().await.map_err(|e| eyre!("heal pass: {e}"))?;
            for key in 0..POOL {
                if !replay.is_warm(key) {
                    continue;
                }
                ttl_lower.reset();
                let own = replay.reader();
                let _ = replay
                    .subject
                    .get(&replay.id, &cell_at(key), own)
                    .await
                    .map_err(|e| eyre!("budget get({key}): {e:?}"))?;
                if ttl_lower.lower_reads() != 0 {
                    return Err(eyre!(
                        "KV5 violated: warm cell {key} paid {} lower read(s)",
                        ttl_lower.lower_reads()
                    ));
                }
            }
            Ok(true)
        })
    }
    QuickCheck::new().quickcheck(property as fn(CacheTrace) -> Result<bool>);
}

// ---------------------------------------------------------------------------
// TTL co-expiry
// ---------------------------------------------------------------------------

/// One mutation in a co-expiry-anchor trace. The clock advances explicitly via
/// [`Advance`](TtlMut::Advance), so a stage→commit gap is part of the input
/// space rather than a single fixed scenario.
#[derive(Clone, Debug)]
enum TtlMut {
    /// `write_resolved` — re-stamps the row's TTL at the current clock.
    Set(u8, u8),
    /// `write_provisional` — stages `data`+`prev`, re-stamping at the clock.
    Stage(u8, u8),
    /// `commit_provisional` — promotes; the settle transform keeps the stage
    /// TTL.
    Commit(u8),
    /// `abort_provisional` — rolls back to `prev`, re-stamping at the clock.
    Abort(u8),
    /// Advances the test clock by the specified milliseconds.
    Advance(u16),
}

/// A random mutator trace with a per-trace collection TTL. Generated with a
/// clean per-key lifecycle (a key is idle or staged; `Commit`/`Abort` target
/// only staged keys, `Set`/`Stage` only idle ones), so any prefix is itself
/// valid — which is how `shrink` minimises.
#[derive(Clone, Debug)]
struct TtlMutTrace {
    ttl: Option<u32>,
    ops: Vec<TtlMut>,
}

/// The keys a co-expiry trace addresses.
const TTL_KEYS: u8 = 5;

impl Arbitrary for TtlMutTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let ttl = if bool::arbitrary(g) {
            None
        } else {
            Some(1 + u32::from(u8::arbitrary(g) % 8))
        };
        let len = usize::arbitrary(g) % 24;
        let mut staged: HashSet<u8> = HashSet::new();
        let mut ops = Vec::with_capacity(len);
        for _ in 0..len {
            // A quarter of ops advance the clock; the rest mutate one key.
            if u8::arbitrary(g) % 4 == 0 {
                // Arbitrary millisecond advances (0–~12 s) cover both sub-second
                // remainders and multi-second gaps relative to the 1–8 s TTL.
                ops.push(TtlMut::Advance(u16::arbitrary(g) % 12_000));
                continue;
            }
            let key = u8::arbitrary(g) % TTL_KEYS;
            let value = u8::arbitrary(g);
            if staged.remove(&key) {
                ops.push(if bool::arbitrary(g) {
                    TtlMut::Commit(key)
                } else {
                    TtlMut::Abort(key)
                });
            } else if bool::arbitrary(g) {
                ops.push(TtlMut::Set(key, value));
            } else {
                staged.insert(key);
                ops.push(TtlMut::Stage(key, value));
            }
        }
        Self { ttl, ops }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let ttl = self.ttl;
        let ops = self.ops.clone();
        // A prefix of a clean-lifecycle trace is itself clean, so truncation is a
        // safe shrink; also try dropping the TTL entirely.
        let prefixes = (0..ops.len()).map(move |n| Self {
            ttl,
            ops: ops[..n].to_vec(),
        });
        let drop_ttl = self
            .ttl
            .map(|_| Self {
                ttl: None,
                ops: self.ops.clone(),
            })
            .into_iter();
        Box::new(drop_ttl.chain(prefixes))
    }
}

/// **The co-expiry anchor (the generalising property).** For any TTL, any
/// interleaving of the four write paths, and any **sub-second** clock movement,
/// the expiry stamped on each cell's fjall entry must equal its durable row's
/// modeled death — `floor(write_clock) + ttl`, mirroring Cassandra's
/// whole-second TTL resolution: `Set`/`Stage`/`Abort` re-stamp at the floored
/// write clock, while `Commit` (the settle transform keeps the stage TTL) must
/// REUSE the stage-time stamp — never a fresh `commit + ttl`, which would let
/// the entry outlive the row. Because the clock advances by arbitrary
/// milliseconds, the model's floor discriminates the sub-second overhang across
/// the full 0–999 ms remainder. Asserted after **every** op (`0` = never).
#[test]
fn prop_cached_ttl_expiry_matches_durable_death() {
    fn property(trace: TtlMutTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(async move {
            const START: u64 = 1_000;
            let now = Arc::new(AtomicU64::new(START));
            let oracle = ScriptedOracle::default();
            let cells = MemoryCells::new();
            let lower =
                MemoryCellStore::new(cells, oracle, Arc::new(CollectionDefRegistry::default()));
            let cached = Cached::new(
                test_db::cache_with_clock("ttl-anchor", Clock::Fixed(now.clone()))?,
                lower,
            );
            let id = collection("ttl-anchor")?;
            let cref = CollectionRef::new(id.clone(), trace.ttl.map(CompactDuration::new));
            let ttl_ms = trace.ttl.map(|s| u64::from(s) * 1_000);
            // The durable death stamped by a write at `clock` (`0` = never).
            // Cassandra anchors at whole-second resolution, so the fjall stamp
            // floors `clock` DOWN to the second before adding the TTL — this is
            // what discriminates the sub-second overhang.
            let death_at = |clock: u64| ttl_ms.map_or(0, |ttl| (clock - clock % 1_000) + ttl);

            // Model: the death stamped on each present key's fjall entry.
            let mut death: HashMap<u8, u64> = HashMap::new();
            let mut committed: HashMap<u8, Committed> = HashMap::new();
            let mut staged: HashMap<u8, u8> = HashMap::new();
            let mut clock = START;
            let event = probe(1);
            let prev_of = |committed: &HashMap<u8, Committed>, key: u8| {
                committed
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| Committed::new(None))
            };

            for (index, op) in trace.ops.iter().enumerate() {
                match *op {
                    TtlMut::Set(key, value) => {
                        cached
                            .write_resolved(&cref, &[(cell_at(key), Some(bytes(value)))], &[])
                            .await?;
                        committed.insert(key, Committed::new(Some(bytes(value))));
                        death.insert(key, death_at(clock));
                    }
                    TtlMut::Stage(key, value) => {
                        let write = ProvisionalWrite::new(
                            Some(bytes(value)),
                            prev_of(&committed, key),
                            event,
                        );
                        let writes = [(cell_at(key), write)];
                        let marker = EventMarker::frozen(event, &writes, &[]);
                        cached
                            .write_provisional(&cref, &writes, Some(&marker))
                            .await?;
                        staged.insert(key, value);
                        death.insert(key, death_at(clock));
                    }
                    TtlMut::Commit(key) => {
                        let Some(value) = staged.remove(&key) else {
                            return Err(eyre!("op {index}: commit without a prior stage"));
                        };
                        let write = ProvisionalWrite::new(
                            Some(bytes(value)),
                            prev_of(&committed, key),
                            event,
                        );
                        cached
                            .commit_provisional(&cref, &[(cell_at(key), write)], &[])
                            .await?;
                        committed.insert(key, Committed::new(Some(bytes(value))));
                        // The settle transform reuses the stage stamp → death
                        // unchanged.
                    }
                    TtlMut::Abort(key) => {
                        let Some(value) = staged.remove(&key) else {
                            return Err(eyre!("op {index}: abort without a prior stage"));
                        };
                        let write = ProvisionalWrite::new(
                            Some(bytes(value)),
                            prev_of(&committed, key),
                            event,
                        );
                        cached
                            .abort_provisional(&cref, &[(cell_at(key), write)])
                            .await?;
                        death.insert(key, death_at(clock));
                    }
                    TtlMut::Advance(ms) => {
                        clock += u64::from(ms);
                        now.store(clock, Ordering::Relaxed);
                    }
                }

                // After every op: each touched cell's stamp equals its modeled
                // death; an untouched key has no entry.
                for key in 0..TTL_KEYS {
                    let got = cached.stored_expiry(&id, &cell_at(key)).await?;
                    let want = death.get(&key).copied();
                    if got != want {
                        return Err(eyre!(
                            "op {index} ({op:?}): key {key} fjall expiry {got:?} != modeled \
                             durable death {want:?}"
                        ));
                    }
                }
            }
            Ok(true)
        })
    }
    QuickCheck::new().quickcheck(property as fn(TtlMutTrace) -> Result<bool>);
}

// ─────────────────────────── batch reads (get_many) ────────────────────────

/// A memory lower store wrapped in the batch tests' read counter.
type CountingLower = CountingCellStore<MemoryCellStore<ScriptedOracle>>;

/// Builds a `Cached` over a [`CountingLower`] on the shared fjall database,
/// returning the cache handle, the counting handle, and the collection — the
/// batch tests' shared arrange.
fn counting_cached(name: &str) -> Result<(Cached<CountingLower>, CountingLower, CollectionId)> {
    let counting = CountingCellStore::new(MemoryCellStore::new(
        MemoryCells::new(),
        ScriptedOracle::default(),
        Arc::new(CollectionDefRegistry::default()),
    ));
    let cached = Cached::new(test_db::cache(name)?, counting.clone());
    let id = collection(name)?;
    Ok((cached, counting, id))
}

/// T-a all-hits: a `CELL_BATCH`-wide chunk whose every coordinate is warm
/// serves from fjall in one blocking hop with ZERO lower reads of any kind.
///
/// Dropping the `Ok(Some) => return` arm (always refetch) makes this test fail
/// this: `batch_cache_reads()` would climb to 1.
#[test]
fn batch_get_all_hits_reads_nothing() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let (cached, counting, id) = counting_cached("batch-all-hits")?;
        let cref = CollectionRef::new(id.clone(), None);
        // Warm 16 coordinates through the write-through path.
        let warm: Vec<(CellKey, Option<Bytes>)> =
            (0u8..16).map(|c| (cell_at(c), Some(bytes(c)))).collect();
        cached.write_resolved(&cref, &warm, &[]).await?;

        counting.reset();
        let out = cached
            .get_many(&id, SECTION, &batch_of(0u8..16)?, probe(1))
            .await?;
        assert_eq!(out.len(), 16, "every position answered");
        for c in 0u8..16 {
            assert_eq!(
                out[c as usize],
                Committed::new(Some(bytes(c))),
                "warm coordinate {c} serves its value from fjall"
            );
        }
        assert_eq!(counting.lower_reads(), 0, "all-hits reads no point durable");
        assert_eq!(
            counting.batch_reads(),
            0,
            "all-hits issues no lower get_many"
        );
        assert_eq!(
            counting.batch_cache_reads(),
            0,
            "all-hits issues no lower cache-fill batch"
        );
        Ok(())
    })
}

/// T-a any-miss: one cold coordinate in an otherwise-warm chunk forces EXACTLY
/// ONE lower cache-fill batch read (never the non-TTL `get_many`), and the
/// served values are durable truth.
///
/// Routing the miss arm to `lower.get_many` (dropping the fill's TTL) makes
/// this test fail this: `batch_reads()==1, batch_cache_reads()==0`.
#[test]
fn batch_get_any_miss_is_one_lower_batch_read() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let (cached, counting, id) = counting_cached("batch-any-miss")?;
        let cref = CollectionRef::new(id.clone(), None);
        // Warm coordinates 0..15; leave 15 cold (and durably absent).
        let warm: Vec<(CellKey, Option<Bytes>)> =
            (0u8..15).map(|c| (cell_at(c), Some(bytes(c)))).collect();
        cached.write_resolved(&cref, &warm, &[]).await?;

        counting.reset();
        let out = cached
            .get_many(&id, SECTION, &batch_of(0u8..16)?, probe(1))
            .await?;
        assert_eq!(out.len(), 16, "every position answered");
        for c in 0u8..15 {
            assert_eq!(
                out[c as usize],
                Committed::new(Some(bytes(c))),
                "coordinate {c} serves durable truth"
            );
        }
        assert_eq!(
            out[15],
            Committed::new(None),
            "the cold coordinate is absent"
        );
        assert_eq!(
            counting.batch_cache_reads(),
            1,
            "any-miss refetches the whole batch via one cache-fill read"
        );
        assert_eq!(
            counting.batch_reads(),
            0,
            "the miss arm never uses lower get_many"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "the fill loops the inner store, not this get"
        );
        Ok(())
    })
}

/// Proves that an accepted cache operation completes after cache disablement.
///
/// One disabled-state check controls the complete operation.
#[test]
fn batch_get_completes_after_cache_disablement() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let fjall = test_db::cache("batch-admitted")?;
        let cached = Cached::new(fjall.clone(), counting.clone());
        let id = collection("batch-admitted")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Stand a prior-event committed clears-only marker over the section (empty
        // survivors) so a fall-through read triggers the prior-clear cache guard
        // delete.
        let prior_event = probe(7);
        let clear = SectionClear::frozen_resolved(SECTION, &[]);
        let marker = EventMarker::frozen(prior_event, &[], slice::from_ref(&clear));
        cached.write_provisional(&cref, &[], Some(&marker)).await?;
        oracle.record_message(Uuid::from_u128(7)).await?;

        // Every must-succeed delete now fails: the prior-clear cache guard delete
        // exhausts its budget and disables the cache during the operation.
        fjall.fail_deletes().store(u64::MAX, Ordering::Relaxed);
        counting.reset();
        cached
            .get_many(&id, SECTION, &batch_of([0])?, probe(99))
            .await?;

        // The injected failure disabled the cache.
        assert!(
            fjall.is_disabled(),
            "the exhausted prior-clear cache guard delete must disable the cache during the \
             operation"
        );
        // Yet the admitted verb still published the fill to fjall.
        assert!(
            fjall.stored_expiry(&id, &cell_at(0)).await?.is_some(),
            "an admitted batch completes its publish after disablement"
        );
        Ok(())
    })
}

/// Proves that one batch miss reloads all values from durable storage.
///
/// A sampled cache hit can be stale after a prior section clear.
#[test]
fn batch_get_discards_sampled_hits_on_any_miss() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(test_db::cache("batch-discard")?, counting.clone());
        let id = collection("batch-discard")?;
        let cref = CollectionRef::new(id.clone(), None);

        // (1) A = V, warm + durable.
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(42)))], &[])
            .await?;
        // (2)+(3) A prior event's unsettled committed clears-only marker over the
        // section (A is not a survivor), leaving A warm but its durable truth
        // resolving to absent.
        let prior_event = probe(7);
        let clear = SectionClear::frozen_resolved(SECTION, &[]);
        let marker = EventMarker::frozen(prior_event, &[], slice::from_ref(&clear));
        cached.write_provisional(&cref, &[], Some(&marker)).await?;
        oracle.record_message(Uuid::from_u128(7)).await?;

        // Batch [A (Hit), B (Miss)]: the miss forces a refetch that discards the
        // sampled A=Some(42) and re-reads post-clear truth — A is absent.
        let out = cached
            .get_many(&id, SECTION, &batch_of([0, 1])?, probe(99))
            .await?;
        assert_eq!(out.len(), 2, "every position answered");
        assert_eq!(
            out[0].get(),
            None,
            "the sampled hit is discarded; A serves post-clear absence, never the stale Some(V)"
        );
        assert_eq!(out[1].get(), None, "B is absent");
        Ok(())
    })
}

/// Proves that a failed cache read does not remove an equal live entry.
#[test]
fn batch_get_failed_publish_keeps_hidden_live_entry_warm() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let fjall = test_db::cache("batch-hidden")?;
        let cached = Cached::new(fjall.clone(), counting.clone());
        let id = collection("batch-hidden")?;
        let cref = CollectionRef::new(id.clone(), None);

        // A = V, warm + durable.
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(9)))], &[])
            .await?;
        counting.reset();
        // Probe errors (over the live A entry) AND the publish fails.
        fjall.fail_reads().store(true, Ordering::Relaxed);
        fjall.fail_puts().store(true, Ordering::Relaxed);
        let out = cached
            .get_many(&id, SECTION, &batch_of([0])?, probe(1))
            .await?;
        assert_eq!(out.len(), 1, "the single position answered");
        assert_eq!(
            out[0].get(),
            Some(&bytes(9)),
            "the refetch resolved the durable value"
        );
        assert_eq!(
            counting.batch_cache_reads(),
            1,
            "the probe error fired: a refetch happened (an unfired fault would be an all-hit)"
        );

        // Heal both faults. The hidden live entry (V) survived untouched, so the
        // next point get is a warm hit with zero lower reads.
        fjall.fail_reads().store(false, Ordering::Relaxed);
        fjall.fail_puts().store(false, Ordering::Relaxed);
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(2)).await?.get(),
            Some(&bytes(9)),
            "the failed publish deleted nothing: A stayed warm"
        );
        assert_eq!(counting.lower_reads(), 0, "A was warm — no fall-through");
        Ok(())
    })
}

/// T-g expired-probe refetch: `get_batch` must classify a floor-expired entry
/// as not-a-hit so the batch refetches durable truth, never serving the stale
/// value. Pins `get_batch`'s own classification (distinct from
/// `expired_entry_reads_as_miss`, which tests the point `FjallCellCache::get`).
///
/// Classifying Expired as a hit makes this test fail: the batch would
/// serve the stale V1 instead of the fresh durable V2.
#[test]
fn batch_get_treats_expired_probe_as_refetch() -> Result<()> {
    const T0: u64 = 1_000;
    const NOW_EXPIRED: u64 = 7_000;

    TEST_RUNTIME.block_on(async {
        let now = Arc::new(AtomicU64::new(T0));
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(
            test_db::cache_with_clock("batch-expired", Clock::Fixed(now.clone()))?,
            counting.clone(),
        );
        let id = collection("batch-expired")?;
        // A 5-second TTL: the fjall entry is stamped floor(T0)+5s = 6_000.
        let cref = CollectionRef::new(id.clone(), Some(CompactDuration::new(5)));

        // Warm A = V1 through the cache; it now holds a stamped fjall entry.
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(1)))], &[])
            .await?;
        // Advance past the floor expiry and rewrite durable A = V2 WITHOUT the
        // cache, so fjall holds the stale (expired) V1 while durable holds V2.
        now.store(NOW_EXPIRED, Ordering::Relaxed);
        counting
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(2)))], &[])
            .await?;

        let out = cached
            .get_many(&id, SECTION, &batch_of([0])?, probe(2))
            .await?;
        assert_eq!(
            out[0].get(),
            Some(&bytes(2)),
            "the expired probe refetches durable truth (V2), never the stale V1"
        );
        Ok(())
    })
}

/// Expiry-boundary degrade test: a warm entry that expires WHILE a degraded
/// batch's delayed lower read is in flight, whose fill publish then FAILS
/// (no-delete degrade), must never be served on a later read — the surviving
/// stale entry re-classifies as expired and refetches durable truth again.
///
/// Parameterized over both refetch triggers the plan names:
/// * a sampled Hit discarded because a second COLD position misses the probe;
/// * an error-probe over the live entry (an injected fjall read fault).
///
/// The failed publish leaves the (now-expired) warm V1 on disk. Classifying
/// Expired as a hit, or a degrade path that re-stamps the surviving entry with
/// a fresh live expiry, would serve the stale V1 on the healed point read →
/// red. (Distinct from `batch_get_treats_expired_probe_as_refetch`, which
/// advances the clock before the call with a succeeding publish; here the entry
/// crosses its floor expiry DURING a parked read and the publish fails.)
#[test]
fn batch_get_expiry_boundary_degrade_never_serves_stale() -> Result<()> {
    /// One case: `coords` is the batch (target is coord 0); `error_probe` arms
    /// a fjall read fault so the whole probe errors instead of a cold miss.
    async fn degrade_case(name: &str, coords: &[u8], error_probe: bool) -> Result<()> {
        const T0: u64 = 1_000;
        const AFTER_EXPIRY: u64 = 7_000; // past floor(T0)+5s = 6_000
        const TTL_SECS: u32 = 5;

        let now = Arc::new(AtomicU64::new(T0));
        let clock = Clock::Fixed(now.clone());
        let lower = HoldingCellStore::new(CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            ScriptedOracle::default(),
            Arc::new(CollectionDefRegistry::default()),
        )));
        let holds = lower.holds();
        let fjall = test_db::cache_with_clock(name, clock)?;
        let cached = Cached::new(fjall.clone(), lower.clone());
        let id = collection(name)?;
        let cref = CollectionRef::new(id.clone(), Some(CompactDuration::new(TTL_SECS)));

        // Warm the target (coord 0) = V1 through the cache: fjall now holds a
        // live entry stamped floor(T0)+5s = 6_000; durable is also V1.
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(1)))], &[])
            .await?;
        // Rewrite durable truth to V2 through the lower store ONLY, so a refetch
        // resolves V2 while fjall still holds the stale warm V1.
        lower
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(2)))], &[])
            .await?;

        // The fill's publish fails (no-delete degrade); optionally the probe
        // errors instead of taking a cold-miss refetch.
        fjall.fail_puts().store(true, Ordering::Relaxed);
        if error_probe {
            fjall.fail_reads().store(true, Ordering::Relaxed);
        }

        // Park the refetch's fill after the target's durable read lands, advance
        // the clock past the entry's floor expiry while parked, then resume: the
        // warm entry expires DURING the delayed lower read.
        holds.get_for_cache().arm(1);
        let batch = batch_of(coords.iter().copied())?;
        let task = tokio::spawn({
            let cached = cached.clone();
            let id = id.clone();
            async move {
                cached
                    .get_many(&id, SECTION, &batch, probe(99))
                    .await
                    .map_err(|error| eyre!("{error:?}"))
            }
        });
        holds.get_for_cache().entered().await;
        now.store(AFTER_EXPIRY, Ordering::Relaxed);
        holds.get_for_cache().release();
        let out = task.await??;
        assert_eq!(
            out[0].get(),
            Some(&bytes(2)),
            "the degraded refetch serves durable V2, never the stale warm V1"
        );

        // Heal the faults, then assert before any further write that the failed
        // publish neither deleted nor re-stamped the surviving entry: it is
        // still V1's original floor stamp (6_000), independent of its payload.
        // A degrade that re-stamped it live, or deleted it, makes this test fail here.
        fjall.fail_puts().store(false, Ordering::Relaxed);
        fjall.fail_reads().store(false, Ordering::Relaxed);
        assert_eq!(
            fjall.stored_expiry(&id, &cell_at(0)).await?,
            Some(6_000),
            "the failed publish left the stale entry's original stamp untouched"
        );

        // The surviving (now-expired) V1 entry must not be served: the point
        // read re-classifies it Expired and refetches V2.
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(100)).await?.get(),
            Some(&bytes(2)),
            "the surviving expired entry is never served; the point read refetches V2"
        );
        Ok(())
    }

    TEST_RUNTIME.block_on(async {
        // (i) sampled Hit at coord 0 discarded because cold coord 1 misses.
        degrade_case("batch-degrade-hit", &[0, 1], false).await?;
        // (ii) an injected read fault errors the probe over the live entry.
        degrade_case("batch-degrade-err", &[0], true).await?;
        Ok(())
    })
}

/// T-h negative caching: an Absent entry is published only from a fully
/// successful batch. The positive arm proves absence IS cached (one durable
/// read for two reads); the erroring arm proves a batch that errors mid-fill
/// publishes NOTHING (A, read successfully before B errored, is not cached).
///
/// Filtering absent positions out of the publish makes this test fail the
/// positive arm (the second read misses). A mixed-merge that publishes
/// per-position as results arrive would red the erroring arm (A cached before B
/// errored).
#[test]
fn batch_get_publishes_absence_only_from_successful_batch() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        // ---- Positive arm: absence is cached from a successful batch. -------
        let (cached, counting, id) = counting_cached("batch-neg-ok")?;
        counting.reset();
        let out = cached
            .get_many(&id, SECTION, &batch_of([0])?, probe(1))
            .await?;
        assert_eq!(out[0].get(), None, "the never-written cell is absent");
        assert_eq!(
            counting.batch_cache_reads(),
            1,
            "the first batch paid one cache-fill read"
        );
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(2)).await?.get(),
            None,
            "the second read serves the cached Absent tag"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "absence was cached: the point get reads nothing"
        );

        // ---- Erroring arm: a mid-fill error publishes nothing. --------------
        let counting_b = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            ScriptedOracle::default(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let failing = FailingCellStore::failing_get_for_cache(
            counting_b.clone(),
            BTreeMap::from([(1u8, ErrorCategory::Transient)]),
        );
        let cached_b = Cached::new(test_db::cache("batch-neg-err")?, failing.clone());
        let id_b = collection("batch-neg-err")?;

        // A (coord 0) is absent; B (coord 1) is poisoned. The default fill loops
        // get_for_cache, reads A, then errors on B — so put_batch never runs.
        let err = cached_b
            .get_many(&id_b, SECTION, &batch_of([0, 1])?, probe(3))
            .await;
        assert!(
            err.is_err(),
            "a poisoned fill position fails the whole batch"
        );

        // Disarm and reset: if A had been cached by the errored batch its point
        // get would read nothing; it was NOT, so it refetches once.
        failing.set_poison(None);
        counting_b.reset();
        assert_eq!(
            cached_b.get(&id_b, &cell_at(0), probe(4)).await?.get(),
            None,
            "A resolves absent on the healed read"
        );
        assert_eq!(
            counting_b.lower_reads(),
            1,
            "the errored batch cached nothing: A is refetched once"
        );
        Ok(())
    })
}

/// T-c co-anchor property: for any within-second start offset and any delay ≥
/// 1s on the lower response, the expiry a batch fill stamps equals `floor(T0) +
/// remaining(T0)` and never overhangs the durable row death — even though the
/// clock advances while the fill's response is parked. The stamp is anchored
/// before the lower read, so a slow resolution can only stamp early.
///
/// Moving the anchor to after `get_many_for_cache` makes this test fail:
/// for a delay crossing a second, `floor(T0 + delay) + remaining > death`.
#[test]
fn prop_batch_fill_expiry_never_overhangs() {
    #[derive(Clone, Copy, Debug)]
    struct Timing {
        t0_offset: u16,
        delay_ms: u16,
    }

    impl Arbitrary for Timing {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                t0_offset: u16::arbitrary(g),
                // ≥ one full second so the delay can cross a floor boundary.
                delay_ms: 1_000 + (u16::arbitrary(g) % 11_000),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let base = *self;
            Box::new(
                self.t0_offset
                    .shrink()
                    .map(move |t0_offset| Timing { t0_offset, ..base }),
            )
        }
    }

    fn property(timing: Timing) -> Result<bool> {
        TEST_RUNTIME.block_on(async move {
            const START: u64 = 1_000;
            // A far, second-aligned death so `remaining` is always positive.
            const DEATH: u64 = 10_000_000;
            let t0 = START + u64::from(timing.t0_offset);
            let delay = u64::from(timing.delay_ms);
            let now = Arc::new(AtomicU64::new(t0));
            let clock = Clock::Fixed(now.clone());
            let lower = HoldingCellStore::new(TtlAwareCellStore::new(
                CountingCellStore::new(MemoryCellStore::new(
                    MemoryCells::new(),
                    ScriptedOracle::default(),
                    Arc::new(CollectionDefRegistry::default()),
                )),
                clock.clone(),
                DEATH,
            ));
            let holds = lower.holds();
            let cached = Cached::new(
                test_db::cache_with_clock("batch-anchor", clock)?,
                lower.clone(),
            );
            let id = collection("batch-anchor")?;
            let cref = CollectionRef::new(id.clone(), None);

            // A present, durable, and COLD (written through the lower store, so
            // the cache probes a miss and refetches).
            lower
                .write_resolved(&cref, &[(cell_at(7), Some(bytes(7)))], &[])
                .await?;

            // Spawn a singleton batch fill; park its lower response.
            holds.get_for_cache().arm(1);
            let task = tokio::spawn({
                let cached = cached.clone();
                let id = id.clone();
                async move {
                    let batch = batch_of([7])?;
                    cached
                        .get_many(&id, SECTION, &batch, probe(1))
                        .await
                        .map_err(|error| eyre!("{error:?}"))
                }
            });
            holds.get_for_cache().entered().await;
            // Advance the clock while the fill's response is parked, then resume.
            now.store(t0 + delay, Ordering::Relaxed);
            holds.get_for_cache().release();
            task.await??;

            // The remaining computed at T0, floored to Cassandra's second grain.
            let remaining_ms = ((DEATH - t0) / 1_000) * 1_000;
            let want = (t0 - t0 % 1_000) + remaining_ms;
            let got = cached.stored_expiry(&id, &cell_at(7)).await?;
            Ok(got == Some(want) && got.is_some_and(|e| e != 0 && e <= DEATH))
        })
    }

    QuickCheck::new().quickcheck(property as fn(Timing) -> Result<bool>);
}
