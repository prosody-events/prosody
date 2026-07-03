//! Memory-backed coverage-cache properties.
//!
//! The production [`Cached`] coverage path (stitched `scan_cells`,
//! covered-negative `get`, punch-on-write, `scan_present`) only assembles over
//! Cassandra in production, so the backend-generic flagship exercises it solely
//! through the live-cluster arm at the 25-iteration `INTEGRATION_TESTS` count.
//! These tests put the **real** `Cached` over a memory lower store and a real
//! tempdir-backed fjall cache, so the same coverage code runs at full
//! `QUICKCHECK_TESTS` with no cluster — and over **multiple collections sharing
//! one fjall partition**, which is what proves a covered scan never bleeds into
//! another collection or section.
//!
//! The crash/recovery and provisional-staging coverage paths are exercised over
//! the real `Cached` by the **Cassandra** crash-equivalence arm (which already
//! proves a covered serve never yields uncommitted `data`, and that a cold
//! restart drops coverage — `CovVolatile`); these memory tests focus on the
//! stitch, the op budget, and cross-collection isolation at full iteration
//! counts.

use super::super::cached::Cached;
use super::super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::super::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use super::super::fjall::Clock;
use super::super::fjall::test_db;
use super::super::memory::{MemoryCellStore, MemoryCells};
use super::super::oracle::CommitOracle;
use super::super::registry::CollectionDefRegistry;
use super::super::store::CellStore;
use super::super::{CollectionId, CollectionRef, EventRef, StateKey, StateName, StateType};
use super::cell_suite::{
    CountingCellStore, OverlayTrace, ScriptedOracle, Trace, bytes, run_crash_equivalence_trace,
    run_overlay_trace,
};
use crate::test_util::TEST_RUNTIME;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::{Stream, StreamExt};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use uuid::Uuid;

/// The single section the cell suites address (mirrors a Map's entry section).
const SECTION: Section = Section::new(0);

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

/// The cell at coordinate `c` in the shared section (single byte, so byte order
/// equals numeric order).
fn cell_at(c: u8) -> CellKey {
    CellKey {
        section: SECTION,
        coordinate: Coordinate::from_bytes(vec![c]),
    }
}

/// A fresh-segment Value collection identity for the named collection.
fn collection(name: &str) -> Result<CollectionId> {
    Ok(CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("k")),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

fn probe(n: u128) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(n),
    }
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

    fn lower_scans(&self) -> usize {
        self.inner.lower_scans()
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

    fn scan_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes, Option<CompactDuration>), Self::Error>> + Send + 'a
    {
        let remaining = self.remaining();
        self.inner
            .scan_for_cache(collection, scan, own)
            .map(move |item| item.map(|(cell, bytes, _)| (cell, bytes, remaining)))
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

    fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.write_provisional(collection, writes)
    }

    fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.write_resolved(collection, cells)
    }

    fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.mark_resolved(collection, cells)
    }
}

/// Unified-view soundness over `Overlay<Cached<MemoryCellStore>>`: the real
/// coverage stitch (warm-serve, point-punch, multi-gap fall-through, exclusive
/// bounds) must answer **identically** to the dirty-over-committed `BTreeMap`
/// oracle after every intermixed `get`/`scan`/`set`/`clear` — the
/// warmth-invariance differential, at full `QUICKCHECK_TESTS`.
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

/// Collects a forward scan over `[start, end]` (inclusive), mapping each cell
/// to its single coordinate byte.
async fn scan_forward<S>(store: &S, id: &CollectionId, start: u8, end: Bound<u8>) -> Result<Vec<u8>>
where
    S: CellStore,
{
    let start_c = Coordinate::from_bytes(vec![start]);
    let end_c = match end {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(b) => Bound::Included(Coordinate::from_bytes(vec![b])),
        Bound::Excluded(b) => Bound::Excluded(Coordinate::from_bytes(vec![b])),
    };
    let scan = Scan {
        section: SECTION,
        start: Bound::Included(&start_c),
        dir: Direction::Forward,
        end: end_c.as_ref(),
        limit: None,
    };
    let stream = store.scan_cells(id, scan, probe(99));
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let (key, _) = item?;
        out.push(key.coordinate.as_bytes()[0]);
    }
    Ok(out)
}

/// Coverage op budget under write-through: a covered re-scan issues zero lower
/// scans, a covered-negative `get` reads nothing, and — the write-through win —
/// a write inside a covered range publishes+re-covers, so a re-scan after the
/// write still issues **zero** lower reads (no read-after-write to the durable
/// store).
#[test]
fn coverage_op_budget() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            cells,
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(test_db::cache("budget")?, counting.clone());
        let id = collection("budget")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Seed present cells at 0, 2, 4, 6, 8 (resolved, committed). Each
        // write-through publishes+covers its point.
        for c in [0u8, 2, 4, 6, 8] {
            cached
                .write_resolved(&cref, &[(cell_at(c), Some(bytes(c)))])
                .await?;
        }

        // Warm the whole section with one unbounded scan (covering the gaps),
        // then verify a covered re-scan issues ZERO lower scans.
        let warm = scan_forward(&cached, &id, 0, Bound::Unbounded).await?;
        assert_eq!(warm, vec![0, 2, 4, 6, 8]);
        counting.reset();
        let covered = scan_forward(&cached, &id, 0, Bound::Unbounded).await?;
        assert_eq!(covered, vec![0, 2, 4, 6, 8]);
        assert_eq!(
            counting.lower_scans(),
            0,
            "a fully covered scan reads no gap"
        );

        // Covered-negative get: coordinate 3 is a gap inside the covered range →
        // genuine absence with zero lower reads.
        counting.reset();
        assert!(
            cached
                .get(&id, &cell_at(3), probe(1))
                .await?
                .get()
                .is_none(),
            "covered absent coordinate is absent"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "covered-negative get reads nothing"
        );

        // The write-through win: a single-cell write inside the covered range
        // publishes the new value into fjall and re-covers the coordinate, so a
        // re-scan serves entirely from the cache — ZERO lower reads after the
        // write (no read-after-write to the durable store).
        cached
            .write_resolved(&cref, &[(cell_at(4), Some(bytes(40)))])
            .await?;
        counting.reset();
        let after = scan_forward(&cached, &id, 0, Bound::Unbounded).await?;
        assert_eq!(after, vec![0, 2, 4, 6, 8]);
        assert_eq!(
            counting.lower_scans(),
            0,
            "a write-through write keeps the range covered, so the re-scan reads nothing"
        );

        // The re-scan must reflect the written value, served from fjall.
        counting.reset();
        assert_eq!(
            cached.get(&id, &cell_at(4), probe(2)).await?.get(),
            Some(&bytes(40)),
            "the covered get serves the written-through value"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "a covered get after a write-through write reads nothing"
        );

        // A scan over a genuinely-uncovered collection falls through exactly
        // once: a fresh collection was never scanned or written, so a bounded
        // scan issues one gap query, then a covered re-scan issues zero.
        let other = collection("budget-cold")?;
        counting.reset();
        let uncovered = scan_forward(&cached, &other, 0, Bound::Included(20)).await?;
        assert!(uncovered.is_empty());
        assert_eq!(
            counting.lower_scans(),
            1,
            "a never-covered range falls through once"
        );
        counting.reset();
        let _ = scan_forward(&cached, &other, 0, Bound::Included(20)).await?;
        assert_eq!(
            counting.lower_scans(),
            0,
            "the re-scan of the now-covered range reads nothing"
        );

        Ok(())
    })
}

/// Warm survival within an assignment: a `Cached` rebuilt over the **same**
/// fjall workspace (not a fresh epoch) is warm — its disk-backed provisional
/// index and coverage both survive. The rebuilt cache's recovery sweep answers
/// from the local fjall index with **zero** cold `provisional_cells` sweeps
/// (only bounded warm point reads), and a covered `get` serves with zero lower
/// reads. This is the in-assignment-warm proxy the crash case (a fresh epoch)
/// is the cold complement of.
#[test]
fn warm_index_and_coverage_survive_same_workspace_rebuild() -> Result<()> {
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

        // First cache instance: cover a value and leave a provisional cell.
        let cached = Cached::new(test_db::cache("warm")?, counting.clone());
        cached
            .write_resolved(&cref, &[(cell_at(9), Some(bytes(9)))])
            .await?;
        let prev = cached.get(&id, &cell_at(3), event).await?;
        let write = ProvisionalWrite::new(Some(bytes(5)), prev, event);
        cached
            .write_provisional(&cref, &[(cell_at(3), write)])
            .await?;

        // Prime the warm provisional index: the first sweep is a cold seed (one
        // `provisional_cells` call), which records the coordinate into fjall and
        // marks the collection seeded.
        counting.reset();
        let cold = drain_provisional(&cached, &id).await?;
        assert_eq!(cold, vec![3], "the cold sweep finds the provisional cell");
        assert_eq!(
            counting.recovery_sweeps(),
            1,
            "the first sweep is a cold seed"
        );

        // Rebuild `Cached` over the SAME workspace (a session clone within one
        // assignment). The disk-backed warm index + coverage survive.
        let restarted = Cached::new(test_db::cache("warm")?, counting.clone());
        counting.reset();

        // The rebuilt sweep is WARM: zero cold `provisional_cells` sweeps, and it
        // still finds the provisional cell via bounded warm point reads.
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
            1,
            "the warm sweep point-reads exactly the one provisional coordinate"
        );

        // Coverage also survives: the covered `9` serves from fjall with no
        // lower read.
        counting.reset();
        assert_eq!(
            restarted.get(&id, &cell_at(9), probe(2)).await?.get(),
            Some(&bytes(9)),
            "the covered value survives the rebuild and serves from fjall"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "a covered get after a same-workspace rebuild reads nothing"
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
        cached
            .write_provisional(
                &cref,
                &[(
                    cell_at(3),
                    ProvisionalWrite::new(Some(bytes(5)), prev, event),
                )],
            )
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
/// from every later warm sweep and strand it (F4). Symmetric with
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
        cached
            .write_provisional(
                &cref,
                &[(
                    cell_at(3),
                    ProvisionalWrite::new(Some(bytes(5)), prev, event),
                )],
            )
            .await?;

        // Cold seed with a failing `index_record`: the sweep still finds the
        // cell (from the durable lower), but the failed record must NOT be
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

/// Drains a `provisional_cells` sweep into the ascending list of covered
/// coordinate first-bytes it yields.
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

/// A covered scan never bleeds into another collection or section sharing the
/// fjall partition: with two collections and a decoy section seeded in one
/// cache, an unbounded scan of each section yields only its own cells.
#[test]
fn coverage_scan_isolation() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let cached = cached_over(&cells, &oracle, "isolation")?;

        let a = collection("alpha")?;
        let b = collection("beta")?;
        let a_ref = CollectionRef::new(a.clone(), None);
        let b_ref = CollectionRef::new(b.clone(), None);

        // Collection A's entry section, collection B's entry section, and a
        // decoy section in A all share the one fjall partition.
        let decoy = CellKey {
            section: Section::new(1),
            coordinate: Coordinate::from_bytes(vec![5]),
        };
        for c in [10u8, 20, 30] {
            cached
                .write_resolved(&a_ref, &[(cell_at(c), Some(bytes(c)))])
                .await?;
        }
        for c in [40u8, 50] {
            cached
                .write_resolved(&b_ref, &[(cell_at(c), Some(bytes(c)))])
                .await?;
        }
        cached
            .write_resolved(&a_ref, &[(decoy.clone(), Some(bytes(5)))])
            .await?;

        // An unbounded scan of A's entry section must yield only A's entries —
        // not B's, and not the decoy section. Run twice so both the cold gap
        // fall-through and the warm covered serve are checked.
        for _ in 0..2u32 {
            assert_eq!(
                scan_forward(&cached, &a, 0, Bound::Unbounded).await?,
                vec![10, 20, 30],
                "scan of A's section must not bleed into B or the decoy section"
            );
            assert_eq!(
                scan_forward(&cached, &b, 0, Bound::Unbounded).await?,
                vec![40, 50]
            );
        }
        Ok(())
    })
}

/// Coop-budget smoke: a fully covered scan over more than the ~128-item coop
/// threshold drives to completion, guarding the `cooperative` wrap on the
/// covered (fjall) serve.
#[test]
fn coverage_covered_scan_coop_over_threshold() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        const N: u8 = 200;
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let cached = cached_over(&cells, &oracle, "wide")?;
        let id = collection("wide")?;
        let cref = CollectionRef::new(id.clone(), None);

        for c in 0..N {
            cached
                .write_resolved(&cref, &[(cell_at(c), Some(bytes(c)))])
                .await?;
        }
        // Warm coverage, then a fully covered re-scan from fjall must yield all
        // N cells.
        let warm = scan_forward(&cached, &id, 0, Bound::Unbounded).await?;
        assert_eq!(warm.len(), usize::from(N));
        let covered = scan_forward(&cached, &id, 0, Bound::Unbounded).await?;
        assert_eq!(covered.len(), usize::from(N));
        assert!(covered.iter().copied().eq(0..N));
        Ok(())
    })
}

/// Crash-recovery equivalence over the **real** `Cached<MemoryCellStore>`: each
/// resolution arm drives `commit_provisional`/`abort_provisional` (the
/// publish-on-settle path), and a "crash" rebuilds the cache cold over the same
/// warm memory cells (a fresh fjall partition — `CovVolatile`). The committed
/// projection must converge to the model on every path, with the write-through
/// publish and the dropped-coverage cold restart both exercised.
#[test]
fn prop_memory_cached_crash_equivalence() {
    fn property(trace: Trace) -> Result<bool> {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        // Each `make` yields a cold cache over the same warm memory cells +
        // oracle, so a crash drops coverage but not durable state.
        // `test_db::cold_cache` reuses the `crash` keyspace pair on the shared
        // database and CLEARS it (a cheap journal marker, no fsync) — modeling a
        // fresh epoch without a ~128 ms keyspace creation. Measured ~3× faster
        // than a fresh database/keyspace per make (682 s → 222 s at 256
        // iterations). Distinct v4 segments per iteration keep the shared
        // keyspace's crashes disjoint.
        let make = || {
            let lower = MemoryCellStore::new(
                cells.clone(),
                oracle.clone(),
                Arc::new(CollectionDefRegistry::default()),
            );
            Ok(Cached::new(test_db::cold_cache("crash")?, lower))
        };
        TEST_RUNTIME.block_on(run_crash_equivalence_trace(make, oracle.clone(), trace))
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> Result<bool>);
}

/// TTL co-expiry over the real `Cached` with a pinned [`Clock`]: a value
/// written through a short-TTL collection is served from fjall while live, then
/// — after the clock advances **past its floor expiry to a sub-second instant**
/// — a covered `get` **falls through** to the lower store, yields its current
/// answer, and **re-stamps the fjall entry to `floor(now) + remaining`**
/// (Cov1). The lower store reports a live `TTL(data)`-style remaining
/// ([`TtlAwareCellStore`]), so the floored re-stamp is exercised and asserted
/// `≤` the row's death. No sleep; the clock is advanced directly.
#[test]
fn ttl_co_expiry_covered_read_falls_through() -> Result<()> {
    use std::sync::atomic::AtomicU64;

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

        // Write through coordinate 7; it is now covered and served from fjall.
        cached
            .write_resolved(&cref, &[(cell_at(7), Some(bytes(7)))])
            .await?;
        lower.reset();
        assert_eq!(
            cached.get(&id, &cell_at(7), probe(1)).await?.get(),
            Some(&bytes(7)),
            "a live covered value serves from fjall"
        );
        assert_eq!(lower.lower_reads(), 0, "a live covered get reads nothing");

        // Advance the clock past the floor expiry (1_000 + 5_000 = 6_000ms).
        now.store(NOW_EXPIRED, Ordering::Relaxed);
        // Rewrite the durable value WITHOUT going through the cache, so fjall
        // still holds the stale (now-expired) `7` while the lower store holds
        // `70`. The expired covered get must fall through and yield `70`.
        lower
            .write_resolved(&cref, &[(cell_at(7), Some(bytes(70)))])
            .await?;
        lower.reset();
        assert_eq!(
            cached.get(&id, &cell_at(7), probe(2)).await?.get(),
            Some(&bytes(70)),
            "an expired covered get falls through to the fresh durable value"
        );
        assert!(
            lower.lower_reads() > 0,
            "the expired covered get must read the lower store"
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
            "Cov1: the re-stamped expiry must not overhang the durable row death"
        );

        // The fall-through re-published a fresh entry; a covered get now serves
        // `70` from fjall again.
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

/// `CovVolatile`: a cold restart (a fresh assignment epoch) trusts nothing
/// uncovered, so the next read falls through to the lower store. Coverage now
/// spills to the per-partition fjall `index` keyspace, so a cold restart is a
/// **fresh epoch** — a brand-new cache + index keyspace pair (the `"restart"`
/// name), which is empty. Reusing the *same* workspace would be legitimately
/// warm by design (the on-disk coverage survives within one assignment); the
/// crash/rebalance case that must trust nothing is exactly the fresh epoch.
#[test]
fn cov_volatile_cold_restart_falls_through() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let lower = MemoryCellStore::new(
            cells.clone(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        );
        let id = collection("volatile")?;
        let cref = CollectionRef::new(id.clone(), None);

        let cached = Cached::new(test_db::cache("volatile")?, lower.clone());
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(1)))])
            .await?;
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(1)).await?.get(),
            Some(&bytes(1))
        );

        // Change the durable value out-of-band, leaving the stale fjall `1`.
        lower
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(99)))])
            .await?;

        // Cold restart = a fresh epoch: a new cache + index keyspace pair, so
        // coverage is empty. The stale value from the old epoch's cache keyspace
        // is gone with it; the uncovered `get` falls through and self-heals to
        // `99`.
        let restarted = Cached::new(test_db::cache("restart")?, lower.clone());
        assert_eq!(
            restarted.get(&id, &cell_at(1), probe(2)).await?.get(),
            Some(&bytes(99)),
            "a cold restart trusts nothing uncovered and re-reads the durable truth"
        );
        Ok(())
    })
}

/// The MAJOR audit-hole regression: a committed change whose fjall publish
/// **fails** must leave the coordinate **uncovered**, so the next read falls
/// through and self-heals — never serving the stale fjall entry forever. Uses
/// the [`FjallCellCache`] put-fault seam to force the publish failure.
#[test]
fn failed_publish_uncovers_and_self_heals() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let fjall = test_db::cache("fault")?;
        let fail = fjall.fail_puts();
        let lower = MemoryCellStore::new(cells, oracle, Arc::new(CollectionDefRegistry::default()));
        let cached = Cached::new(fjall, lower);
        let id = collection("fault")?;
        let cref = CollectionRef::new(id.clone(), None);

        // First write publishes cleanly and covers `1`.
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(1)))])
            .await?;
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(1)).await?.get(),
            Some(&bytes(1))
        );

        // Now force every publish to fail. The lower write still succeeds
        // (durable truth is `2`), but the fjall publish fails → the coordinate
        // is punched out of coverage.
        fail.store(true, Ordering::Relaxed);
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(2)))])
            .await?;

        // Heal the cache fault; the next get is now uncovered, so it falls
        // through to the durable `2` and re-publishes — never serving the stale
        // fjall `1`.
        fail.store(false, Ordering::Relaxed);
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(2)).await?.get(),
            Some(&bytes(2)),
            "a failed publish uncovers the coordinate, so the next read self-heals"
        );
        Ok(())
    })
}

/// Atomic-batch guard: a *multi-cell* write-through whose fjall batch commit
/// **fails** must leave **every** coordinate in the batch uncovered — not just
/// some — so each one falls through and self-heals. The batch is atomic, so a
/// commit failure lands nothing; the single-cell `failed_publish_uncovers...`
/// regression cannot observe this all-or-nothing uncovering.
#[test]
fn failed_batch_publish_uncovers_all_cells() -> Result<()> {
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

        // One multi-cell write-through publishes cleanly and covers both cells.
        cached.write_resolved(&cref, &seed).await?;
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
        // (durable truth is `11`/`22`), but the atomic batch lands nothing → all
        // coordinates in the batch are punched out of coverage.
        fail.store(true, Ordering::Relaxed);
        cached.write_resolved(&cref, &update).await?;

        // Heal the fault; both gets are now uncovered, so each falls through to
        // its fresh durable value — never serving the stale batch.
        fail.store(false, Ordering::Relaxed);
        for (c, v) in [(1u8, 11u8), (2, 22)] {
            assert_eq!(
                cached
                    .get(&id, &cell_at(c), probe(u128::from(c) + 100))
                    .await?
                    .get(),
                Some(&bytes(v)),
                "a failed batch uncovers every coordinate, so each read self-heals"
            );
        }
        Ok(())
    })
}

/// The Incomplete trap: a fjall publish failure inside `commit_provisional`
/// must NOT turn the settle into an error — the lower promote succeeded, so the
/// committed value is correct and the method returns `Ok` verbatim (otherwise a
/// healthy store would arm `StateRecovery` forever). Stages a provisional cell,
/// then commits it with the publish forced to fail.
#[test]
fn commit_provisional_swallows_fjall_publish_failure() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let fjall = test_db::cache("incomplete")?;
        let fail = fjall.fail_puts();
        let lower = MemoryCellStore::new(
            cells,
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        );
        let cached = Cached::new(fjall, lower);
        let id = collection("incomplete")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = probe(1);

        // Stage a provisional cell (lower write succeeds; the publish of `prev`
        // may fail or not — irrelevant to this test).
        let prev = cached.get(&id, &cell_at(0), event).await?;
        let write = ProvisionalWrite::new(Some(bytes(5)), prev, event);
        cached
            .write_provisional(&cref, &[(cell_at(0), write.clone())])
            .await?;
        // `probe(1)` is a message event with dedup id `1`; record it committed
        // so the promote arm resolves to `data`.
        oracle.record_message(Uuid::from_u128(1)).await?;

        // Commit with the publish forced to fail: the result MUST be Ok (the
        // lower promote succeeded), never folded into an error.
        fail.store(true, Ordering::Relaxed);
        let result = cached
            .commit_provisional(&cref, &[(cell_at(0), write)])
            .await;
        assert!(
            result.is_ok(),
            "commit_provisional must swallow a fjall publish failure and return the lower Ok"
        );

        // The committed value is correct regardless of the cache failure: heal
        // the cache and read `5`.
        fail.store(false, Ordering::Relaxed);
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(2)).await?.get(),
            Some(&bytes(5)),
            "the promoted value is durable despite the publish failure"
        );
        Ok(())
    })
}

/// One mutation in a co-expiry-anchor trace. The clock advances explicitly via
/// [`Advance`](TtlMut::Advance), so a stage→commit gap is part of the input
/// space rather than a single fixed scenario.
#[derive(Clone, Debug)]
enum TtlMut {
    /// `write_resolved` — re-stamps the row's TTL at the current clock.
    Set(u8, u8),
    /// `write_provisional` — stages `data`+`prev`, re-stamping at the clock.
    Stage(u8, u8),
    /// `commit_provisional` — promotes; `mark_resolved` keeps the stage TTL.
    Commit(u8),
    /// `abort_provisional` — rolls back to `prev`, re-stamping at the clock.
    Abort(u8),
    /// Advance the pinned clock by N **milliseconds** — deliberately sub-second
    /// so the floor in `expiry_at` is exercised across the full 0–999 ms
    /// remainder, not just second-aligned instants.
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

/// **Cov1 co-expiry anchor (the generalising property).** For any TTL, any
/// interleaving of the four write paths, and any **sub-second** clock movement,
/// the expiry stamped on each cell's fjall entry must equal its durable row's
/// modeled death — `floor(write_clock) + ttl`, mirroring Cassandra's
/// whole-second TTL resolution: `Set`/`Stage`/`Abort` re-stamp at the floored
/// write clock, while `Commit` (`mark_resolved` keeps the stage TTL) must REUSE
/// the stage-time stamp — never a fresh `commit + ttl`, which would let the
/// entry outlive the row. Because the clock advances by arbitrary milliseconds,
/// the model's floor discriminates the sub-second overhang across the full
/// 0–999 ms remainder. Asserted after **every** op (`0` = never). Subsumes the
/// commit-overhang example: that is one shrunk trace of this property.
#[test]
fn prop_cached_ttl_expiry_matches_durable_death() {
    fn property(trace: TtlMutTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(async move {
            use std::sync::atomic::AtomicU64;

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
                            .write_resolved(&cref, &[(cell_at(key), Some(bytes(value)))])
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
                        cached
                            .write_provisional(&cref, &[(cell_at(key), write)])
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
                            .commit_provisional(&cref, &[(cell_at(key), write)])
                            .await?;
                        committed.insert(key, Committed::new(Some(bytes(value))));
                        // `mark_resolved` keeps the stage TTL → death
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

/// The covered-SCAN expired-refill path (the plan's critical adversarial
/// finding, previously unproven): under FLOOR rounding a fjall entry expires
/// slightly before its durable row, so a covered scan that meets an expired
/// entry must REFILL that sub-range from the lower store — never read the
/// expired coordinate as absent. Warms a covered scan, advances the clock past
/// the floor expiry to a **sub-second** instant, asserts the re-scan still
/// yields every coordinate and falls through, and asserts each refilled cell is
/// re-stamped to `floor(now) + remaining` (`≤` the row death; Cov1) — the lower
/// store reports a live `TTL(data)`-style remaining ([`TtlAwareCellStore`]).
#[test]
fn ttl_co_expiry_covered_scan_refills() -> Result<()> {
    use std::sync::atomic::AtomicU64;

    // A sub-second instant past the floor expiry (6_000), so the refill's floor
    // sheds the 500 ms remainder; the lower rows die at 30_000 (`TTL(data)`).
    const NOW_EXPIRED: u64 = 7_500;
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
            test_db::cache_with_clock("scan-ttl", Clock::Fixed(now.clone()))?,
            lower.clone(),
        );
        let id = collection("scan-ttl")?;
        let cref = CollectionRef::new(id.clone(), Some(CompactDuration::new(5)));

        // Write through 3, 5, 7 (each covered, expiry 1_000 + 5_000 = 6_000), then
        // warm the whole section with one scan so the gaps cover too.
        for c in [3u8, 5, 7] {
            cached
                .write_resolved(&cref, &[(cell_at(c), Some(bytes(c)))])
                .await?;
        }
        let warm = scan_forward(&cached, &id, 0, Bound::Unbounded).await?;
        assert_eq!(warm, vec![3, 5, 7]);
        lower.reset();
        let covered = scan_forward(&cached, &id, 0, Bound::Unbounded).await?;
        assert_eq!(covered, vec![3, 5, 7]);
        assert_eq!(lower.lower_scans(), 0, "a live covered scan reads nothing");

        // Advance past the floor expiry. The covered scan must fall through for
        // the expired cells and still yield all three — never reading an expired
        // coordinate as absent (Cov1).
        now.store(NOW_EXPIRED, Ordering::Relaxed);
        lower.reset();
        let after = scan_forward(&cached, &id, 0, Bound::Unbounded).await?;
        assert_eq!(
            after,
            vec![3, 5, 7],
            "an expired covered scan refills from the lower store, never dropping cells"
        );
        assert!(
            lower.lower_scans() > 0,
            "the expired covered scan must fall through to the lower store"
        );

        // Each refilled cell is re-stamped to `floor(now) + remaining`, flooring
        // the sub-second remainder so it never overhangs the row death.
        let remaining_ms = ((ROW_DEATH - NOW_EXPIRED) / 1_000) * 1_000;
        let want_expiry = NOW_EXPIRED - NOW_EXPIRED % 1_000 + remaining_ms;
        for c in [3u8, 5, 7] {
            let stamped = cached.stored_expiry(&id, &cell_at(c)).await?;
            assert_eq!(
                stamped,
                Some(want_expiry),
                "cell {c} must be re-stamped to floor(now) + remaining"
            );
            assert!(
                stamped.is_some_and(|e| e <= ROW_DEATH),
                "Cov1: cell {c}'s re-stamped expiry must not overhang the row death"
            );
        }
        Ok(())
    })
}
