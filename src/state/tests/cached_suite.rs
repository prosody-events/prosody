//! Memory-backed properties and pins for the write-through K/V cache.
//!
//! The production [`Cached`] path only assembles over Cassandra in production,
//! so the backend-generic flagship exercises it solely through the
//! live-cluster arm at the 25-iteration `INTEGRATION_TESTS` count. These tests
//! put the **real** `Cached` over a memory lower store and a real fjall cache
//! (the shared test database), so the same cache code runs at full
//! `QUICKCHECK_TESTS` with no cluster.
//!
//! The flagship is [`prop_cached_is_transparent`]: a differential/parity
//! property driving one generated cell-op trace through `Cached` over a memory
//! store and through a bare memory twin, asserting every `get` and a
//! full-section scan answer identically after **every** op — with bounded
//! fjall fault injection inside the property — plus a second arm pinning the
//! KV5 budget (a warm cell's re-get issues zero lower reads). The example pins
//! around it reach the seams the sequential property cannot: the D5 settle
//! transform's windows, the cache fuse, the fill budgets, and negative
//! caching.

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
use super::super::store::CellStore;
use super::super::{CollectionId, CollectionRef, EventRef};
use super::cell_suite::{
    FailingCellStore, MemoryShapeProbe, OverlayTrace, Poison, PoisonHandle, SECTION,
    ScriptedOracle, Trace, bytes, cell_at, run_crash_equivalence_trace, run_overlay_trace,
};
use super::support::{CountingCellStore, HoldingCellStore, fresh_collection as collection, probe};
use crate::error::ErrorCategory;
use crate::test_util::TEST_RUNTIME;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::{Stream, StreamExt};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::collections::{HashMap, HashSet};
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

    fn standing_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Future<Output = Result<Option<EventMarker>, Self::Error>> + Send + 'a {
        self.inner.standing_marker(collection)
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
/// **zero** cold `provisional_cells` sweeps (only bounded warm point reads),
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

/// TTL co-expiry over the real `Cached` with a pinned [`Clock`]: a value
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
/// abstraction, so the fall-through pin cannot be generalized into the
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

/// D1 (single-cell): a committed change whose fjall publish **fails** must
/// **delete** the stale entry, so the next read falls through and self-heals —
/// never serving the stale fjall entry forever — and its fill re-warms the
/// cell. Uses the put-fault seam to force the publish failure; the entry-level
/// asserts ride the lower-read budget (a value-only assert would be masked by
/// the self-healing fall-through).
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
        // entry is deleted (D1).
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

/// D1 (batch): a *multi-cell* write-through whose fjall batch commit **fails**
/// must delete **every** coordinate in the batch — not just some — so each one
/// falls through and self-heals. The batch is atomic, so a commit failure
/// lands nothing; the single-cell regression cannot observe the all-or-nothing
/// repair.
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
        // every coordinate in the batch is deleted (D1).
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

/// D2 lands-or-fuses: `mark_resolved` (the recovery sweep's raw promote)
/// cannot re-publish `data` from keys alone, so it deletes the entries — and a
/// transiently-failing delete must **retry within its budget and land**, never
/// be swallowed. Swallowed, the pre-promote `prev` stays warm and a hit serves
/// it verbatim (no read-side mismatch detection) for the rest of the
/// assignment. Injects delete failures within the budget and asserts the
/// promoted value is served after the promote — and the fuse stays intact.
#[test]
fn promote_delete_lands_or_fuses() -> Result<()> {
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

        // The raw promote with the next D2 deletes failing within the budget:
        // the delete must retry until it lands, dropping the stale warm `1`.
        fail_deletes.store(u64::try_from(DELETE_RETRY_BUDGET - 1)?, Ordering::Relaxed);
        cached.mark_resolved(&cref, &[cell_at(0)]).await?;
        assert_eq!(
            fail_deletes.load(Ordering::Relaxed),
            0,
            "the injected delete failures must have fired"
        );
        assert!(
            !fjall.fuse_blown(),
            "an in-budget delete never blows the fuse"
        );
        assert_eq!(
            cached.get(&id, &cell_at(0), probe(2)).await?.get(),
            Some(&bytes(5)),
            "a failed delete must not leave the stale pre-promote value warm"
        );
        Ok(())
    })
}

/// D1's retry: a write-through whose fjall publish fails deletes the written
/// cells, and if that delete also transiently fails it must retry within its
/// budget and land — a double fault must not freeze the pre-write value in the
/// cache, and an in-budget recovery must NOT blow the fuse (the over-budget
/// regime belongs to the cache-fuse pin).
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
            !fjall.fuse_blown(),
            "a within-budget delete recovery never blows the fuse"
        );
        assert_eq!(
            cached.get(&id, &cell_at(1), probe(1)).await?.get(),
            Some(&bytes(2)),
            "a doubly-failed publish+delete must still evict, so the read self-heals"
        );
        Ok(())
    })
}

/// Establish-then-publish over a faulted lower store: a failed lower
/// `write_resolved` leaves the cache untouched, still serving the pre-write
/// value with zero lower reads and zero phantom publishes — and when the
/// failed write carries a section clear, the delete-first D4 degrades to a
/// correct slow read, never a wrong one.
///
/// Example test by necessity: the crash/overlay properties observe values,
/// not the serving layer — "which layer answered and how many lower reads it
/// cost" is physical grain below the model's abstraction, so the counter pin
/// cannot be generalized into the generator.
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

        // A clears-free lower write fault: the write must surface Err and
        // leave the cache untouched.
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
            0,
            "cache untouched: still a warm fjall serve (a phantom publish would serve the new \
             value; a spurious delete would cost a lower read)"
        );

        // A clears-bearing lower write fault: delete-first (D4) is the
        // contract, so the section delete DID run before the lower rejection —
        // the follow-up get serves the pre-write value via fall-through
        // (exactly one lower read: merely cold, never wrong), then re-warms.
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

/// D4 precision: `delete_section` removes exactly the cleared section's
/// entries — a sibling section's and another collection's entries stay warm
/// (the cross-collection isolation duty), while the cleared section's
/// non-written entries are gone (cold, one fall-through each).
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

/// Stages a 3-cell marker (`data` = 100+c over committed base `c`) through
/// `cached` and records its commit verdict — the shared prologue of the D5
/// pins. Returns the staged writes and the frozen marker.
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
    // The verdict is fixed BEFORE commit_provisional ever runs (the settle
    // boundary records the marker first) — the D5 precondition.
    oracle.record_message(Uuid::from_u128(dedup)).await?;
    Ok(writes)
}

/// The D5 transform pin: the settle transform installs the oracle-committed
/// `data` **before** the lower promote, so a multi-cell marker's staged cells
/// read back WARM with the committed value — never the staged `prev` — through
/// both failure windows: (a) a lower promote that fails (`Incomplete`), and
/// (b) a settle future **dropped** after the lower batch landed. The warm
/// asserts ride the lower-read budget (the observable a self-healing
/// fall-through cannot mask). The cold arm then force-deletes one cell's entry
/// and asserts the siblings still never serve `prev` (the marker-grain lemma).
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

/// The D5 failure companion (absorbing the former Incomplete-trap example): a
/// failed transform batch degrades to the must-succeed delete — the staged
/// cells go COLD (next get = one durable read, correct data), never stale —
/// and the **lower result is returned verbatim** in both directions: a
/// poisoned lower promote's error surfaces (never a folded fjall error), and a
/// healthy lower promote returns `Ok` despite the fjall failure. A get after
/// the fallback is a cold miss that republishes the committed value (the
/// negative-caching trio's third arm — never a ghost of the deleted entry).
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

/// D5 idempotence: a sweep-retried transform (a second `commit_provisional` of
/// the same writes — the lower verb is idempotent) re-reads the stage-anchored
/// expiry it wrote the first time and rewrites byte-equivalent bytes: the
/// entries stay warm with the SAME expiry and value.
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
/// committed by a FOREIGN event beneath the cache resolves PRESENT on the
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

        // Stage BENEATH the cache (the crash/foreign shape): the cache never
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
            "the fill resolves the committed foreign provisional to present"
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
/// FOREIGN event that never committed resolves ABSENT on the fall-through
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
            "the fill resolves the aborted foreign provisional to its absent prev"
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

/// The recovery sweep never calls `scan_cells` — it rides `standing_marker`,
/// the warm index, and `provisional_cell_at` point reads — so "scans are
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
            "the sweep issues no scan_cells — recovery rides point reads only"
        );
        assert!(
            counting.marker_reads() >= 1,
            "the sweep rode the standing-marker leg"
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

/// The cache-fuse pin, over TWO `Cached` clones of ONE workspace — the only
/// shape that can red-prove assignment-wide propagation. Clone A's D1 delete
/// fails past the retry budget and blows the fuse; the verb still RETURNS
/// (settlement never stalls). Clone B then: never observes the undeleted
/// stale entry (every get is a durable read), its publishes no-op (a repeat
/// get is still durable), and the fuse stays blown. The recovery arm proves
/// `provisional_cells` yields durable truth under BOTH warm-set staleness
/// causes: a pre-blow `index_record_batch` failure whose unseed exhausted its
/// budget (blowing the fuse over a stale seeded latch), and a coordinate
/// staged after the blow (its index record no-oped).
#[test]
fn cache_fuse_partitions_the_workspace() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let fjall = test_db::cache("fuse")?;
        let fail_puts = fjall.fail_puts();
        let fail_deletes = fjall.fail_deletes();
        let fail_index_record = fjall.fail_index_record();
        let cached_a = Cached::new(fjall.clone(), counting.clone());
        let cached_b = cached_a.clone();
        let id = collection("fuse")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = probe(1);

        // Warm a stale-candidate entry and seed the warm index (a cold sweep
        // over an early stage) so the pre-blow latch is genuinely seeded.
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
        assert_eq!(seeded, vec![0], "the pre-blow cold seed ran");

        // Blow the fuse through the pre-blow staleness cause: a stage whose
        // index record fails AND whose must-succeed unseed exhausts its budget.
        // The stage boundary sees A's own standing marker (same event), so no
        // boundary deletes fire; the record failure routes to the unseed.
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
            fjall.fuse_blown(),
            "the exhausted unseed blew the fuse; settlement completed regardless"
        );

        // Clone B: every get is a durable read — the stale warm `1` (whose D1
        // delete never landed) is unreachable.
        counting.reset();
        assert_eq!(
            cached_b.get(&id, &cell_at(1), event).await?.get(),
            Some(&bytes(1)),
            "B reads the committed base through the oracle, never the stale entry"
        );
        assert!(counting.lower_reads() >= 1, "B's get is a durable read");
        // B's publishes no-op: the repeat get is still durable.
        counting.reset();
        let _ = cached_b.get(&id, &cell_at(1), event).await?;
        assert!(
            counting.lower_reads() >= 1,
            "B's fill publish no-oped — the repeat get is still durable"
        );
        assert!(
            fjall.fuse_blown(),
            "the fuse stays blown for the assignment"
        );

        // Recovery arm, cause (ii): stage a NEW coordinate after the blow (its
        // index record no-ops) — the blown-fuse sweep must bypass the stale
        // seeded latch wholesale and yield the durable truth.
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
            "the blown-fuse sweep bypasses the stale warm seed and finds the post-blow stage"
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
    /// Record the standing stage's verdict, then `commit_provisional`.
    Commit,
    /// `abort_provisional` of the standing stage.
    Abort,
    /// Raw `mark_resolved` of the standing stage's cells (the sweep's promote
    /// path) — leaves the marker standing, as the raw verb does.
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
    /// Charge the delete fault seam with N failures — always strictly within
    /// the retry budget: a blown fuse is one-way, so no healed state would
    /// exist to assert against (the over-budget regime belongs to the
    /// cache-fuse pin alone).
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
            // D5 argument (the staged rows still hold the verdict's data when
            // commit_provisional runs) rests on exactly that.
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
                        // Settle the standing stage.
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

/// A standing stage's replay bookkeeping.
struct Staged {
    dedup: u128,
    writes: Vec<(CellKey, ProvisionalWrite)>,
    clears: Vec<SectionClear>,
}

/// A consumed-but-unsettled marker (left standing by [`CacheOp::Promote`]).
struct StaleMarker {
    staged: Vec<CellKey>,
    clears: bool,
}

/// The KV5 warm-set model: cell → the expiry its entry carries (`u64::MAX`
/// standing in for a fill's effectively-unreachable stamp). A cell present
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
    /// The `own` event verification reads use: the standing stage's event
    /// while one stands (an own-event read short-circuits to `prev` without
    /// resolving the stage), a fixed foreign reader otherwise.
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
    /// clean atomic publish) or — under the puts fault — lands nothing and D1
    /// deletes every cell.
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

    /// D3 model update: a consumed stale marker resolved beneath the cache
    /// drops its staged coordinates — and, when it carried clears, the whole
    /// section's warmth.
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
            // D4 whole-section delete ran before the lower write.
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
    /// (including the boundary D3 over a stale marker).
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
        // The subject's stage boundary resolves a stale foreign marker
        // beneath and fires the boundary D3.
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
                    return Err(eyre!("commit without a standing stage"));
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
                    // Scoped D4: everything but the staged coordinates goes.
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
                    return Err(eyre!("abort without a standing stage"));
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
                    return Err(eyre!("promote without a standing stage"));
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
                // D2 deleted the promoted entries; the raw verb leaves the
                // marker standing (consumed later by a read or the next
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
        // A fall-through read of a stale clears-bearing marker fires D3 and
        // resolves the marker beneath; a warm hit touches neither.
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

    /// One parity full-section scan. Scans fire D3 on a stale clears-bearing
    /// marker and resolve it beneath, but publish nothing (KV3).
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
    /// Advance the pinned clock by N **milliseconds** — deliberately sub-second
    /// so the floor in the expiry stamp is exercised across the full 0–999 ms
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
