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
use super::super::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use super::super::fjall::FjallCellCache;
use super::super::memory::{MemoryCellStore, MemoryCells};
use super::super::registry::CollectionDefRegistry;
use super::super::store::CellStore;
use super::super::{CollectionId, CollectionRef, EventRef, StateKey, StateName, StateType};
use super::cell_suite::{
    CountingCellStore, OverlayTrace, ScriptedOracle, bytes, run_overlay_trace,
};
use crate::test_util::TEST_RUNTIME;
use color_eyre::eyre::Result;
use fjall::{CompressionType, Config, Keyspace, PartitionCreateOptions};
use futures::StreamExt;
use quickcheck::QuickCheck;
use std::ops::Bound;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

/// The single section the cell suites address (mirrors a Map's entry section).
const SECTION: Section = Section::new(0);

/// LZ4 block compression, matching the production workspace.
fn partition_options() -> PartitionCreateOptions {
    PartitionCreateOptions::default().compression(CompressionType::Lz4)
}

/// A fresh tempdir-backed keyspace; every `Cached` opens its cache partition
/// under it, so a "crash" (a new partition) starts cold while the keyspace —
/// and the warm `MemoryCells` — outlives the run.
fn open_keyspace() -> Result<(TempDir, Keyspace)> {
    let dir = tempfile::tempdir()?;
    let keyspace = Config::new(dir.path()).open()?;
    Ok((dir, keyspace))
}

/// Builds a production-shaped `Cached` over a fresh fjall partition and the
/// shared memory cells.
fn cached_over(
    keyspace: &Keyspace,
    cells: &MemoryCells,
    oracle: &ScriptedOracle,
    name: &str,
) -> Result<Cached<MemoryCellStore<ScriptedOracle>>> {
    let handle = keyspace.open_partition(name, partition_options())?;
    let lower = MemoryCellStore::new(
        cells.clone(),
        oracle.clone(),
        Arc::new(CollectionDefRegistry::default()),
    );
    Ok(Cached::new(FjallCellCache::new(handle), lower))
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
        let (_dir, keyspace) = open_keyspace()?;
        let lower = cached_over(&keyspace, &cells, &oracle, "overlay")?;
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

/// Coverage op budget: a covered-negative `get` reads nothing from the lower
/// store, a single-cell write punches only its coordinate (so a re-scan
/// re-reads just the one-cell gap, not the section), and three separately
/// covered ranges leave exactly the two gap queries between them.
#[test]
fn coverage_op_budget() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let (_dir, keyspace) = open_keyspace()?;
        let counting = CountingCellStore::new(MemoryCellStore::new(
            cells,
            oracle,
            Arc::new(CollectionDefRegistry::default()),
        ));
        let handle = keyspace.open_partition("budget", partition_options())?;
        let cached = Cached::new(FjallCellCache::new(handle), counting.clone());
        let id = collection("budget")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Seed present cells at 0, 2, 4, 6, 8 (resolved, committed).
        for c in [0u8, 2, 4, 6, 8] {
            cached
                .write_resolved(&cref, &[(cell_at(c), Some(bytes(c)))])
                .await?;
        }

        // Warm the whole section with one unbounded scan, then verify a covered
        // re-scan issues ZERO lower scans (served entirely from fjall).
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

        // Covered-negative get: coordinate 3 is covered (in range, no cell) →
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

        // A single-cell write punches only coordinate 4. The re-scan then
        // re-reads exactly the one punched-out gap — one bounded gap query, not
        // a section-wide scan.
        cached
            .write_resolved(&cref, &[(cell_at(4), Some(bytes(40)))])
            .await?;
        counting.reset();
        let after = scan_forward(&cached, &id, 0, Bound::Unbounded).await?;
        assert_eq!(after, vec![0, 2, 4, 6, 8]);
        assert_eq!(
            counting.lower_scans(),
            1,
            "a one-cell write evicts one coordinate, so the re-scan reads one gap"
        );

        // The re-scan above re-covered the whole section. Now punch *two*
        // coordinates with no healing scan between them, splitting coverage into
        // three covered ranges with two gaps — a full re-scan then issues
        // exactly two bounded gap queries (never a section scan).
        cached
            .write_resolved(&cref, &[(cell_at(2), Some(bytes(20)))])
            .await?;
        cached
            .write_resolved(&cref, &[(cell_at(6), Some(bytes(60)))])
            .await?;
        counting.reset();
        let after = scan_forward(&cached, &id, 0, Bound::Unbounded).await?;
        assert_eq!(after, vec![0, 2, 4, 6, 8]);
        assert_eq!(
            counting.lower_scans(),
            2,
            "three covered ranges leave exactly the two gap queries between them"
        );

        Ok(())
    })
}

/// A covered scan never bleeds into another collection or section sharing the
/// fjall partition: with two collections and a decoy section seeded in one
/// cache, an unbounded scan of each section yields only its own cells.
#[test]
fn coverage_scan_isolation() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let (_dir, keyspace) = open_keyspace()?;
        let cached = cached_over(&keyspace, &cells, &oracle, "isolation")?;

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
        let (_dir, keyspace) = open_keyspace()?;
        let cached = cached_over(&keyspace, &cells, &oracle, "wide")?;
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
