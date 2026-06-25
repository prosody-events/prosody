mod cached_suite;
pub(crate) mod cell_suite;
mod collection_suite;
mod identity;
pub(crate) mod identity_suite;

use self::cell_suite::{
    OverlayTrace, OverwriteTrace, ScanTrace, ScriptedOracle, Trace, run_bottom_scan_trace,
    run_crash_equivalence_trace, run_overlay_trace, run_overwrite_trace,
};
use self::collection_suite::{DequeTrace, MapTrace, run_deque_trace, run_map_trace};
use super::memory::{MemoryCellStore, MemoryCells};
use super::registry::CollectionDefRegistry;
use super::{CollectionId, CollectionRef, StateKey, StateName, StateType};
use color_eyre::eyre::Result;
use futures::executor;
use quickcheck::QuickCheck;
use std::sync::Arc;
use uuid::Uuid;

/// A fresh-segment Value collection identity for the named collection.
fn collection_id(name: &str) -> Result<CollectionId> {
    Ok(CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

/// `CollectionRef` equality and hashing key on the inner `CollectionId` only —
/// the TTL is a per-write hint, not part of identity. Two refs to the same
/// collection with different TTLs must compare and hash equal, so a
/// `CollectionRef` used as a map key is not split by an incidental TTL
/// difference.
#[test]
fn collection_ref_eq_and_hash_ignore_ttl() -> Result<()> {
    use crate::timers::duration::CompactDuration;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let id = collection_id("profile")?;
    let with_ttl = CollectionRef::new(id.clone(), Some(CompactDuration::new(3_600)));
    let without_ttl = CollectionRef::new(id.clone(), None);
    let other_ttl = CollectionRef::new(id, Some(CompactDuration::new(7_200)));

    assert_eq!(with_ttl, without_ttl);
    assert_eq!(with_ttl, other_ttl);

    let hash = |r: &CollectionRef| {
        let mut h = DefaultHasher::new();
        r.hash(&mut h);
        h.finish()
    };
    assert_eq!(hash(&with_ttl), hash(&without_ttl));
    assert_eq!(hash(&with_ttl), hash(&other_ttl));
    Ok(())
}

/// A fresh memory cell store over shared cells, resolving through `oracle`.
fn memory_store(cells: MemoryCells, oracle: ScriptedOracle) -> MemoryCellStore<ScriptedOracle> {
    MemoryCellStore::new(cells, oracle, Arc::new(CollectionDefRegistry::default()))
}

/// Crash-recovery equivalence over the memory cell store: every resolution path
/// (clean promote, inline rollback, crash → sweep / first-touch) converges each
/// cell's committed projection to the model (invariants 1, 5).
#[test]
fn prop_memory_cell_crash_equivalence() {
    fn property(trace: Trace) -> Result<bool> {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let make = || Ok(memory_store(cells.clone(), oracle.clone()));
        executor::block_on(run_crash_equivalence_trace(make, oracle.clone(), trace))
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> Result<bool>);
}

/// Implicit-overwrite soundness over the memory cell store: a sequence of
/// events that never promote or roll back explicitly converges every cell to
/// the model, each overwrite resolving its predecessor's provisional cell
/// through the oracle (both arms) on read.
#[test]
fn prop_memory_cell_implicit_overwrite() {
    fn property(trace: OverwriteTrace) -> Result<bool> {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let make = || Ok(memory_store(cells.clone(), oracle.clone()));
        executor::block_on(run_overwrite_trace(make, oracle.clone(), trace))
    }
    QuickCheck::new().quickcheck(property as fn(OverwriteTrace) -> Result<bool>);
}

/// Unified view soundness over `Overlay<MemoryCellStore>`: point `get`s, range
/// `scan`s (bounded, bidirectional, limited, early-stopped), dirty buffering,
/// and committed writes **intermixed** in one trace all match the
/// dirty-over-committed oracle — dirty-wins, clear-hides, the dirty leg bounded
/// to the scan range, the limit applied to the merge (invariants 3, 5; DT7).
#[test]
fn prop_memory_overlay_view() {
    fn property(trace: OverlayTrace) -> Result<bool> {
        let oracle = ScriptedOracle::default();
        let lower = memory_store(MemoryCells::new(), oracle);
        executor::block_on(run_overlay_trace(lower, trace))
    }
    QuickCheck::new().quickcheck(property as fn(OverlayTrace) -> Result<bool>);
}

/// Scan correctness directly over `MemoryCellStore::scan_cells` (no overlay):
/// the backend's own ordering, range bounds, and limit handling match the
/// committed-only oracle.
#[test]
fn prop_memory_bottom_scan() {
    fn property(trace: ScanTrace) -> Result<bool> {
        let oracle = ScriptedOracle::default();
        let store = memory_store(MemoryCells::new(), oracle);
        executor::block_on(run_bottom_scan_trace(store, trace))
    }
    QuickCheck::new().quickcheck(property as fn(ScanTrace) -> Result<bool>);
}

/// Deque collection soundness over the real session lifecycle: random
/// push/pop traces with commit/abort/crash outcomes keep the handle's
/// `len`/`stream`/`get` and every `pop` return value in step with a `VecDeque`
/// oracle — the dense-window invariant and the bounds+entries crash atomicity
/// (invariants 1, 4).
#[test]
fn prop_deque_collection_lifecycle() {
    fn property(trace: DequeTrace) -> Result<bool> {
        executor::block_on(run_deque_trace(trace))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// Map collection soundness over the real session lifecycle: random
/// set/remove/get traces with commit/abort/crash outcomes keep the handle's
/// `get` and key-ordered `stream` in step with a `BTreeMap` oracle — the
/// loose-superset bounds and crash atomicity (invariants 1, 4).
#[test]
fn prop_map_collection_lifecycle() {
    fn property(trace: MapTrace) -> Result<bool> {
        executor::block_on(run_map_trace(trace))
    }
    QuickCheck::new().quickcheck(property as fn(MapTrace) -> Result<bool>);
}
