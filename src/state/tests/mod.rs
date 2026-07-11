mod cached_suite;
pub(crate) mod cell_suite;
mod collection_suite;
pub(crate) mod identity_suite;
pub(crate) mod support;

use self::cell_suite::{
    ApplyTrace, FailingCellStore, MemoryShapeProbe, OverlayTrace, OverwriteTrace, PoisonHandle,
    ScanTrace, ScriptedOracle, Trace, run_apply_idempotence, run_bottom_scan_trace,
    run_crash_equivalence_trace, run_overlay_trace, run_overwrite_trace,
};
use self::cell_suite::{SECTIONS, bytes, cell_in};
use self::collection_suite::{
    DequeHoles, DequeTrace, MapTrace, run_deque_holes, run_deque_trace, run_map_trace,
    run_map_ttl_bounds_trace,
};
use self::support::fresh_collection;
use super::cell::ProvisionalWrite;
use super::marker::{EventMarker, SectionClear};
use super::memory::{MemoryCellStore, MemoryCells};
use super::oracle::CommitOracle;
use super::registry::CollectionDefRegistry;
use super::store::CellStore;
use super::{CollectionRef, CommitMode, EventRef};
use color_eyre::eyre::{Result, eyre};
use futures::StreamExt;
use futures::executor;
use quickcheck::QuickCheck;
use std::sync::Arc;
use uuid::Uuid;

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

    let id = fresh_collection("profile")?;
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
/// cell's committed projection to the model (crash-recovery equivalence and
/// oracle-correctness properties). For a bare store the runner's lower fault
/// seam wraps the bottom store directly (wrapper and lower depth coincide).
#[test]
fn prop_memory_cell_crash_equivalence() {
    fn property(trace: Trace) -> Result<bool> {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let make = |lower: &PoisonHandle| {
            Ok(FailingCellStore::with_handle(
                memory_store(cells.clone(), oracle.clone()),
                lower.clone(),
            ))
        };
        let probe = MemoryShapeProbe(cells.clone());
        executor::block_on(run_crash_equivalence_trace(
            make,
            oracle.clone(),
            trace,
            &probe,
        ))
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
/// to the scan range, the limit applied to the merge (unified-view soundness
/// with point-range interleaving and oracle-correctness properties).
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
/// committed-only oracle — including post-clear (gap-erased) section states.
#[test]
fn prop_memory_bottom_scan() {
    fn property(trace: ScanTrace) -> Result<bool> {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let store = memory_store(cells.clone(), oracle);
        let probe = MemoryShapeProbe(cells);
        executor::block_on(run_bottom_scan_trace(store, trace, &probe))
    }
    QuickCheck::new().quickcheck(property as fn(ScanTrace) -> Result<bool>);
}

/// Deque collection soundness over the real session lifecycle: random
/// push/pop/clear/mid-handler-checkpoint traces with commit/abort/crash
/// outcomes keep the handle's `len`/`stream`/`get` and every `pop` return
/// value in step with a `VecDeque` oracle — the window invariant (incl. the
/// index-space reset on clear), bounds+entries crash atomicity, and the
/// at-least-once checkpoint contract (checkpointed ops survive
/// abort/crash-rollback; post-checkpoint ops roll back — so
/// checkpoint-then-clear-then-abort restores the checkpointed state).
#[test]
fn prop_deque_collection_lifecycle() {
    fn property(trace: DequeTrace) -> Result<bool> {
        executor::block_on(run_deque_trace(trace, CommitMode::ReadCommitted))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// The deque lifecycle property in `ReadUncommitted` mode: `finalize` commits
/// everything, so every outcome that reaches it — including crash-abort —
/// converges to the full scratch model.
#[test]
fn prop_deque_collection_lifecycle_read_uncommitted() {
    fn property(trace: DequeTrace) -> Result<bool> {
        executor::block_on(run_deque_trace(trace, CommitMode::ReadUncommitted))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// Map collection soundness over the real session lifecycle: random
/// set/remove/get/clear/mid-handler-checkpoint traces with commit/abort/crash
/// outcomes keep the handle's `get` and key-ordered `stream` in step with a
/// `BTreeMap` oracle — the loose-superset bounds (cleared with the entries),
/// crash atomicity, and the at-least-once checkpoint contract (checkpointed
/// ops survive abort/crash-rollback; post-checkpoint ops roll back — so
/// checkpoint-then-clear-then-abort restores the checkpointed state).
#[test]
fn prop_map_collection_lifecycle() {
    fn property(trace: MapTrace) -> Result<bool> {
        executor::block_on(run_map_trace(trace, CommitMode::ReadCommitted))
    }
    QuickCheck::new().quickcheck(property as fn(MapTrace) -> Result<bool>);
}

/// The map lifecycle property in `ReadUncommitted` mode: `finalize` commits
/// everything, so every outcome that reaches it — including crash-abort —
/// converges to the full scratch model.
#[test]
fn prop_map_collection_lifecycle_read_uncommitted() {
    fn property(trace: MapTrace) -> Result<bool> {
        executor::block_on(run_map_trace(trace, CommitMode::ReadUncommitted))
    }
    QuickCheck::new().quickcheck(property as fn(MapTrace) -> Result<bool>);
}

/// Map TTL bound-refresh: on a TTL'd map every `set` — including a re-set of a
/// key already within the committed bounds — buffers both `MapBound` cells, so
/// the bounds' TTL is refreshed and they outlive every entry (absent bounds ⇔
/// no live entries). Staged-set composition, so no clock is needed.
#[test]
fn prop_map_ttl_bounds_refresh() {
    fn property(trace: MapTrace) -> Result<bool> {
        executor::block_on(run_map_ttl_bounds_trace(trace))
    }
    QuickCheck::new().quickcheck(property as fn(MapTrace) -> Result<bool>);
}

/// Deque TTL holes: over a directly-seeded sparse window, `len` is the full
/// span (an upper bound on live elements) and `get`/`stream` skip expired
/// indices without error — the TTL'd-deque hole read contract.
#[test]
fn prop_deque_ttl_holes() {
    fn property(shape: DequeHoles) -> Result<bool> {
        executor::block_on(run_deque_holes(shape))
    }
    QuickCheck::new().quickcheck(property as fn(DequeHoles) -> Result<bool>);
}

/// Staging over a standing **foreign** marker with live cells: event A stages
/// coordinates {0, 1}, the process crashes with no recovery (a fresh store over
/// the same warm cells), then event B stages coordinate {1} on the same
/// collection. B's stage boundary must resolve A's standing marker first, so
/// A's untouched coordinate 0 settles to A's verdict, B's marker replaces A's,
/// and only B's cell stays provisional. The generated crash/reassignment
/// alphabet (the `Defer` recovery in the crash-equivalence trace) subsumes
/// this shape; these two pins are kept as the fast, deterministic falsifiers
/// for the boundary arm.
async fn boundary_resolve_pin(a_committed: bool) -> Result<()> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let id = fresh_collection("boundary")?;
    let collection = CollectionRef::new(id.clone(), None);
    let store = memory_store(cells.clone(), oracle.clone());

    // Stage event A at coordinates {0, 1} over an empty base.
    let a_dedup = Uuid::from_u128(1);
    let a = EventRef::Message { dedup_id: a_dedup };
    let prev0 = store.get(&id, &cell_in(0, 0), a).await?;
    let prev1 = store.get(&id, &cell_in(0, 1), a).await?;
    let writes_a = [
        (
            cell_in(0, 0),
            ProvisionalWrite::new(Some(bytes(10)), prev0, a),
        ),
        (
            cell_in(0, 1),
            ProvisionalWrite::new(Some(bytes(11)), prev1, a),
        ),
    ];
    let marker_a = EventMarker::frozen(a, &writes_a, &[]);
    store
        .write_provisional(&collection, &writes_a, Some(&marker_a))
        .await?;
    if a_committed {
        oracle.record_message(a_dedup).await?;
    }

    // Crash with no recovery: a fresh store over the same warm cells (A's
    // provisional cells and marker survive in `MemoryCells`).
    let store = memory_store(cells.clone(), oracle.clone());

    // Stage event B at coordinate {1}; the boundary resolves A's marker.
    let b = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let prev_b = store.get(&id, &cell_in(0, 1), b).await?;
    let writes_b = [(
        cell_in(0, 1),
        ProvisionalWrite::new(Some(bytes(21)), prev_b, b),
    )];
    let marker_b = EventMarker::frozen(b, &writes_b, &[]);
    store
        .write_provisional(&collection, &writes_b, Some(&marker_b))
        .await?;

    // Exactly B's one staged cell remains provisional — checked BEFORE any
    // resolving read, so a skipped boundary resolve (A's coordinate 0 left
    // provisional) surfaces here rather than being masked by a later `get`.
    let mut provisional = 0usize;
    let stream = store.provisional_cells(&id);
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        item?;
        provisional += 1;
    }
    assert_eq!(
        provisional, 1,
        "the boundary resolved A's cells; only B's staged cell is provisional"
    );

    // B's marker replaces A's.
    assert_eq!(
        cells.standing_marker_of(&id).map(|marker| marker.event()),
        Some(b),
        "B's marker stands after the boundary overwrite"
    );

    // A's untouched coordinate 0 is resolved per A's verdict: A's data on
    // commit, exact absence (A's `None` base) on abort.
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    let resolved0 = store.get(&id, &cell_in(0, 0), probe).await?.into_inner();
    assert_eq!(
        resolved0,
        a_committed.then(|| bytes(10)),
        "A's coordinate 0 resolves per A's verdict at B's stage boundary"
    );
    Ok(())
}

/// Boundary resolve when A committed: A's coordinate 0 promotes to A's data.
#[test]
fn boundary_resolves_committed_foreign_marker() -> Result<()> {
    executor::block_on(boundary_resolve_pin(true))
}

/// Boundary resolve when A aborted: A's coordinate 0 rolls back to its absent
/// base.
#[test]
fn boundary_resolves_aborted_foreign_marker() -> Result<()> {
    executor::block_on(boundary_resolve_pin(false))
}

/// The clears-only stage boundary: event A stages cells {0, 1}, the process
/// crashes with no recovery, then event B stages **clears only** — an empty
/// write set whose marker carries a cleared section. The boundary must resolve
/// A's marker exactly as a writing stage would (A's cells settle per A's
/// verdict, nothing of A stays provisional) while B's clears-bearing marker
/// stands. The crash-trace generator's clears dimension produces this shape
/// organically; this pin is its fast deterministic falsifier, matching the
/// documented role of [`boundary_resolve_pin`].
///
/// Deliberately kept parallel to [`boundary_resolve_pin`] (B stages a
/// **clears-only** marker here, cell writes there) rather than folded: a shared
/// body would thread a "writes vs clears" flag through a ~60-line B stage — a
/// flag-parameter contortion the net-negative bar rejects.
async fn clears_only_boundary_pin(a_committed: bool) -> Result<()> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let id = fresh_collection("clears-only-boundary")?;
    let collection = CollectionRef::new(id.clone(), None);
    let store = memory_store(cells.clone(), oracle.clone());

    // Stage event A at section-0 coordinates {0, 1} over an empty base.
    let a_dedup = Uuid::from_u128(1);
    let a = EventRef::Message { dedup_id: a_dedup };
    let prev0 = store.get(&id, &cell_in(0, 0), a).await?;
    let prev1 = store.get(&id, &cell_in(0, 1), a).await?;
    let writes_a = [
        (
            cell_in(0, 0),
            ProvisionalWrite::new(Some(bytes(10)), prev0, a),
        ),
        (
            cell_in(0, 1),
            ProvisionalWrite::new(Some(bytes(11)), prev1, a),
        ),
    ];
    let marker_a = EventMarker::frozen(a, &writes_a, &[]);
    store
        .write_provisional(&collection, &writes_a, Some(&marker_a))
        .await?;
    if a_committed {
        oracle.record_message(a_dedup).await?;
    }

    // Crash with no recovery, then event B stages CLEARS ONLY on section 1:
    // writes = [], marker with one cleared section (no survivors).
    let store = memory_store(cells.clone(), oracle.clone());
    let b = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let clears_b = [SectionClear::frozen(SECTIONS[1], &[])];
    let marker_b = EventMarker::frozen(b, &[], &clears_b);
    store
        .write_provisional(&collection, &[], Some(&marker_b))
        .await?;

    // Raw probes BEFORE any resolving read — a `get` would read-help-resolve
    // B's clears-bearing marker and destroy the shape under test.
    let standing = cells
        .standing_marker_of(&id)
        .ok_or_else(|| eyre!("B's clears-only marker must stand after the stage"))?;
    assert_eq!(standing.event(), b, "B's marker replaced A's");
    assert_eq!(
        standing.clears().len(),
        1,
        "B's marker carries its cleared section"
    );
    assert!(
        cells.provisional_coordinates(&id).is_empty(),
        "the boundary resolved all of A's cells; B staged nothing"
    );

    // A's cells settled per A's verdict (these reads resolve B's marker via
    // read-help — after the shape assertions above, and section 0 is
    // untouched by B's section-1 clear either way).
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    for (c, value) in [(0, bytes(10)), (1, bytes(11))] {
        let resolved = store.get(&id, &cell_in(0, c), probe).await?.into_inner();
        assert_eq!(
            resolved,
            a_committed.then(|| value.clone()),
            "A's coordinate {c} resolves per A's verdict at B's clears-only boundary"
        );
    }
    Ok(())
}

/// Clears-only boundary resolve when A committed.
#[test]
fn clears_only_boundary_resolves_committed_foreign_marker() -> Result<()> {
    executor::block_on(clears_only_boundary_pin(true))
}

/// Clears-only boundary resolve when A aborted.
#[test]
fn clears_only_boundary_resolves_aborted_foreign_marker() -> Result<()> {
    executor::block_on(clears_only_boundary_pin(false))
}

/// Apply idempotence over the memory cell store: any generated interleaving of
/// marker resolution, verdict-matching settle re-applies, and per-cell
/// first-touches over one staged set with durable section clears converges to
/// the verdict state — no marker, no provisional residue, exact row shape.
#[test]
fn prop_memory_apply_idempotence() {
    fn property(input: ApplyTrace) -> Result<bool> {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let store = memory_store(cells.clone(), oracle.clone());
        let probe = MemoryShapeProbe(cells);
        executor::block_on(run_apply_idempotence(store, oracle, input, &probe))
    }
    QuickCheck::new().quickcheck(property as fn(ApplyTrace) -> Result<bool>);
}
