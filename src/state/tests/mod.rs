mod cached_suite;
pub(crate) mod cell_suite;
pub(crate) mod collection_suite;
mod gate_suite;
pub(crate) mod identity_suite;
pub(crate) mod publication_suite;
pub(crate) mod support;

use self::cell_suite::{
    ApplyTrace, BatchReadTrace, FailingCellStore, FailingOracle, MemoryShapeProbe, OverlayTrace,
    OverwriteTrace, PoisonHandle, RawBatchTrace, ScanTrace, ScriptedOracle, Trace,
    run_apply_idempotence, run_batch_alignment, run_batch_duplicate_co_observation,
    run_batch_read_parity_trace, run_blind_write_leaves_clears_free_marker,
    run_blind_write_survives_stale_clear, run_bottom_scan_trace, run_crash_equivalence_trace,
    run_overlay_precedence_pin, run_overlay_trace, run_overwrite_trace,
    run_raw_batch_ascending_output, run_raw_batch_no_side_effects, run_raw_batch_parity_trace,
    run_repair_after_marker_abort_converges, run_repair_defers_beneath_stale_clear,
};
use self::cell_suite::{SECTIONS, bytes, cell_in};
use self::collection_suite::{
    DequeCapacityShape, DequeHoles, DequeInterleave, DequeTrace, MapGetManyInput, MapInterleave,
    MapKeyHoles, MapTrace, finalize_and_promote, run_deque_capacity_convergence, run_deque_holes,
    run_deque_stream_interleave, run_deque_trace, run_map_get_many_parity_trace,
    run_map_key_scan_holes, run_map_keyset_exact_trace, run_map_stream_interleave, run_map_trace,
    run_map_ttl_keyset_refresh_trace,
};
use self::publication_suite::{PublicationTrace, run_publication_trace};
use self::support::{
    CountingCellStore, CountingOracle, CountingResolver, FixedOracle, ResolveCounter,
    fresh_collection,
};
use super::cell::{Committed, ProvisionalWrite};
use super::cell_key::CellKey;
use super::descriptor::{StateDescriptor, WithResolver, deque, deque_state, map_state};
use super::manager::ArmedKeys;
use super::marker::{EventMarker, SectionClear};
use super::memory::{
    MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore,
};
use super::oracle::CommitOracle;
use super::order_codec::{I64KeyCodec, OrderedKeyCodec};
use super::registry::{CollectionDef, CollectionDefRegistry};
use super::resolve::{ResolveCellError, resolve_marker};
use super::session::{KeyedStateSession, SessionParts, TerminationWatch};
use super::store::{CELL_BATCH, CellBuffer, CellStore, CoordinateBatch, dedupe};
use super::{
    CELLS_INLINE, CollectionId, CollectionRef, CommitMode, Coordinate, Direction, EventRef,
    PartitionBackend, StateKey, StateName, StateType,
};
use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::error::ErrorCategory;
use crate::loader::MemoryLoader;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::StreamExt;
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::Value;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::sync::watch;
use uuid::Uuid;

/// The bounded-deque capacity the lifecycle properties run under. `match`, not
/// `NonZeroUsize::new(..).unwrap_or(..)`: `Option::unwrap_or` is not const, and
/// the tests forbid `unwrap`.
const BOUNDED_TEST_CAP: NonZeroUsize = match NonZeroUsize::new(2) {
    Some(n) => n,
    None => NonZeroUsize::MIN,
};

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

/// Regression pin over the memory store: a blind `write_resolved` into a
/// section whose clears-bearing marker still stands survives the marker's later
/// resolution (the write-side committed-unapplied boundary). Falsify by
/// deleting the `standing_marker` + `help_write_window` lines in
/// `MemoryCellStore::write_resolved`.
#[test]
fn blind_write_survives_stale_clear() -> Result<()> {
    let oracle = ScriptedOracle::default();
    let store = memory_store(MemoryCells::new(), oracle.clone());
    executor::block_on(run_blind_write_survives_stale_clear(store, oracle))
}

/// Posture-parity pin over the memory store: a blind `write_resolved` leaves a
/// standing clears-FREE marker standing (the boundary triggers on clears only).
#[test]
fn blind_write_leaves_clears_free_marker() -> Result<()> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let store = memory_store(cells.clone(), oracle);
    let probe = MemoryShapeProbe(cells);
    executor::block_on(run_blind_write_leaves_clears_free_marker(store, &probe))
}

/// Regression pin over the memory store: a repair whose payload predates a
/// standing committed clears-bearing marker defers to peek semantics, so the
/// marker's own resolution erases the cell rather than a stale repair
/// resurrecting it. Falsify by deleting the `deferred` guard in `resolve_cell`.
#[test]
fn repair_defers_beneath_stale_clear() -> Result<()> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let stage = memory_store(cells.clone(), oracle.clone());
    let store = memory_store(cells.clone(), oracle.clone());
    let probe = MemoryShapeProbe(cells);
    executor::block_on(run_repair_defers_beneath_stale_clear(
        &stage, store, oracle, &probe,
    ))
}

/// Convergence pin over the memory store: the deferral wedges nothing — when
/// the standing marker aborts, x's committed projection stays its base.
#[test]
fn repair_after_marker_abort_converges() -> Result<()> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let stage = memory_store(cells.clone(), oracle.clone());
    let store = memory_store(cells.clone(), oracle.clone());
    let probe = MemoryShapeProbe(cells);
    executor::block_on(run_repair_after_marker_abort_converges(
        &stage, store, oracle, &probe,
    ))
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

/// `CoordinateBatch::chunks` reassembles to its input exactly, and every
/// yielded batch is non-empty and `≤ CELL_BATCH` with only the last possibly
/// short — the batch-bound invariant the store verb relies on.
#[test]
fn prop_chunk_reassembly() {
    fn property(coords: Vec<u8>) -> bool {
        let input: Vec<Coordinate> = coords
            .into_iter()
            .map(|b| Coordinate::from_bytes(vec![b]))
            .collect();
        let batches: Vec<CoordinateBatch> = CoordinateBatch::chunks(input.clone()).collect();
        let mut flat: Vec<Coordinate> = Vec::new();
        for batch in &batches {
            if batch.len() == 0 || batch.len() > CELL_BATCH {
                return false;
            }
            flat.extend(batch.as_slice().iter().cloned());
        }
        // All but the last batch are exactly CELL_BATCH.
        let full_prefix = batches
            .split_last()
            .is_none_or(|(_, rest)| rest.iter().all(|b| b.len() == CELL_BATCH));
        flat == input && full_prefix && (input.is_empty() == batches.is_empty())
    }
    QuickCheck::new().quickcheck(property as fn(Vec<u8>) -> bool);
}

/// Keyed-state buffers keep small operations inline but spill well before a
/// full store batch can become part of an async future's stack footprint.
#[test]
fn cell_buffers_spill_before_full_batch() {
    let small: CellBuffer<usize> = (0..CELLS_INLINE).collect();
    assert!(!small.spilled(), "the common small case stays inline");

    let full: CellBuffer<usize> = (0..CELL_BATCH).collect();
    assert_eq!(full.len(), CELL_BATCH);
    assert!(
        full.spilled(),
        "a full batch must not remain inline in an async state machine"
    );
}

/// `dedupe` keeps unique coordinates in first-occurrence order and maps every
/// input position to its unique's index — the dedup + first-occurrence leg the
/// batch verbs and the Cassandra `IN` override share (a value-only test cannot
/// observe client-side dedup, so it is pinned directly here).
#[test]
fn dedupe_uniques_and_plan() -> Result<()> {
    let bytes_in = [5u8, 9, 5, 2, 9, 5];
    let batch = CoordinateBatch::chunks(bytes_in.iter().map(|&b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("non-empty read list must yield one batch"))?;
    let (uniques, plan) = dedupe(&batch);
    let unique_bytes: Vec<u8> = uniques.iter().map(|c| c.as_bytes()[0]).collect();
    assert_eq!(
        unique_bytes,
        vec![5, 9, 2],
        "first-occurrence order, deduped"
    );
    assert_eq!(
        plan.as_slice(),
        &[0, 1, 0, 2, 1, 0],
        "each position maps to its unique"
    );
    Ok(())
}

/// Batch-read parity over the memory cell store: `get_many` answers each
/// position exactly as the sequential point-`get` oracle, across duplicates,
/// unknowns, absence, and provisional resolution.
#[test]
fn prop_memory_batch_read_parity() {
    fn property(trace: BatchReadTrace) -> Result<bool> {
        let oracle = ScriptedOracle::default();
        let store = memory_store(MemoryCells::new(), oracle.clone());
        executor::block_on(run_batch_read_parity_trace(
            store.clone(),
            store,
            oracle,
            trace,
        ))
    }
    QuickCheck::new().quickcheck(property as fn(BatchReadTrace) -> Result<bool>);
}

/// Raw-provisional batch parity over the memory store: `provisional_many`
/// returns exactly the survivors the sequential `provisional_cell_at` loop
/// does.
#[test]
fn prop_memory_raw_batch_parity() {
    fn property(trace: RawBatchTrace) -> Result<bool> {
        let store = memory_store(MemoryCells::new(), ScriptedOracle::default());
        executor::block_on(run_raw_batch_parity_trace(store, trace))
    }
    QuickCheck::new().quickcheck(property as fn(RawBatchTrace) -> Result<bool>);
}

/// Ascending-output pin over the memory store (deterministic): the sort in
/// `provisional_point_loop` is load-bearing here — without it the output
/// collapses to input byte order.
#[test]
fn memory_raw_batch_ascending_output() -> Result<()> {
    let store = memory_store(MemoryCells::new(), ScriptedOracle::default());
    executor::block_on(run_raw_batch_ascending_output(store))
}

/// No-side-effects pin over the memory store built on a [`CountingOracle`]:
/// `provisional_many` never resolves, writes, or caches.
#[test]
fn memory_raw_batch_no_side_effects() -> Result<()> {
    let oracle = CountingOracle::default();
    let store = MemoryCellStore::new(
        MemoryCells::new(),
        oracle.clone(),
        Arc::new(CollectionDefRegistry::default()),
    );
    executor::block_on(run_raw_batch_no_side_effects(store, oracle))
}

/// Within-batch duplicate co-observation + scatter alignment over the memory
/// store (deterministic).
#[test]
fn memory_batch_duplicate_co_observation() -> Result<()> {
    let store = memory_store(MemoryCells::new(), ScriptedOracle::default());
    executor::block_on(run_batch_duplicate_co_observation(store))
}

/// Every input position is answered over two chunks (deterministic alignment).
#[test]
fn memory_batch_alignment() -> Result<()> {
    let store = memory_store(MemoryCells::new(), ScriptedOracle::default());
    executor::block_on(run_batch_alignment(store))
}

/// `resolve_marker` rebuilds the staged set through per-section
/// `<=CELL_BATCH` `provisional_many` batches (never per-coordinate point
/// reads) and consults the oracle exactly ONCE: staging 129 cells in section 0
/// and 3 in section 1 makes the marker leg issue `ceil(129/128) + ceil(3/128)
/// = 3` raw batch reads, zero raw point reads, and one oracle resolve. The
/// memory `provisional_many` is a point-loop, so `raw_batch_reads` counts the
/// LOGICAL batch calls (one per chunk) — a faithful pin that the marker leg
/// issues `ceil` batch calls and no direct point reads.
/// FALSIFICATION: revert `resolve_marker`'s chunk loop to `provisional_cell_at`
/// → `raw_batch_reads == 0` and `raw_point_reads == 132`, both asserts red.
#[test]
fn memory_resolve_marker_batches_reads() -> Result<()> {
    executor::block_on(async {
        let counting =
            CountingCellStore::new(memory_store(MemoryCells::new(), ScriptedOracle::default()));
        let oracle = CountingOracle::default();
        let id = fresh_collection("resolve-marker-batches")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };

        let mut writes: Vec<(CellKey, ProvisionalWrite)> = (0..129u8)
            .map(|i| {
                (
                    cell_in(0, i),
                    ProvisionalWrite::new(Some(bytes(1)), Committed::new(None), event),
                )
            })
            .collect();
        writes.extend((0..3u8).map(|c| {
            (
                cell_in(1, c),
                ProvisionalWrite::new(Some(bytes(2)), Committed::new(None), event),
            )
        }));
        let marker = EventMarker::frozen(event, &writes, &[]);
        counting
            .write_provisional(&cref, &writes, Some(&marker))
            .await
            .map_err(|e| eyre!("stage: {e}"))?;

        // The oracle answers NotCommitted ⇒ abort; the verdict is irrelevant to
        // the read counts this pin measures.
        counting.reset();
        resolve_marker(&counting, &oracle, &cref, &marker)
            .await
            .map_err(|e| eyre!("resolve_marker: {e}"))?;

        assert_eq!(
            counting.raw_batch_reads(),
            3,
            "ceil(129/128) + ceil(3/128) batch calls (two sections)"
        );
        assert_eq!(
            counting.raw_point_reads(),
            0,
            "the marker leg issues no per-coordinate point read"
        );
        assert_eq!(oracle.resolves(), 1, "exactly one oracle verdict");
        Ok(())
    })
}

/// Section-rekey pin: a marker staging the SAME coordinate byte in BOTH
/// sections with DIFFERENT values must commit each survivor at its own
/// `(section, coordinate)`, never collapse to one section. A regression that
/// drops the chunk's section (keys every survivor at section 0) commits both
/// survivors onto `(0, 7)` and leaves `(1, 7)` unresolved-provisional.
///
/// The discriminator is a provisional-sweep drain taken IMMEDIATELY after
/// `resolve_marker`, before any `get`: a foreign-reader `get` self-heals a
/// still-provisional cell through its own oracle consult (`resolve_cell`
/// promotes the cell's stored bytes in place), so a `get`-after-drain would
/// repair the collapse before an assertion could observe it. The drain reads
/// `resolve_marker`'s own output.
/// FALSIFICATION: replace the chunk's `section` with a fixed `SECTIONS[0]` in
/// `resolve_marker`/`section_batches` → the drain reports `(1, 7)` still
/// provisional (`remaining.is_empty()` red).
#[test]
fn resolve_marker_rekeys_survivors_by_section() -> Result<()> {
    executor::block_on(async {
        let store = MemoryCellStore::new(
            MemoryCells::new(),
            FixedOracle::committed(),
            Arc::new(CollectionDefRegistry::default()),
        );
        let id = fresh_collection("resolve-marker-rekey")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let writes = [
            (
                cell_in(0, 7),
                ProvisionalWrite::new(Some(bytes(70)), Committed::new(None), event),
            ),
            (
                cell_in(1, 7),
                ProvisionalWrite::new(Some(bytes(90)), Committed::new(None), event),
            ),
        ];
        let marker = EventMarker::frozen(event, &writes, &[]);
        store
            .write_provisional(&cref, &writes, Some(&marker))
            .await
            .map_err(|e| eyre!("stage: {e}"))?;

        let oracle = FixedOracle::committed();
        resolve_marker(&store, &oracle, &cref, &marker)
            .await
            .map_err(|e| eyre!("resolve_marker: {e}"))?;

        // Observe `resolve_marker`'s own output BEFORE any `get`: nothing may be
        // left provisional. A `get` here would self-heal a survivor the collapse
        // regression left provisional (see the doc comment), so this drain must
        // run first.
        let remaining = drain_memory_provisional(&store, &id)
            .await
            .map_err(|e| eyre!("drain: {e}"))?;
        assert!(
            remaining.is_empty(),
            "both survivors are resolved by resolve_marker, none left provisional: {remaining:?}"
        );

        // A foreign reader event: the cells are already resolved, so `get`
        // returns the committed value directly.
        let reader = EventRef::Message {
            dedup_id: Uuid::from_u128(2),
        };
        assert_eq!(
            store
                .get(&id, &cell_in(0, 7), reader)
                .await
                .map_err(|e| eyre!("get s0: {e}"))?,
            Committed::new(Some(bytes(70))),
            "the section-0 survivor commits at (0, 7)"
        );
        assert_eq!(
            store
                .get(&id, &cell_in(1, 7), reader)
                .await
                .map_err(|e| eyre!("get s1: {e}"))?,
            Committed::new(Some(bytes(90))),
            "the section-1 survivor commits at (1, 7), not collided onto (0, 7)"
        );
        Ok(())
    })
}

/// Overlap-precedence pin: when BOTH the oracle read and a raw batch read fail,
/// `resolve_marker` surfaces the ORACLE error (its retry/skip classification
/// governs), and the overlap leaves the oracle-read and raw-batch-read counts
/// unchanged. [`FailingOracle`] yields once so the oracle is observed `Pending`
/// on the first poll pass while the poisoned batch read is `Ready(Err)` — the
/// `join`-plus-oracle-first ordering is what surfaces the oracle error.
/// FALSIFICATION: swap `join(oracle, reads)` + oracle-first for
/// `try_join!(oracle, reads)` → the store error surfaces (matches! fails) and
/// `oracle.resolves() == 0`.
#[test]
fn resolve_marker_double_failure_surfaces_oracle() -> Result<()> {
    executor::block_on(async {
        let id = fresh_collection("resolve-marker-double-fail")?;
        let name = id.name().clone();
        let cref = CollectionRef::new(id.clone(), None);
        // Arm the raw-read poison from the start; it targets `provisional_many`
        // only, so seeding the marker via `write_provisional` still works.
        let counting = CountingCellStore::new(FailingCellStore::armed_provisional_many(
            memory_store(MemoryCells::new(), ScriptedOracle::default()),
            name,
            ErrorCategory::Transient,
        ));
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let writes = [(
            cell_in(0, 1),
            ProvisionalWrite::new(Some(bytes(1)), Committed::new(None), event),
        )];
        let marker = EventMarker::frozen(event, &writes, &[]);
        counting
            .write_provisional(&cref, &writes, Some(&marker))
            .await
            .map_err(|e| eyre!("stage: {e}"))?;

        counting.reset();
        let oracle = FailingOracle::default();
        let err = match resolve_marker(&counting, &oracle, &cref, &marker).await {
            Ok(()) => return Err(eyre!("expected a double failure, got Ok")),
            Err(err) => err,
        };
        assert!(
            matches!(err, ResolveCellError::Oracle(_)),
            "a double failure surfaces the oracle error, got {err:?}"
        );
        assert_eq!(oracle.resolves(), 1, "the oracle is consulted exactly once");
        assert_eq!(
            counting.raw_batch_reads(),
            1,
            "the overlap leaves the raw-batch-read count unchanged (one chunk)"
        );
        Ok(())
    })
}

/// Drains a memory store's `provisional_cells` sweep into its yielded cells,
/// for the recovery pins that assert nothing is left provisional.
async fn drain_memory_provisional<S: CellStore>(
    store: &S,
    id: &CollectionId,
) -> Result<Vec<CellKey>, S::Error> {
    let stream = store.provisional_cells(id);
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?.0);
    }
    Ok(out)
}

/// A dirty `Set` inside a standing dirty section-clear answers its bytes
/// through `Overlay::get_many` (precedence + duplicate co-observation), and the
/// dirty-answered positions never reach the lower batch.
#[test]
fn memory_overlay_precedence_set_beats_section_clear() -> Result<()> {
    let counting =
        CountingCellStore::new(memory_store(MemoryCells::new(), ScriptedOracle::default()));
    executor::block_on(run_overlay_precedence_pin(counting))
}

/// A cold, dense `CELL_BATCH`-entry `Tracked` map streamed to exhaustion issues
/// exactly ONE lower batch read for its entries — a full-width scan chunk is
/// one [`CoordinateBatch`], one lower `get_many`; only the keyset meta cell
/// stays a point read.
/// FALSIFICATION: revert `CoordinatePlan`'s chunk source to a per-key point
/// `get` loop →
/// `batch_reads == 0` and `lower_reads == CELL_BATCH` (+ keyset) → both asserts
/// red. Counters are read after the full drain, so nothing masks them.
#[test]
fn map_cold_chunk_is_one_batch_read() -> Result<()> {
    executor::block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let descriptor =
            map_state::<I64KeyCodec, WithResolver<JsonCodec, CountingResolver>>("chunk1");
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &descriptor,
            CollectionDef {
                keyset_limit: 4096,
                ..CollectionDef::new(None)
            },
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(
            cells.clone(),
            oracle.clone(),
            registry.clone(),
        ));
        let armed: ArmedKeys = Arc::default();
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("chunk1")?,
        );

        // Seed a dense committed map of exactly one full chunk.
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let session = resolve_session(
            &counting,
            &oracle,
            &registry,
            &state_key,
            &armed,
            event,
            ResolveCounter::default(),
        );
        let seed = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        for i in 0..CELL_BATCH as i64 {
            seed.set(i, Value::from(i))
                .await
                .map_err(|e| eyre!("{e}"))?;
        }
        finalize_and_promote(&session, &oracle, Uuid::from_u128(1), &cells, &id).await?;

        // Fresh cold session, zeroed counters; drain the WHOLE stream.
        counting.reset();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(2),
        };
        let session = resolve_session(
            &counting,
            &oracle,
            &registry,
            &state_key,
            &armed,
            event,
            ResolveCounter::default(),
        );
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        let drained: Vec<_> = {
            let stream = handle.stream(Direction::Forward);
            futures::pin_mut!(stream);
            let mut out = Vec::new();
            while let Some(item) = stream.next().await {
                out.push(item.map_err(|e| eyre!("stream: {e}"))?);
            }
            out
        };
        assert_eq!(drained.len(), CELL_BATCH, "all entries drained");
        assert_eq!(
            counting.batch_reads(),
            1,
            "a cold full-width chunk is ONE lower batch read"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "only the keyset meta read is a point read (lower_reads={})",
            counting.lower_reads()
        );
        Ok(())
    })
}

/// `contains_key` answers presence through the dirty overlay
/// (read-your-writes: committed/absent/uncommitted-set/uncommitted-remove/
/// set-after-clear) while never running the resolver — contrasted against
/// `get`, which resolves on the very same collection.
/// FALSIFICATION: an always-`Ok(true)` body flips every `!... .await?` assert
/// red; a `get`-delegating body (`self.get(key).await.map(|o|
/// o.is_some())`) makes `resolves.resolves()` nonzero before the `assert_eq!`
/// runs, since `contains_key(&k1)` on the seeded, resolvable key is the FIRST
/// call. Both revert to green.
#[test]
fn map_contains_key_presence_without_resolving() -> Result<()> {
    const K1: i64 = 1;
    const K2: i64 = 2;
    const K3: i64 = 3;
    const K_ABSENT: i64 = 99;

    executor::block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let descriptor =
            map_state::<I64KeyCodec, WithResolver<JsonCodec, CountingResolver>>("presence");
        let mut registry = CollectionDefRegistry::default();
        registry.register(&descriptor, CollectionDef::new(None))?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(
            cells.clone(),
            oracle.clone(),
            registry.clone(),
        ));
        let armed: ArmedKeys = Arc::default();
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("presence")?,
        );

        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let seed_session = resolve_session(
            &counting,
            &oracle,
            &registry,
            &state_key,
            &armed,
            event,
            ResolveCounter::default(),
        );
        let seed = descriptor
            .bind(&seed_session)
            .map_err(|e| eyre!("bind: {e}"))?;
        seed.set(K1, Value::from(K1))
            .await
            .map_err(|e| eyre!("{e}"))?;
        finalize_and_promote(&seed_session, &oracle, Uuid::from_u128(1), &cells, &id).await?;

        counting.reset();
        let resolves = ResolveCounter::default();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(2),
        };
        let session = resolve_session(
            &counting,
            &oracle,
            &registry,
            &state_key,
            &armed,
            event,
            resolves.clone(),
        );
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

        assert!(
            handle.contains_key(&K1).await.map_err(|e| eyre!("{e}"))?,
            "committed key is present"
        );
        assert!(
            !handle
                .contains_key(&K_ABSENT)
                .await
                .map_err(|e| eyre!("{e}"))?,
            "never-set key is absent"
        );
        handle
            .set(K2, Value::from(K2))
            .await
            .map_err(|e| eyre!("{e}"))?;
        assert!(
            handle.contains_key(&K2).await.map_err(|e| eyre!("{e}"))?,
            "uncommitted set -> true"
        );
        handle.remove(&K1).await.map_err(|e| eyre!("{e}"))?;
        assert!(
            !handle.contains_key(&K1).await.map_err(|e| eyre!("{e}"))?,
            "uncommitted remove -> false (was committed)"
        );
        handle.clear().await.map_err(|e| eyre!("{e}"))?;
        handle
            .set(K3, Value::from(K3))
            .await
            .map_err(|e| eyre!("{e}"))?;
        assert!(
            handle.contains_key(&K3).await.map_err(|e| eyre!("{e}"))?,
            "set after clear -> true"
        );
        assert_contains_presence_counts(&counting, &resolves);

        assert!(handle.get(&K3).await.map_err(|e| eyre!("{e}"))?.is_some());
        assert!(
            resolves.resolves() >= 1,
            "get resolves; contains_key did not"
        );
        Ok(())
    })
}

fn assert_contains_presence_counts(
    counting: &CountingCellStore<MemoryCellStore<ScriptedOracle>>,
    resolves: &ResolveCounter,
) {
    assert_eq!(resolves.resolves(), 0);
    assert_eq!(counting.presence_reads(), 2);
    assert_eq!(counting.batch_reads(), 0);
}

/// The key-scan resolver-skip pin: `keys()` runs the resolver zero times on
/// BOTH arms (tracked point-get and degrade scan) over a dense committed map,
/// so a message-backed map enumerates keys with no Kafka fetch. The tracked
/// arm additionally contrasts a `get()` on a present key (which DOES resolve),
/// proving the zero is a real skip on a resolvable cell — not an unresolvable
/// one.
/// FALSIFICATION: routing `MapHandle::keys` through `self.stream(dir)` (mapping
/// `(k, _)`) resolves every drained key → `resolves() == n > 0` on either arm →
/// red; routing only the `MapPlan::Scan` arm through the resolving `scan`
/// reddens the degrade arm alone. Both revert to green.
#[test]
fn map_keys_no_resolve() -> Result<()> {
    executor::block_on(async {
        // Tracked arm: keyset_limit >= n keeps the map Tracked; contrast get().
        map_keys_drain_resolves(4096, 6, true).await?;
        // Degrade arm: keyset_limit < n overflows → the full-section scan.
        map_keys_drain_resolves(2, 6, false).await?;
        Ok(())
    })
}

/// Seeds a dense `n`-entry committed map at `keyset_limit`, then over a fresh
/// cold session drains `keys()` in both directions and asserts the resolver ran
/// zero times. With `get_contrast`, also asserts a `get()` on a present key
/// resolves — so the zero above is a real skip on a resolvable cell.
async fn map_keys_drain_resolves(keyset_limit: usize, n: usize, get_contrast: bool) -> Result<()> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, WithResolver<JsonCodec, CountingResolver>>("kz");
    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &descriptor,
        CollectionDef {
            keyset_limit,
            ..CollectionDef::new(None)
        },
    )?;
    let registry = Arc::new(registry);
    let counting = CountingCellStore::new(MemoryCellStore::new(
        cells.clone(),
        oracle.clone(),
        registry.clone(),
    ));
    let armed: ArmedKeys = Arc::default();
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("kz")?,
    );

    // Seed a dense committed map (a blind `set` never resolves).
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = resolve_session(
        &counting,
        &oracle,
        &registry,
        &state_key,
        &armed,
        event,
        ResolveCounter::default(),
    );
    let seed = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    for i in 0..n {
        let key = i64::try_from(i)?;
        seed.set(key, Value::from(key))
            .await
            .map_err(|e| eyre!("{e}"))?;
    }
    finalize_and_promote(&session, &oracle, Uuid::from_u128(1), &cells, &id).await?;

    // Fresh cold session, zeroed resolve counter; drain keys() both directions.
    counting.reset();
    let resolves = ResolveCounter::default();
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = resolve_session(
        &counting,
        &oracle,
        &registry,
        &state_key,
        &armed,
        event,
        resolves.clone(),
    );
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

    assert!(!handle.is_empty().await.map_err(|e| eyre!("{e}"))?);

    for dir in [Direction::Forward, Direction::Backward] {
        let drained: Vec<i64> = {
            let stream = handle.keys(dir);
            futures::pin_mut!(stream);
            let mut out = Vec::new();
            while let Some(item) = stream.next().await {
                out.push(item.map_err(|e| eyre!("keys: {e}"))?);
            }
            out
        };
        let mut expected: Vec<i64> = (0..i64::try_from(n)?).collect();
        if dir == Direction::Backward {
            expected.reverse();
        }
        assert_eq!(
            drained, expected,
            "keys() enumerates every present key in order"
        );
    }
    assert_eq!(
        resolves.resolves(),
        0,
        "is_empty and keys resolve nothing on either arm"
    );

    if get_contrast {
        assert!(handle.get(&0).await.map_err(|e| eyre!("{e}"))?.is_some());
        assert!(resolves.resolves() >= 1, "get resolves; keys() did not");
    }
    Ok(())
}

/// A store overriding only `get_for_cache` (returning a TTL) inherits the
/// default `get_many_for_cache`, which must carry that TTL metadata through for
/// every position — the guard against defaulting the cache-fill batch to
/// `get_many` + `None` TTLs (the `commit_provisional`-wrapper bug class).
#[test]
fn forwarding_default_preserves_ttl() -> Result<()> {
    use self::support::TtlStub;

    let ttl = CompactDuration::new(3_600);
    let store = TtlStub::new(bytes(7), Some(ttl));
    let id = CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("key")),
        StateType::Application,
        StateName::try_new("entries")?,
    );
    let own = EventRef::Message {
        dedup_id: Uuid::from_u128(3),
    };
    let batch = CoordinateBatch::chunks([0u8, 1].map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("non-empty read list must yield one batch"))?;
    let got = executor::block_on(store.get_many_for_cache(&id, SECTIONS[0], &batch, own))?;
    assert_eq!(got.len(), 2, "every position answered");
    for (_, remaining) in &got {
        assert_eq!(
            *remaining,
            Some(ttl),
            "the inherited default carries the TTL through"
        );
    }
    Ok(())
}

/// Deque collection soundness over the real session lifecycle: random
/// push/pop/clear/mid-handler-commit traces with commit/abort/crash
/// outcomes keep the handle's `len`/`stream`/`get` and every `pop` return
/// value in step with a `VecDeque` oracle — the window invariant (incl. the
/// index-space reset on clear), bounds+entries crash atomicity, and the
/// at-least-once `commit()` contract (`commit()`-landed ops survive
/// abort/crash-rollback; post-commit ops roll back — so a
/// commit-then-clear-then-abort trace restores the `commit()`-landed state).
#[test]
fn prop_deque_collection_lifecycle() {
    fn property(trace: DequeTrace) -> Result<bool> {
        executor::block_on(run_deque_trace(trace, CommitMode::ReadCommitted, None))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// The deque lifecycle property in `ReadUncommitted` mode: `finalize` commits
/// everything, so every outcome that reaches it — including crash-abort —
/// converges to the full scratch model.
#[test]
fn prop_deque_collection_lifecycle_read_uncommitted() {
    fn property(trace: DequeTrace) -> Result<bool> {
        executor::block_on(run_deque_trace(trace, CommitMode::ReadUncommitted, None))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// The deque lifecycle property on a **bounded** deque (capacity 2, under the
/// push burst so eviction fires on nearly every push-to-full): the handle keeps
/// step with a `VecDeque` model that applies the identical capped-trim rule, in
/// both commit modes — so lazy push-only eviction, its rollback under
/// abort/crash, and the at-least-once `commit()` floor all hold with a cap in
/// play. The unbounded lifecycle properties above pin the `capacity = None`
/// path. FALSIFICATION: make `evictions` always return `0` (skip enforcement) →
/// after a push-to-full the handle window exceeds the trimmed model →
/// `assert_deque` mismatch → red.
#[test]
fn prop_deque_bounded_lifecycle() {
    fn property(trace: DequeTrace) -> Result<bool> {
        executor::block_on(run_deque_trace(
            trace,
            CommitMode::ReadCommitted,
            Some(BOUNDED_TEST_CAP),
        ))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// The bounded deque lifecycle property in `ReadUncommitted` mode.
#[test]
fn prop_deque_bounded_lifecycle_read_uncommitted() {
    fn property(trace: DequeTrace) -> Result<bool> {
        executor::block_on(run_deque_trace(
            trace,
            CommitMode::ReadUncommitted,
            Some(BOUNDED_TEST_CAP),
        ))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// Deque runtime-capacity convergence: over a directly-seeded over-wide (and
/// possibly holed) window, lazy push-only eviction converges to `len <= cap`
/// within the computed catch-up pushes, evicting at most `TRIM_MAX` slots per
/// push (read from the buffered dirty overlay). See
/// [`run_deque_capacity_convergence`] for the full disposition.
/// FALSIFICATION: drop `.min(TRIM_MAX)` from `evictions` → an over-wide
/// window's first push buffers `> TRIM_MAX` entry deletes → the per-push cap
/// assert → red.
#[test]
fn prop_deque_capacity_convergence() {
    fn property(shape: DequeCapacityShape) -> Result<bool> {
        executor::block_on(run_deque_capacity_convergence(shape))
    }
    QuickCheck::new().quickcheck(property as fn(DequeCapacityShape) -> Result<bool>);
}

/// Map collection soundness over the real session lifecycle: random
/// set/remove/get/clear/mid-handler-commit traces with commit/abort/crash
/// outcomes keep the handle's `get` and key-ordered `stream` in step with a
/// `BTreeMap` oracle — the current-membership keyset (cleared with the
/// entries; `KeysetPresence`), crash atomicity, the at-least-once `commit()`
/// contract (`commit()`-landed ops survive abort/crash-rollback; post-commit
/// ops roll back — so a commit-then-clear-then-abort trace restores the
/// `commit()`-landed state), and `contains_key` parity (`contains_key(k) ==
/// get(k).is_some()`) at every step.
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

/// Keyset exactness: over an arbitrary committed trace on a non-overflowing
/// map, the stored keyset decodes to exactly the live key set after every
/// settled event — `set` adds, `remove` subtracts, `clear` erases. A loose
/// superset (the pre-keyset design, or a `remove` that failed to subtract)
/// would fail here.
#[test]
fn prop_map_keyset_exact() {
    fn property(trace: MapTrace) -> Result<bool> {
        executor::block_on(run_map_keyset_exact_trace(trace))
    }
    QuickCheck::new().quickcheck(property as fn(MapTrace) -> Result<bool>);
}

/// `Map::get_many` parity: it answers each position exactly as the point `get`
/// over random populations and query lists (duplicates, absent keys, and
/// `> CELL_BATCH` lengths crossing sub-batches), in both the dirty-overlay and
/// committed arms. See `run_map_get_many_parity_trace` for why the point path
/// is a valid oracle here.
#[test]
fn prop_map_get_many_parity() {
    fn property(input: MapGetManyInput) -> Result<bool> {
        executor::block_on(run_map_get_many_parity_trace(input))
    }
    QuickCheck::new().quickcheck(property as fn(MapGetManyInput) -> Result<bool>);
}

/// Map TTL keyset-refresh: on a TTL'd map every `set` — including a re-set of
/// an already-tracked key, and once overflowed — buffers the keyset cell, so
/// its TTL is refreshed and it outlives every entry. Staged-set composition, so
/// no clock is needed.
#[test]
fn prop_map_ttl_keyset_refresh() {
    fn property(trace: MapTrace) -> Result<bool> {
        executor::block_on(run_map_ttl_keyset_refresh_trace(trace))
    }
    QuickCheck::new().quickcheck(property as fn(MapTrace) -> Result<bool>);
}

/// Map key-scan presence: over a directly-seeded map whose keyset frame
/// over-reports a TTL-expired coordinate, `keys()` yields exactly the present
/// keys in order across both arms (tracked point-get and degrade scan), and
/// agrees with `stream()` on the live key set — the presence-only key scan
/// skips a coordinate the keyset lists but the store no longer holds.
#[test]
fn prop_map_key_scan_holes() {
    fn property(shape: MapKeyHoles) -> Result<bool> {
        executor::block_on(run_map_key_scan_holes(shape))
    }
    QuickCheck::new().quickcheck(property as fn(MapKeyHoles) -> Result<bool>);
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

/// Stage event A (dedup 1) at section-0 coordinates {0, 1} over an empty base,
/// optionally recording its commit, then crash with no recovery: returns a
/// fresh store over the same warm `MemoryCells`, so A's provisional cells and
/// marker survive. The shared prologue of the two foreign-marker boundary pins;
/// each caller then stages event B and asserts the stage boundary resolved A.
async fn stage_a_then_crash(
    name: &str,
    a_committed: bool,
) -> Result<(MemoryCellStore<ScriptedOracle>, MemoryCells, CollectionId)> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let id = fresh_collection(name)?;
    let collection = CollectionRef::new(id.clone(), None);
    let store = memory_store(cells.clone(), oracle.clone());

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
    Ok((memory_store(cells.clone(), oracle), cells, id))
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
    let (store, cells, id) = stage_a_then_crash("boundary", a_committed).await?;
    let collection = CollectionRef::new(id.clone(), None);

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
    let (store, cells, id) = stage_a_then_crash("clears-only-boundary", a_committed).await?;
    let collection = CollectionRef::new(id.clone(), None);

    // Event B stages CLEARS ONLY on section 1: writes = [], marker with one
    // cleared section (no survivors).
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

/// The backend-generic publication-store contract over the memory backend.
/// The Cassandra instantiation in `state::cassandra::tests` runs the same
/// runner.
#[test]
fn prop_memory_publication_trace() {
    fn property(trace: PublicationTrace) -> Result<bool> {
        let store = MemoryPublicationStore::new();
        let token = Uuid::new_v4().to_string();
        executor::block_on(run_publication_trace(&store, &token, trace))
    }
    QuickCheck::new().quickcheck(property as fn(PublicationTrace) -> Result<bool>);
}

/// The per-partition backend over a [`CountingCellStore`], so a directed test
/// can pin the lower-store scan count a collection op issues.
type CountingBackend = PartitionBackend<
    ScriptedOracle,
    MemoryDescriptorIdentityStore,
    CountingCellStore<MemoryCellStore<ScriptedOracle>>,
>;

/// Mints a session over `counting` carrying `loader` for one event. Dropped
/// senders are fine — `watch::Receiver::borrow` keeps returning the last value.
fn session_with_loader<L>(
    counting: &CountingCellStore<MemoryCellStore<ScriptedOracle>>,
    oracle: &ScriptedOracle,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    armed: &ArmedKeys,
    event: EventRef,
    loader: L,
) -> KeyedStateSession<CountingBackend, L> {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    KeyedStateSession::new(SessionParts::<CountingBackend, _> {
        cell: counting.clone(),
        dirty: Arc::default(),
        oracle: oracle.clone(),
        loader,
        registry: registry.clone(),
        state_key: state_key.clone(),
        event,
        recovery_delay: CompactDuration::new(30),
        armed: armed.clone(),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        publisher: None,
    })
}

/// Mints a session over `counting` for one event with the default in-memory
/// loader.
pub(super) fn counting_session(
    counting: &CountingCellStore<MemoryCellStore<ScriptedOracle>>,
    oracle: &ScriptedOracle,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    armed: &ArmedKeys,
    event: EventRef,
) -> KeyedStateSession<CountingBackend, MemoryLoader<Value>> {
    session_with_loader(
        counting,
        oracle,
        registry,
        state_key,
        armed,
        event,
        MemoryLoader::new(),
    )
}

/// Binds `map_state(name)` on `session` and fully drains its `stream(dir)`.
/// Called with a fresh (clean-overlay) session so every read falls through to
/// the underlying store.
async fn drain_map_stream(
    session: &KeyedStateSession<CountingBackend, MemoryLoader<Value>>,
    name: &str,
    dir: Direction,
) -> Result<Vec<(i64, Value)>> {
    let handle = map_state::<I64KeyCodec, JsonCodec>(name)
        .bind(session)
        .map_err(|e| eyre!("bind: {e}"))?;
    let mut out = Vec::new();
    let stream = handle.stream(dir);
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// The Map keyset budget pins for the point-get arms (parity of
/// `deque_stream_issues_no_scans`): a never-written map yields nothing and
/// issues zero scans, and a small committed map streams through the batch verb
/// — zero scans, one keyset point-get, and one batch read for the entries, in
/// both directions. The liveness of these zeros is proved by
/// `map_overflowed_stream_issues_one_scan`.
#[test]
fn map_stream_issues_no_scans() -> Result<()> {
    executor::block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &map_state::<I64KeyCodec, JsonCodec>("mp"),
            CollectionDef::new(None),
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(
            cells.clone(),
            oracle.clone(),
            registry.clone(),
        ));
        let armed: ArmedKeys = Arc::default();

        // Empty-map arm: absent keyset ⇒ Empty ⇒ no scan (KeysetPresence).
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(0),
        };
        let session = counting_session(&counting, &oracle, &registry, &state_key, &armed, event);
        let drained = drain_map_stream(&session, "mp", Direction::Forward).await?;
        assert!(drained.is_empty(), "an unwritten map yields no entries");
        assert_eq!(
            counting.lower_scans(),
            0,
            "streaming an empty map must issue no lower-store scan"
        );

        // Commit a three-entry map — its keyset tracks all three keys.
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let session = counting_session(&counting, &oracle, &registry, &state_key, &armed, event);
        let handle = map_state::<I64KeyCodec, JsonCodec>("mp")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        handle.set(0, Value::from(10_i64)).await?;
        handle.set(1, Value::from(11_i64)).await?;
        handle.set(2, Value::from(12_i64)).await?;
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("mp")?,
        );
        finalize_and_promote(&session, &oracle, Uuid::from_u128(1), &cells, &id).await?;

        // Warm-Tracked arm: pure point gets in key order, both directions.
        for (n, (dir, expected)) in [
            (Direction::Forward, vec![(0_i64, 10_i64), (1, 11), (2, 12)]),
            (Direction::Backward, vec![(2_i64, 12_i64), (1, 11), (0, 10)]),
        ]
        .into_iter()
        .enumerate()
        {
            counting.reset();
            let event = EventRef::Message {
                dedup_id: Uuid::from_u128(u128::MAX - n as u128),
            };
            let session =
                counting_session(&counting, &oracle, &registry, &state_key, &armed, event);
            let out = drain_map_stream(&session, "mp", dir).await?;
            let want: Vec<(i64, Value)> = expected
                .into_iter()
                .map(|(k, v)| (k, Value::from(v)))
                .collect();
            assert_eq!(
                out, want,
                "{dir:?} Tracked stream yields the committed entries"
            );
            assert_eq!(
                counting.lower_scans(),
                0,
                "a Tracked stream must issue no lower scan"
            );
            assert_eq!(
                counting.lower_reads(),
                1,
                "the keyset cell — entries flow through the batch verb, not point get"
            );
            assert_eq!(
                counting.batch_reads(),
                1,
                "the three entries ride one lower batch read"
            );
        }
        Ok(())
    })
}

/// The overflowed-map budget pin (the liveness proof for
/// `map_stream_issues_no_scans`): a keyset-disabled map (`keyset_limit = 0`)
/// overflows on its first set and streams through **exactly one** full-section
/// scan (plus the single keyset get — bounds are gone).
#[test]
fn map_overflowed_stream_issues_one_scan() -> Result<()> {
    executor::block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &map_state::<I64KeyCodec, JsonCodec>("mp-of"),
            CollectionDef {
                keyset_limit: 0,
                ..CollectionDef::new(None)
            },
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(
            cells.clone(),
            oracle.clone(),
            registry.clone(),
        ));
        let armed: ArmedKeys = Arc::default();

        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(0),
        };
        let session = counting_session(&counting, &oracle, &registry, &state_key, &armed, event);
        map_state::<I64KeyCodec, JsonCodec>("mp-of")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?
            .set(0, Value::from(99_i64))
            .await?;
        let of_id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("mp-of")?,
        );
        finalize_and_promote(&session, &oracle, Uuid::from_u128(0), &cells, &of_id).await?;

        counting.reset();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX - 100),
        };
        let session = counting_session(&counting, &oracle, &registry, &state_key, &armed, event);
        let handle = map_state::<I64KeyCodec, JsonCodec>("mp-of")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        assert!(
            !handle.is_empty().await?,
            "a live overflowed map is not empty"
        );
        counting.reset();
        let out = drain_map_stream(&session, "mp-of", Direction::Forward).await?;
        assert_eq!(
            out,
            vec![(0_i64, Value::from(99_i64))],
            "the overflowed map streams its entry via the scan"
        );
        assert_eq!(
            counting.lower_scans(),
            1,
            "an overflowed map issues exactly one lower scan"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the single keyset get — no bound reads"
        );

        handle.remove(&0).await?;
        finalize_and_promote(
            &session,
            &oracle,
            Uuid::from_u128(u128::MAX - 100),
            &cells,
            &of_id,
        )
        .await?;
        counting.reset();
        let empty_session = counting_session(
            &counting,
            &oracle,
            &registry,
            &state_key,
            &armed,
            EventRef::Message {
                dedup_id: Uuid::from_u128(u128::MAX - 99),
            },
        );
        let empty = map_state::<I64KeyCodec, JsonCodec>("mp-of")
            .bind(&empty_session)
            .map_err(|e| eyre!("bind: {e}"))?;
        assert!(
            empty.is_empty().await?,
            "a removed overflowed entry leaves an empty map"
        );
        assert_eq!(
            counting.presence_scans(),
            1,
            "the empty overflowed map scans once"
        );
        Ok(())
    })
}

/// Binds `deque_state(name)` on `session` and fully drains its `stream(dir)`,
/// returning the yielded values. Called with a fresh (clean-overlay) session so
/// every read falls through to the underlying store.
async fn drain_deque_stream(
    session: &KeyedStateSession<CountingBackend, MemoryLoader<Value>>,
    name: &str,
    dir: Direction,
) -> Result<Vec<Value>> {
    let handle = deque_state::<JsonCodec>(name)
        .bind(session)
        .map_err(|e| eyre!("bind: {e}"))?;
    let mut out = Vec::new();
    let stream = handle.stream(dir);
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// Seeds a committed deque window of `width` entries named `name` directly into
/// `counting`'s lower store, valued by its own index so a stream-order
/// assertion is possible. It also seeds **one entry at index `width`, which
/// sits deliberately outside the seeded `[0, width)` bounds**.
///
/// That extra row makes the wide arm's range bound falsifiable. The entries
/// section then holds a row that the window does not. A scan over the whole
/// section, instead of exactly `[head, tail − 1]`, would yield that row.
async fn seed_wide_deque(
    counting: &CountingCellStore<MemoryCellStore<ScriptedOracle>>,
    state_key: &StateKey,
    name: &str,
    width: usize,
) -> Result<()> {
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new(name)?,
    );
    let wide_ref = CollectionRef::new(id, None);
    let mut seeded = vec![(
        deque::meta_cell(),
        Some(Bytes::from(deque::seed_frame(0, i64::try_from(width)?))),
    )];
    // `0..=width`: the last entry sits past `tail`, outside the window.
    for i in 0..=width {
        let index = i64::try_from(i)?;
        seeded.push((
            deque::entry_cell_for(&I64KeyCodec::encode(&index)),
            Some(Bytes::from(serde_json::to_vec(&Value::from(index))?)),
        ));
    }
    counting.write_resolved(&wide_ref, &seeded, &[]).await?;
    Ok(())
}

/// Sub-threshold deque iteration streams through the batch verb: a small
/// committed deque issues **zero** lower-store scans, one bounds point-get, and
/// one batch read for the entries, in both directions. The test then hands the
/// same fixture to [`assert_wide_deque_scan_is_window_bounded`], which pins the
/// fallback arm.
#[test]
fn deque_stream_issues_no_scans() -> Result<()> {
    executor::block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let mut registry = CollectionDefRegistry::default();
        registry.register(&deque_state::<JsonCodec>("dq"), CollectionDef::new(None))?;
        registry.register(
            &deque_state::<JsonCodec>("dq-wide"),
            CollectionDef::new(None),
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(
            cells.clone(),
            oracle.clone(),
            registry.clone(),
        ));
        let armed: ArmedKeys = Arc::default();

        // One committed event of pushes and pops: the deque reads [0, 1, 2].
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let session = counting_session(&counting, &oracle, &registry, &state_key, &armed, event);
        let handle = deque_state::<JsonCodec>("dq")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        handle.push_back(Value::from(1_u8)).await?;
        handle.push_back(Value::from(2_u8)).await?;
        handle.push_back(Value::from(9_u8)).await?;
        handle.push_front(Value::from(0_u8)).await?;
        assert_eq!(handle.pop_back().await?, Some(Value::from(9_u8)));
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("dq")?,
        );
        finalize_and_promote(&session, &oracle, Uuid::from_u128(1), &cells, &id).await?;

        // Stream in both directions; each is a pure sequence of point gets.
        for (n, (dir, expected)) in [
            (Direction::Forward, [0_u8, 1, 2]),
            (Direction::Backward, [2_u8, 1, 0]),
        ]
        .into_iter()
        .enumerate()
        {
            counting.reset();
            let event = EventRef::Message {
                dedup_id: Uuid::from_u128(u128::MAX - n as u128),
            };
            let session =
                counting_session(&counting, &oracle, &registry, &state_key, &armed, event);
            let out = drain_deque_stream(&session, "dq", dir).await?;
            let expected: Vec<Value> = expected.into_iter().map(Value::from).collect();
            assert_eq!(out, expected, "{dir:?} stream yields the committed window");
            assert_eq!(
                counting.lower_scans(),
                0,
                "a sub-threshold stream must issue no lower scan"
            );
            assert_eq!(
                counting.lower_reads(),
                1,
                "the bounds cell — entries flow through the batch verb, not point get"
            );
            assert_eq!(
                counting.batch_reads(),
                1,
                "the window's entries ride one lower batch read"
            );
        }

        assert_wide_deque_scan_is_window_bounded(&counting, &oracle, &registry, &state_key, &armed)
            .await
    })
}

/// The wide-window companion of [`deque_stream_issues_no_scans`]. A
/// directly-seeded window one entry wider than
/// [`deque::DEQUE_POINT_ITERATION_MAX`] falls back to exactly one lower scan.
/// That count proves the sub-threshold zero is a live counter. The window
/// bounds that scan, not the section.
///
/// [`seed_wide_deque`] plants one row past `tail`. A scan over the whole
/// section, rather than `[head, tail − 1]`, would yield that row. This helper
/// drains both directions, because a regression that keeps the limit but drops
/// the edges hides forward and shows backward. Forward, the limit stops the
/// walk short of the extra row. Backward, the extra row becomes the first item.
async fn assert_wide_deque_scan_is_window_bounded(
    counting: &CountingCellStore<MemoryCellStore<ScriptedOracle>>,
    oracle: &ScriptedOracle,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    armed: &ArmedKeys,
) -> Result<()> {
    let width = deque::DEQUE_POINT_ITERATION_MAX + 1;
    seed_wide_deque(counting, state_key, "dq-wide", width).await?;
    let ascending: Vec<Value> = (0..width)
        .map(i64::try_from)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(Value::from)
        .collect();

    for (n, dir) in [Direction::Forward, Direction::Backward]
        .into_iter()
        .enumerate()
    {
        counting.reset();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX - 2 - n as u128),
        };
        let session = counting_session(counting, oracle, registry, state_key, armed, event);
        let drained = drain_deque_stream(&session, "dq-wide", dir).await?;
        let mut expected = ascending.clone();
        if dir == Direction::Backward {
            expected.reverse();
        }
        assert_eq!(
            drained, expected,
            "the wide {dir:?} scan streams exactly the window's entries, in order"
        );
        assert_eq!(
            counting.lower_scans(),
            1,
            "a wide window pays exactly one lower scan"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the wide arm reads only the bounds cell"
        );
    }
    Ok(())
}

/// A dense stream-laziness case: a collection of `n` entries drained
/// `stream(..).take(k)`, with `n` on the deque's point-get arm (`≤ 128`) and
/// far above `k`, so "fetch/resolve only the consumed prefix" is a strictly
/// stronger claim than "fetch everything".
#[derive(Clone, Copy, Debug)]
struct StreamPrefix {
    n: usize,
    k: usize,
}

impl Arbitrary for StreamPrefix {
    fn arbitrary(g: &mut Gen) -> Self {
        // 48..=127: dense, ≤ DEQUE_POINT_ITERATION_MAX (128) so the deque stays
        // on the chunked point-get arm, and always > k + one chunk width (16) so the
        // "materialize everything" defect is observable.
        let n = 48 + usize::arbitrary(g) % 80;
        // 1..=12: well under one chunk width and far under n.
        let k = 1 + usize::arbitrary(g) % 12;
        Self { n, k }
    }
}

/// Mints a session over `counting` carrying a [`ResolveCounter`] loader, so a
/// stream-laziness pin can bound resolutions independently of fetches.
fn resolve_session(
    counting: &CountingCellStore<MemoryCellStore<ScriptedOracle>>,
    oracle: &ScriptedOracle,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    armed: &ArmedKeys,
    event: EventRef,
    loader: ResolveCounter,
) -> KeyedStateSession<CountingBackend, ResolveCounter> {
    session_with_loader(counting, oracle, registry, state_key, armed, event, loader)
}

/// The stream-laziness property (map): a `stream(dir).take(k)` over a **dense**
/// `n`-entry `Tracked` map is genuinely incremental. It issues at most one
/// batch read beyond `k`, because entries flow through the batch verb and only
/// the keyset meta cell is a point read. It resolves at most `k + CELL_BATCH`
/// values, never the whole `n`-entry collection. The counting store bounds the
/// fetches and the counting resolver bounds the resolutions. Both counters sit
/// at the lowest layer, so nothing masks a materialization.
/// FALSIFICATION: widen `keys.by_ref().take(CELL_BATCH)` in
/// `CoordinatePlan::entry_source` to `.take(usize::MAX)` (drain every tracked
/// key in one chunk) → `take(k)` fetches and resolves all `n` → `batch_reads ==
/// n.div_ceil(16)` and `resolves == n`, both over their bounds for `n ≫ k` →
/// red. (Inflating `CELL_BATCH` itself cannot falsify: the assertion bound
/// moves with it.)
async fn run_map_stream_prefix_lazy(n: usize, k: usize, dir: Direction) -> Result<()> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, WithResolver<JsonCodec, CountingResolver>>("lz");
    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &descriptor,
        CollectionDef {
            // ≥ n so the map stays Tracked (the dense point-get arm).
            keyset_limit: 4096,
            ..CollectionDef::new(None)
        },
    )?;
    let registry = Arc::new(registry);
    let counting = CountingCellStore::new(MemoryCellStore::new(
        cells.clone(),
        oracle.clone(),
        registry.clone(),
    ));
    let armed: ArmedKeys = Arc::default();
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("lz")?,
    );

    // Seed a dense committed map of n entries (a blind `set` never resolves).
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = resolve_session(
        &counting,
        &oracle,
        &registry,
        &state_key,
        &armed,
        event,
        ResolveCounter::default(),
    );
    let seed = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    for i in 0..n {
        let key = i64::try_from(i)?;
        seed.set(key, Value::from(key))
            .await
            .map_err(|e| eyre!("{e}"))?;
    }
    finalize_and_promote(&session, &oracle, Uuid::from_u128(1), &cells, &id).await?;

    // Fresh cold session, zeroed counters; drain only the k-prefix.
    counting.reset();
    let resolves = ResolveCounter::default();
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = resolve_session(
        &counting,
        &oracle,
        &registry,
        &state_key,
        &armed,
        event,
        resolves.clone(),
    );
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    let taken: Vec<_> = {
        let stream = handle.stream(dir).take(k);
        futures::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(|e| eyre!("stream: {e}"))?);
        }
        out
    };
    assert_eq!(
        taken.len(),
        k.min(n),
        "take(k) yields exactly k.min(n) entries"
    );
    assert!(
        counting.batch_reads() <= k.div_ceil(CELL_BATCH) + 1,
        "a lazy map take(k) issues at most one batch read beyond k (batches={}, k={k}, n={n})",
        counting.batch_reads()
    );
    assert_eq!(
        counting.lower_reads(),
        1,
        "entries flow through the batch verb, not point get; only the keyset meta read remains a \
         point read (lower_reads={})",
        counting.lower_reads()
    );
    assert!(
        resolves.resolves() <= k + CELL_BATCH,
        "a lazy map take(k) resolves at most k + one chunk (resolves={}, k={k}, n={n})",
        resolves.resolves()
    );
    Ok(())
}

/// The stream-laziness property (deque): the structural twin of
/// [`run_map_stream_prefix_lazy`] over a dense `n`-entry window on the
/// point-get arm — at most one batch read beyond `k` (entries flow through the
/// batch verb; only the bounds meta cell is a point read) and at most
/// `k + CELL_BATCH` resolved. FALSIFICATION: widen
/// `keys.by_ref().take(CELL_BATCH)` in `CoordinatePlan::entry_source` to
/// `.take(usize::MAX)` (fetch the whole window in one chunk) →
/// `batch_reads == n.div_ceil(16)` and `resolves == n`, both over their bounds
/// for `n ≫ k` → red. (Inflating `CELL_BATCH` itself cannot falsify: the
/// assertion bound moves with it.)
async fn run_deque_stream_prefix_lazy(n: usize, k: usize, dir: Direction) -> Result<()> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<WithResolver<JsonCodec, CountingResolver>>("lz");
    let mut registry = CollectionDefRegistry::default();
    registry.register(&descriptor, CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let counting = CountingCellStore::new(MemoryCellStore::new(
        cells.clone(),
        oracle.clone(),
        registry.clone(),
    ));
    let armed: ArmedKeys = Arc::default();
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("lz")?,
    );

    // Seed a dense committed window of n entries (a blind `push_back` never
    // resolves).
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = resolve_session(
        &counting,
        &oracle,
        &registry,
        &state_key,
        &armed,
        event,
        ResolveCounter::default(),
    );
    let seed = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    for i in 0..n {
        seed.push_back(Value::from(i64::try_from(i)?))
            .await
            .map_err(|e| eyre!("{e}"))?;
    }
    finalize_and_promote(&session, &oracle, Uuid::from_u128(1), &cells, &id).await?;

    counting.reset();
    let resolves = ResolveCounter::default();
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = resolve_session(
        &counting,
        &oracle,
        &registry,
        &state_key,
        &armed,
        event,
        resolves.clone(),
    );
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    let taken: Vec<_> = {
        let stream = handle.stream(dir).take(k);
        futures::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(|e| eyre!("stream: {e}"))?);
        }
        out
    };
    assert_eq!(
        taken.len(),
        k.min(n),
        "take(k) yields exactly k.min(n) elements"
    );
    assert!(
        counting.batch_reads() <= k.div_ceil(CELL_BATCH) + 1,
        "a lazy deque take(k) issues at most one batch read beyond k (batches={}, k={k}, n={n})",
        counting.batch_reads()
    );
    assert_eq!(
        counting.lower_reads(),
        1,
        "entries flow through the batch verb, not point get; only the bounds meta read remains a \
         point read (lower_reads={})",
        counting.lower_reads()
    );
    assert!(
        resolves.resolves() <= k + CELL_BATCH,
        "a lazy deque take(k) resolves at most k + one chunk (resolves={}, k={k}, n={n})",
        resolves.resolves()
    );
    Ok(())
}

/// A bounded push evicts the opposite end **decode-free**: no value decode, no
/// resolver run for the discarded slot. Over a message-backed
/// (resolver-carrying) deque capped at one slot, a `push_back` that evicts the
/// front resolves **nothing** — a blind push never resolves, so a nonzero count
/// could come only from the eviction path reading the evicted slot.
/// FALSIFICATION: change the eviction `entries.clear` to an `entries.get` then
/// `clear` (as `pop_front` does) → the evicted slot resolves through
/// `CountingResolver` → `resolves() == 1 != 0` → red. The `resolves()` assert
/// is read **before** the survivor peek (which does resolve), so nothing masks
/// it.
#[test]
fn deque_bounded_eviction_does_not_resolve() -> Result<()> {
    executor::block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let descriptor = deque_state::<WithResolver<JsonCodec, CountingResolver>>("cap");
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &descriptor,
            CollectionDef {
                capacity: Some(NonZeroUsize::MIN),
                ..CollectionDef::new(None)
            },
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(
            cells.clone(),
            oracle.clone(),
            registry.clone(),
        ));
        let armed: ArmedKeys = Arc::default();
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("cap")?,
        );

        // Seed a committed one-element window (a blind `push_back` never resolves).
        let seed_event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let session = resolve_session(
            &counting,
            &oracle,
            &registry,
            &state_key,
            &armed,
            seed_event,
            ResolveCounter::default(),
        );
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        handle
            .push_back(Value::from(1_u8))
            .await
            .map_err(|e| eyre!("{e}"))?;
        finalize_and_promote(&session, &oracle, Uuid::from_u128(1), &cells, &id).await?;

        // Fresh event: push a second value, evicting the front (the capacity is 1).
        let resolves = ResolveCounter::default();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(2),
        };
        let session = resolve_session(
            &counting,
            &oracle,
            &registry,
            &state_key,
            &armed,
            event,
            resolves.clone(),
        );
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        handle
            .push_back(Value::from(2_u8))
            .await
            .map_err(|e| eyre!("{e}"))?;
        assert_eq!(
            resolves.resolves(),
            0,
            "a bounded push resolves nothing — the eviction is decode/resolver-free"
        );

        // Only now (after the assert) read the survivor, which does resolve.
        let survivor = handle.peek_front().await.map_err(|e| eyre!("{e}"))?;
        assert_eq!(
            survivor,
            Some(Value::from(2_u8)),
            "the newest element survives"
        );
        Ok(())
    })
}

/// The stream-laziness property: both collections' `stream(dir).take(k)` are
/// genuinely incremental — the fetch/resolve budget tracks the consumed prefix,
/// not the collection size. A `QuickCheck` property over dense `(n, k)` in both
/// directions.
#[test]
fn stream_take_is_lazy() {
    fn property(input: StreamPrefix) -> Result<bool> {
        let StreamPrefix { n, k } = input;
        executor::block_on(async move {
            for dir in [Direction::Forward, Direction::Backward] {
                run_map_stream_prefix_lazy(n, k, dir).await?;
                run_deque_stream_prefix_lazy(n, k, dir).await?;
            }
            Ok(true)
        })
    }
    QuickCheck::new().quickcheck(property as fn(StreamPrefix) -> Result<bool>);
}

/// The `StreamYieldFree` interleaving property (map): random
/// `next()`/mutator interleavings on one live session never deadlock or error
/// and stay weakly consistent with the init snapshot. A fresh `current_thread`
/// runtime with a time driver per iteration powers the hang-guard; no sleeps.
#[test]
fn map_stream_interleave_is_yield_free() {
    fn property(input: MapInterleave) -> Result<bool> {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|e| eyre!("runtime: {e}"))?
            .block_on(run_map_stream_interleave(input))
    }
    QuickCheck::new().quickcheck(property as fn(MapInterleave) -> Result<bool>);
}

/// The `StreamYieldFree` interleaving property (deque): the structural twin.
#[test]
fn deque_stream_interleave_is_yield_free() {
    fn property(input: DequeInterleave) -> Result<bool> {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|e| eyre!("runtime: {e}"))?
            .block_on(run_deque_stream_interleave(input))
    }
    QuickCheck::new().quickcheck(property as fn(DequeInterleave) -> Result<bool>);
}
