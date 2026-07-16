mod cached_suite;
pub(crate) mod cell_suite;
mod collection_suite;
mod gate_suite;
pub(crate) mod identity_suite;
pub(crate) mod support;

use self::cell_suite::{
    ApplyTrace, BatchReadTrace, FailingCellStore, MemoryShapeProbe, OverlayTrace, OverwriteTrace,
    PoisonHandle, ScanTrace, ScriptedOracle, Trace, run_apply_idempotence, run_batch_alignment,
    run_batch_duplicate_co_observation, run_batch_read_parity_trace, run_bottom_scan_trace,
    run_crash_equivalence_trace, run_overlay_precedence_pin, run_overlay_trace,
    run_overwrite_trace,
};
use self::cell_suite::{SECTIONS, bytes, cell_in};
use self::collection_suite::{
    DequeHoles, DequeInterleave, DequeTrace, MapGetManyInput, MapInterleave, MapTrace,
    finalize_and_promote, run_deque_holes, run_deque_stream_interleave, run_deque_trace,
    run_map_get_many_parity_trace, run_map_keyset_exact_trace, run_map_stream_interleave,
    run_map_trace, run_map_ttl_keyset_refresh_trace,
};
use self::support::{CountingCellStore, CountingResolver, ResolveCounter, fresh_collection};
use super::cell::ProvisionalWrite;
use super::descriptor::{
    STREAM_CHUNK, StateDescriptor, WithResolver, deque, deque_state, map_state,
};
use super::manager::ArmedKeys;
use super::marker::{EventMarker, SectionClear};
use super::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use super::oracle::CommitOracle;
use super::order_codec::{I64KeyCodec, OrderedKeyCodec};
use super::registry::{CollectionDef, CollectionDefRegistry};
use super::session::{KeyedStateSession, SessionParts, TerminationWatch};
use super::store::{CELL_BATCH, CellStore, CoordinateBatch, dedupe};
use super::{
    CollectionId, CollectionRef, CommitMode, Coordinate, Direction, EventRef, PartitionBackend,
    StateKey, StateName, StateType,
};
use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::StreamExt;
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::Value;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::sync::watch;
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
        executor::block_on(run_batch_read_parity_trace(store, oracle, trace))
    }
    QuickCheck::new().quickcheck(property as fn(BatchReadTrace) -> Result<bool>);
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
/// FALSIFICATION: revert `coordinate_source` to the per-key point-`get` loop →
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
        let mut registry = CollectionDefRegistry::new(None);
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
        assert!(
            counting.lower_reads() <= 1,
            "only the keyset meta read is a point read (lower_reads={})",
            counting.lower_reads()
        );
        Ok(())
    })
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
/// set/remove/get/clear/mid-handler-commit traces with commit/abort/crash
/// outcomes keep the handle's `get` and key-ordered `stream` in step with a
/// `BTreeMap` oracle — the current-membership keyset (cleared with the
/// entries; `KeysetPresence`), crash atomicity, and the at-least-once
/// `commit()` contract (`commit()`-landed ops survive abort/crash-rollback;
/// post-commit ops roll back — so a commit-then-clear-then-abort trace restores
/// the `commit()`-landed state).
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
    })
}

/// Mints a session over `counting` for one event with the default in-memory
/// loader.
fn counting_session(
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
        let mut registry = CollectionDefRegistry::new(None);
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
        let mut registry = CollectionDefRegistry::new(None);
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
/// assertion is possible.
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
    for i in 0..width {
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
/// one batch read for the entries, in both directions. The companion
/// wide-window assertions prove the counters are live and pin the fallback: a
/// directly-seeded window wider than [`deque::DEQUE_POINT_ITERATION_MAX`]
/// streams every entry in order through exactly one lower scan (plus the one
/// bounds get).
#[test]
fn deque_stream_issues_no_scans() -> Result<()> {
    executor::block_on(async {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let mut registry = CollectionDefRegistry::new(None);
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

        // Companion: a directly-seeded window one wider than the threshold
        // falls back to exactly one lower scan — proving the zero above is a
        // live counter and pinning the fallback arm streams every entry in
        // order.
        let width = deque::DEQUE_POINT_ITERATION_MAX + 1;
        seed_wide_deque(&counting, &state_key, "dq-wide", width).await?;

        counting.reset();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX - 2),
        };
        let session = counting_session(&counting, &oracle, &registry, &state_key, &armed, event);
        let drained = drain_deque_stream(&session, "dq-wide", Direction::Forward).await?;
        let expected: Vec<Value> = (0..width)
            .map(i64::try_from)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(Value::from)
            .collect();
        assert_eq!(
            drained, expected,
            "the wide window streams every seeded entry in order"
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
        Ok(())
    })
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
/// `n`-entry `Tracked` map is genuinely incremental — it issues at most one
/// batch read beyond `k` (entries flow through the batch verb; only the keyset
/// meta cell is a point read) and resolves at most `k + STREAM_CHUNK` values,
/// never the whole `n`-entry collection. The counting store bounds fetches and
/// the counting resolver bounds resolutions; both counters sit at the lowest
/// layer, so nothing masks a materialization.
/// FALSIFICATION: widen `coords.by_ref().take(STREAM_CHUNK)` in
/// `coordinate_source` to `.take(usize::MAX)` (drain every tracked key in one
/// chunk) → `take(k)` fetches and resolves all `n` → `batch_reads ==
/// n.div_ceil(16)` and `resolves == n`, both over their bounds for `n ≫ k` →
/// red. (Inflating `STREAM_CHUNK` itself cannot falsify: the assertion bound
/// moves with it.)
async fn run_map_stream_prefix_lazy(n: usize, k: usize, dir: Direction) -> Result<()> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, WithResolver<JsonCodec, CountingResolver>>("lz");
    let mut registry = CollectionDefRegistry::new(None);
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
        counting.batch_reads() <= k.div_ceil(STREAM_CHUNK) + 1,
        "a lazy map take(k) issues at most one batch read beyond k (batches={}, k={k}, n={n})",
        counting.batch_reads()
    );
    assert!(
        counting.lower_reads() <= 1,
        "entries flow through the batch verb, not point get; only the keyset meta read remains a \
         point read (lower_reads={})",
        counting.lower_reads()
    );
    assert!(
        resolves.resolves() <= k + STREAM_CHUNK,
        "a lazy map take(k) resolves at most k + one chunk (resolves={}, k={k}, n={n})",
        resolves.resolves()
    );
    Ok(())
}

/// The stream-laziness property (deque): the structural twin of
/// [`run_map_stream_prefix_lazy`] over a dense `n`-entry window on the
/// point-get arm — at most one batch read beyond `k` (entries flow through the
/// batch verb; only the bounds meta cell is a point read) and at most
/// `k + STREAM_CHUNK` resolved. FALSIFICATION: widen
/// `coords.by_ref().take(STREAM_CHUNK)` in `coordinate_source` to
/// `.take(usize::MAX)` (fetch the whole window in one chunk) →
/// `batch_reads == n.div_ceil(16)` and `resolves == n`, both over their bounds
/// for `n ≫ k` → red. (Inflating `STREAM_CHUNK` itself cannot falsify: the
/// assertion bound moves with it.)
async fn run_deque_stream_prefix_lazy(n: usize, k: usize, dir: Direction) -> Result<()> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<WithResolver<JsonCodec, CountingResolver>>("lz");
    let mut registry = CollectionDefRegistry::new(None);
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
        counting.batch_reads() <= k.div_ceil(STREAM_CHUNK) + 1,
        "a lazy deque take(k) issues at most one batch read beyond k (batches={}, k={k}, n={n})",
        counting.batch_reads()
    );
    assert!(
        counting.lower_reads() <= 1,
        "entries flow through the batch verb, not point get; only the bounds meta read remains a \
         point read (lower_reads={})",
        counting.lower_reads()
    );
    assert!(
        resolves.resolves() <= k + STREAM_CHUNK,
        "a lazy deque take(k) resolves at most k + one chunk (resolves={}, k={k}, n={n})",
        resolves.resolves()
    );
    Ok(())
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
