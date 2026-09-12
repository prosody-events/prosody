use crate::state::CommitDecision;
use crate::state::store::CellRead;
use crate::state::store::CommittedBatch;
use crate::state::tests::support::{StageInspection, evidence};
use crate::test_util::TEST_RUNTIME;
mod cached_suite;
pub(crate) mod cell_suite;
pub(crate) mod collection_suite;
mod gate_suite;
pub(crate) mod identity_suite;
pub(crate) mod publication_suite;
pub(crate) mod support;

use self::cell_suite::{
    ApplyTrace, BatchReadTrace, FailingCellStore, MemoryDeduplicationStore, MemoryShapeProbe,
    OverlayTrace, OverwriteTrace, PoisonHandle, RawBatchTrace, ScanTrace, Trace,
    run_apply_idempotence, run_batch_alignment, run_batch_duplicate_co_observation,
    run_batch_read_parity_trace, run_blind_write_leaves_clears_free_marker, run_bottom_scan_trace,
    run_crash_equivalence_trace, run_overlay_precedence_pin, run_overlay_trace,
    run_overwrite_trace, run_raw_batch_ascending_output, run_raw_batch_no_side_effects,
    run_raw_batch_parity_trace,
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
use self::support::{CountingCellStore, CountingResolver, ResolveCounter, fresh_collection};
use super::cell::{Cell, Committed, ProvisionalWrite, Values};
use super::cell_key::CellKey;
use super::descriptor::{StateDescriptor, WithResolver, deque, deque_state, map_state};
use super::marker::EventMarker;
use super::memory::{
    MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore,
};
use super::order_codec::{I64KeyCodec, OrderedKeyCodec};
use super::registry::{CollectionDef, CollectionDefRegistry};
use super::resolve::{EvidenceLookup, resolve_event_marker};
use super::session::{KeyedStateSession, SessionParts, TerminationWatch};
use super::store::{CELL_BATCH, CellBuffer, CellStore, CoordinateBatch, dedupe};
use super::{
    CELLS_INLINE, CollectionId, CollectionRef, CommitMode, Coordinate, Direction, EventRef,
    PartitionBackend, StateKey, StateName, StateType,
};
use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::{StreamExt, pin_mut, stream};
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

/// A crash preserves committed values across stage and promote cuts.
/// Admission resolves residue before the next event.
#[test]
fn prop_memory_cell_crash_equivalence() {
    fn property(trace: Trace) -> Result<bool> {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let make = |lower: &PoisonHandle| {
            Ok(FailingCellStore::with_handle(
                MemoryCellStore::new(cells.clone()),
                lower.clone(),
            ))
        };
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

/// Posture-parity test over the memory store: a blind `write_resolved` leaves a
/// unsettled clears-FREE marker unsettled (the boundary triggers on clears
/// only).
#[test]
fn blind_write_leaves_clears_free_marker() -> Result<()> {
    let cells = MemoryCells::new();
    let store = MemoryCellStore::new(cells.clone());
    let probe = MemoryShapeProbe(cells);
    TEST_RUNTIME.block_on(run_blind_write_leaves_clears_free_marker(store, &probe))
}

/// Implicit-overwrite soundness over the memory cell store: a sequence of
/// events that never promote or roll back explicitly converges every cell to
/// the model, each overwrite resolving its predecessor's provisional cell
/// through collection evidence on read.
#[test]
fn prop_memory_cell_implicit_overwrite() {
    fn property(trace: OverwriteTrace) -> Result<bool> {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let make = || Ok(MemoryCellStore::new(cells.clone()));
        TEST_RUNTIME.block_on(run_overwrite_trace(make, dedup.clone(), trace))
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
        let lower = MemoryCellStore::new(MemoryCells::new());
        TEST_RUNTIME.block_on(run_overlay_trace(lower, trace))
    }
    QuickCheck::new().quickcheck(property as fn(OverlayTrace) -> Result<bool>);
}

/// Both memory scan projections match the committed model across bounds and
/// section clears.
#[test]
fn prop_memory_bottom_scan() {
    fn property(trace: ScanTrace) -> Result<bool> {
        let cells = MemoryCells::new();
        let store = MemoryCellStore::new(cells.clone());
        let probe = MemoryShapeProbe(cells);
        TEST_RUNTIME.block_on(run_bottom_scan_trace(store, trace, &probe))
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
/// observe client-side dedup, so it is verified directly here).
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
        let store = MemoryCellStore::new(MemoryCells::new());
        TEST_RUNTIME.block_on(run_batch_read_parity_trace(store, trace))
    }
    QuickCheck::new().quickcheck(property as fn(BatchReadTrace) -> Result<bool>);
}

/// Raw-provisional batch parity over the memory store: `provisional_many`
/// returns exactly the survivors the sequential `provisional_cell_at` loop
/// does.
#[test]
fn prop_memory_raw_batch_parity() {
    fn property(trace: RawBatchTrace) -> Result<bool> {
        let store = MemoryCellStore::new(MemoryCells::new());
        TEST_RUNTIME.block_on(run_raw_batch_parity_trace(store, trace))
    }
    QuickCheck::new().quickcheck(property as fn(RawBatchTrace) -> Result<bool>);
}

/// Ascending-output test over the memory store (deterministic): the sort in
/// `provisional_point_loop` is load-bearing here — without it the output
/// collapses to input byte order.
#[test]
fn memory_raw_batch_ascending_output() -> Result<()> {
    let store = MemoryCellStore::new(MemoryCells::new());
    TEST_RUNTIME.block_on(run_raw_batch_ascending_output(store))
}

/// Raw batch reads leave provisional rows unchanged.
#[test]
fn memory_raw_batch_no_side_effects() -> Result<()> {
    let store = MemoryCellStore::new(MemoryCells::new());
    TEST_RUNTIME.block_on(run_raw_batch_no_side_effects(store))
}

/// Within-batch duplicate co-observation + scatter alignment over the memory
/// store (deterministic).
#[test]
fn memory_batch_duplicate_co_observation() -> Result<()> {
    let store = MemoryCellStore::new(MemoryCells::new());
    TEST_RUNTIME.block_on(run_batch_duplicate_co_observation(store))
}

/// Every input position is answered over two chunks (deterministic alignment).
#[test]
fn memory_batch_alignment() -> Result<()> {
    let store = MemoryCellStore::new(MemoryCells::new());
    TEST_RUNTIME.block_on(run_batch_alignment(store))
}

/// Proves that marker resolution reads provisional cells in bounded batches.
///
/// The test expects three batch reads, no point reads, and one event check.
///
/// Falsification: Replace `resolve_event_marker` batch reads with point reads.
/// Then `raw_batch_reads` becomes zero and the read-count asserts fail.
#[test]
fn memory_resolve_event_marker_batches_reads() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
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
        let marker = EventMarker::frozen(event, &writes, &[], &evidence([].into(), None));
        counting
            .write_provisional(&cref, &writes, Some(&marker))
            .await
            .map_err(|e| eyre!("stage: {e}"))?;

        // Count raw batch reads during a committed resolution.
        counting.reset();
        resolve_event_marker(&counting, &cref, &marker, CommitDecision::Committed)
            .await
            .map_err(|e| eyre!("resolve_event_marker: {e}"))?;

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

        Ok(())
    })
}

/// Proves that marker resolution keeps the section for each coordinate.
///
/// Two sections contain different values at coordinate 7.
/// Each value must remain in its original section.
///
/// Falsification: Use `SECTIONS[0]` for each `section_batches` result.
/// Then one survivor stays provisional and the `remaining.is_empty()` assert
/// fails.
#[test]
fn resolve_event_marker_rekeys_survivors_by_section() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let store = MemoryCellStore::new(MemoryCells::new());
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
        let marker = EventMarker::frozen(event, &writes, &[], &evidence([].into(), None));
        store
            .write_provisional(&cref, &writes, Some(&marker))
            .await
            .map_err(|e| eyre!("stage: {e}"))?;

        resolve_event_marker(&store, &cref, &marker, CommitDecision::Committed)
            .await
            .map_err(|e| eyre!("resolve_event_marker: {e}"))?;

        // Check raw rows: visible reads can hide unresolved provisional cells.
        let remaining = drain_memory_provisional(&store, &id)
            .await
            .map_err(|e| eyre!("drain: {e}"))?;
        assert!(
            remaining.is_empty(),
            "both survivors are resolved by resolve_event_marker, none left provisional: \
             {remaining:?}"
        );

        // Resolved cells return the committed value directly.
        assert_eq!(
            CellRead::<Values>::read(&store, &id, &cell_in(0, 7))
                .await
                .map(|(committed, _)| committed)
                .map_err(|e| eyre!("get s0: {e}"))?,
            Committed::new(Some(bytes(70))),
            "the section-0 survivor commits at (0, 7)"
        );
        assert_eq!(
            CellRead::<Values>::read(&store, &id, &cell_in(1, 7))
                .await
                .map(|(committed, _)| committed)
                .map_err(|e| eyre!("get s1: {e}"))?,
            Committed::new(Some(bytes(90))),
            "the section-1 survivor commits at (1, 7), not collided onto (0, 7)"
        );
        Ok(())
    })
}

/// Reads the provisional cells listed by the memory store's current marker,
/// for the recovery tests that assert nothing is left provisional.
async fn drain_memory_provisional<S: CellStore>(
    store: &S,
    id: &CollectionId,
) -> Result<Vec<CellKey>, S::Error> {
    let stream = store.staged_cells(id);
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?.0);
    }
    Ok(out)
}

/// A dirty `Set` inside an unsettled dirty section-clear answers its bytes
/// through `Overlay::get_many` (precedence + duplicate co-observation), and the
/// dirty-answered positions never reach the lower batch.
#[test]
fn memory_overlay_precedence_set_beats_section_clear() -> Result<()> {
    let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
    TEST_RUNTIME.block_on(run_overlay_precedence_pin(counting))
}

/// A cold, dense `CELL_BATCH`-entry `Tracked` map streamed to exhaustion issues
/// exactly ONE lower batch read for its entries — a full-width scan chunk is
/// one [`CoordinateBatch`], one lower `get_many`; only the keyset meta cell
/// stays a point read.
/// Falsification: Replace `CoordinatePlan` batch reads with per-key `get`
/// calls. Then `batch_reads` becomes zero and both read-count asserts fail.
#[test]
fn map_cold_chunk_is_one_batch_read() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
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
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
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
            &dedup,
            &registry,
            &state_key,
            event,
            ResolveCounter::default(),
        );
        let seed = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        for i in 0..CELL_BATCH as i64 {
            seed.set(i, Value::from(i))
                .await
                .map_err(|e| eyre!("{e}"))?;
        }
        finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

        // Fresh cold session, zeroed counters; drain the complete stream.
        counting.reset();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(2),
        };
        let session = resolve_session(
            &counting,
            &dedup,
            &registry,
            &state_key,
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
///
/// Falsification: Make `contains_key` always return `Ok(true)`.
/// Then absent-key checks return true and the presence asserts fail.
#[test]
fn map_contains_key_presence_without_resolving() -> Result<()> {
    const K1: i64 = 1;
    const K2: i64 = 2;
    const K3: i64 = 3;
    const K_ABSENT: i64 = 99;

    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let descriptor =
            map_state::<I64KeyCodec, WithResolver<JsonCodec, CountingResolver>>("presence");
        let mut registry = CollectionDefRegistry::default();
        registry.register(&descriptor, CollectionDef::new(None))?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("presence")?,
        );

        // Seed one committed present key.
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let seed_session = resolve_session(
            &counting,
            &dedup,
            &registry,
            &state_key,
            event,
            ResolveCounter::default(),
        );
        let seed = descriptor
            .bind(&seed_session)
            .map_err(|e| eyre!("bind: {e}"))?;
        seed.set(K1, Value::from(K1))
            .await
            .map_err(|e| eyre!("{e}"))?;
        finalize_and_promote(&seed_session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

        // Fresh cold session, fresh resolve counter, one live dirty overlay.
        counting.reset();
        let resolves = ResolveCounter::default();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(2),
        };
        let session = resolve_session(
            &counting,
            &dedup,
            &registry,
            &state_key,
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
        assert!(handle.contains_key(&K3).await.map_err(|e| eyre!("{e}"))?);
        assert_eq!(resolves.resolves(), 0);
        assert_eq!(counting.presence_reads(), 2);
        assert_eq!(counting.batch_reads(), 0);

        // Contrast: the K3 cell IS resolvable, so the zero above is a real skip.
        assert!(handle.get(&K3).await.map_err(|e| eyre!("{e}"))?.is_some());
        assert!(
            resolves.resolves() >= 1,
            "get resolves; contains_key did not"
        );
        Ok(())
    })
}

/// Proves that key scans do not resolve cell values.
///
/// Falsification: Route `MapHandle::keys` through the resolving stream.
/// Then the resolver count becomes nonzero and the count assert fails.
#[test]
fn map_keys_no_resolve() -> Result<()> {
    TEST_RUNTIME.block_on(async {
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
    let dedup = MemoryDeduplicationStore::default();
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
    let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
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
        &dedup,
        &registry,
        &state_key,
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
    finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

    // Fresh cold session, zeroed resolve counter; drain keys() both directions.
    counting.reset();
    let resolves = ResolveCounter::default();
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = resolve_session(
        &counting,
        &dedup,
        &registry,
        &state_key,
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

/// The default batch read preserves the TTL from each projected point read.
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
    let batch = CoordinateBatch::chunks([0u8, 1].map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("non-empty read list must yield one batch"))?;
    let got = TEST_RUNTIME.block_on(async {
        CellRead::<Values>::read_many(&store, &id, SECTIONS[0], &batch).await
    })?;
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
        TEST_RUNTIME.block_on(run_deque_trace(trace, CommitMode::ReadCommitted, None))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// The deque lifecycle property in `ReadUncommitted` mode: `finalize` commits
/// everything, so every outcome that reaches it — including crash-abort —
/// converges to the full scratch model.
#[test]
fn prop_deque_collection_lifecycle_read_uncommitted() {
    fn property(trace: DequeTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(run_deque_trace(trace, CommitMode::ReadUncommitted, None))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// The deque lifecycle property on a **bounded** deque (capacity 2, under the
/// push burst so eviction fires on nearly every push-to-full): the handle keeps
/// step with a `VecDeque` model that applies the identical capped-trim rule, in
/// both commit modes — so lazy push-only eviction, its rollback under
/// abort/crash, and the at-least-once `commit()` floor all hold with a cap in
/// play. The unbounded lifecycle properties above test the `capacity = None`
/// path.
///
/// Falsification: Make `evictions` always return zero.
/// Then the handle exceeds the model and the `assert_deque` check fails.
#[test]
fn prop_deque_bounded_lifecycle() {
    fn property(trace: DequeTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(run_deque_trace(
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
        TEST_RUNTIME.block_on(run_deque_trace(
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
///
/// Falsification: Remove `.min(TRIM_MAX)` from `evictions`.
/// Then one push exceeds the delete limit and the per-push cap assert fails.
#[test]
fn prop_deque_capacity_convergence() {
    fn property(shape: DequeCapacityShape) -> Result<bool> {
        TEST_RUNTIME.block_on(run_deque_capacity_convergence(shape))
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
        TEST_RUNTIME.block_on(run_map_trace(trace, CommitMode::ReadCommitted))
    }
    QuickCheck::new().quickcheck(property as fn(MapTrace) -> Result<bool>);
}

/// The map lifecycle property in `ReadUncommitted` mode: `finalize` commits
/// everything, so every outcome that reaches it — including crash-abort —
/// converges to the full scratch model.
#[test]
fn prop_map_collection_lifecycle_read_uncommitted() {
    fn property(trace: MapTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(run_map_trace(trace, CommitMode::ReadUncommitted))
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
        TEST_RUNTIME.block_on(run_map_keyset_exact_trace(trace))
    }
    QuickCheck::new().quickcheck(property as fn(MapTrace) -> Result<bool>);
}

/// Map batch-read parity: values and presence answer each position exactly as
/// their point twins over random populations and query lists. The inputs cover
/// duplicates, absent keys, and lengths above `CELL_BATCH` in dirty and
/// committed arms.
#[test]
fn prop_map_get_many_parity() {
    fn property(input: MapGetManyInput) -> Result<bool> {
        TEST_RUNTIME.block_on(run_map_get_many_parity_trace(input))
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
        TEST_RUNTIME.block_on(run_map_ttl_keyset_refresh_trace(trace))
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
        TEST_RUNTIME.block_on(run_map_key_scan_holes(shape))
    }
    QuickCheck::new().quickcheck(property as fn(MapKeyHoles) -> Result<bool>);
}

/// Deque TTL holes: over a directly-seeded sparse window, `len` is the full
/// span (an upper bound on live elements) and `get`/`stream` skip expired
/// indices without error — the TTL'd-deque hole read contract.
#[test]
fn prop_deque_ttl_holes() {
    fn property(shape: DequeHoles) -> Result<bool> {
        TEST_RUNTIME.block_on(run_deque_holes(shape))
    }
    QuickCheck::new().quickcheck(property as fn(DequeHoles) -> Result<bool>);
}

/// Apply idempotence over the memory cell store: any generated interleaving of
/// marker resolution, verdict-matching settle re-applies, and per-cell
/// reads over one staged set with durable section clears converges to
/// the verdict state — no marker, no provisional residue, exact row shape.
#[test]
fn prop_memory_apply_idempotence() {
    fn property(input: ApplyTrace) -> Result<bool> {
        let cells = MemoryCells::new();
        let store = MemoryCellStore::new(cells.clone());
        let probe = MemoryShapeProbe(cells);
        TEST_RUNTIME.block_on(run_apply_idempotence(store, input, &probe))
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
        TEST_RUNTIME.block_on(run_publication_trace(&store, &token, trace))
    }
    QuickCheck::new().quickcheck(property as fn(PublicationTrace) -> Result<bool>);
}

/// The per-partition backend over a [`CountingCellStore`], so a directed test
/// can test the lower-store scan count a collection op issues.
type CountingBackend = PartitionBackend<
    MemoryDeduplicationStore,
    MemoryDescriptorIdentityStore,
    CountingCellStore<MemoryCellStore>,
    (),
>;

/// Mints a session over `counting` carrying `loader` for one event. Dropped
/// senders are fine — `watch::Receiver::borrow` keeps returning the last value.
fn session_with_loader<L>(
    counting: &CountingCellStore<MemoryCellStore>,
    dedup: &MemoryDeduplicationStore,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    event: EventRef,
    loader: L,
) -> KeyedStateSession<CountingBackend, L> {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    KeyedStateSession::new(SessionParts::<CountingBackend, _> {
        cell: counting.clone(),
        dirty: Arc::default(),
        dedup: dedup.clone(),
        loader,
        registry: registry.clone(),
        state_key: state_key.clone(),
        event,
        dedup_ttl: CompactDuration::new(30),
        checks: (),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
    })
}

/// Mints a session over `counting` for one event with the default in-memory
/// loader.
pub(super) fn counting_session(
    counting: &CountingCellStore<MemoryCellStore>,
    dedup: &MemoryDeduplicationStore,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    event: EventRef,
) -> KeyedStateSession<CountingBackend, MemoryLoader<Value>> {
    session_with_loader(
        counting,
        dedup,
        registry,
        state_key,
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

/// The Map keyset budget tests for the point-get arms (parity of
/// `deque_stream_issues_no_scans`): a never-written map yields nothing and
/// issues zero scans, and a small committed map streams through the batch verb
/// — zero scans, one keyset point-get, and one batch read for the entries, in
/// both directions. The liveness of these zeros is proved by
/// `map_overflowed_stream_issues_one_scan`.
#[test]
fn map_stream_issues_no_scans() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &map_state::<I64KeyCodec, JsonCodec>("mp"),
            CollectionDef::new(None),
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));

        // Empty-map arm: absent keyset ⇒ Empty ⇒ no scan (KeysetPresence).
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(0),
        };
        let session = counting_session(&counting, &dedup, &registry, &state_key, event);
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
        let session = counting_session(&counting, &dedup, &registry, &state_key, event);
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
        finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

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
            let session = counting_session(&counting, &dedup, &registry, &state_key, event);
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

/// The overflowed-map budget test (the liveness proof for
/// `map_stream_issues_no_scans`): a keyset-disabled map (`keyset_limit = 0`)
/// overflows on its first set and streams through **exactly one** full-section
/// scan (plus the single keyset get — bounds are gone).
#[test]
fn map_overflowed_stream_issues_one_scan() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
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
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));

        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(0),
        };
        let session = counting_session(&counting, &dedup, &registry, &state_key, event);
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
        finalize_and_promote(&session, &dedup, Uuid::from_u128(0), &cells, &of_id).await?;

        counting.reset();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX - 100),
        };
        let session = counting_session(&counting, &dedup, &registry, &state_key, event);
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
            &dedup,
            Uuid::from_u128(u128::MAX - 100),
            &cells,
            &of_id,
        )
        .await?;
        counting.reset();
        let empty_session = counting_session(
            &counting,
            &dedup,
            &registry,
            &state_key,
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
    counting: &CountingCellStore<MemoryCellStore>,
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
/// same fixture to [`assert_wide_deque_scan_is_window_bounded`], which tests
/// the fallback arm.
#[test]
fn deque_stream_issues_no_scans() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let mut registry = CollectionDefRegistry::default();
        registry.register(&deque_state::<JsonCodec>("dq"), CollectionDef::new(None))?;
        registry.register(
            &deque_state::<JsonCodec>("dq-wide"),
            CollectionDef::new(None),
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));

        // One committed event of pushes and pops: the deque reads [0, 1, 2].
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let session = counting_session(&counting, &dedup, &registry, &state_key, event);
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
        finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

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
            let session = counting_session(&counting, &dedup, &registry, &state_key, event);
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

        assert_wide_deque_scan_is_window_bounded(&counting, &dedup, &registry, &state_key).await
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
    counting: &CountingCellStore<MemoryCellStore>,
    dedup: &MemoryDeduplicationStore,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
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
        let session = counting_session(counting, dedup, registry, state_key, event);
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
/// stream-laziness test can bound resolutions independently of fetches.
fn resolve_session(
    counting: &CountingCellStore<MemoryCellStore>,
    dedup: &MemoryDeduplicationStore,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    event: EventRef,
    loader: ResolveCounter,
) -> KeyedStateSession<CountingBackend, ResolveCounter> {
    session_with_loader(counting, dedup, registry, state_key, event, loader)
}

/// The stream-laziness property (map): a `stream(dir).take(k)` over a **dense**
/// `n`-entry `Tracked` map is genuinely incremental. It issues at most one
/// batch read beyond `k`, because entries flow through the batch verb and only
/// the keyset meta cell is a point read. It resolves at most `k + CELL_BATCH`
/// values, never the whole `n`-entry collection. The counting store bounds the
/// fetches and the counting resolver bounds the resolutions. Both counters sit
/// at the lowest layer, so nothing masks a materialization.
///
/// Falsification: Make `CoordinatePlan::entry_source` consume all tracked keys.
/// Then the read and resolver counts exceed their bounds, and both asserts
/// fail. A larger `CELL_BATCH` cannot falsify: the bound moves with it.
async fn run_map_stream_prefix_lazy(n: usize, k: usize, dir: Direction) -> Result<()> {
    let dedup = MemoryDeduplicationStore::default();
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
    let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
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
        &dedup,
        &registry,
        &state_key,
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
    finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

    // Fresh cold session, zeroed counters; drain only the k-prefix.
    counting.reset();
    let resolves = ResolveCounter::default();
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = resolve_session(
        &counting,
        &dedup,
        &registry,
        &state_key,
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
/// `k + CELL_BATCH` resolved.
///
/// Falsification: Make `CoordinatePlan::entry_source` consume all tracked keys.
/// Then the read and resolver counts exceed their bounds, and both asserts
/// fail. A larger `CELL_BATCH` cannot falsify: the bound moves with it.
async fn run_deque_stream_prefix_lazy(n: usize, k: usize, dir: Direction) -> Result<()> {
    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<WithResolver<JsonCodec, CountingResolver>>("lz");
    let mut registry = CollectionDefRegistry::default();
    registry.register(&descriptor, CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
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
        &dedup,
        &registry,
        &state_key,
        event,
        ResolveCounter::default(),
    );
    let seed = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    for i in 0..n {
        seed.push_back(Value::from(i64::try_from(i)?))
            .await
            .map_err(|e| eyre!("{e}"))?;
    }
    finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

    counting.reset();
    let resolves = ResolveCounter::default();
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = resolve_session(
        &counting,
        &dedup,
        &registry,
        &state_key,
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
///
/// Falsification: Replace the eviction clear with a get followed by a clear.
/// Then the evicted slot resolves and the zero-count assert fails.
#[test]
fn deque_bounded_eviction_does_not_resolve() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
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
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
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
            &dedup,
            &registry,
            &state_key,
            seed_event,
            ResolveCounter::default(),
        );
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        handle
            .push_back(Value::from(1_u8))
            .await
            .map_err(|e| eyre!("{e}"))?;
        finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

        // Fresh event: push a second value, evicting the front (the capacity is 1).
        let resolves = ResolveCounter::default();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(2),
        };
        let session = resolve_session(
            &counting,
            &dedup,
            &registry,
            &state_key,
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
        TEST_RUNTIME.block_on(async move {
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

/// Batches and scans share one marker snapshot and preserve reader parity.
#[test]
fn prop_resolve_reads_each_marker_once() {
    fn property(value: u8, siblings: u8, certificate: u8, length: u8) -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let cells = MemoryCells::new();
            let id = fresh_collection("read-budget")?;
            let count = usize::from(siblings % 8) + 1;
            let mut collections = Vec::with_capacity(count);
            collections.push(CollectionRef::new(id.clone(), None));
            for index in 1..count {
                collections.push(CollectionRef::new(
                    CollectionId::new(
                        id.state_key().clone(),
                        id.state_type(),
                        StateName::try_new(format!("sibling-{index}"))?,
                    ),
                    None,
                ));
            }
            let memory = MemoryCellStore::new(cells.clone());
            let store = CountingCellStore::new(memory.clone()).with_marker_counts(&collections);
            let touched = collections
                .iter()
                .map(CollectionRef::id)
                .map(|id| (id.state_type(), id.name().clone()))
                .collect();
            let evidence = evidence(touched, None);
            let event = support::probe(1);
            let data = bytes(value);
            let prev = bytes(value.wrapping_add(1));
            let length = length % 32 + 2;
            let writes: Vec<_> = (0..length)
                .map(|index| {
                    (
                        cell_in(0, index),
                        ProvisionalWrite::new(
                            Some(data.clone()),
                            Committed::new(Some(prev.clone())),
                            event,
                        ),
                    )
                })
                .collect();
            let marker = EventMarker::frozen(event, &writes, &[], &evidence);
            for collection in &collections {
                store
                    .write_provisional(collection, &writes, Some(&marker))
                    .await?;
            }
            let certificate = usize::from(certificate) % (count + 1);
            if let Some(collection) = collections.get(certificate) {
                support::seed_commit_evidence(&store, collection).await?;
            }
            let expected = Some(if certificate < count { data } else { prev });

            check_memory_read_parity(&memory, &id, &writes, expected.as_ref()).await?;

            // Exercise a batch and both scan directions with separate lookups.
            for direction in [None, Some(Direction::Forward), Some(Direction::Backward)] {
                store.reset();
                let mut lookup = EvidenceLookup::new(&store, &id);
                assert_eq!(
                    lookup
                        .resolve(Cell::Resolved(Committed::<Values>::new(None)))
                        .await?
                        .into_inner(),
                    None
                );
                assert_eq!(store.marker_reads(), 0, "resolved cells need no evidence");
                let mut keys: Vec<_> = writes.iter().map(|(cell, _)| cell.clone()).collect();
                if direction == Some(Direction::Backward) {
                    keys.reverse();
                }
                if direction.is_none() {
                    keys.extend_from_within(..);
                }
                let rows = stream::iter(keys);
                pin_mut!(rows);
                while let Some(cell) = rows.next().await {
                    let actual = lookup
                        .resolve(Cell::Provisional(
                            store
                                .provisional_cell_at(&id, &cell)
                                .await?
                                .ok_or_else(|| eyre!("provisional cell missing"))?,
                        ))
                        .await?
                        .into_inner();
                    assert_eq!(actual, expected);
                    assert_eq!(actual, cells.read_committed(&id, &cell));
                }
                assert_eq!(store.marker_reads(), count, "one snapshot per call");
                for collection in &collections {
                    assert_eq!(
                        store.marker_reads_for(collection.id()),
                        1,
                        "read each marker once"
                    );
                }
                assert_eq!(store.durable_writes(), 0, "reads preserve durable state");
            }
            Ok(())
        })
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8, u8, u8) -> Result<()>);
}

async fn check_memory_read_parity(
    store: &MemoryCellStore,
    id: &CollectionId,
    writes: &[(CellKey, ProvisionalWrite)],
    expected: Option<&Bytes>,
) -> Result<()> {
    use super::{Scan, ScanEdge};
    use futures::TryStreamExt;

    let batch = CoordinateBatch::chunks(writes.iter().map(|(cell, _)| cell.coordinate.clone()))
        .next()
        .ok_or_else(|| eyre!("batch missing"))?;
    let values = CellRead::<Values>::read_many(store, id, writes[0].0.section, &batch)
        .await
        .map(|cells| {
            cells
                .into_iter()
                .map(|(committed, _)| committed)
                .collect::<CommittedBatch>()
        })?;
    assert_eq!(values.len(), batch.len());
    for value in values {
        assert_eq!(value.into_inner().as_ref(), expected);
    }

    let values = CellRead::<Values>::read_many(store, id, writes[0].0.section, &batch).await?;
    assert_eq!(values.len(), batch.len());
    for (value, ttl) in values {
        assert_eq!(value.into_inner().as_ref(), expected);
        assert_eq!(ttl, None);
    }

    for dir in [Direction::Forward, Direction::Backward] {
        let scan = Scan {
            section: writes[0].0.section,
            start: ScanEdge::Unbounded,
            end: ScanEdge::Unbounded,
            dir,
            limit: None,
        };
        let rows: Vec<_> = CellRead::<Values>::scan(store, id, scan)
            .try_collect()
            .await?;
        assert_eq!(rows.len(), writes.len());
        for (_, value) in rows {
            assert_eq!(Some(&value), expected);
        }
    }
    Ok(())
}
