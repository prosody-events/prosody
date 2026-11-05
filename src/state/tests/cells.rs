//! Memory cell operations preserve the shared store invariants.

use super::*;
use crate::state::store::sorted_unique_coordinates;
use crate::state::tests::support::listed;
use std::collections::BTreeSet;

/// `CollectionRef` equality and hashing key on the inner `CollectionId` only —
/// the TTL is a per-write hint, not part of identity. Two refs to the same
/// collection with different TTLs must compare and hash equal, so a
/// `CollectionRef` used as a map key is not split by an incidental TTL
/// difference.
#[test]
pub(super) fn collection_ref_eq_and_hash_ignore_ttl() -> Result<()> {
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
pub(super) fn prop_memory_cell_crash_equivalence() {
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
pub(super) fn blind_write_leaves_clears_free_marker() -> Result<()> {
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
pub(super) fn prop_memory_cell_implicit_overwrite() {
    fn property(trace: OverwriteTrace) -> Result<bool> {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let make = || Ok(MemoryCellStore::new(cells.clone()));
        TEST_RUNTIME.block_on(run_overwrite_trace(make, dedup.clone(), trace))
    }
    QuickCheck::new().quickcheck(property as fn(OverwriteTrace) -> Result<bool>);
}

/// Point reads and range scans match the dirty-over-committed oracle.
/// The trace mixes bounded scans, both directions, early stops, dirty writes,
/// and committed writes. Dirty values win, and dirty clears hide cells.
#[test]
pub(super) fn prop_memory_overlay_view() {
    fn property(trace: OverlayTrace) -> Result<bool> {
        let lower = MemoryCellStore::new(MemoryCells::new());
        TEST_RUNTIME.block_on(run_overlay_trace(lower, trace))
    }
    QuickCheck::new().quickcheck(property as fn(OverlayTrace) -> Result<bool>);
}

/// Both memory scan projections match the committed model across bounds and
/// section clears.
#[test]
pub(super) fn prop_memory_bottom_scan() {
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
pub(super) fn prop_chunk_reassembly() {
    fn property(coords: Vec<u8>) -> bool {
        let input: Vec<Coordinate> = coords
            .into_iter()
            .map(|b| Coordinate::from_bytes(vec![b]))
            .collect();
        let batches: Vec<CoordinateBatch> = CoordinateBatch::chunks(input.clone()).collect();
        let mut flat: Vec<Coordinate> = Vec::new();
        for batch in &batches {
            if batch.len() == 0 || batch.len() > CELL_BATCH.get() {
                return false;
            }
            flat.extend(batch.as_slice().iter().cloned());
        }
        // All but the last batch are exactly CELL_BATCH.
        let full_prefix = batches
            .split_last()
            .is_none_or(|(_, rest)| rest.iter().all(|b| b.len() == CELL_BATCH.get()));
        flat == input && full_prefix && (input.is_empty() == batches.is_empty())
    }
    QuickCheck::new().quickcheck(property as fn(Vec<u8>) -> bool);
}

/// Keyed-state buffers keep small operations inline but spill well before a
/// full store batch can become part of an async future's stack footprint.
#[test]
pub(super) fn cell_buffers_spill_before_full_batch() {
    let small: CellBuffer<usize> = (0..CELLS_INLINE).collect();
    assert!(!small.spilled(), "the common small case stays inline");

    let full: CellBuffer<usize> = (0..CELL_BATCH.get()).collect();
    assert_eq!(full.len(), CELL_BATCH.get());
    assert!(
        full.spilled(),
        "a full batch must not remain inline in an async state machine"
    );
}

/// The two batch coordinate sets name each input coordinate exactly once.
/// `distinct` keeps first-occurrence order for the Cassandra `IN` read.
/// `sorted_unique_coordinates` sorts for the ascending provisional batch.
/// `prop_cassandra_batch_read_parity` covers the answers built from them.
#[test]
pub(super) fn prop_batch_coordinate_sets() {
    fn property(input: Vec<u8>) -> bool {
        // A small alphabet makes duplicates common.
        let input: Vec<u8> = input
            .into_iter()
            .take(CELL_BATCH.get())
            .map(|byte| byte % 8)
            .collect();
        let Some(batch) =
            CoordinateBatch::chunks(input.iter().map(|&byte| Coordinate::from_bytes(vec![byte])))
                .next()
        else {
            return input.is_empty();
        };
        let mut first_seen = Vec::new();
        for &byte in &input {
            if !first_seen.contains(&byte) {
                first_seen.push(byte);
            }
        }
        let ascending: Vec<u8> = input
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let distinct_bytes: Vec<u8> = distinct(&batch.as_ref()).iter().map(|c| c[0]).collect();
        let sorted_bytes: Vec<u8> = sorted_unique_coordinates(&batch)
            .iter()
            .map(|coordinate| coordinate.as_bytes()[0])
            .collect();
        distinct_bytes == first_seen && sorted_bytes == ascending
    }
    QuickCheck::new().quickcheck(property as fn(Vec<u8>) -> bool);
}

/// Batch-read parity over the memory cell store: `get_many` answers each
/// position exactly as the sequential point-`get` oracle, across duplicates,
/// unknowns, absence, and provisional resolution.
#[test]
pub(super) fn prop_memory_batch_read_parity() {
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
pub(super) fn prop_memory_raw_batch_parity() {
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
pub(super) fn memory_raw_batch_ascending_output() -> Result<()> {
    let store = MemoryCellStore::new(MemoryCells::new());
    TEST_RUNTIME.block_on(run_raw_batch_ascending_output(store))
}

/// Raw batch reads leave provisional rows unchanged.
#[test]
pub(super) fn memory_raw_batch_no_side_effects() -> Result<()> {
    let store = MemoryCellStore::new(MemoryCells::new());
    TEST_RUNTIME.block_on(run_raw_batch_no_side_effects(store))
}

/// Within-batch duplicate co-observation + scatter alignment over the memory
/// store (deterministic).
#[test]
pub(super) fn memory_batch_duplicate_co_observation() -> Result<()> {
    let store = MemoryCellStore::new(MemoryCells::new());
    TEST_RUNTIME.block_on(run_batch_duplicate_co_observation(store))
}

/// Every input position is answered over two chunks (deterministic alignment).
#[test]
pub(super) fn memory_batch_alignment() -> Result<()> {
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
pub(super) fn memory_resolve_event_marker_batches_reads() -> Result<()> {
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
        let marker = EventMarker::frozen(event, &writes, Vec::new(), &evidence([].into(), None));
        counting
            .write_provisional(&cref, listed(&marker, &writes)?)
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
pub(super) fn resolve_event_marker_rekeys_survivors_by_section() -> Result<()> {
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
        let marker = EventMarker::frozen(event, &writes, Vec::new(), &evidence([].into(), None));
        store
            .write_provisional(&cref, listed(&marker, &writes)?)
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
            CellRead::<Values>::read(&store, &id, cell_in(0, 7).as_ref())
                .await
                .map(|(committed, _)| committed)
                .map_err(|e| eyre!("get s0: {e}"))?,
            Committed::new(Some(bytes(70))),
            "the section-0 survivor commits at (0, 7)"
        );
        assert_eq!(
            CellRead::<Values>::read(&store, &id, cell_in(1, 7).as_ref())
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
pub(super) async fn drain_memory_provisional<S: CellStore>(
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
pub(super) fn memory_overlay_precedence_set_beats_section_clear() -> Result<()> {
    let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
    TEST_RUNTIME.block_on(run_overlay_precedence_pin(counting))
}
