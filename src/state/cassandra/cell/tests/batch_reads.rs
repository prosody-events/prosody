use super::*;

/// Batch-read parity over the live `CassandraStore`: the single-`IN`-query
/// override answers each position exactly as the sequential point-`get` oracle
/// over an identically-seeded sibling collection — across duplicates, unknowns,
/// absence, and provisional resolution. Runs directly on the bare store so the
/// override (deduplication, input-order expansion, and resolution) is
/// exercised, not the `Cached` default.
#[test]
fn prop_cassandra_batch_read_parity() {
    async fn run(trace: BatchReadTrace) -> Result<bool> {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        let store = fx.bottom_store(oracle.clone());
        Box::pin(run_batch_read_parity_trace(store, oracle, trace)).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(
            (|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(BatchReadTrace) -> TestResult,
        );
}

/// Within-batch duplicate co-observation and input-order expansion.
#[tokio::test]
async fn cassandra_batch_duplicate_co_observation() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    Box::pin(run_batch_duplicate_co_observation(
        fx.bottom_store(ScriptedOracle::default()),
    ))
    .await
}

/// Every input position answered over two chunks on the live store.
#[tokio::test]
async fn cassandra_batch_preserves_input_positions() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    Box::pin(run_batch_alignment(
        fx.bottom_store(ScriptedOracle::default()),
    ))
    .await
}

/// Seeds two raw-CQL corrupt cells (unreachable through the store verbs) in
/// `id`'s section 0, returning `(cell_a, cell_b)`. A (`0x01`, low): `prev_data`
/// present with a valid frame, `event` NULL ⇒ `PrevWithoutEvent`. B (`0xFE`,
/// high): `data` present, `encoding` NULL ⇒ `BlobWithoutEncoding`. The
/// clustering order (A < B) is the reverse of the `[B, A]` read list both
/// resolve-order pins issue, so the decode order the reader picks decides which
/// error surfaces.
async fn seed_prev_without_event_and_blob_without_encoding(
    session: &Session,
    id: &CollectionId,
) -> Result<(CellKey, CellKey)> {
    use super::encoding::{Encoding, encode_payload};
    use crate::cassandra::TABLE_KEYED_STATE_CELL;

    let cell_a = cell_in(0, 0x01);
    let cell_b = cell_in(0, 0xFE);
    let prev_blob = encode_payload(&bytes(0xAA), Encoding::Zstd)?;
    let insert_a = format!(
        "INSERT INTO {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} (segment_id, key, state_type, name, \
         kind, section, coordinate, prev_data, encoding, version) VALUES (?, ?, ?, ?, 0, 0, ?, ?, \
         4, 1)"
    );
    let insert_b = format!(
        "INSERT INTO {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} (segment_id, key, state_type, name, \
         kind, section, coordinate, data) VALUES (?, ?, ?, ?, 0, 0, ?, ?)"
    );
    session
        .query_unpaged(
            insert_a,
            (
                id.state_key().segment_id,
                id.state_key().key.as_ref(),
                i8::from(id.state_type()),
                id.name().as_str(),
                cell_a.coordinate.as_bytes(),
                prev_blob.as_ref(),
            ),
        )
        .await?;
    session
        .query_unpaged(
            insert_b,
            (
                id.state_key().segment_id,
                id.state_key().key.as_ref(),
                i8::from(id.state_type()),
                id.name().as_str(),
                cell_b.coordinate.as_bytes(),
                bytes(0xBB).as_ref(),
            ),
        )
        .await?;
    Ok((cell_a, cell_b))
}

/// Resolve-order pin: two rows with DISTINCT corruption shapes at coordinates
/// whose clustering order (A `0x01` < B `0xFE`) is the reverse of the read list
/// `[B, A]`. `get_many` decodes unique rows in first-occurrence order. Thus, it
/// must surface B's `BlobWithoutEncoding`, not A's `PrevWithoutEvent`,
/// which the `IN` query returns first in clustering order. The corruptions are
/// seeded by raw CQL (unreachable through the store verbs), and the sequential
/// point `get`s confirm the two rows are distinguishable.
#[tokio::test]
async fn first_error_is_first_input_position() -> Result<()> {
    use super::CellCorruptReason;
    use crate::state::cassandra::CassandraCellStoreError;
    use crate::state::resolve::ResolveCellError;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("resolve-order")?;
    let id = c.id();
    let own = event(9);
    let (cell_a, cell_b) =
        seed_prev_without_event_and_blob_without_encoding(fx.cassandra.session(), id).await?;

    // The two rows are distinguishable through the sequential oracle.
    assert!(
        matches!(
            store.get(id, &cell_a, own).await,
            Err(ResolveCellError::Store(
                CassandraCellStoreError::CorruptCell(CellCorruptReason::PrevWithoutEvent)
            ))
        ),
        "A alone decodes as PrevWithoutEvent"
    );
    assert!(
        matches!(
            store.get(id, &cell_b, own).await,
            Err(ResolveCellError::Store(
                CassandraCellStoreError::CorruptCell(CellCorruptReason::BlobWithoutEncoding)
            ))
        ),
        "B alone decodes as BlobWithoutEncoding"
    );

    // Read list `[B, A]`: first-occurrence resolution must surface B's error.
    let batch = CoordinateBatch::chunks([0xFEu8, 0x01].map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("non-empty read list must yield one batch"))?;
    match Box::pin(store.get_many(id, SECTIONS[0], &batch, own)).await {
        Err(ResolveCellError::Store(CassandraCellStoreError::CorruptCell(reason))) => {
            assert_eq!(
                reason,
                CellCorruptReason::BlobWithoutEncoding,
                "the earliest input position (B) determines the surfaced error"
            );
        }
        other => return Err(eyre!("expected B's BlobWithoutEncoding, got {other:?}")),
    }
    Ok(())
}

/// Sort-necessity unit pin (no cluster): a SHUFFLED raw batch with two corrupt
/// rows — low coord `0x01` = `PrevWithoutEvent`, high coord `0xFE` =
/// `BlobWithoutEncoding`, pushed high-then-low — must surface the LOWEST
/// coordinate's error after the borrowed batch follows input resolution order.
/// A live test cannot prove this because `IN` returns clustering order.
#[test]
fn borrowed_batch_decodes_in_resolution_order() -> Result<()> {
    use super::CellCorruptReason;
    use super::decode::BorrowedCellTtlRow;
    use super::decode_rows_for_coordinates;
    use super::encoding::{Encoding, encode_payload};
    use crate::state::cassandra::CassandraCellStoreError;
    use crate::state::store::CellBuffer;
    use smallvec::SmallVec;

    let prev_blob = encode_payload(&bytes(0xAA), Encoding::Zstd)?;
    // event = None throughout, so no RawEventRef construction is needed.
    let high_bytes = [0xBB];
    let high: BorrowedCellTtlRow<'_> = (Some(&high_bytes), None, None, None, None, None, None);
    let low: BorrowedCellTtlRow<'_> = (
        None,
        Some(prev_blob.as_ref()),
        Some(4_i16),
        Some(1_i32),
        None,
        None,
        None,
    );
    let high_coordinate = Coordinate::from_bytes(vec![0xFE]);
    let low_coordinate = Coordinate::from_bytes(vec![0x01]);
    let mut rows: CellBuffer<(&[u8], BorrowedCellTtlRow<'_>)> = SmallVec::new();
    rows.push((high_coordinate.as_bytes(), high));
    rows.push((low_coordinate.as_bytes(), low));
    match decode_rows_for_coordinates(rows, &[&low_coordinate, &high_coordinate]) {
        Err(CassandraCellStoreError::CorruptCell(reason)) => assert_eq!(
            reason,
            CellCorruptReason::PrevWithoutEvent,
            "the first coordinate's error must surface first"
        ),
        other => return Err(eyre!("expected PrevWithoutEvent, got {other:?}")),
    }
    Ok(())
}

#[test]
fn borrowed_batch_matches_requested_coordinates() -> Result<()> {
    use super::decode::BorrowedCellTtlRow;
    use super::decode_rows_for_coordinates;
    use crate::state::store::CellBuffer;
    use smallvec::smallvec;

    let low_data = [0x11];
    let high_data = [0x33];
    let row = |data| -> BorrowedCellTtlRow<'_> {
        (Some(data), None, Some(1_i16), Some(1_i32), None, None, None)
    };
    let low = Coordinate::from_bytes(vec![1]);
    let absent = Coordinate::from_bytes(vec![2]);
    let high = Coordinate::from_bytes(vec![3]);
    let rows: CellBuffer<_> = smallvec![
        (high.as_bytes(), row(&high_data)),
        (low.as_bytes(), row(&low_data)),
    ];

    let decoded = decode_rows_for_coordinates(rows, &[&low, &absent, &high])?;
    assert_eq!(decoded.len(), 3);
    assert_eq!(
        decoded[0]
            .as_ref()
            .and_then(|(cell, _)| cell.project_committed())
            .map(Bytes::as_ref),
        Some(&low_data[..])
    );
    assert!(decoded[1].is_none());
    assert_eq!(
        decoded[2]
            .as_ref()
            .and_then(|(cell, _)| cell.project_committed())
            .map(Bytes::as_ref),
        Some(&high_data[..])
    );
    Ok(())
}

#[test]
fn provisional_batch_coordinates_are_sorted_and_distinct() -> Result<()> {
    let batch = CoordinateBatch::chunks(
        [0xFE_u8, 0x01, 0x80, 0x01].map(|byte| Coordinate::from_bytes(vec![byte])),
    )
    .next()
    .ok_or_else(|| eyre!("non-empty input must yield one batch"))?;
    let coordinates = sorted_unique_coordinates(&batch);
    assert_eq!(
        coordinates
            .iter()
            .map(|coordinate| coordinate.as_bytes())
            .collect::<Vec<_>>(),
        vec![&[0x01_u8][..], &[0x80_u8][..], &[0xFE_u8][..]]
    );
    Ok(())
}

/// Recovery validates resolved row metadata before it skips unused blobs.
#[tokio::test]
async fn resolved_corrupt_rows_fail_before_blob_decode() -> Result<()> {
    use super::CellCorruptReason;
    use crate::state::cassandra::CassandraCellStoreError;
    use crate::state::resolve::ResolveCellError;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("resolved-corrupt-error")?;
    let id = c.id();
    seed_prev_without_event_and_blob_without_encoding(fx.cassandra.session(), id).await?;

    let batch = CoordinateBatch::chunks([0xFEu8, 0x01].map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("non-empty read list must yield one batch"))?;
    match Box::pin(store.provisional_many(id, SECTIONS[0], &batch)).await {
        Err(ResolveCellError::Store(CassandraCellStoreError::CorruptCell(reason))) => assert_eq!(
            reason,
            CellCorruptReason::PrevWithoutEvent,
            "the lowest corrupt coordinate determines the recovery error"
        ),
        other => return Err(eyre!("expected PrevWithoutEvent, got {other:?}")),
    }
    Ok(())
}

/// Query-count pin: `provisional_many` issues exactly ONE `IN` query per chunk
/// and NO point reads or marker reads. A fresh reader store (cold counters)
/// stages nothing itself, so the counters reflect the verb alone; the dedicated
/// `provisional_in_queries` counter proves it BATCHED rather than merely "no
/// point reads".
#[tokio::test]
async fn cassandra_raw_batch_is_one_query() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let seed = fx.bottom_store(ScriptedOracle::default());
    let c = collection("raw-one-query")?;
    let id = c.id();
    let staging = event(0x11);
    let mut writes = Vec::new();
    for b in [1u8, 2] {
        let cell = cell_in(0, b);
        let prev = seed.get(id, &cell, staging).await?;
        writes.push((
            cell,
            ProvisionalWrite::new(Some(bytes(b * 10)), prev, staging),
        ));
    }
    let marker = EventMarker::frozen(staging, &writes, &[]);
    seed.write_provisional(&c, &writes, Some(&marker)).await?;

    // A fresh store: cold counters shared across its clones.
    let reader = fx.bottom_store(ScriptedOracle::default());
    let counters = reader.recovery_reads();
    let batch = CoordinateBatch::chunks([1u8, 2].map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("non-empty read list must yield one batch"))?;
    let out = Box::pin(reader.provisional_many(id, SECTIONS[0], &batch)).await?;
    assert_eq!(out.len(), 2, "both staged provisional cells survive");
    assert_eq!(
        counters.provisional_in_queries.load(Ordering::Relaxed),
        1,
        "exactly one IN query"
    );
    assert_eq!(
        counters.cell_point_reads.load(Ordering::Relaxed),
        0,
        "no per-coordinate point reads"
    );
    assert_eq!(
        counters.marker_point_reads.load(Ordering::Relaxed),
        0,
        "no marker point read"
    );
    Ok(())
}

/// Cold recovery `provisional_cells` batches by section at the `CELL_BATCH`
/// boundary: staging `n` provisional cells and draining the sweep issues
/// exactly `ceil(n / CELL_BATCH)` raw `IN` queries and ZERO per-coordinate
/// point reads at n = 127/128/129, plus one query per additional section — the
/// query-count formula `raw_batch_reads = Σ_s ceil(n_s / CELL_BATCH)`,
/// `raw_point_reads = 0`. One store per case so its `recovery_reads` counters
/// start clean; staging never touches `provisional_in_queries`, so the drain's
/// count is the whole assertion.
#[tokio::test]
async fn cassandra_recovery_batches_by_section_at_boundary() -> Result<()> {
    use crate::state::store::CELL_BATCH;

    init_test_logging();
    let fx = fixture().await?;
    let staging = event(0x22);

    for n in [127u32, 128, 129] {
        let store = fx.bottom_store(ScriptedOracle::default());
        let c = collection(&format!("recovery-boundary-{n}"))?;
        let counters = store.recovery_reads();
        let writes: Vec<(CellKey, ProvisionalWrite)> = (0..n)
            .map(|i| {
                (
                    cell_i(i),
                    ProvisionalWrite::new(Some(bytes(1)), Committed::new(None), staging),
                )
            })
            .collect();
        let marker = EventMarker::frozen(staging, &writes, &[]);
        store.write_provisional(&c, &writes, Some(&marker)).await?;

        let found = provisional_cells(&store, c.id()).await?;
        assert_eq!(
            found.len(),
            n as usize,
            "every staged cell recovers (n={n})"
        );
        assert_eq!(
            counters.provisional_in_queries.load(Ordering::Relaxed),
            n.div_ceil(CELL_BATCH as u32) as usize,
            "one IN query per <=CELL_BATCH chunk, not #cells (n={n})"
        );
        assert_eq!(
            counters.cell_point_reads.load(Ordering::Relaxed),
            0,
            "the batched sweep issues no per-coordinate point reads (n={n})"
        );
    }

    // Two sections: 129 in section 0 (two chunks) + 1 in section 1 (one chunk)
    // ⇒ ceil(129/128) + ceil(1/128) = 3 IN queries.
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("recovery-boundary-two-sections")?;
    let counters = store.recovery_reads();
    let mut writes: Vec<(CellKey, ProvisionalWrite)> = (0..129u32)
        .map(|i| {
            (
                cell_i(i),
                ProvisionalWrite::new(Some(bytes(1)), Committed::new(None), staging),
            )
        })
        .collect();
    writes.push((
        CellKey {
            section: Section::new(1),
            coordinate: Coordinate::from_bytes(vec![0, 0, 0, 0]),
        },
        ProvisionalWrite::new(Some(bytes(2)), Committed::new(None), staging),
    ));
    let marker = EventMarker::frozen(staging, &writes, &[]);
    store.write_provisional(&c, &writes, Some(&marker)).await?;

    let found = provisional_cells(&store, c.id()).await?;
    assert_eq!(found.len(), 130, "all cells across both sections recover");
    assert_eq!(
        counters.provisional_in_queries.load(Ordering::Relaxed),
        3,
        "ceil(129/128) + ceil(1/128) = 2 + 1"
    );
    assert_eq!(
        counters.cell_point_reads.load(Ordering::Relaxed),
        0,
        "no per-coordinate point reads across sections"
    );
    Ok(())
}

/// Raw-provisional batch parity over the bare live store: `provisional_many`
/// returns exactly the survivors the sequential `provisional_cell_at` loop
/// does.
#[test]
fn prop_cassandra_raw_batch_parity() {
    async fn run(trace: RawBatchTrace) -> Result<bool> {
        let fx = fixture().await?;
        let store = fx.bottom_store(ScriptedOracle::default());
        Box::pin(run_raw_batch_parity_trace(store, trace)).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(
            (|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(RawBatchTrace) -> TestResult,
        );
}

/// Ascending-output pin over the live store. The sort requirement is also
/// pinned by `borrowed_batch_decodes_in_resolution_order` and
/// `provisional_batch_coordinates_are_sorted_and_distinct`.
#[tokio::test]
async fn cassandra_raw_batch_ascending_output() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    Box::pin(run_raw_batch_ascending_output(
        fx.bottom_store(ScriptedOracle::default()),
    ))
    .await
}

/// No-side-effects pin over the live store built on a [`CountingOracle`]:
/// `provisional_many` never resolves, writes, or caches.
#[tokio::test]
async fn cassandra_raw_batch_no_side_effects() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let oracle = CountingOracle::default();
    let store = fx.bottom_store_with(oracle.clone(), fx.presence.clone());
    Box::pin(run_raw_batch_no_side_effects(store, oracle)).await
}
