use super::*;
use crate::state::cassandra::CassandraCellStoreError;
use crate::state::cell::Values;
use crate::state::store::{CellBuffer, CellRead, CommittedBatch};
use crate::state::tests::support::evidence;
use crate::state::tests::support::listed;

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
        let store = fx.bottom_store();
        run_batch_read_parity_trace(store, trace).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(ModelProperty(|trace| TEST_RUNTIME.block_on(run(trace))));
}

/// Within-batch duplicate co-observation and input-order expansion.
#[tokio::test]
async fn cassandra_batch_duplicate_co_observation() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    run_batch_duplicate_co_observation(fx.bottom_store()).await
}

/// Every input position answered over two chunks on the live store.
#[tokio::test]
async fn cassandra_batch_preserves_input_positions() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    run_batch_alignment(fx.bottom_store()).await
}

/// Seeds two raw-CQL corrupt cells (unreachable through the store verbs) in
/// `id`'s section 0, returning `(cell_a, cell_b)`. A (`0x01`, low): `prev_data`
/// present with a valid frame, `event` NULL ⇒ `PrevWithoutEvent`. B (`0xFE`,
/// high): `data` present, `encoding` NULL ⇒ `BlobWithoutEncoding`. The
/// clustering order (A < B) is the reverse of the `[B, A]` read list both
/// resolve-order tests issue, so the decode order the reader picks decides
/// which error surfaces.
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

/// Resolve-order test: two rows with DISTINCT corruption shapes at coordinates
/// whose clustering order (A `0x01` < B `0xFE`) is the reverse of the read list
/// `[B, A]`. `get_many` decodes unique rows in first-occurrence order. Thus, it
/// must surface B's `BlobWithoutEncoding`, not A's `PrevWithoutEvent`,
/// which the `IN` query returns first in clustering order. The corruptions are
/// seeded by raw CQL (unreachable through the store verbs), and the sequential
/// point `get`s confirm the two rows are distinguishable.
#[tokio::test]
async fn first_error_is_first_input_position() -> Result<()> {
    use super::CellCorruptReason;
    use crate::state::resolve::ResolveCellError;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store();
    let c = collection("resolve-order")?;
    let id = c.id();
    let (cell_a, cell_b) =
        seed_prev_without_event_and_blob_without_encoding(fx.cassandra.session(), id).await?;

    // The two rows are distinguishable through the sequential oracle.
    assert!(
        matches!(
            CellRead::<Values>::read(&store, id, cell_a.as_ref())
                .await
                .map(|(committed, _)| committed),
            Err(ResolveCellError::Store(
                CassandraCellStoreError::CorruptCell(CellCorruptReason::PrevWithoutEvent)
            ))
        ),
        "A alone decodes as PrevWithoutEvent"
    );
    assert!(
        matches!(
            CellRead::<Values>::read(&store, id, cell_b.as_ref())
                .await
                .map(|(committed, _)| committed),
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
    match async {
        CellRead::<Values>::read_many(&store, id, SECTIONS[0], &batch.as_ref())
            .await
            .map(|cells| {
                cells
                    .into_iter()
                    .map(|(committed, _)| committed)
                    .collect::<CommittedBatch>()
            })
    }
    .await
    {
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

/// Shuffled rows decode in input order and report the first coordinate's error.
/// A live query returns clustering order and cannot prove this rule.
#[test]
fn borrowed_batch_decodes_in_resolution_order() -> Result<()> {
    use super::super::read::{decode_point, take_row};
    use super::CellCorruptReason;
    use super::decode::PointRow;
    use super::encoding::{Encoding, encode_payload};
    use crate::state::cell::Values;
    use smallvec::SmallVec;

    let prev_blob = encode_payload(&bytes(0xAA), Encoding::Zstd)?;
    // event = None throughout, so no RawEventRef construction is needed.
    let high_bytes = [0xBB];
    let high: PointRow<Values> = (
        Some(Bytes::copy_from_slice(&high_bytes)),
        None,
        None,
        None,
        None,
        None,
        None,
    );
    let low: PointRow<Values> = (
        None,
        Some(prev_blob),
        Some(4_i16),
        Some(1_i32),
        None,
        None,
        None,
    );
    let high_coordinate = Coordinate::from_bytes(vec![0xFE]);
    let low_coordinate = Coordinate::from_bytes(vec![0x01]);
    let mut rows: CellBuffer<(Bytes, PointRow<Values>)> = SmallVec::new();
    rows.push((Bytes::copy_from_slice(high_coordinate.as_bytes()), high));
    rows.push((Bytes::copy_from_slice(low_coordinate.as_bytes()), low));
    let decoded = [low_coordinate.as_bytes(), high_coordinate.as_bytes()]
        .into_iter()
        .map(|coordinate| {
            take_row(&mut rows, coordinate)
                .map(decode_point::<Values>)
                .transpose()
                .map(|cell| cell.map(|(cell, _)| cell))
        })
        .collect::<Result<CellBuffer<_>, CassandraCellStoreError>>();
    match decoded {
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
    use super::super::read::{decode_point, take_row};
    use super::decode::PointRow;
    use crate::state::cell::Values;
    use smallvec::smallvec;

    let low_data = [0x11];
    let high_data = [0x33];
    let row = |data: &[u8]| -> PointRow<Values> {
        (
            Some(Bytes::copy_from_slice(data)),
            None,
            Some(1_i16),
            Some(1_i32),
            None,
            None,
            None,
        )
    };
    let low = Coordinate::from_bytes(vec![1]);
    let absent = Coordinate::from_bytes(vec![2]);
    let high = Coordinate::from_bytes(vec![3]);
    let mut rows: CellBuffer<_> = smallvec![
        (Bytes::copy_from_slice(high.as_bytes()), row(&high_data)),
        (Bytes::copy_from_slice(low.as_bytes()), row(&low_data)),
    ];

    let decoded = [low.as_bytes(), absent.as_bytes(), high.as_bytes()]
        .into_iter()
        .map(|coordinate| {
            take_row(&mut rows, coordinate)
                .map(decode_point::<Values>)
                .transpose()
                .map(|cell| cell.map(|(cell, _)| cell))
        })
        .collect::<Result<CellBuffer<_>, CassandraCellStoreError>>()?;
    assert_eq!(decoded.len(), 3);
    assert_eq!(
        decoded[0]
            .as_ref()
            .and_then(|cell| cell.project_committed())
            .map(Bytes::as_ref),
        Some(&low_data[..])
    );
    assert!(decoded[1].is_none());
    assert_eq!(
        decoded[2]
            .as_ref()
            .and_then(|cell| cell.project_committed())
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
    use crate::state::resolve::ResolveCellError;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store();
    let c = collection("resolved-corrupt-error")?;
    let id = c.id();
    seed_prev_without_event_and_blob_without_encoding(fx.cassandra.session(), id).await?;

    let batch = CoordinateBatch::chunks([0xFEu8, 0x01].map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("non-empty read list must yield one batch"))?;
    match store.provisional_many(id, SECTIONS[0], &batch).await {
        Err(ResolveCellError::Store(CassandraCellStoreError::CorruptCell(reason))) => assert_eq!(
            reason,
            CellCorruptReason::PrevWithoutEvent,
            "the lowest corrupt coordinate determines the recovery error"
        ),
        other => return Err(eyre!("expected PrevWithoutEvent, got {other:?}")),
    }
    Ok(())
}

/// `provisional_many` issues one IN query per chunk, with no point or marker
/// reads. A fresh reader isolates the counters. The IN counter proves that the
/// method uses a batch query.
#[tokio::test]
async fn cassandra_raw_batch_is_one_query() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let seed = fx.bottom_store();
    let c = collection("raw-one-query")?;
    let id = c.id();
    let staging = event(0x11);
    let mut writes = Vec::new();
    for b in [1u8, 2] {
        let cell = cell_in(0, b);
        let prev = CellRead::<Values>::read(&seed, id, cell.as_ref()).await?.0;
        writes.push((
            cell,
            ProvisionalWrite::new(Some(bytes(b * 10)), prev, staging),
        ));
    }
    let marker = EventMarker::frozen(staging, &writes, &[], &evidence([].into(), None));
    seed.write_provisional(&c, listed(&marker, &writes)?)
        .await?;

    // A fresh store: cold counters shared across its clones.
    let reader = fx.bottom_store();
    let counters = reader.read_counts();
    let batch = CoordinateBatch::chunks([1u8, 2].map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("non-empty read list must yield one batch"))?;
    let out = reader.provisional_many(id, SECTIONS[0], &batch).await?;
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
        "no marker read"
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
        let store = fx.bottom_store();
        run_raw_batch_parity_trace(store, trace).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(ModelProperty(|trace| TEST_RUNTIME.block_on(run(trace))));
}

/// Ascending-output test over the live store. The sort requirement is also
/// verified by `borrowed_batch_decodes_in_resolution_order` and
/// `provisional_batch_coordinates_are_sorted_and_distinct`.
#[tokio::test]
async fn cassandra_raw_batch_ascending_output() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    run_raw_batch_ascending_output(fx.bottom_store()).await
}

/// No-side-effects test over the live store:
/// `provisional_many` never resolves, writes, or caches.
#[tokio::test]
async fn cassandra_raw_batch_no_side_effects() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store();
    run_raw_batch_no_side_effects(store).await
}

/// A bounded scan must not decode a corrupt row beyond its end.
#[tokio::test]
async fn scan_excludes_corrupt_endpoint() -> Result<()> {
    use crate::state::cell::Presence;
    use crate::state::cell_key::{Direction, Scan};
    use futures::TryStreamExt;
    use std::num::NonZeroUsize;
    use std::ops::Bound;

    let fx = fixture().await?;
    let store = fx.bottom_store();
    let collection = collection("bounded-corruption")?;
    let (low, high) =
        seed_prev_without_event_and_blob_without_encoding(fx.cassandra.session(), collection.id())
            .await?;
    let middle = cell_in(0, 0x80);
    store
        .write_resolved(&collection, &[(middle.clone(), Some(bytes(42)))], &[])
        .await?;

    for (dir, end) in [(Direction::Forward, high), (Direction::Backward, low)] {
        for fetch_hint in [None, Some(NonZeroUsize::MIN)] {
            let scan = Scan {
                section: middle.section,
                start: Bound::Included(middle.coordinate.as_bytes()),
                dir,
                end: Bound::Excluded(end.coordinate.as_bytes()),
                fetch_hint,
            };
            let values: Vec<_> = CellRead::<Values>::scan(&store, collection.id(), scan)
                .try_collect()
                .await?;
            assert_eq!(values, vec![(middle.clone(), bytes(42))]);
            let keys: Vec<_> = CellRead::<Presence>::scan(&store, collection.id(), scan)
                .try_collect()
                .await?;
            assert_eq!(keys, vec![(middle.clone(), ())]);
        }
    }
    Ok(())
}
