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
/// override (distinct fetch, per-position answers, and resolution) is
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

/// Within-batch duplicate co-observation: a repeated coordinate reuses its
/// first answer.
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
/// point `get`s confirm the two rows are distinguishable. The standalone
/// reader's batch read must surface the same error.
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

    // The standalone reader's batch read follows the same input order.
    let resources = CassandraCellResources::new(fx.cassandra.clone(), fx.queries.clone());
    match resources
        .read_committed_many::<Values>(id, SECTIONS[0], &batch.as_ref())
        .await
    {
        Err(CassandraCellStoreError::CorruptCell(reason)) => {
            assert_eq!(
                reason,
                CellCorruptReason::BlobWithoutEncoding,
                "the reader path surfaces the earliest input position's error"
            );
        }
        other => return Err(eyre!("expected B's BlobWithoutEncoding, got {other:?}")),
    }
    Ok(())
}

/// `take_row` answers each coordinate with its own fetched row, or `None` when
/// no row was fetched. Fetched rows arrive in any order, and each row answers
/// once.
#[test]
fn prop_take_row_matches_coordinates() {
    use super::super::read::take_row;
    use std::collections::BTreeSet;

    fn property(mut present: BTreeSet<u8>, shuffle: u8, requests: Vec<u8>) -> bool {
        // Multiplying by an odd factor permutes the byte values.
        let mut rows: CellBuffer<(Bytes, u8)> = present
            .iter()
            .map(|&coordinate| (Bytes::copy_from_slice(&[coordinate]), coordinate))
            .collect();
        rows.sort_by_key(|&(_, coordinate)| coordinate.wrapping_mul(shuffle | 1));
        requests.into_iter().all(|coordinate| {
            let expected = present.remove(&coordinate).then_some(coordinate);
            take_row(&mut rows, &[coordinate]) == expected
        })
    }
    QuickCheck::new().quickcheck(property as fn(BTreeSet<u8>, u8, Vec<u8>) -> bool);
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
    let marker = EventMarker::frozen(staging, &writes, Vec::new(), &evidence([].into(), None));
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

/// Ascending-output test over the live store. `prop_batch_coordinate_sets`
/// covers the sorted coordinate list that the read binds.
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
