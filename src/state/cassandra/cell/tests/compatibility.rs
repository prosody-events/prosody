use super::*;
use crate::cassandra::TABLE_KEYED_STATE_CELL;

async fn read_cell_blob(fx: &Fixture, id: &CollectionId) -> Result<(Vec<u8>, i16)> {
    let cql = format!(
        "SELECT data, encoding FROM {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} WHERE segment_id = ? \
         AND key = ? AND state_type = ? AND name = ?"
    );
    let (data, encoding) = fx
        .cassandra
        .session()
        .query_unpaged(
            cql,
            (
                id.state_key().segment_id,
                id.state_key().key.as_ref(),
                i8::from(id.state_type()),
                id.name().as_str(),
            ),
        )
        .await?
        .into_rows_result()?
        .maybe_first_row::<(Option<Vec<u8>>, Option<i16>)>()?
        .ok_or_else(|| eyre!("cell row missing"))?;
    Ok((
        data.ok_or_else(|| eyre!("data column missing"))?,
        encoding.ok_or_else(|| eyre!("encoding column missing"))?,
    ))
}

/// Legacy decode tolerance: the null-null-with-encoding residue shape is no
/// longer produced by any statement (a committed-absent cell deletes its row),
/// but rows written by earlier builds may still carry it. Seeded directly via
/// raw CQL, it must still read `Committed(None)`, never corruption — the
/// decoder's tolerance kept honest now that no code path produces the shape.
#[tokio::test]
async fn legacy_null_null_residue_reads_committed_none() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("legacy-residue")?;
    let cell = value_cell();
    let id = c.id();

    // Both blobs and `event` absent, `encoding` = 4 (`Zstd`), `version` = 1
    // (INITIAL_VERSION) — the legacy null-null residue shape.
    let insert = format!(
        "INSERT INTO {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} (segment_id, key, state_type, name, \
         kind, section, coordinate, encoding, version) VALUES (?, ?, ?, ?, 0, ?, ?, 4, 1)"
    );
    fx.cassandra
        .session()
        .query_unpaged(
            insert,
            (
                id.state_key().segment_id,
                id.state_key().key.as_ref(),
                i8::from(id.state_type()),
                id.name().as_str(),
                i8::from(cell.section),
                cell.coordinate.as_bytes(),
            ),
        )
        .await?;

    assert_eq!(
        store.get(id, &cell, event(1)).await?,
        Committed::new(None),
        "the decoder must read the legacy residue as committed-absence"
    );
    Ok(())
}

/// Cassandra uses Zstd for a payload larger than its compression block.
/// A raw CQL read proves that the store wrote the selected durable form.
#[tokio::test]
async fn cassandra_data_column_is_zstd_compressed() -> Result<()> {
    use super::encoding::{Encoding, decode_payload};

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("cart")?;
    let cell = value_cell();
    let payload = Bytes::from(vec![0xAB_u8; 16 * 1024 + 1]);
    store
        .write_resolved(&c, &[(cell, Some(payload.clone()))], &[])
        .await?;

    let id = c.id();
    let (raw, encoding) = read_cell_blob(&fx, id).await?;

    assert_ne!(
        raw.as_slice(),
        payload.as_ref(),
        "durable data column must be compressed, not stored raw"
    );
    assert!(
        raw.len() < payload.len(),
        "zstd frame ({} bytes) should be smaller than the {} raw bytes",
        raw.len(),
        payload.len()
    );
    assert_eq!(
        decode_payload(&raw, Encoding::Zstd)?,
        payload,
        "zstd frame must decompress to the payload"
    );
    assert_eq!(encoding, i16::from(Encoding::Zstd));
    Ok(())
}

/// Cassandra stores a payload at or below its compression block unchanged.
/// A raw CQL read freezes both the bytes and the encoding discriminator.
#[tokio::test]
async fn cassandra_data_column_is_raw_through_the_block_size() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("raw-format")?;
    let cell = value_cell();
    let payload = Bytes::from_static(b"raw durable payload");
    store
        .write_resolved(&c, &[(cell, Some(payload.clone()))], &[])
        .await?;

    let id = c.id();
    let (data, encoding) = read_cell_blob(&fx, id).await?;

    assert_eq!(data.as_slice(), payload.as_ref());
    assert_eq!(encoding, 1);
    Ok(())
}

/// A cell whose `event_ref` UDT carries a `timer_type` outside `{0,1,2,3}`
/// (external CQL corruption, or a forward-compat cross-version writer) must
/// reject the **one row** as `CorruptUdt`/`Permanent`, not tear the partition
/// down with scylla's `Terminal` `DeserializationError`. Injects via raw CQL so
/// the bad byte travels the real wire/materialization path — the unit test
/// cannot, since it bypasses scylla deserialization — and reads through the
/// **bare** `bottom_store` so no fjall cache hit serves the read without
/// decoding the row.
#[tokio::test]
async fn corrupt_timer_type_is_permanent_not_terminal() -> Result<()> {
    use crate::error::{ClassifyError, ErrorCategory};
    use crate::state::cassandra::CassandraCellStoreError;
    use crate::state::resolve::ResolveCellError;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("corrupt-timer")?;
    let id = c.id();

    // Stage one healthy provisional cell through the real store (so the event
    // marker lists its coordinate on the real wire path), then corrupt the
    // cell row's `event` UDT by raw CQL: the Timer arm rejects the unknown
    // `timer_type: 99` in the literal (whose own `kind: 1` field means Timer —
    // distinct from the clustering `kind` column). Recovery batch-reads the
    // marker-listed coordinate (a one-row `IN` query) and must reject the ONE
    // row, not the partition.
    let cell = value_cell();
    let writes = [(
        cell.clone(),
        ProvisionalWrite::new(
            Some(Bytes::from_static(b"v")),
            Committed::new(None),
            event(1),
        ),
    )];
    let marker = EventMarker::frozen(event(1), &writes, &[]);
    store.write_provisional(&c, &writes, Some(&marker)).await?;
    let corrupt_cell = format!(
        "UPDATE {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} SET event = {{kind: 1, msg_dedup_id: \
         null, timer_type: 99, time: 0, tag: 0}} WHERE segment_id = ? AND key = ? AND state_type \
         = ? AND name = ? AND kind = 0 AND section = ? AND coordinate = ?"
    );
    let binds = (
        id.state_key().segment_id,
        id.state_key().key.as_ref(),
        i8::from(id.state_type()),
        id.name().as_str(),
        0_i8,
        b"" as &[u8],
    );
    fx.cassandra
        .session()
        .query_unpaged(corrupt_cell, binds)
        .await?;

    let stream = store.provisional_cells(id);
    futures::pin_mut!(stream);
    let err = loop {
        match stream.next().await {
            Some(Ok(_)) => {}
            Some(Err(e)) => break e,
            None => return Err(eyre!("expected a CorruptUdt error, got a clean scan")),
        }
    };
    assert_eq!(err.classify_error(), ErrorCategory::Permanent);
    assert!(
        matches!(
            err,
            ResolveCellError::Store(CassandraCellStoreError::CorruptUdt(_))
        ),
        "expected Store(CorruptUdt), got {err:?}"
    );
    Ok(())
}

/// Read-path uniqueness invariant: the Cassandra decode path returns a present
/// cell that is **uniquely owned** (`try_into_mut().is_ok()`). A collection's
/// typed read relies on that fast path. Runs over random non-empty payloads.
#[test]
fn prop_cassandra_present_cell_is_uniquely_owned() {
    async fn check(payload: Vec<u8>) -> Result<bool> {
        let fx = fixture().await?;
        let store = fx.bottom_store(ScriptedOracle::default());
        let c = collection("uniq")?;
        let cell = value_cell();
        let data = Bytes::from(payload);
        store
            .write_resolved(&c, &[(cell.clone(), Some(data))], &[])
            .await?;
        let Some(bytes) = store.get(c.id(), &cell, event(1)).await?.into_inner() else {
            return Err(eyre!("expected a present committed value"));
        };
        Ok(bytes.try_into_mut().is_ok())
    }

    fn prop(payload: Vec<u8>) -> TestResult {
        if payload.is_empty() {
            return TestResult::discard();
        }
        match TEST_RUNTIME.block_on(check(payload)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("present cell was a shared clone, not uniquely owned"),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(prop as fn(Vec<u8>) -> TestResult);
}

/// Co-anchoring regression-prover: every cell of one multi-cell
/// write under a collection TTL must share a single write timestamp **and**
/// TTL. One same-partition `UNLOGGED BATCH` carries one batch write timestamp
/// and one coordinator TTL anchor, so `WRITETIME(data)` and `TTL(data)` are
/// identical across the cells; the old per-cell `execute()` loop stamped each
/// statement with a *distinct* monotonic client timestamp, so this fails
/// **deterministically** against it — the discriminator is the timestamp, not
/// wall-clock TTL drift, so there is no second-boundary flakiness. Run over
/// multi-cell writes of varying cardinality and payload sizes.
#[test]
fn prop_multi_cell_write_co_anchors_writetime_and_ttl() {
    use crate::cassandra::TABLE_KEYED_STATE_CELL;
    use crate::state::cell_key::Coordinate;
    use crate::timers::duration::CompactDuration;

    async fn check(payloads: Vec<Vec<u8>>) -> Result<bool> {
        let fx = fixture().await?;
        let store = fx.bottom_store(ScriptedOracle::default());
        let id = CollectionId::new(
            StateKey::new(Uuid::new_v4(), Arc::from("k")),
            StateType::Application,
            StateName::try_new("co-anchor")?,
        );
        // A collection TTL so the `USING TTL` path is exercised; the batch must
        // apply one coordinator anchor across every cell.
        let c = CollectionRef::new(id.clone(), Some(CompactDuration::new(3_600)));
        let cells: Vec<(CellKey, Option<Bytes>)> = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let cell = CellKey {
                    section: Section::new(0),
                    coordinate: Coordinate::from_bytes(vec![i as u8]),
                };
                (cell, Some(Bytes::from(p.clone())))
            })
            .collect();
        store.write_resolved(&c, &cells, &[]).await?;

        // `WRITETIME`/`TTL` are read functions (no schema change); both are
        // non-null because every cell wrote a present `data`.
        let cql = format!(
            "SELECT WRITETIME(data), TTL(data) FROM {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? AND kind = 0 AND \
             section = ?"
        );
        let result = fx
            .cassandra
            .session()
            .query_unpaged(
                cql,
                (
                    id.state_key().segment_id,
                    id.state_key().key.as_ref(),
                    i8::from(id.state_type()),
                    id.name().as_str(),
                    0_i8,
                ),
            )
            .await?
            .into_rows_result()?;
        let mut writetimes: Vec<Option<i64>> = Vec::new();
        let mut ttls: Vec<Option<i32>> = Vec::new();
        for row in result.rows::<(Option<i64>, Option<i32>)>()? {
            let (writetime, ttl) = row?;
            writetimes.push(writetime);
            ttls.push(ttl);
        }
        // One batch ⇒ every cell shares the batch timestamp and the TTL anchor.
        let writetime_equal = writetimes.windows(2).all(|w| w[0] == w[1]);
        let ttl_equal = ttls.windows(2).all(|w| w[0] == w[1]);
        Ok(writetime_equal && ttl_equal)
    }

    fn prop(payloads: Vec<Vec<u8>>) -> TestResult {
        // ≥2 cells for "equal across cells" to discriminate; ≤256 so the
        // index-as-coordinate-byte stays unique.
        if payloads.len() < 2 || payloads.len() > 256 {
            return TestResult::discard();
        }
        match TEST_RUNTIME.block_on(check(payloads)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(
                "cells of one multi-cell write had differing WRITETIME/TTL — not one batch",
            ),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(prop as fn(Vec<Vec<u8>>) -> TestResult);
}

/// Builds the mixed-statement batch for the binding-order test: five one-row
/// units whose single flatten interleaves every [`RowShape`] across five
/// distinct prepared statements — stage A, promote B, resolved-write C,
/// `cell_delete` D, and the `marker_write` of the collection's event marker.
/// (`marker_delete` cannot join this batch: it targets the same fixed-address
/// row as the marker write, and within one batch the shared timestamp's
/// delete-wins tie-break would erase the write it is meant to prove — so the
/// delete is proven in a follow-up batch.) The blobs, cells, and `id` outlive
/// the returned borrows (the caller holds them).
pub(super) fn mixed_binding_batch<'a>(
    q: &'a CellQueries,
    id: &'a CollectionId,
    blob_a: &'a CellBlobs,
    blob_c: &'a CellBlobs,
    marker_blob: &'a MarkerBlob,
    cells: [&'a CellKey; 4],
) -> Vec<BatchUnit<CellBatchRow<'a>>> {
    use super::Pk;
    use smallvec::smallvec;

    let [cell_a, cell_b, cell_c, cell_d] = cells;
    let pk = Pk::of(id);
    let (addr_a, addr_b, addr_c, addr_d) = (
        CellAddr::new(pk, cell_a),
        CellAddr::new(pk, cell_b),
        CellAddr::new(pk, cell_c),
        CellAddr::new(pk, cell_d),
    );
    vec![
        BatchUnit::new(
            1_024,
            smallvec![CellBatchRow {
                statement: &q.write_provisional_no_ttl,
                row: RowShape::Stage(StageRow {
                    ttl: None,
                    data: blob_a.data(),
                    prev_data: None,
                    encoding: blob_a.encoding(),
                    version: blob_a.version(),
                    event: event(2),
                    addr: addr_a,
                }),
            }],
        ),
        BatchUnit::new(
            1_024,
            smallvec![CellBatchRow {
                statement: &q.mark_resolved,
                row: RowShape::Key(KeyRow {
                    kind: CellKind::Cell,
                    addr: addr_b,
                }),
            }],
        ),
        BatchUnit::new(
            1_024,
            smallvec![CellBatchRow {
                statement: &q.write_resolved_no_ttl,
                row: RowShape::Resolved(ResolvedRow {
                    ttl: None,
                    data: blob_c.data(),
                    encoding: blob_c.encoding(),
                    version: blob_c.version(),
                    addr: addr_c,
                }),
            }],
        ),
        BatchUnit::new(
            1_024,
            smallvec![CellBatchRow {
                statement: &q.cell_delete,
                row: RowShape::Key(KeyRow {
                    kind: CellKind::Cell,
                    addr: addr_d,
                }),
            }],
        ),
        BatchUnit::new(
            1_024,
            smallvec![CellBatchRow {
                statement: &q.marker_write_no_ttl,
                row: RowShape::MarkerWrite(MarkerWriteRow {
                    ttl: None,
                    payload: marker_blob.payload.as_ref(),
                    encoding: marker_blob.payload.encoding(),
                    event: event(2),
                    addr: CellAddr::marker(pk),
                }),
            }],
        ),
    ]
}
