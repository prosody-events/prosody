use super::*;

/// Positional binding-order proof (the one silent-failure surface):
/// `scylla::Batch` binds its statement list 1:1 with the value list, and on a
/// count/order mismatch scylla falls back to an **empty** context with no
/// error — a misordered `unzip` flatten would bind a row against the wrong
/// statement's columns silently. Build ONE batch whose single flatten
/// interleaves every [`RowShape`] across **five distinct prepared
/// statements** — stage a cell, promote a pre-provisioned cell, write a fresh
/// resolved value, row-delete a pre-seeded cell, and write the event marker —
/// with **distinct payloads** so any cross-binding corrupts an observable
/// value. Read everything back through a FRESH store (a cold marker memo, so
/// the recovery read decodes the batch-written marker): each value landing
/// correctly proves its statement bound its own columns. A follow-up batch
/// proves `marker_delete` (same-row timestamp ties bar it from the first
/// batch — see [`mixed_binding_batch`]).
#[tokio::test]
async fn mixed_statement_batch_binds_each_statement_to_its_own_columns() -> Result<()> {
    use super::encoding::encode;
    use super::{Pk, marker_delete_unit};
    use crate::state::cell_key::Coordinate;
    use crate::state::marker::encode_marker_payload;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("mixed-batch")?;
    let id = c.id().clone();

    let cell = |b: u8| CellKey {
        section: Section::new(0),
        coordinate: Coordinate::from_bytes(vec![b]),
    };
    let (cell_a, cell_b, cell_c, cell_d) = (cell(1), cell(2), cell(3), cell(4));
    let (data_a, data_b, data_c, data_d) = (
        Bytes::from_static(b"aaa"),
        Bytes::from_static(b"bbb"),
        Bytes::from_static(b"ccc"),
        Bytes::from_static(b"ddd"),
    );

    // Pre-provision B so the mixed batch can promote it.
    let writes_b = [(
        cell_b.clone(),
        ProvisionalWrite::new(Some(data_b.clone()), Committed::new(None), event(1)),
    )];
    let marker_b = EventMarker::frozen(event(1), &writes_b, &[]);
    store
        .write_provisional(&c, &writes_b, Some(&marker_b))
        .await?;
    // Pre-seed D resolved so the batch's `cell_delete` has a row to remove.
    store
        .write_resolved(&c, &[(cell_d.clone(), Some(data_d.clone()))], &[])
        .await?;

    // Owned blobs the bound rows borrow into; must outlive the awaited batch.
    let blob_a = encode_cell_blobs(Some(&data_a), None)?;
    let blob_c = encode_cell_blobs(Some(&data_c), None)?;
    // The batch's event marker lists exactly A (the one cell it stages),
    // encoded through the production payload + blob conventions.
    let staged_a = [(
        cell_a.clone(),
        ProvisionalWrite::new(Some(data_a.clone()), Committed::new(None), event(2)),
    )];
    let marker_payload = encode_marker_payload(&EventMarker::frozen(event(2), &staged_a, &[]))?;
    let payload = encode(&marker_payload)?;
    let marker_blob = MarkerBlob {
        payload,
        event: event(2),
    };
    // One batch, one flatten, five distinct statements interleaved.
    let units = mixed_binding_batch(
        &fx.queries,
        &id,
        &blob_a,
        &blob_c,
        &marker_blob,
        [&cell_a, &cell_b, &cell_c, &cell_d],
    );
    fx.cassandra
        .execute_unlogged_batches(&units, 1 << 20, 4_096, SHARD_FANOUT_CONCURRENCY)
        .await?;

    // Read back through a FRESH store: its cold memo forces the durable
    // marker read, so recovery decodes the batch-written marker (proving the
    // `marker_write` row bound its own columns), finds it lists exactly A,
    // and reads A back provisional with A's payload (the stage row bound its
    // columns). B was promoted out of the provisional set by the key-only
    // promote row.
    //
    // Fresh-assignment presence for each fresh reader: the presence latch is
    // per-assignment state; without a cold domain the reader would presence-hit
    // (the writer staged into the shared fixture latch) into its empty standing
    // map and skip the durable marker read this test exists to exercise.
    // Exclusive keyspace name (clearing-test rule).
    let (_db, _cache, presence_index) = test_db::keyspace_pair("cassandra_mixed_presence")?;
    presence_index.clear()?;
    let reader = fx.bottom_store_with(
        ScriptedOracle::default(),
        test_db::presence("cassandra_mixed_presence")?,
    );
    let staged = provisional_cells(&reader, &id).await?;
    assert_eq!(staged.len(), 1, "only A stays provisional: {staged:?}");
    let (key, prov) = staged
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("expected A provisional"))?;
    assert_eq!(key, cell_a);
    assert_eq!(prov.data(), Some(&data_a));

    // B promoted to its own payload (the key-only promote row bound its
    // columns); C written fresh resolved to its own payload (the resolved-write
    // row bound its columns).
    assert_eq!(
        reader.get(&id, &cell_b, event(3)).await?,
        Committed::new(Some(data_b))
    );
    assert_eq!(
        reader.get(&id, &cell_c, event(3)).await?,
        Committed::new(Some(data_c))
    );
    // D's row was deleted (the `cell_delete` bound its own `kind=Cell` key
    // columns, not the marker slice's `kind=Marker`), so it reads absent.
    assert_eq!(
        reader.get(&id, &cell_d, event(3)).await?,
        Committed::new(None)
    );

    // Follow-up batch: `marker_delete` removes the fixed-address marker row —
    // a second fresh store's cold recovery then finds no marker at all.
    let delete = [marker_delete_unit(Pk::of(&id), &fx.queries)];
    fx.cassandra
        .execute_unlogged_batches(&delete, 1 << 20, 4_096, SHARD_FANOUT_CONCURRENCY)
        .await?;
    // Fresh reader = fresh assignment: reset the exclusive presence latch so
    // this reader's cold memo forces the durable read that now finds no marker.
    presence_index.clear()?;
    let reader = fx.bottom_store_with(
        ScriptedOracle::default(),
        test_db::presence("cassandra_mixed_presence")?,
    );
    assert!(
        provisional_cells(&reader, &id).await?.is_empty(),
        "marker_delete removed the marker row, so cold recovery lists nothing"
    );
    Ok(())
}
