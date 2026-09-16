use super::*;
use crate::state::cell::Values;
use crate::state::store::CellRead;
use crate::state::tests::support::evidence;

/// Stage a set, observe it provisional, promote, read back resolved — the
/// hot-path round-trip — then a direct resolved clear reads back absent. A fast
/// deterministic smoke of shapes the crash-equivalence property and its
/// physical oracle (`assert_physical`) reach organically over generated traces.
#[tokio::test]
async fn provisional_set_promote_and_resolved_clear_round_trip() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store();
    let c = collection("cart")?;
    let cell = value_cell();
    let data = Bytes::from_static(b"v1");

    let writes = [(
        cell.clone(),
        ProvisionalWrite::new(Some(data.clone()), Committed::new(None), event(1)),
    )];
    let marker = EventMarker::frozen(event(1), &writes, &[], &evidence([].into(), None));
    store.write_provisional(&c, &writes, Some(&marker)).await?;
    let staged = provisional_cells(&store, c.id()).await?;
    let (key, prov) = staged
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("expected a provisional cell after stage"))?;
    assert_eq!(key, cell);
    assert_eq!(prov.data(), Some(&data));
    assert_eq!(prov.prev(), None);
    assert_eq!(prov.event(), event(1));

    store.mark_resolved(&c, slice::from_ref(&cell)).await?;
    assert_eq!(
        CellRead::<Values>::read(&store, c.id(), &cell).await?.0,
        Committed::new(Some(data))
    );
    assert!(provisional_cells(&store, c.id()).await?.is_empty());

    store
        .write_resolved(&c, &[(cell.clone(), None)], &[])
        .await?;
    assert_eq!(
        CellRead::<Values>::read(&store, c.id(), &cell).await?.0,
        Committed::new(None)
    );
    Ok(())
}

/// Committing a staged clear over a present base **deletes the row** (the
/// row-absence invariant): the cell reads back absent, and no residue row
/// lingers — a stale `encoding`/`version` would still be selected. Settles
/// through the routed `commit_provisional` path (the promote arm that owns
/// clear→delete). A deterministic falsifier of the row-absence shape
/// `assert_physical` asserts for every model-absent coordinate on the crash
/// traces; it isolates the committed-clear delete leg at the bottom store.
#[tokio::test]
async fn committed_clear_deletes_the_row() -> Result<()> {
    use crate::cassandra::TABLE_KEYED_STATE_CELL;

    init_test_logging();
    let fx = fixture().await?;
    let dedup = MemoryDeduplicationStore::default();
    let store = fx.bottom_store();
    let c = collection("clear-deletes")?;
    let cell = value_cell();
    let old = Bytes::from_static(b"old");

    // Committed base present, then stage a clear over it and settle committed.
    store
        .write_resolved(&c, &[(cell.clone(), Some(old.clone()))], &[])
        .await?;
    let write = ProvisionalWrite::new(None, Committed::new(Some(old.clone())), event(2));
    let writes = [(cell.clone(), write.clone())];
    let marker = EventMarker::frozen(event(2), &writes, &[], &evidence([].into(), None));
    store.write_provisional(&c, &writes, Some(&marker)).await?;
    let staged = provisional_cells(&store, c.id()).await?;
    let (_, prov) = staged
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("expected a provisional cell after clear-over-present"))?;
    assert_eq!(prov.data(), None);
    assert_eq!(prov.prev(), Some(&old));

    dedup.insert(Uuid::from_u128(2)).await?;
    store
        .commit_provisional(&c, &marker, &[(cell.clone(), write)])
        .await?;

    assert_eq!(
        CellRead::<Values>::read(&store, c.id(), &cell).await?.0,
        Committed::new(None)
    );

    // The residue row would still be selected by its live `encoding`/`version`;
    // its absence proves the commit deleted the row rather than nulling columns.
    let cql = format!(
        "SELECT encoding, version FROM {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} WHERE segment_id \
         = ? AND key = ? AND state_type = ? AND name = ? AND kind = 0 AND section = ? AND \
         coordinate = ?"
    );
    let id = c.id();
    let residue = fx
        .cassandra
        .session()
        .query_unpaged(
            cql,
            (
                id.state_key().segment_id,
                id.state_key().key.as_ref(),
                i8::from(id.state_type()),
                id.name().as_str(),
                i8::from(cell.section),
                cell.coordinate.as_bytes(),
            ),
        )
        .await?
        .into_rows_result()?
        .maybe_first_row::<(Option<i16>, Option<i32>)>()?;
    assert!(
        residue.is_none(),
        "committed clear must delete the row, leaving no residue: {residue:?}"
    );
    Ok(())
}
