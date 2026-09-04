use super::*;
use crate::cassandra::TABLE_KEYED_STATE_CELL;

async fn corrupt_cleared_window(
    name: &str,
) -> Result<(Fixture, CassandraStore<ScriptedOracle>, CollectionRef)> {
    let fx = fixture().await?;
    let oracle = ScriptedOracle::default();
    let store = fx.bottom_store(oracle.clone())?;
    let collection = collection(name)?;
    let id = collection.id();

    store
        .write_resolved(
            &collection,
            &[
                (cell_in(0, 1), Some(bytes(1))),
                (cell_in(0, 2), Some(bytes(2))),
                (cell_in(0, 3), Some(bytes(3))),
            ],
            &[],
        )
        .await?;

    let foreign = event(0xF0);
    let survivors = [(cell_in(0, 2), Some(bytes(2)))];
    let clear = SectionClear::frozen_resolved(SECTIONS[0], &survivors);
    let marker = EventMarker::frozen(foreign, &[], slice::from_ref(&clear));
    store
        .write_provisional(&collection, &[], Some(&marker))
        .await?;
    oracle.record_message(Uuid::from_u128(0xF0)).await?;

    let stale = cell_in(0, 1);
    let corrupt = format!(
        "UPDATE {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} SET encoding = null WHERE segment_id = ? \
         AND key = ? AND state_type = ? AND name = ? AND kind = 0 AND section = ? AND coordinate \
         = ?",
    );
    fx.cassandra
        .session()
        .query_unpaged(
            corrupt,
            (
                id.state_key().segment_id,
                id.state_key().key.as_ref(),
                i8::from(id.state_type()),
                id.name().as_str(),
                i8::from(stale.section),
                stale.coordinate.as_bytes(),
            ),
        )
        .await?;

    Ok((fx, store, collection))
}

/// A point read repairs its unsettled section clear before it decodes the stale
/// row that the clear removes.
#[tokio::test]
async fn point_read_repairs_before_decode() -> Result<()> {
    init_test_logging();
    let (_fx, store, collection) = corrupt_cleared_window("point-repair-order").await?;

    assert_eq!(
        store.get(collection.id(), &cell_in(0, 1), event(7)).await?,
        Committed::new(None)
    );
    Ok(())
}

/// A batch read repairs its unsettled section clear before it decodes any stale
/// row that the clear removes.
#[tokio::test]
async fn batch_read_repairs_before_decode() -> Result<()> {
    init_test_logging();
    let (_fx, store, collection) = corrupt_cleared_window("batch-repair-order").await?;
    let batch = CoordinateBatch::chunks([1_u8, 2, 3].map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("non-empty read list must yield one batch"))?;

    let got = Box::pin(store.get_many(collection.id(), SECTIONS[0], &batch, event(7))).await?;
    assert_eq!(
        got.as_slice(),
        &[
            Committed::new(None),
            Committed::new(Some(bytes(2))),
            Committed::new(None),
        ]
    );
    Ok(())
}
