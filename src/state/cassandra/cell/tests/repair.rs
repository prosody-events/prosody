use super::*;
use crate::cassandra::TABLE_KEYED_STATE_CELL;
use crate::state::tests::support::{evidence, seed_commit_evidence};

async fn corrupt_cleared_window(name: &str) -> Result<(Fixture, CassandraStore, CollectionRef)> {
    let fx = fixture().await?;
    let dedup = MemoryDeduplicationStore::default();
    let store = fx.bottom_store();
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
    let marker = EventMarker::frozen(
        foreign,
        &[],
        slice::from_ref(&clear),
        &evidence([].into(), None),
    );
    store
        .write_provisional(&collection, &[], Some(&marker))
        .await?;
    seed_commit_evidence(&store, &collection).await?;

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

    assert!(admit_collection(&store, &dedup, &collection).await?);
    Ok((fx, store, collection))
}

/// Admission applies a committed clear before the point read decodes rows.
#[tokio::test]
async fn admit_removes_corrupt_cleared_rows_before_point_read() -> Result<()> {
    init_test_logging();
    let (_fx, store, collection) = corrupt_cleared_window("point-repair-order").await?;

    assert_eq!(
        store.get(collection.id(), &cell_in(0, 1)).await?,
        Committed::new(None)
    );
    Ok(())
}

/// Admission applies a committed clear before the batch read decodes rows.
#[tokio::test]
async fn admit_removes_corrupt_cleared_rows_before_batch_read() -> Result<()> {
    init_test_logging();
    let (_fx, store, collection) = corrupt_cleared_window("batch-repair-order").await?;
    let batch = CoordinateBatch::chunks([1_u8, 2, 3].map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("non-empty read list must yield one batch"))?;

    let got = Box::pin(store.get_many(collection.id(), SECTIONS[0], &batch)).await?;
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
