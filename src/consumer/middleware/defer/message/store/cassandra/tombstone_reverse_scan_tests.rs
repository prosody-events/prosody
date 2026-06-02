//! Regression: `get_next_deferred_message` must resolve the live tail of a
//! partition even when a dense band of low-offset tombstones sits below it.
//! A forward `LIMIT 1` scan walks the whole graveyard; the store uses a
//! reverse scan so it lands on the live row directly.

use super::*;
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::{ConsumerGroup, Partition, Topic};

/// Density of the tombstone band — chosen to mimic the post-FIFO
/// graveyard observed in production (~5k cells per partition).
const TOMBSTONE_COUNT: i64 = 5_000;

async fn build_store() -> color_eyre::Result<CassandraMessageDeferStore> {
    let config = CassandraConfiguration::builder()
        .nodes(vec!["localhost:9042".to_owned()])
        .keyspace("prosody_test".to_owned())
        .build()
        .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;
    let cassandra_store = CassandraStore::new(&config).await?;
    let segment_store = CassandraSegmentStore::new(cassandra_store.clone(), "prosody_test").await?;
    let queries = Arc::new(Queries::new(cassandra_store.session(), "prosody_test").await?);
    let segment = LazySegment::new(
        segment_store,
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from(format!("test-consumer-group-{}", uuid::Uuid::new_v4())) as ConsumerGroup,
    );
    Ok(CassandraMessageDeferStore::new(
        cassandra_store,
        queries,
        segment,
        1_024,
    ))
}

#[tokio::test]
async fn test_get_next_skips_low_offset_tombstones() -> color_eyre::Result<()> {
    let store = build_store().await?;
    let k: Key = Arc::from(format!("tombstone-msg-{}", uuid::Uuid::new_v4()));
    let segment_id = store.segment_id().await?;
    let ttl = store.store.base_ttl();

    // Seed the graveyard: INSERT + raw DELETE at low offsets, leaving
    // tombstones without touching `next_offset`.
    for offset in 0..TOMBSTONE_COUNT {
        let offset = Offset::from(offset);
        store
            .session()
            .execute_unpaged(
                &store.queries.insert_deferred_message_without_retry_count,
                (&segment_id, k.as_ref(), offset, ttl),
            )
            .await?;
        store
            .session()
            .execute_unpaged(
                &store.queries.remove_deferred_message,
                (&segment_id, k.as_ref(), offset),
            )
            .await?;
    }

    // Live row + `next_offset` above the tombstone band — the
    // post-FIFO-completion shape that triggers the bug.
    let live_offset = Offset::from(TOMBSTONE_COUNT);
    store
        .session()
        .execute_unpaged(
            &store.queries.insert_deferred_message_with_retry_count,
            (
                &segment_id,
                k.as_ref(),
                live_offset,
                0_i32,
                live_offset,
                ttl,
            ),
        )
        .await?;
    let _ = store.cache.remove(k.as_ref());

    // Forward `LIMIT 1` walked the graveyard; reverse `LIMIT 1`
    // resolves on the live tail.
    let got = store.get_next_deferred_message(&k).await?;
    assert_eq!(got, Some((live_offset, 0)));

    store.delete_key(&k).await?;
    Ok(())
}
