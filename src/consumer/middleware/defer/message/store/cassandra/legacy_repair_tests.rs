use super::*;
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::{ConsumerGroup, Partition, Topic};

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

/// Build a fresh store and seed a unique key into the pre-migration
/// (legacy) shape: `clustering` offsets with `next_offset = NULL` and the
/// given static `retry_count`. Returns the store and the seeded key.
async fn seeded_legacy(
    clustering: &[i64],
    retry_count: Option<u32>,
) -> color_eyre::Result<(CassandraMessageDeferStore, Key)> {
    let store = build_store().await?;
    let k: Key = Arc::from(format!("legacy-key-{}", uuid::Uuid::new_v4()));
    store
        .seed_legacy_for_test(&k, clustering, retry_count)
        .await?;
    Ok((store, k))
}

/// Assert the persisted `next_offset` static column matches `expected`.
async fn assert_next_offset(
    store: &CassandraMessageDeferStore,
    key: &Key,
    expected: Option<i64>,
) -> color_eyre::Result<()> {
    let db_next = store.read_next_offset_for_invariant_check(key).await?;
    assert_eq!(db_next, expected);
    Ok(())
}

#[tokio::test]
async fn test_legacy_get_next_repairs() -> color_eyre::Result<()> {
    let (store, k) = seeded_legacy(&[10], Some(2)).await?;

    let got = store.get_next_deferred_message(&k).await?;
    assert_eq!(got, Some((Offset::from(10_i64), 2)));

    // Next-offset static column is populated after the repair UPDATE.
    assert_next_offset(&store, &k, Some(10)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_is_deferred_repairs() -> color_eyre::Result<()> {
    let (store, k) = seeded_legacy(&[7], Some(1)).await?;

    let rc = store.is_deferred(&k).await?;
    assert_eq!(rc, Some(1));

    assert_next_offset(&store, &k, Some(7)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_defer_first_on_legacy_partition() -> color_eyre::Result<()> {
    let (store, k) = seeded_legacy(&[5], Some(3)).await?;

    // A fresh-key precondition is already violated on legacy partitions;
    // the store must still converge `next_offset` to the true minimum.
    store.defer_first_message(&k, Offset::from(20_i64)).await?;

    assert_next_offset(&store, &k, Some(5)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_append_on_legacy_partition() -> color_eyre::Result<()> {
    let (store, k) = seeded_legacy(&[9], Some(0)).await?;

    store
        .append_deferred_message(&k, Offset::from(3_i64))
        .await?;

    assert_next_offset(&store, &k, Some(3)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_complete_retry_at_min() -> color_eyre::Result<()> {
    let (store, k) = seeded_legacy(&[5, 10], Some(2)).await?;

    let result = store
        .complete_retry_success(&k, Offset::from(5_i64))
        .await?;
    assert!(matches!(
        result,
        MessageRetryCompletionResult::MoreMessages { next_offset } if next_offset == Offset::from(10_i64)
    ));

    assert_next_offset(&store, &k, Some(10)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_complete_retry_above_min() -> color_eyre::Result<()> {
    let (store, k) = seeded_legacy(&[5, 10], Some(2)).await?;

    let result = store
        .complete_retry_success(&k, Offset::from(10_i64))
        .await?;
    assert!(matches!(
        result,
        MessageRetryCompletionResult::MoreMessages { next_offset } if next_offset == Offset::from(5_i64)
    ));

    // Repair advanced next_offset to 5 on the initial read; completing a
    // non-min offset leaves next_offset anchored at the true minimum.
    assert_next_offset(&store, &k, Some(5)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_orphan_retry_count_only() -> color_eyre::Result<()> {
    // No clustering rows — only the static retry_count. This matches a
    // post-migration orphan (`set_retry_count` on a fresh key).
    let (store, k) = seeded_legacy(&[], Some(4)).await?;

    let got = store.get_next_deferred_message(&k).await?;
    assert_eq!(got, None);

    // No bogus repair UPDATE was issued; next_offset remains NULL.
    assert_next_offset(&store, &k, None).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_repair_idempotent() -> color_eyre::Result<()> {
    let (store, k) = seeded_legacy(&[11], Some(5)).await?;

    let first = store.get_next_deferred_message(&k).await?;
    assert_eq!(first, Some((Offset::from(11_i64), 5)));

    // Cache is populated after the first call; second call must not
    // re-probe or re-repair. We verify by asserting the cache now holds
    // the synthesized entry — a subsequent read served from cache.
    assert_eq!(
        store.cache.get(k.as_ref()),
        Some(Some((Offset::from(11_i64), 5)))
    );

    let second = store.get_next_deferred_message(&k).await?;
    assert_eq!(second, Some((Offset::from(11_i64), 5)));
    Ok(())
}

#[tokio::test]
async fn test_legacy_remove_at_min() -> color_eyre::Result<()> {
    let (store, k) = seeded_legacy(&[5, 10], Some(1)).await?;

    store
        .remove_deferred_message(&k, Offset::from(5_i64))
        .await?;

    assert_next_offset(&store, &k, Some(10)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_remove_above_min() -> color_eyre::Result<()> {
    let (store, k) = seeded_legacy(&[5, 10], Some(1)).await?;

    store
        .remove_deferred_message(&k, Offset::from(10_i64))
        .await?;

    // Repair anchored next_offset at the true minimum; removing a
    // non-min clustering row leaves that anchor unchanged.
    assert_next_offset(&store, &k, Some(5)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_delete_key_wipes_partition() -> color_eyre::Result<()> {
    let (store, k) = seeded_legacy(&[5, 10], Some(3)).await?;

    // delete_key bypasses resolve_cache_or_read, so repair never fires;
    // the partition must still be wiped cleanly.
    store.delete_key(&k).await?;

    assert_next_offset(&store, &k, None).await?;
    // A subsequent read must agree: no partition row, not an ambiguous
    // legacy state the next reader would probe.
    let got = store.get_next_deferred_message(&k).await?;
    assert_eq!(got, None);
    Ok(())
}

#[tokio::test]
async fn test_legacy_set_retry_count_triggers_repair() -> color_eyre::Result<()> {
    let (store, k) = seeded_legacy(&[7], Some(2)).await?;

    // set_retry_count now reads via resolve_cache_or_read so it can
    // no-op on a truly empty partition (no orphan creation). On a
    // legacy partition this triggers eager repair: next_offset is
    // populated and retry_count is updated.
    store.set_retry_count(&k, 9).await?;
    assert_next_offset(&store, &k, Some(7)).await?;

    // Subsequent read serves from cache with the repaired state.
    let got = store.get_next_deferred_message(&k).await?;
    assert_eq!(got, Some((Offset::from(7_i64), 9)));

    assert_next_offset(&store, &k, Some(7)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_none_none_returns_none_without_repair() -> color_eyre::Result<()> {
    // Seed clustering rows but no static retry_count — produces the
    // (next_offset=NULL, retry_count=NULL) state the plan deliberately
    // treats as "truly empty" to avoid probing every empty partition.
    let (store, k) = seeded_legacy(&[4], None).await?;

    let got = store.get_next_deferred_message(&k).await?;
    assert_eq!(got, None);

    // No repair UPDATE was issued; next_offset stays NULL. Cache is
    // populated with Some(None) so subsequent reads stay on the fast path.
    assert_next_offset(&store, &k, None).await?;
    assert_eq!(store.cache.get(k.as_ref()), Some(None));
    Ok(())
}
