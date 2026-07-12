mod invariant;
mod legacy_repair;
mod tombstone_reverse_scan;
mod ttl_drift;

use super::*;
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::test_util::TEST_KEYSPACE;
use crate::{ConsumerGroup, Partition, Topic};
use chrono::Utc;

/// A unique, prefix-tagged key. The prefix tags which test seeded a row in
/// the shared `prosody_test` keyspace; the uuid suffix keeps runs isolated.
fn key(prefix: &str) -> Key {
    Arc::from(format!("{prefix}-{}", uuid::Uuid::new_v4()))
}

/// A `CompactDateTime` `offset_secs` in the future, saturating on the
/// `i64`→`u32` epoch conversion rather than wrapping.
fn future_time(offset_secs: u32) -> CompactDateTime {
    let now = u32::try_from(Utc::now().timestamp()).unwrap_or(u32::MAX);
    CompactDateTime::from(now.saturating_add(offset_secs))
}

pub(super) async fn build_test_store() -> color_eyre::Result<CassandraTimerDeferStore> {
    let config = CassandraConfiguration::builder()
        .nodes(vec!["localhost:9042".to_owned()])
        .keyspace(TEST_KEYSPACE.to_owned())
        .build()
        .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;
    let cassandra_store = CassandraStore::new(&config).await?;
    let segment_store = CassandraSegmentStore::new(cassandra_store.clone(), TEST_KEYSPACE).await?;
    let queries = Arc::new(Queries::new(cassandra_store.session(), TEST_KEYSPACE).await?);
    let segment = LazySegment::new(
        segment_store,
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from(format!("test-consumer-group-{}", uuid::Uuid::new_v4())) as ConsumerGroup,
    );
    Ok(CassandraTimerDeferStore::new(
        cassandra_store,
        queries,
        segment,
        SpanRelation::default(),
        1_024,
    ))
}

crate::timer_defer_store_tests!(async { build_test_store().await });

/// End-to-end smoke test of the single-key lifecycle against a live cluster:
/// absent → `defer_first_timer` → `get_next_deferred_timer` → `delete_key` →
/// absent. The shared `timer_defer_store_tests!` suite proves the invariants;
/// this guards the happy path with a readable, concrete assertion.
#[tokio::test]
async fn test_cassandra_timer_defer_store() -> color_eyre::Result<()> {
    let defer_store = build_test_store().await?;

    let key = key("timer-defer-smoke");
    let time = CompactDateTime::from(1000_u32);
    let trigger = Trigger::new(
        key.clone(),
        time,
        TimerType::Application,
        tracing::Span::current(),
    );

    assert!(defer_store.is_deferred(&key).await?.is_none());

    defer_store.defer_first_timer(&trigger).await?;
    assert_eq!(defer_store.is_deferred(&key).await?, Some(0));

    let (retrieved, retry_count) = defer_store
        .get_next_deferred_timer(&key)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
    assert_eq!(retrieved.time, time);
    assert_eq!(retry_count, 0);

    defer_store.delete_key(&key).await?;
    assert!(defer_store.is_deferred(&key).await?.is_none());

    Ok(())
}
