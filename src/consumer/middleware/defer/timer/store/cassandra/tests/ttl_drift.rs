use super::*;
use crate::cassandra::TABLE_DEFERRED_TIMERS;
use chrono::Utc;

/// Tolerance for the gap between our local clock reading of `target` and
/// the wall-clock the cluster used when it bound the TTL: request latency
/// plus any skew between the test box and the Cassandra node. Generous on
/// purpose — every test below uses a multi-hour lead, so the assertion
/// still rejects a `base_ttl()` regression with room to spare.
const CLOCK_SKEW_SECS: i32 = 60;

fn key(prefix: &str) -> Key {
    Arc::from(format!("{prefix}-{}", uuid::Uuid::new_v4()))
}

fn future_time(offset_secs: u32) -> CompactDateTime {
    let now = u32::try_from(Utc::now().timestamp()).unwrap_or(u32::MAX);
    CompactDateTime::from(now.saturating_add(offset_secs))
}

fn trigger(key: &Key, time: CompactDateTime) -> Trigger {
    Trigger::new(
        key.clone(),
        time,
        TimerType::Application,
        tracing::Span::current(),
    )
}

/// Seeds a two-row FIFO partition: a `low` timer at the head followed by a
/// `high` successor. Every drift case starts here, then removes the head
/// and asserts the surviving `next_timer` re-anchors on `high`.
async fn seed_low_then_high(
    store: &CassandraTimerDeferStore,
    k: &Key,
    low: CompactDateTime,
    high: CompactDateTime,
) -> color_eyre::Result<()> {
    store.defer_first_timer(&trigger(k, low)).await?;
    store.append_deferred_timer(&trigger(k, high)).await?;
    Ok(())
}

/// Reads `TTL(next_timer)` directly from Cassandra. `Ok(None)` means the
/// column is unset (NULL); a present TTL is returned in seconds.
async fn read_next_timer_ttl(
    store: &CassandraTimerDeferStore,
    key: &Key,
) -> color_eyre::Result<Option<i32>> {
    let segment_id = store
        .segment_id()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
    let cql = format!(
        "SELECT TTL(next_timer) FROM prosody_test.{TABLE_DEFERRED_TIMERS} WHERE segment_id = ? \
         AND key = ?"
    );
    let row = store
        .session()
        .query_unpaged(cql, (segment_id, key.as_ref()))
        .await?
        .into_rows_result()?
        .maybe_first_row::<(Option<i32>,)>()?;
    Ok(row.and_then(|(ttl_opt,)| ttl_opt))
}

/// Asserts `next_timer`'s TTL is anchored on the natural end time of the
/// row it now points at — `calculate_ttl(target)`, which adds `base_ttl()`
/// (the grace period) to the lead time of `target` — rather than on
/// `base_ttl()` alone. The drift bug surfaces as `ttl == base_ttl`; the fix
/// lifts the TTL by the referenced row's lead time.
async fn assert_next_timer_ttl_anchored(
    store: &CassandraTimerDeferStore,
    k: &Key,
    target: CompactDateTime,
    site: &str,
) -> color_eyre::Result<()> {
    let ttl = read_next_timer_ttl(store, k)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("{site}: expected TTL on next_timer"))?;
    let base_ttl_secs: i32 = store.store.base_ttl().seconds().try_into()?;
    let expected = store
        .store
        .calculate_ttl(target)
        .ok_or_else(|| color_eyre::eyre::eyre!("{site}: calculate_ttl returned None"))?;

    assert!(
        ttl >= expected - CLOCK_SKEW_SECS,
        "{site}: TTL drift — expected ≈ {expected} (calculate_ttl), got {ttl} (base_ttl = \
         {base_ttl_secs})"
    );
    assert!(
        ttl > base_ttl_secs,
        "{site}: TTL still equals base_ttl ({base_ttl_secs}); fix did not take effect: ttl = {ttl}"
    );
    Ok(())
}

#[tokio::test]
async fn test_complete_retry_success_fifo_ttl_matches_successor() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("ttl-drift-complete");
    let low = future_time(30);
    let high = future_time(7200);
    seed_low_then_high(&store, &k, low, high).await?;

    store.complete_retry_success(&k, low).await?;

    assert_next_timer_ttl_anchored(&store, &k, high, "complete_retry_success").await
}

#[tokio::test]
async fn test_remove_deferred_timer_min_ttl_matches_successor() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("ttl-drift-remove");
    let low = future_time(30);
    let high = future_time(7200);
    seed_low_then_high(&store, &k, low, high).await?;

    store.remove_deferred_timer(&k, low).await?;

    assert_next_timer_ttl_anchored(&store, &k, high, "remove_deferred_timer").await
}

#[tokio::test]
async fn test_legacy_repair_ttl_matches_min_clustering_row() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("ttl-drift-legacy");
    let t = future_time(7200);
    store.seed_legacy_for_test(&k, &[t], Some(0)).await?;

    // First read fires the lazy on-read repair UPDATE on next_timer.
    let got = store.get_next_deferred_timer(&k).await?;
    assert!(got.is_some());

    assert_next_timer_ttl_anchored(&store, &k, t, "repair_legacy_partition").await
}
