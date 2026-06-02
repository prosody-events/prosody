use super::*;
use chrono::Utc;

/// Density of the tombstone band — chosen to mimic the post-FIFO
/// graveyard observed in production (~5k cells per partition).
const TOMBSTONE_COUNT: u32 = 5_000;

fn key() -> Key {
    Arc::from(format!("tombstone-timer-{}", uuid::Uuid::new_v4()))
}

fn future_time(offset_secs: u32) -> CompactDateTime {
    let now = u32::try_from(Utc::now().timestamp()).unwrap_or(u32::MAX);
    CompactDateTime::from(now.saturating_add(offset_secs))
}

#[tokio::test]
async fn test_get_next_skips_low_time_tombstones() -> color_eyre::Result<()> {
    let store = tests::build_test_store().await?;
    let k = key();
    let segment_id = store
        .segment_id()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
    let empty_span: HashMap<String, String> = HashMap::new();

    // Anchor the graveyard high enough in the future that each
    // clustering row's TTL is large; the live tail sits above the
    // band so its `original_time` strictly orders after the
    // tombstones.
    let base = future_time(3_600);

    for i in 0..TOMBSTONE_COUNT {
        let t = CompactDateTime::from(base.epoch_seconds().saturating_add(i));
        let ttl = store.store.calculate_ttl(t);
        store
            .session()
            .execute_unpaged(
                &store.queries.insert_deferred_timer_without_retry_count,
                (&segment_id, k.as_ref(), t, &empty_span, ttl),
            )
            .await?;
        store
            .session()
            .execute_unpaged(
                &store.queries.remove_deferred_timer,
                (&segment_id, k.as_ref(), t),
            )
            .await?;
    }

    // Live row + `next_timer` above the tombstone band.
    let live_time = CompactDateTime::from(base.epoch_seconds().saturating_add(TOMBSTONE_COUNT));
    let live_ttl = store.store.calculate_ttl(live_time);
    let next_udt = DeferredNextTimer {
        time: live_time,
        span: empty_span.clone(),
    };
    store
        .session()
        .execute_unpaged(
            &store.queries.insert_deferred_timer_with_retry_count,
            (
                &segment_id,
                k.as_ref(),
                live_time,
                &empty_span,
                0_i32,
                &next_udt,
                live_ttl,
            ),
        )
        .await?;
    let _ = store.cache.remove(k.as_ref());

    // Forward `LIMIT 1` walked the graveyard; reverse `LIMIT 1`
    // resolves on the live tail.
    let got = store.get_next_deferred_timer(&k).await?;
    let (trigger, rc) = got.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
    assert_eq!(trigger.time, live_time);
    assert_eq!(rc, 0);

    store.delete_key(&k).await?;
    Ok(())
}
