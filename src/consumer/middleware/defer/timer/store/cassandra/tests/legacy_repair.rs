use super::*;

/// Asserts the persisted static `next_timer` anchor for `k` has the
/// expected time (or is absent). `None` handles the unrepaired/empty case.
async fn assert_next_timer(
    store: &CassandraTimerDeferStore,
    k: &Key,
    expected: Option<CompactDateTime>,
) -> color_eyre::Result<()> {
    let db_next = store.read_next_timer_for_invariant_check(k).await?;
    assert_eq!(db_next.map(|(time, _)| time), expected);
    Ok(())
}

#[tokio::test]
async fn test_legacy_get_next_repairs() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let t = future_time(60);
    store.seed_legacy_for_test(&k, &[t], Some(2)).await?;

    let got = store.get_next_deferred_timer(&k).await?;
    let (trigger, rc) = got.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
    assert_eq!(trigger.time, t);
    assert_eq!(rc, 2);

    assert_next_timer(&store, &k, Some(t)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_is_deferred_repairs() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let t = future_time(90);
    store.seed_legacy_for_test(&k, &[t], Some(1)).await?;

    let rc = store.is_deferred(&k).await?;
    assert_eq!(rc, Some(1));

    assert_next_timer(&store, &k, Some(t)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_defer_first_on_legacy_partition() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let low = future_time(30);
    let high = future_time(120);
    store.seed_legacy_for_test(&k, &[low], Some(3)).await?;

    let trigger = Trigger::new(
        k.clone(),
        high,
        TimerType::Application,
        tracing::Span::current(),
    );
    store.defer_first_timer(&trigger).await?;

    assert_next_timer(&store, &k, Some(low)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_append_on_legacy_partition() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let seeded = future_time(120);
    let earlier = future_time(10);
    store.seed_legacy_for_test(&k, &[seeded], Some(0)).await?;

    let trigger = Trigger::new(
        k.clone(),
        earlier,
        TimerType::Application,
        tracing::Span::current(),
    );
    store.append_deferred_timer(&trigger).await?;

    assert_next_timer(&store, &k, Some(earlier)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_complete_retry_at_min() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let low = future_time(30);
    let high = future_time(90);
    store
        .seed_legacy_for_test(&k, &[low, high], Some(2))
        .await?;

    let result = store.complete_retry_success(&k, low).await?;
    let advanced = matches!(
        result,
        TimerRetryCompletionResult::MoreTimers { next_time, .. } if next_time == high
    );
    assert!(advanced);

    assert_next_timer(&store, &k, Some(high)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_complete_retry_above_min() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let low = future_time(30);
    let high = future_time(90);
    store
        .seed_legacy_for_test(&k, &[low, high], Some(2))
        .await?;

    let result = store.complete_retry_success(&k, high).await?;
    let anchored = matches!(
        result,
        TimerRetryCompletionResult::MoreTimers { next_time, .. } if next_time == low
    );
    assert!(anchored);

    assert_next_timer(&store, &k, Some(low)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_orphan_retry_count_only() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    store.seed_legacy_for_test(&k, &[], Some(4)).await?;

    let got = store.get_next_deferred_timer(&k).await?;
    assert!(got.is_none());

    assert_next_timer(&store, &k, None).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_timer_span_preserved_on_repair() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let t = future_time(45);

    // Seed a clustering row with a distinct span map via the normal
    // timer defer API (which writes both the span and a next_timer).
    // Then strip `next_timer` back to NULL to simulate a legacy
    // partition while preserving the clustering row's span.
    let span_map = {
        let mut m: HashMap<String, String> = HashMap::new();
        m.insert(
            "traceparent".to_owned(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_owned(),
        );
        m
    };
    let segment_id = store
        .segment_id()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
    let ttl = store.store.calculate_ttl(t);
    // Seed clustering row with the distinct span.
    store
        .session()
        .execute_unpaged(
            &store.queries.insert_deferred_timer_without_retry_count,
            (&segment_id, k.as_ref(), t, &span_map, ttl),
        )
        .await?;
    // Orphan retry_count to produce the ambiguous state.
    store
        .session()
        .execute_unpaged(
            &store.queries.update_retry_count,
            (store.store.base_ttl(), 0_i32, &segment_id, k.as_ref()),
        )
        .await?;
    let _ = store.cache.remove(k.as_ref());

    let got = store.get_next_deferred_timer(&k).await?;
    let (trigger, _) = got.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
    assert_eq!(trigger.time, t);

    // The repaired next_timer UDT carries the seeded span map verbatim.
    let db_next = store.read_next_timer_for_invariant_check(&k).await?;
    let (db_time, db_span) = db_next.ok_or_else(|| color_eyre::eyre::eyre!("no next_timer"))?;
    assert_eq!(db_time, t);
    assert_eq!(db_span, span_map);
    Ok(())
}

#[tokio::test]
async fn test_legacy_repair_idempotent() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let t = future_time(60);
    store.seed_legacy_for_test(&k, &[t], Some(5)).await?;

    let first = store.get_next_deferred_timer(&k).await?;
    assert!(first.is_some());

    // Cache populated; second call served from cache, no re-probe.
    let cached = store.cache.get(k.as_ref());
    assert!(matches!(cached, Some(Some(entry)) if entry.time == t && entry.retry_count == 5));

    let second = store.get_next_deferred_timer(&k).await?;
    let (trigger, rc) = second.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
    assert_eq!(trigger.time, t);
    assert_eq!(rc, 5);
    Ok(())
}

#[tokio::test]
async fn test_legacy_remove_at_min() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let low = future_time(30);
    let high = future_time(90);
    store
        .seed_legacy_for_test(&k, &[low, high], Some(1))
        .await?;

    store.remove_deferred_timer(&k, low).await?;

    assert_next_timer(&store, &k, Some(high)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_remove_above_min() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let low = future_time(30);
    let high = future_time(90);
    store
        .seed_legacy_for_test(&k, &[low, high], Some(1))
        .await?;

    store.remove_deferred_timer(&k, high).await?;

    // Repair anchored next_timer at the true minimum; removing a
    // non-min clustering row leaves that anchor unchanged.
    assert_next_timer(&store, &k, Some(low)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_delete_key_wipes_partition() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let t = future_time(45);
    store.seed_legacy_for_test(&k, &[t], Some(3)).await?;

    // delete_key bypasses resolve_cache_or_read; repair never fires,
    // but the partition must still be wiped cleanly.
    store.delete_key(&k).await?;

    assert_next_timer(&store, &k, None).await?;
    let got = store.get_next_deferred_timer(&k).await?;
    assert!(got.is_none());
    Ok(())
}

#[tokio::test]
async fn test_legacy_set_retry_count_triggers_repair() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let t = future_time(60);
    store.seed_legacy_for_test(&k, &[t], Some(2)).await?;

    // set_retry_count now reads via resolve_cache_or_read so it can
    // no-op on a truly empty partition (no orphan creation). On a
    // legacy partition this triggers eager repair: next_timer is
    // populated and retry_count is updated.
    store.set_retry_count(&k, 9).await?;
    assert_next_timer(&store, &k, Some(t)).await?;

    // Subsequent read serves from cache with the repaired state.
    let got = store.get_next_deferred_timer(&k).await?;
    let (trigger, rc) = got.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
    assert_eq!(trigger.time, t);
    assert_eq!(rc, 9);

    assert_next_timer(&store, &k, Some(t)).await?;
    Ok(())
}

#[tokio::test]
async fn test_legacy_none_none_returns_none_without_repair() -> color_eyre::Result<()> {
    let store = build_test_store().await?;
    let k = key("legacy-timer");
    let t = future_time(60);
    // Seed clustering row but no static retry_count — produces the
    // (next_timer=NULL, retry_count=NULL) state the plan deliberately
    // treats as "truly empty" to avoid probing every empty partition.
    store.seed_legacy_for_test(&k, &[t], None).await?;

    let got = store.get_next_deferred_timer(&k).await?;
    assert!(got.is_none());

    // No repair UPDATE was issued; next_timer stays NULL. Cache is
    // populated with Some(None) so subsequent reads stay on the fast path.
    assert_next_timer(&store, &k, None).await?;
    let cached = store.cache.get(k.as_ref());
    assert!(matches!(cached, Some(None)));
    Ok(())
}
