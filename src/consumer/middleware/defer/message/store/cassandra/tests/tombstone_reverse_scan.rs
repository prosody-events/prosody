//! Regression: `get_next_deferred_message` must resolve the live tail of a
//! partition even when a dense band of low-offset tombstones sits below it.
//! A forward `LIMIT 1` scan walks the whole graveyard; the store uses a
//! reverse scan so it lands on the live row directly.

use super::*;

/// Density of the tombstone band — chosen to mimic the post-FIFO
/// graveyard observed in production (~5k cells per partition).
const TOMBSTONE_COUNT: i64 = 5_000;

#[tokio::test]
async fn test_get_next_skips_low_offset_tombstones() -> color_eyre::Result<()> {
    let store = build_store().await?;
    let k = key("tombstone-msg");
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
