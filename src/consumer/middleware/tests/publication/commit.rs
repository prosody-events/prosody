//! The mid-handler `commit()` path: publication precedes its direct durable
//! write, exactly as it does at the settle boundary.

use super::*;
/// The mid-handler `commit()` path publishes before its direct durable
/// write. A successful publication lets `commit()` write the cell. A
/// failing publication store makes `commit()` return `Err` and leaves no
/// durable cell: the routing row gates `write_resolved`.
#[tokio::test]
async fn commit_path_publishes_before_write_resolved() -> Result<()> {
    // Success: commit publishes then writes.
    let store = ScriptedPublicationStore::new();
    let observer = observing(GROUP, &[(TOPIC, 3_i32)]);
    let (context, cell_store, cart_id) =
        published_context(store.clone(), &observer, &["cart"], 0x9)?;
    let handle = context
        .state(Registered::new(cart()))
        .map_err(|e| eyre!("bind cart: {e}"))?;
    handle.set(json!({ "x": 1_i32 })).await?;
    assert_eq!(
        handle.commit().await.map_err(|e| eyre!("commit: {e}"))?,
        StoreOutcome::Applied,
        "commit() durably applied the cell",
    );

    let rows = publication_rows(&store, "cart").await?;
    assert_eq!(rows.len(), 1, "commit() published a routing row");
    assert_eq!(i32::from(rows[0].partition_count), 3_i32);
    assert!(
        committed_value(&cell_store, &cart_id).await?.is_some(),
        "commit() wrote the cell durably",
    );

    // Failure: a failing store makes commit() error and write nothing.
    let store = ScriptedPublicationStore::failing();
    let (context, cell_store, cart_id) =
        published_context(store.clone(), &observer, &["cart"], 0xA)?;
    let handle = context
        .state(Registered::new(cart()))
        .map_err(|e| eyre!("bind cart: {e}"))?;
    handle.set(json!({ "x": 1_i32 })).await?;
    assert!(
        handle.commit().await.is_err(),
        "commit() fails when publication fails",
    );
    assert_eq!(
        committed_value(&cell_store, &cart_id).await?,
        None,
        "no durable cell when publication gates the write",
    );
    assert!(
        publication_rows(&store, "cart").await?.is_empty(),
        "no routing row when the failing store rejects the upsert",
    );
    Ok(())
}
