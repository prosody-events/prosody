use super::*;
use crate::Key;

#[tokio::test]
async fn test_concurrent_access() -> color_eyre::Result<()> {
    let store = MemoryMessageDeferStore::new();

    let key1: Key = Arc::from("test-key-1");
    let key2: Key = Arc::from("test-key-2");

    let store_clone = store.clone();
    let k1 = key1.clone();
    let handle1 = tokio::spawn(async move {
        store_clone
            .defer_first_message(&k1, Offset::from(1_i64))
            .await
    });

    let store_clone = store.clone();
    let k2 = key2.clone();
    let handle2 = tokio::spawn(async move {
        store_clone
            .defer_first_message(&k2, Offset::from(2_i64))
            .await
    });

    assert!(handle1.await.is_ok());
    assert!(handle2.await.is_ok());

    let result1 = store.get_next_deferred_message(&key1).await?;
    assert_eq!(result1, Some((Offset::from(1_i64), 0)));

    let result2 = store.get_next_deferred_message(&key2).await?;
    assert_eq!(result2, Some((Offset::from(2_i64), 0)));

    Ok(())
}

// Property-based tests using model equivalence
defer_store_tests!(async { Ok::<_, color_eyre::Report>(MemoryMessageDeferStore::new()) });
