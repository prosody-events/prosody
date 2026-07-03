use super::*;
use crate::Key;

fn create_test_store() -> MemoryMessageDeferStore {
    MemoryMessageDeferStore::new()
}

#[tokio::test]
async fn test_get_nonexistent_key() -> color_eyre::Result<()> {
    let store = create_test_store();
    let key: Key = Arc::from("test-key-1");

    let result = store.get_next_deferred_message(&key).await?;
    assert!(result.is_none());
    Ok(())
}

#[tokio::test]
async fn test_append_and_get() -> color_eyre::Result<()> {
    let store = create_test_store();
    let key: Key = Arc::from("test-key-1");
    let offset = Offset::from(42_i64);

    // Use defer_first_message for first failure
    store.defer_first_message(&key, offset).await?;

    let result = store.get_next_deferred_message(&key).await?;
    assert_eq!(result, Some((offset, 0)));
    Ok(())
}

#[tokio::test]
async fn test_multiple_offsets_returns_oldest() -> color_eyre::Result<()> {
    let store = create_test_store();
    let key: Key = Arc::from("test-key-1");

    // Defer first message
    store
        .defer_first_message(&key, Offset::from(100_i64))
        .await?;
    // Append additional messages
    store
        .append_deferred_message(&key, Offset::from(50_i64))
        .await?;
    store
        .append_deferred_message(&key, Offset::from(150_i64))
        .await?;

    // Should return the oldest (smallest) offset
    let result = store.get_next_deferred_message(&key).await?;
    assert_eq!(result, Some((Offset::from(50_i64), 0)));
    Ok(())
}

#[tokio::test]
async fn test_remove_offset() -> color_eyre::Result<()> {
    let store = create_test_store();
    let key: Key = Arc::from("test-key-1");
    let offset = Offset::from(42_i64);

    store.defer_first_message(&key, offset).await?;
    store.remove_deferred_message(&key, offset).await?;

    let result = store.get_next_deferred_message(&key).await?;
    assert!(result.is_none());
    Ok(())
}

#[tokio::test]
async fn test_remove_nonexistent() -> color_eyre::Result<()> {
    let store = create_test_store();
    let key: Key = Arc::from("test-key-1");

    // Should not error
    store
        .remove_deferred_message(&key, Offset::from(42_i64))
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_set_retry_count() -> color_eyre::Result<()> {
    let store = create_test_store();
    let key: Key = Arc::from("test-key-1");
    let offset = Offset::from(42_i64);

    // Defer first message
    store.defer_first_message(&key, offset).await?;

    // Update retry_count to 5
    store.set_retry_count(&key, 5).await?;

    let result = store.get_next_deferred_message(&key).await?;
    assert_eq!(result, Some((offset, 5)));
    Ok(())
}

#[tokio::test]
async fn test_concurrent_access() -> color_eyre::Result<()> {
    let store = create_test_store();

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
