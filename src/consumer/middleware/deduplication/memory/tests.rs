use super::*;

#[tokio::test]
async fn exists_returns_false_for_new_id() -> color_eyre::Result<()> {
    let store = MemoryDeduplicationStore::new();
    let id = Uuid::new_v4();
    assert!(!store.exists(id).await?);
    Ok(())
}

#[tokio::test]
async fn insert_then_exists_returns_true() -> color_eyre::Result<()> {
    let store = MemoryDeduplicationStore::new();
    let id = Uuid::new_v4();
    store.insert(id).await?;
    assert!(store.exists(id).await?);
    Ok(())
}

#[tokio::test]
async fn concurrent_access() -> color_eyre::Result<()> {
    let store = MemoryDeduplicationStore::new();
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    let s1 = store.clone();
    let h1 = tokio::spawn(async move { s1.insert(id1).await });

    let s2 = store.clone();
    let h2 = tokio::spawn(async move { s2.insert(id2).await });

    h1.await??;
    h2.await??;

    assert!(store.exists(id1).await?);
    assert!(store.exists(id2).await?);
    Ok(())
}
