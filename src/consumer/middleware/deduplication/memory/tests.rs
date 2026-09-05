use super::*;

#[tokio::test]
async fn lookup_returns_absent_for_new_id() -> color_eyre::Result<()> {
    let store = MemoryDeduplicationStore::new();
    let id = Uuid::new_v4();
    assert_eq!(store.lookup(id).await?, Presence::Absent);
    Ok(())
}

#[tokio::test]
async fn insert_then_lookup_returns_settled() -> color_eyre::Result<()> {
    let store = MemoryDeduplicationStore::new();
    let id = Uuid::new_v4();
    store.insert(id).await?;
    assert_eq!(store.lookup(id).await?, Presence::Settled);
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

    assert_eq!(store.lookup(id1).await?, Presence::Settled);
    assert_eq!(store.lookup(id2).await?, Presence::Settled);
    Ok(())
}
