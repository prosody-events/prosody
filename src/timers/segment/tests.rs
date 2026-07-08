use super::*;
use crate::timers::store::memory::memory_store;
use crate::timers::test_support::test_segment;
use color_eyre::eyre::{self as eyre, Result};
use futures::future;

#[tokio::test]
async fn test_get_or_create_segment_new() -> Result<()> {
    let test_segment = test_segment("test-segment", 60_u32);
    let store = memory_store(test_segment.clone());

    let segment = get_or_create_segment(&store).await?;
    assert_eq!(segment.id, test_segment.id);
    assert_eq!(segment.name, test_segment.name);
    assert_eq!(segment.slab_size, test_segment.slab_size);
    assert_eq!(segment.version, test_segment.version);

    let retrieved = store.get_segment().await?;
    let retrieved = retrieved.ok_or_else(|| eyre::eyre!("Expected segment to be stored"))?;
    assert_eq!(retrieved.id, test_segment.id);
    assert_eq!(retrieved.name, test_segment.name);
    assert_eq!(retrieved.version, test_segment.version);
    Ok(())
}

#[tokio::test]
async fn test_get_or_create_segment_existing() -> Result<()> {
    let test_segment = test_segment("test-segment", 60_u32);
    let store = memory_store(test_segment.clone());

    let segment1 = get_or_create_segment(&store).await?;
    let segment2 = get_or_create_segment(&store).await?;

    // Second call returns the existing segment unchanged.
    assert_eq!(segment1.id, segment2.id);
    assert_eq!(segment1.name, segment2.name);
    assert_eq!(segment1.version, segment2.version);
    Ok(())
}

#[tokio::test]
async fn test_get_or_create_segment_concurrent() -> Result<()> {
    let test_segment = test_segment("test-segment", 60_u32);
    let store = memory_store(test_segment.clone());

    let futures: Vec<_> = (0_i32..10_i32)
        .map(|_| {
            let store_clone = store.clone();
            async move { get_or_create_segment(&store_clone).await }
        })
        .collect();

    let results: Vec<_> = future::join_all(futures).await;
    for result in results {
        let segment = result?;
        assert_eq!(segment.id, test_segment.id);
        assert_eq!(segment.name, test_segment.name);
        assert_eq!(segment.slab_size, test_segment.slab_size);
    }

    let stored_segment = store.get_segment().await?;
    let stored_segment =
        stored_segment.ok_or_else(|| eyre::eyre!("Expected segment to be stored"))?;
    assert_eq!(stored_segment.id, test_segment.id);
    Ok(())
}
