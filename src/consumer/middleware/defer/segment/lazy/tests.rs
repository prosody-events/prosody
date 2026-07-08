use super::*;
use crate::consumer::middleware::defer::segment::MemorySegmentStore;
use color_eyre::eyre::bail;

#[tokio::test]
async fn test_lazy_segment_persists_on_first_access() -> color_eyre::Result<()> {
    let store = MemorySegmentStore::new();
    let segment = LazySegment::new(
        store.clone(),
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from("test-group") as ConsumerGroup,
    );

    // Not initialized yet
    assert!(!segment.is_initialized());

    // First access creates and persists
    let seg = segment.get().await?;
    assert!(segment.is_initialized());

    // Verify it was persisted
    let Some(stored) = store.get_segment(&seg.id()).await? else {
        bail!("segment should exist");
    };
    assert_eq!(stored.id(), seg.id());
    Ok(())
}

#[tokio::test]
async fn test_lazy_segment_clone_shares_state() -> color_eyre::Result<()> {
    let store = MemorySegmentStore::new();
    let segment1 = LazySegment::new(
        store,
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from("test-group") as ConsumerGroup,
    );
    let segment2 = segment1.clone();

    // Initialize via first clone
    let seg1 = segment1.get().await?;

    // Second clone sees initialized state
    assert!(segment2.is_initialized());
    let seg2 = segment2.get().await?;

    // Same segment
    assert_eq!(seg1.id(), seg2.id());
    Ok(())
}
