use super::*;
use crate::{ConsumerGroup, Partition, Topic};
use std::sync::Arc;

#[tokio::test]
async fn test_memory_get_or_create_new_segment() -> color_eyre::Result<()> {
    let store = MemorySegmentStore::new();
    let segment = Segment::new(
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from("test-group") as ConsumerGroup,
    );
    let segment_id = segment.id();

    let returned = store.get_or_create_segment(segment.clone()).await?;

    assert_eq!(returned.id(), segment_id);
    assert_eq!(returned, segment);
    Ok(())
}

#[tokio::test]
async fn test_memory_get_or_create_existing_segment() -> color_eyre::Result<()> {
    let store = MemorySegmentStore::new();
    let segment = Segment::new(
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from("test-group") as ConsumerGroup,
    );

    // Create first time
    store.get_or_create_segment(segment.clone()).await?;

    // Create again - should be idempotent
    let returned = store.get_or_create_segment(segment.clone()).await?;

    assert_eq!(returned, segment);
    Ok(())
}

#[tokio::test]
async fn test_memory_get_segment_existing() -> color_eyre::Result<()> {
    let store = MemorySegmentStore::new();
    let segment = Segment::new(
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from("test-group") as ConsumerGroup,
    );
    let segment_id = segment.id();

    // Create segment
    store.get_or_create_segment(segment.clone()).await?;

    // Get by ID
    let result = store.get_segment(&segment_id).await?;

    assert_eq!(result, Some(segment));
    Ok(())
}

#[tokio::test]
async fn test_memory_get_segment_nonexistent() -> color_eyre::Result<()> {
    let store = MemorySegmentStore::new();
    let segment_id = uuid::Uuid::new_v4();

    let result = store.get_segment(&segment_id).await?;

    assert_eq!(result, None);
    Ok(())
}
