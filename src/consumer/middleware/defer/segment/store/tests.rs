use super::*;
use crate::{ConsumerGroup, Partition, Topic};
use std::sync::Arc;

fn test_segment() -> Segment {
    Segment::new(
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from("test-group") as ConsumerGroup,
    )
}

#[tokio::test]
async fn test_memory_get_or_create_segment_is_idempotent() -> color_eyre::Result<()> {
    let store = MemorySegmentStore::new();
    let segment = test_segment();

    let first = store.get_or_create_segment(segment.clone()).await?;
    assert_eq!(first, segment);

    let second = store.get_or_create_segment(segment.clone()).await?;
    assert_eq!(second, segment);
    Ok(())
}

#[tokio::test]
async fn test_memory_get_segment_tracks_creation() -> color_eyre::Result<()> {
    let store = MemorySegmentStore::new();
    let segment = test_segment();
    let segment_id = segment.id();

    assert_eq!(store.get_segment(&segment_id).await?, None);

    store.get_or_create_segment(segment.clone()).await?;

    assert_eq!(store.get_segment(&segment_id).await?, Some(segment));
    Ok(())
}
