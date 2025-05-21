use crate::timers::store::TriggerStore;
use crate::timers::store::tests::common::{delete_segment, insert_segment, verify_segment};
use crate::timers::store::tests::{SegmentTestInput, TestStoreResult};
use std::fmt::Debug;

/// Tests the segment operations of a `TriggerStore` implementation.
///
/// This test verifies that the store can correctly:
/// - Insert segments
/// - Retrieve segments with correct properties
/// - Delete segments
pub async fn test_segment_operations<S>(store: &S, input: &SegmentTestInput) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Insert all segments
    for segment in &input.segments {
        insert_segment(store, segment).await?;
    }

    // Verify all segments can be retrieved with correct values
    for segment in &input.segments {
        verify_segment(store, segment).await?;
    }

    // Delete all segments and verify they're gone
    for segment in &input.segments {
        delete_segment(store, &segment.id).await?;
    }

    Ok(())
}
