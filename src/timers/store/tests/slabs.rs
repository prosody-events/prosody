use crate::Key;
use crate::timers::store::tests::TestStoreResult;
use crate::timers::store::tests::common::{add_trigger, insert_segment};
use crate::timers::store::{Segment, TriggerStore};
use crate::timers::{TimerType, Trigger, datetime::CompactDateTime};
use ahash::HashSet;
use std::fmt::Debug;

// Removed test_slab_operations - uses non-public API (insert_slab, get_slabs)
// This functionality is covered by property-based tests in
// prop_slab_metadata.rs

/// Tests the `get_slab_range` operation of a `TriggerStore` implementation.
///
/// This test verifies that the store can correctly:
/// - Query slabs within a specific range
/// - Return only slabs that fall within the specified inclusive range
/// - Handle empty ranges correctly
///
/// Uses `add_trigger` to implicitly create slabs (public API).
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn test_get_slab_range<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    use futures::TryStreamExt;

    // Insert the segment first
    insert_segment(store, segment).await?;

    // Create slabs with IDs 0, 5, 10, 15, 20 by adding triggers at appropriate
    // times Slabs are created implicitly when triggers are added
    let all_slab_ids: Vec<u32> = vec![0, 5, 10, 15, 20];
    for &slab_id in &all_slab_ids {
        // Calculate a time that falls within this slab
        let time_secs = slab_id * segment.slab_size.seconds();
        let time = CompactDateTime::from(time_secs);
        let trigger = Trigger::new(
            Key::from(format!("test-key-{slab_id}")),
            time,
            TimerType::Application,
            tracing::Span::current(),
        );
        add_trigger(store, &trigger).await?;
    }

    // Test range 5..=15 should return [5, 10, 15]
    let range_slabs: Vec<u32> = store
        .get_slab_range(5..=15)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get slab range: {e:?}"))?;

    let expected: HashSet<u32> = [5, 10, 15].into_iter().collect();
    let actual: HashSet<u32> = range_slabs.into_iter().collect();

    if expected != actual {
        return Err(format!(
            "Slab range query returned incorrect results. Expected: {expected:?}, Got: {actual:?}"
        ));
    }

    // Test range 0..=0 should return [0]
    let range_slabs: Vec<u32> = store
        .get_slab_range(0..=0)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get slab range: {e:?}"))?;

    if range_slabs != vec![0] {
        return Err(format!(
            "Single slab range query failed. Expected: [0], Got: {range_slabs:?}"
        ));
    }

    // Test range 25..=30 should return [] (no slabs in range)
    let range_slabs: Vec<u32> = store
        .get_slab_range(25..=30)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get slab range: {e:?}"))?;

    if !range_slabs.is_empty() {
        return Err(format!(
            "Empty range query should return no slabs. Got: {range_slabs:?}"
        ));
    }

    Ok(())
}
