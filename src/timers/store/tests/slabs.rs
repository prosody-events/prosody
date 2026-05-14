use crate::timers::slab::Slab;
use crate::timers::store::tests::TestStoreResult;
use crate::timers::store::tests::common::insert_segment;
use crate::timers::store::{Segment, TriggerStore};
use ahash::HashSet;
use std::fmt::Debug;

/// Tests the `get_slab_range` operation of a `TriggerStore` implementation.
///
/// Slab metadata is owned by the scheduler actor, not implicitly written
/// by `add_trigger`. This test seeds slabs via `insert_slab` directly and
/// verifies range scans against them.
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

    insert_segment(store, segment).await?;

    let all_slab_ids: Vec<u32> = vec![0, 5, 10, 15, 20];
    for &slab_id in &all_slab_ids {
        store
            .insert_slab(Slab::new(slab_id, segment.slab_size))
            .await
            .map_err(|e| format!("Failed to insert slab {slab_id}: {e:?}"))?;
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
