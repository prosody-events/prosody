use crate::timers::slab::Slab;
use crate::timers::store::tests::TestStoreResult;
use crate::timers::store::tests::common::{delete_slab, get_slabs, insert_segment, insert_slab};
use crate::timers::store::{Segment, TriggerStore};
use ahash::HashSet;
use std::fmt::Debug;

/// Tests the slab operations of a `TriggerStore` implementation.
///
/// This test verifies that the store can correctly:
/// - Insert slab IDs
/// - Retrieve slab IDs
/// - Delete slab IDs
/// - Handle slab ID registrations independently from segment data
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn test_slab_operations<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Insert the segment first
    insert_segment(store, segment).await?;

    // Generate some slab IDs to test with
    let slabs: Vec<u32> = (0..5).collect();
    let mut inserted_slabs = HashSet::default();

    // Insert slabs
    for &slab_id in &slabs {
        let slab = Slab::new(segment.id, slab_id, segment.slab_size);
        insert_slab(store, &segment.id, slab).await?;
        inserted_slabs.insert(slab_id);
    }

    // Retrieve and verify slabs
    let retrieved_slabs = get_slabs(store, &segment.id).await?;

    if retrieved_slabs != inserted_slabs {
        return Err(format!(
            "Retrieved slabs don't match inserted slabs. Expected: {inserted_slabs:?}, Got: \
             {retrieved_slabs:?}"
        ));
    }

    // Delete a slab and verify it's gone
    if let Some(&slab_to_delete) = slabs.first() {
        delete_slab(store, &segment.id, slab_to_delete).await?;

        inserted_slabs.remove(&slab_to_delete);

        let retrieved_slabs = get_slabs(store, &segment.id).await?;

        if retrieved_slabs != inserted_slabs {
            return Err(format!(
                "Retrieved slabs after deletion don't match. Expected: {inserted_slabs:?}, Got: \
                 {retrieved_slabs:?}"
            ));
        }
    }

    Ok(())
}

/// Tests the `get_slab_range` operation of a `TriggerStore` implementation.
///
/// This test verifies that the store can correctly:
/// - Query slabs within a specific range
/// - Return only slabs that fall within the specified inclusive range
/// - Handle empty ranges correctly
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

    // Insert slabs with IDs 0, 5, 10, 15, 20
    let all_slab_ids: Vec<u32> = vec![0, 5, 10, 15, 20];
    for &slab_id in &all_slab_ids {
        let slab = Slab::new(segment.id, slab_id, segment.slab_size);
        insert_slab(store, &segment.id, slab).await?;
    }

    // Test range 5..=15 should return [5, 10, 15]
    let range_slabs: Vec<u32> = store
        .get_slab_range(&segment.id, 5..=15)
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
        .get_slab_range(&segment.id, 0..=0)
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
        .get_slab_range(&segment.id, 25..=30)
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
