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
