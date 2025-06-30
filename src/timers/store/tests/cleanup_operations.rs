use crate::timers::slab::Slab;
use crate::timers::store::TriggerStore;
use crate::timers::store::tests::common::{
    add_trigger, get_key_triggers, get_slab_triggers, insert_segment,
};
use crate::timers::store::tests::{TestStoreResult, TriggerTestInput};
use ahash::HashSet;
use std::fmt::Debug;

/// Tests cleanup operations and their interaction with the underlying data
///
/// This test verifies the behavior of `delete_slab` and the cleanup process:
/// 1. Deleting a slab registration doesn't automatically delete the associated
///    triggers
/// 2. Explicit cleanup with clear_* functions is required to remove the actual
///    data
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn test_cleanup_operations<S>(store: &S, input: &TriggerTestInput) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    if input.triggers.is_empty() {
        return Err("No triggers in test input".to_owned());
    }

    // Insert the segment
    insert_segment(store, &input.segment).await?;

    // Add a few triggers
    let test_triggers = if input.triggers.len() > 3 {
        &input.triggers[0..3]
    } else {
        &input.triggers
    };

    for trigger in test_triggers {
        add_trigger(store, &input.segment, trigger).await?;
    }

    // Test slabs and registration
    let sample_trigger = &test_triggers[0];
    let slab = Slab::from_time(
        input.segment.id,
        input.segment.slab_size,
        sample_trigger.time,
    );

    // Register the slab
    store
        .insert_slab(&input.segment.id, slab.clone())
        .await
        .map_err(|e| format!("Failed to insert slab: {e:?}"))?;

    // Delete the slab registration
    store
        .delete_slab(&input.segment.id, slab.id())
        .await
        .map_err(|e| format!("Failed to delete slab: {e:?}"))?;

    // IMPORTANT: Verify that delete_slab only removes the registration
    // but NOT the actual triggers. This is the expected behavior.
    let slab_triggers = get_slab_triggers(store, &slab).await?;

    // Find triggers that should be in this slab
    let expected_triggers: HashSet<_> = test_triggers
        .iter()
        .filter(|t| {
            let t_slab = Slab::from_time(input.segment.id, input.segment.slab_size, t.time);
            t_slab.id() == slab.id()
        })
        .cloned()
        .collect();

    // If no triggers in this slab, test isn't valid
    if expected_triggers.is_empty() {
        return Err("No test triggers in the selected slab, test inconclusive".to_owned());
    }

    // Compare with actual triggers
    if slab_triggers != expected_triggers {
        return Err(format!(
            "Unexpectedly modified triggers after slab deletion. Expected: {expected_triggers:?}, \
             Got: {slab_triggers:?}"
        ));
    }

    // Test explicit cleanup with clear_slab_triggers
    store
        .clear_slab_triggers(&slab)
        .await
        .map_err(|e| format!("Failed to clear slab triggers: {e:?}"))?;

    // Verify slab is empty
    let remaining_triggers = get_slab_triggers(store, &slab).await?;
    if !remaining_triggers.is_empty() {
        return Err(format!(
            "Slab still has triggers after clear: {remaining_triggers:?}"
        ));
    }

    // Test clear_triggers_for_key
    let key_trigger = &test_triggers[0];

    // Add a trigger for testing
    add_trigger(store, &input.segment, key_trigger).await?;

    // Clear triggers for the key
    store
        .clear_triggers_for_key(&input.segment.id, &key_trigger.key, input.segment.slab_size)
        .await
        .map_err(|e| format!("Failed to clear triggers for key: {e:?}"))?;

    // Verify the key index is clear
    let key_times = get_key_triggers(store, &input.segment.id, &key_trigger.key).await?;
    if !key_times.is_empty() {
        return Err(format!("Key triggers not cleared: {key_times:?}"));
    }

    // Verify also removed from the slab index
    let key_slab = Slab::from_time(input.segment.id, input.segment.slab_size, key_trigger.time);
    let slab_triggers = get_slab_triggers(store, &key_slab).await?;

    let found_in_slab = slab_triggers
        .iter()
        .any(|t| t.key == key_trigger.key && t.time == key_trigger.time);

    if found_in_slab {
        return Err("Trigger still in slab after clear_triggers_for_key".to_owned());
    }

    Ok(())
}
