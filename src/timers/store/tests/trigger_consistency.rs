use crate::timers::slab::Slab;
use crate::timers::store::TriggerStore;
use crate::timers::store::tests::common::{
    add_trigger, get_key_triggers, get_slab_triggers, insert_segment,
};
use crate::timers::store::tests::{TestStoreResult, TriggerTestInput};
use std::fmt::Debug;

/// Tests the consistency between key and slab indices.
///
/// This test verifies that:
/// - When a trigger is added, it's correctly accessible from both key and slab
///   indices
/// - Data remains consistent between the two access patterns
pub async fn test_trigger_consistency<S>(store: &S, input: &TriggerTestInput) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    if input.triggers.is_empty() {
        return Err("No triggers in test input".to_owned());
    }

    // Insert the segment
    insert_segment(store, &input.segment).await?;

    // Add just one trigger for consistency testing
    let trigger = &input.triggers[0];
    add_trigger(store, &input.segment, trigger).await?;

    // Check it exists in key index
    let key_times = get_key_triggers(store, &input.segment.id, &trigger.key).await?;

    if !key_times.contains(&trigger.time) {
        return Err("Trigger not found in key index".to_owned());
    }

    // Check it exists in slab index
    let slab = Slab::from_time(input.segment.id, input.segment.slab_size, trigger.time);

    let slab_triggers = get_slab_triggers(store, &slab).await?;

    let trigger_in_slab = slab_triggers
        .iter()
        .any(|t| t.key == trigger.key && t.time == trigger.time);

    if !trigger_in_slab {
        return Err("Trigger not found in slab index".to_owned());
    }

    Ok(())
}
