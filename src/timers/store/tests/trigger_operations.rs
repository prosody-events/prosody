#![allow(
    clippy::mutable_key_type,
    reason = "Trigger's ArcSwap field is excluded from hash/equality via Educe"
)]

use crate::timers::slab::Slab;
use crate::timers::store::TriggerStore;
use crate::timers::store::tests::common::{
    add_trigger, clear_triggers_for_key, get_key_triggers, get_slab_triggers, insert_segment,
    remove_trigger,
};
use crate::timers::store::tests::{
    TestStoreResult, TriggerOperation, TriggerSequence, TriggerTestInput,
};
use crate::timers::{TimerType, Trigger};
use ahash::HashSet;
use std::fmt::Debug;

use tracing::Span;

/// Tests basic trigger operations: add, get, remove, clear
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn test_trigger_operations<S>(store: &S, input: &TriggerTestInput) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    if input.triggers.is_empty() {
        return Err("No triggers in test input".to_owned());
    }

    // Insert the segment
    insert_segment(store, &input.segment).await?;

    // Add all triggers
    for trigger in &input.triggers {
        add_trigger(store, &input.segment, trigger).await?;
    }

    // Verify triggers by key
    let mut key_triggers = HashSet::default();
    let all_keys: HashSet<_> = input.triggers.iter().map(|t| t.key.clone()).collect();

    for key in all_keys {
        let times = get_key_triggers(store, &input.segment.id, &key).await?;

        for time in times {
            key_triggers.insert((key.clone(), time));
        }
    }

    // Compare with original triggers
    let original_triggers: HashSet<_> = input
        .triggers
        .iter()
        .map(|t| (t.key.clone(), t.time))
        .collect();

    if key_triggers != original_triggers {
        return Err(format!(
            "Retrieved triggers don't match original triggers. Expected: {original_triggers:?}, \
             Got: {key_triggers:?}"
        ));
    }

    // Check slab-based retrieval
    if let Some(trigger) = input.triggers.first() {
        let slab = Slab::from_time(input.segment.id, input.segment.slab_size, trigger.time);

        let slab_triggers = get_slab_triggers(store, &slab).await?;

        // Filter original triggers for this slab
        let expected_slab_triggers: HashSet<_> = input
            .triggers
            .iter()
            .filter(|t| {
                let t_slab = Slab::from_time(input.segment.id, input.segment.slab_size, t.time);
                t_slab.id() == slab.id()
            })
            .cloned()
            .collect();

        if slab_triggers != expected_slab_triggers {
            return Err(format!(
                "Retrieved slab triggers don't match expected. Expected: \
                 {expected_slab_triggers:?}, Got: {slab_triggers:?}"
            ));
        }
    }

    // Test remove_trigger
    if let Some(trigger) = input.triggers.first() {
        remove_trigger(store, &input.segment, &trigger.key, trigger.time).await?;

        let remaining_key_times = get_key_triggers(store, &input.segment.id, &trigger.key).await?;

        let removed_present = remaining_key_times.contains(&trigger.time);
        if removed_present {
            return Err("Trigger still present after removal".to_owned());
        }

        // Check it's also gone from the slab
        let trigger_slab = Slab::from_time(input.segment.id, input.segment.slab_size, trigger.time);
        let remaining_slab_triggers = get_slab_triggers(store, &trigger_slab).await?;

        let trigger_in_slab = remaining_slab_triggers
            .iter()
            .any(|t| t.key == trigger.key && t.time == trigger.time);

        if trigger_in_slab {
            return Err("Trigger still present in slab after removal".to_owned());
        }
    }

    // Test clear_triggers_for_key
    if let Some(trigger) = input.triggers.first() {
        clear_triggers_for_key(
            store,
            &input.segment.id,
            &trigger.key,
            input.segment.slab_size,
        )
        .await?;

        let remaining_key_times = get_key_triggers(store, &input.segment.id, &trigger.key).await?;

        if !remaining_key_times.is_empty() {
            return Err(format!(
                "Key triggers still present after clear: {remaining_key_times:?}"
            ));
        }
    }

    Ok(())
}

/// Tests sequences of operations to ensure proper state transitions
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn test_operation_sequences<S>(store: &S, input: &TriggerSequence) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    if input.times.is_empty() || input.operations.is_empty() {
        return Err("Empty test input".to_owned());
    }

    // Insert the segment
    insert_segment(store, &input.segment).await?;

    // Track which times should be present based on the operations
    let mut expected_times = HashSet::default();

    // Execute the sequence of operations
    for (i, op) in input.operations.iter().enumerate() {
        // Pick a time from the available times
        let time_idx = i % input.times.len();
        let time = input.times[time_idx];

        match op {
            TriggerOperation::Add => {
                let trigger = Trigger::new(
                    input.key.clone(),
                    time,
                    TimerType::Application,
                    Span::current(),
                );

                add_trigger(store, &input.segment, &trigger).await?;
                expected_times.insert(time);
            }
            TriggerOperation::Remove => {
                remove_trigger(store, &input.segment, &input.key, time).await?;
                expected_times.remove(&time);
            }
            TriggerOperation::Clear => {
                clear_triggers_for_key(
                    store,
                    &input.segment.id,
                    &input.key,
                    input.segment.slab_size,
                )
                .await?;
                expected_times.clear();
            }
        }
    }

    // Verify final state after all operations
    let actual_times = get_key_triggers(store, &input.segment.id, &input.key).await?;

    if actual_times != expected_times {
        return Err(format!(
            "Final state mismatch. Expected: {expected_times:?}, Got: {actual_times:?}"
        ));
    }

    Ok(())
}
