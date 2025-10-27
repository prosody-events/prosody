#![allow(
    clippy::mutable_key_type,
    reason = "Trigger's ArcSwap field is excluded from hash/equality via Educe"
)]

use crate::timers::TimerType;
use crate::timers::slab::Slab;
use crate::timers::store::TriggerStore;
use crate::timers::store::tests::common::{get_key_triggers, get_slab_triggers, insert_segment};
use crate::timers::store::tests::{TestStoreResult, TriggerTestInput};
use futures::TryStreamExt;
use std::fmt::Debug;

/// Tests primitive (low-level) store operations directly
///
/// This test focuses on the direct usage of low-level operations:
/// - Slab trigger operations (insert, delete, clear)
/// - Key trigger operations (insert, delete, clear)
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn test_primitive_operations<S>(store: &S, input: &TriggerTestInput) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    if input.triggers.is_empty() {
        return Err("No triggers in test input".to_owned());
    }

    // Set up
    insert_segment(store, &input.segment).await?;

    // Get a sample trigger
    let sample_trigger = &input.triggers[0];
    let slab = Slab::from_time(
        input.segment.id,
        input.segment.slab_size,
        sample_trigger.time,
    );

    // Test slab trigger insert/delete
    store
        .insert_slab_trigger(slab.clone(), sample_trigger.clone())
        .await
        .map_err(|e| format!("Failed to insert slab trigger: {e:?}"))?;

    let slab_triggers = get_slab_triggers(store, &slab).await?;
    if !slab_triggers.contains(sample_trigger) {
        return Err("Inserted trigger not found in slab".to_owned());
    }

    store
        .delete_slab_trigger(
            &slab,
            TimerType::Application,
            &sample_trigger.key,
            sample_trigger.time,
        )
        .await
        .map_err(|e| format!("Failed to delete slab trigger: {e:?}"))?;

    let slab_triggers = get_slab_triggers(store, &slab).await?;
    if slab_triggers.contains(sample_trigger) {
        return Err("Deleted trigger still present in slab".to_owned());
    }

    // Test slab clear operation
    if input.triggers.len() >= 2 {
        // Add two triggers to the slab
        let triggers = &input.triggers[0..2];
        for trigger in triggers {
            store
                .insert_slab_trigger(slab.clone(), trigger.clone())
                .await
                .map_err(|e| format!("Failed to insert test trigger: {e:?}"))?;
        }

        // Clear the slab
        store
            .clear_slab_triggers(&slab)
            .await
            .map_err(|e| format!("Failed to clear slab: {e:?}"))?;

        // Verify slab is empty
        let slab_triggers = get_slab_triggers(store, &slab).await?;
        if !slab_triggers.is_empty() {
            return Err(format!("Slab not cleared, contains: {slab_triggers:?}"));
        }
    }

    // Test key trigger operations
    store
        .insert_key_trigger(&input.segment.id, sample_trigger.clone())
        .await
        .map_err(|e| format!("Failed to insert key trigger: {e:?}"))?;

    let times = get_key_triggers(store, &input.segment.id, &sample_trigger.key).await?;
    if !times.contains(&sample_trigger.time) {
        return Err("Key trigger not found after insertion".to_owned());
    }

    store
        .delete_key_trigger(
            &input.segment.id,
            TimerType::Application,
            &sample_trigger.key,
            sample_trigger.time,
        )
        .await
        .map_err(|e| format!("Failed to delete key trigger: {e:?}"))?;

    let times = get_key_triggers(store, &input.segment.id, &sample_trigger.key).await?;
    if times.contains(&sample_trigger.time) {
        return Err("Key trigger still present after deletion".to_owned());
    }

    // Test get_key_triggers trait method (returns full Trigger objects)
    store
        .insert_key_trigger(&input.segment.id, sample_trigger.clone())
        .await
        .map_err(|e| format!("Failed to insert key trigger for full test: {e:?}"))?;

    let full_triggers: Vec<_> = store
        .get_key_triggers(
            &input.segment.id,
            TimerType::Application,
            &sample_trigger.key,
        )
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get full key triggers: {e:?}"))?;

    if !full_triggers.iter().any(|t| t == sample_trigger) {
        return Err("Full trigger not found via get_key_triggers".to_owned());
    }

    // Clean up for next test
    store
        .delete_key_trigger(
            &input.segment.id,
            TimerType::Application,
            &sample_trigger.key,
            sample_trigger.time,
        )
        .await
        .map_err(|e| format!("Failed to clean up key trigger: {e:?}"))?;

    // Test key clear operation
    if input.triggers.len() >= 2 {
        // Add triggers for the key
        let triggers = &input.triggers[0..2];
        for trigger in triggers.iter().filter(|t| t.key == sample_trigger.key) {
            store
                .insert_key_trigger(&input.segment.id, trigger.clone())
                .await
                .map_err(|e| format!("Failed to insert test key trigger: {e:?}"))?;
        }

        // Clear all triggers for the key
        store
            .clear_key_triggers(
                &input.segment.id,
                TimerType::Application,
                &sample_trigger.key,
            )
            .await
            .map_err(|e| format!("Failed to clear key triggers: {e:?}"))?;

        // Verify key triggers are gone
        let times = get_key_triggers(store, &input.segment.id, &sample_trigger.key).await?;
        if !times.is_empty() {
            return Err(format!("Key triggers not cleared, found: {times:?}"));
        }
    }

    Ok(())
}
