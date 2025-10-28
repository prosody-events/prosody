#![allow(
    clippy::mutable_key_type,
    reason = "Trigger's ArcSwap field is excluded from hash/equality via Educe"
)]

use crate::timers::TimerType;
use crate::timers::slab::Slab;
use crate::timers::store::{Trigger, TriggerStore};
use crate::timers::store::tests::common::{get_key_triggers, get_slab_triggers, insert_segment};
use crate::timers::store::tests::{TestStoreResult, TriggerTestInput};
use futures::TryStreamExt;
use std::fmt::Debug;

/// Tests slab trigger insert and delete operations.
///
/// # Errors
///
/// Returns an error if insert or delete operations fail.
async fn test_slab_trigger_insert_delete<S>(
    store: &S,
    slab: &Slab,
    sample_trigger: &Trigger,
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .insert_slab_trigger(slab.clone(), sample_trigger.clone())
        .await
        .map_err(|e| format!("Failed to insert slab trigger: {e:?}"))?;

    let slab_triggers = get_slab_triggers(store, slab).await?;
    if !slab_triggers.contains(sample_trigger) {
        return Err("Inserted trigger not found in slab".to_owned());
    }

    store
        .delete_slab_trigger(
            slab,
            TimerType::Application,
            &sample_trigger.key,
            sample_trigger.time,
        )
        .await
        .map_err(|e| format!("Failed to delete slab trigger: {e:?}"))?;

    let slab_triggers = get_slab_triggers(store, slab).await?;
    if slab_triggers.contains(sample_trigger) {
        return Err("Deleted trigger still present in slab".to_owned());
    }

    Ok(())
}

/// Tests clearing all triggers from a slab.
///
/// # Errors
///
/// Returns an error if clear operation fails.
async fn test_slab_clear<S>(
    store: &S,
    slab: &Slab,
    triggers: &[Trigger],
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    if triggers.len() < 2 {
        return Ok(());
    }

    let test_triggers = &triggers[0..2];
    for trigger in test_triggers {
        store
            .insert_slab_trigger(slab.clone(), trigger.clone())
            .await
            .map_err(|e| format!("Failed to insert test trigger: {e:?}"))?;
    }

    store
        .clear_slab_triggers(slab)
        .await
        .map_err(|e| format!("Failed to clear slab: {e:?}"))?;

    let slab_triggers = get_slab_triggers(store, slab).await?;
    if !slab_triggers.is_empty() {
        return Err(format!("Slab not cleared, contains: {slab_triggers:?}"));
    }

    Ok(())
}

/// Tests key trigger insert and delete operations.
///
/// # Errors
///
/// Returns an error if insert or delete operations fail.
async fn test_key_trigger_insert_delete<S>(
    store: &S,
    segment_id: &uuid::Uuid,
    sample_trigger: &Trigger,
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .insert_key_trigger(segment_id, sample_trigger.clone())
        .await
        .map_err(|e| format!("Failed to insert key trigger: {e:?}"))?;

    let times = get_key_triggers(store, segment_id, &sample_trigger.key).await?;
    if !times.contains(&sample_trigger.time) {
        return Err("Key trigger not found after insertion".to_owned());
    }

    store
        .delete_key_trigger(
            segment_id,
            TimerType::Application,
            &sample_trigger.key,
            sample_trigger.time,
        )
        .await
        .map_err(|e| format!("Failed to delete key trigger: {e:?}"))?;

    let times = get_key_triggers(store, segment_id, &sample_trigger.key).await?;
    if times.contains(&sample_trigger.time) {
        return Err("Key trigger still present after deletion".to_owned());
    }

    Ok(())
}

/// Tests `get_key_triggers` method that returns full Trigger objects.
///
/// # Errors
///
/// Returns an error if operations fail or trigger not found.
async fn test_key_trigger_full_retrieval<S>(
    store: &S,
    segment_id: &uuid::Uuid,
    sample_trigger: &Trigger,
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .insert_key_trigger(segment_id, sample_trigger.clone())
        .await
        .map_err(|e| format!("Failed to insert key trigger for full test: {e:?}"))?;

    let full_triggers: Vec<_> = store
        .get_key_triggers(segment_id, TimerType::Application, &sample_trigger.key)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get full key triggers: {e:?}"))?;

    if !full_triggers.iter().any(|t| t == sample_trigger) {
        return Err("Full trigger not found via get_key_triggers".to_owned());
    }

    store
        .delete_key_trigger(
            segment_id,
            TimerType::Application,
            &sample_trigger.key,
            sample_trigger.time,
        )
        .await
        .map_err(|e| format!("Failed to clean up key trigger: {e:?}"))?;

    Ok(())
}

/// Tests clearing all triggers for a key.
///
/// # Errors
///
/// Returns an error if clear operation fails.
async fn test_key_clear<S>(
    store: &S,
    segment_id: &uuid::Uuid,
    sample_trigger: &Trigger,
    triggers: &[Trigger],
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    if triggers.len() < 2 {
        return Ok(());
    }

    let test_triggers = &triggers[0..2];
    for trigger in test_triggers.iter().filter(|t| t.key == sample_trigger.key) {
        store
            .insert_key_trigger(segment_id, trigger.clone())
            .await
            .map_err(|e| format!("Failed to insert test key trigger: {e:?}"))?;
    }

    store
        .clear_key_triggers(segment_id, TimerType::Application, &sample_trigger.key)
        .await
        .map_err(|e| format!("Failed to clear key triggers: {e:?}"))?;

    let times = get_key_triggers(store, segment_id, &sample_trigger.key).await?;
    if !times.is_empty() {
        return Err(format!("Key triggers not cleared, found: {times:?}"));
    }

    Ok(())
}

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

    insert_segment(store, &input.segment).await?;

    let sample_trigger = &input.triggers[0];
    let slab = Slab::from_time(
        input.segment.id,
        input.segment.slab_size,
        sample_trigger.time,
    );

    test_slab_trigger_insert_delete(store, &slab, sample_trigger).await?;
    test_slab_clear(store, &slab, &input.triggers).await?;
    test_key_trigger_insert_delete(store, &input.segment.id, sample_trigger).await?;
    test_key_trigger_full_retrieval(store, &input.segment.id, sample_trigger).await?;
    test_key_clear(store, &input.segment.id, sample_trigger, &input.triggers).await?;

    Ok(())
}
