#![allow(
    clippy::mutable_key_type,
    reason = "Trigger's ArcSwap field is excluded from hash/equality via Educe"
)]

//! Common utilities and helper functions for `TriggerStore` testing.
//!
//! This module provides a comprehensive set of helper functions that simplify
//! testing `TriggerStore` implementations. The functions abstract common
//! operations like inserting segments, managing slabs, and verifying store
//! state consistency across different storage backends.
//!
//! # Testing Philosophy
//!
//! The helpers focus on:
//! - **Data verification**: Ensuring operations produce expected results
//! - **Error handling**: Converting storage errors into test failures
//! - **State consistency**: Verifying dual indices (time and key) remain in
//!   sync
//! - **Cross-backend compatibility**: Working with any `TriggerStore`
//!   implementation

use super::TestStoreResult;
use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use crate::timers::{TimerType, Trigger};
use ahash::{HashMap, HashSet};
use futures::{StreamExt, TryStreamExt};
use std::fmt::Debug;

/// Helper function to insert a segment.
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn insert_segment<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .insert_segment(segment.clone())
        .await
        .map_err(|e| format!("Failed to insert segment: {e:?}"))?;
    Ok(())
}

/// Helper function to verify a segment exists and matches expected values
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn verify_segment<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    match store.get_segment(&segment.id).await {
        Ok(Some(retrieved))
            if retrieved.id == segment.id
                && retrieved.name == segment.name
                && retrieved.slab_size == segment.slab_size =>
        {
            Ok(())
        }
        Ok(Some(_)) => Err("Retrieved segment doesn't match original".to_owned()),
        Ok(None) => Err("Failed to retrieve segment".to_owned()),
        Err(e) => Err(format!("Error retrieving segment: {e:?}")),
    }
}

/// Helper function to delete a segment and verify it's gone
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn delete_segment<S>(store: &S, segment_id: &SegmentId) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .delete_segment(segment_id)
        .await
        .map_err(|e| format!("Failed to delete segment: {e:?}"))?;

    match store.get_segment(segment_id).await {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err("Segment still exists after deletion".to_owned()),
        Err(e) => Err(format!("Error checking segment existence: {e:?}")),
    }
}

/// Helper function to insert a slab
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn insert_slab<S>(store: &S, segment_id: &SegmentId, slab: Slab) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .insert_slab(segment_id, slab)
        .await
        .map_err(|e| format!("Failed to insert slab: {e:?}"))?;
    Ok(())
}

/// Helper function to get and verify slabs
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn get_slabs<S>(store: &S, segment_id: &SegmentId) -> Result<HashSet<SlabId>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .get_slabs(segment_id)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|e| format!("Error retrieving slabs: {e:?}"))
}

/// Helper function to delete a slab
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn delete_slab<S>(store: &S, segment_id: &SegmentId, slab_id: SlabId) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .delete_slab(segment_id, slab_id)
        .await
        .map_err(|e| format!("Failed to delete slab: {e:?}"))?;
    Ok(())
}

/// Helper function to add a trigger
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn add_trigger<S>(store: &S, segment: &Segment, trigger: &Trigger) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let slab = Slab::from_time(segment.id, segment.slab_size, trigger.time);
    store
        .add_trigger(segment, slab, trigger.clone())
        .await
        .map_err(|e| format!("Failed to add trigger: {e:?}"))?;
    Ok(())
}

/// Helper function to get triggers by key (Application timers only)
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn get_key_triggers<S>(
    store: &S,
    segment_id: &SegmentId,
    key: &Key,
) -> Result<HashSet<CompactDateTime>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    get_key_triggers_by_type(store, segment_id, key, TimerType::Application).await
}

/// Helper function to get triggers by key for a specific timer type
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn get_key_triggers_by_type<S>(
    store: &S,
    segment_id: &SegmentId,
    key: &Key,
    timer_type: TimerType,
) -> Result<HashSet<CompactDateTime>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .get_key_times(segment_id, timer_type, key)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|e| format!("Error retrieving key triggers for {timer_type:?}: {e:?}"))
}

/// Helper function to get ALL triggers for a key (both Application and `DeferRetry`)
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn get_key_triggers_all_types<S>(
    store: &S,
    segment_id: &SegmentId,
    key: &Key,
) -> Result<HashSet<CompactDateTime>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .get_key_triggers_all_types(segment_id, key)
        .map_ok(|trigger| trigger.time)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|e| format!("Error retrieving all key times: {e:?}"))
}

/// Helper function to get triggers by slab (Application timers only)
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn get_slab_triggers<S>(store: &S, slab: &Slab) -> Result<HashSet<Trigger>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    get_slab_triggers_by_type(store, slab, TimerType::Application).await
}

/// Helper function to get triggers by slab for a specific timer type
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn get_slab_triggers_by_type<S>(
    store: &S,
    slab: &Slab,
    timer_type: TimerType,
) -> Result<HashSet<Trigger>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .get_slab_triggers(slab, timer_type)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|e| format!("Error retrieving slab triggers for {timer_type:?}: {e:?}"))
}

/// Helper function to get ALL triggers from a slab (both Application and `DeferRetry`)
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn get_slab_triggers_all_types<S>(store: &S, slab: &Slab) -> Result<HashSet<Trigger>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .get_slab_triggers_all_types(slab)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|e| format!("Error retrieving all slab triggers: {e:?}"))
}

/// Helper function to remove a trigger
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn remove_trigger<S>(
    store: &S,
    segment: &Segment,
    key: &Key,
    time: CompactDateTime,
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let slab = Slab::from_time(segment.id, segment.slab_size, time);
    store
        .remove_trigger(segment, &slab, key, time, TimerType::Application)
        .await
        .map_err(|e| format!("Failed to remove trigger: {e:?}"))?;
    Ok(())
}

/// Helper function to clear triggers for a key
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn clear_triggers_for_key<S>(
    store: &S,
    segment_id: &SegmentId,
    key: &Key,
    slab_size: CompactDuration,
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .clear_triggers_for_key(segment_id, TimerType::Application, key, slab_size)
        .await
        .map_err(|e| format!("Failed to clear triggers for key: {e:?}"))?;
    Ok(())
}

/// Helper function to clear triggers for a slab
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn clear_slab_triggers<S>(store: &S, slab: &Slab) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .clear_slab_triggers(slab)
        .await
        .map_err(|e| format!("Failed to clear slab triggers: {e:?}"))?;
    Ok(())
}

/// Helper function to verify that the store matches our expected state
///
/// # Errors
///
/// Returns an error if the store operation fails.
#[allow(clippy::implicit_hasher)]
pub async fn verify_store_state<S>(
    store: &S,
    segment: &Segment,
    expected_state: &HashMap<Key, HashSet<CompactDateTime>>,
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Check all keys in our expected state
    for (key, expected_times) in expected_state {
        if expected_times.is_empty() {
            continue; // Skip keys with no times
        }

        // Get actual times for this key from the store
        let actual_times = get_key_triggers(store, &segment.id, key).await?;

        // Verify each key's times match what we expect
        if &actual_times != expected_times {
            return Err(format!(
                "Key times mismatch for key {key:?}. Expected: {expected_times:?}, Got: \
                 {actual_times:?}"
            ));
        }

        // Verify slab consistency for each time
        for &time in expected_times {
            let slab = Slab::from_time(segment.id, segment.slab_size, time);
            let slab_triggers = get_slab_triggers(store, &slab).await?;

            // Check if this trigger is in the correct slab
            let found_in_slab = slab_triggers
                .iter()
                .any(|t| t.key == *key && t.time == time);

            if !found_in_slab {
                return Err(format!(
                    "Trigger for key {key:?} at time {time:?} not found in expected slab"
                ));
            }
        }
    }

    Ok(())
}
