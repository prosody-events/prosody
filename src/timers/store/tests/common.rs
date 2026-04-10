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

use std::collections::{HashMap, HashSet as StdHashSet};
use std::fmt::Debug;
use std::hash::BuildHasher;

use ahash::HashSet;
use futures::StreamExt;

use super::TestStoreResult;
use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use crate::timers::{TimerType, Trigger};

/// Helper function to insert a segment.
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn insert_segment<S>(store: &S, _segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .insert_segment()
        .await
        .map_err(|e| format!("Failed to insert segment: {e:?}"))?;
    Ok(())
}

/// Helper function to delete a slab
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn delete_slab<S>(store: &S, slab_id: SlabId) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .delete_slab(slab_id)
        .await
        .map_err(|e| format!("Failed to delete slab: {e:?}"))?;
    Ok(())
}

/// Helper function to add a trigger
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn add_trigger<S>(store: &S, trigger: &Trigger) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .add_trigger(trigger.clone())
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
    _segment_id: &SegmentId,
    key: &Key,
) -> Result<HashSet<CompactDateTime>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    get_key_triggers_by_type(store, key, TimerType::Application).await
}

/// Helper function to get triggers by key for a specific timer type
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn get_key_triggers_by_type<S>(
    store: &S,
    key: &Key,
    timer_type: TimerType,
) -> Result<HashSet<CompactDateTime>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .get_key_times(timer_type, key)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|e| format!("Error retrieving key triggers for {timer_type:?}: {e:?}"))
}

/// Helper function to get ALL triggers from a slab (both Application and
/// `DeferredMessage`)
///
/// Uses the public API method `get_slab_triggers_all_types`.
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn get_slab_triggers_all_types<S>(
    store: &S,
    slab_id: SlabId,
) -> Result<HashSet<Trigger>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .get_slab_triggers_all_types(slab_id)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|e| format!("Error retrieving all slab triggers: {e:?}"))
}

/// Helper function to get triggers from a slab (all timer types)
///
/// This is an alias for `get_slab_triggers_all_types` for backwards
/// compatibility with existing tests.
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn get_slab_triggers<S>(store: &S, slab_id: SlabId) -> Result<HashSet<Trigger>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    get_slab_triggers_all_types(store, slab_id).await
}

/// Helper function to remove a trigger
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn remove_trigger<S>(
    store: &S,
    key: &Key,
    time: CompactDateTime,
    timer_type: TimerType,
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .remove_trigger(key, time, timer_type)
        .await
        .map_err(|e| format!("Failed to remove trigger: {e:?}"))?;
    Ok(())
}

/// Helper function to clear all triggers for a key and timer type
///
/// Implemented using public API: get all triggers, then remove each one
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn clear_triggers_for_key<S>(
    store: &S,
    timer_type: TimerType,
    key: &Key,
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Get all trigger times for this key
    let times: Vec<CompactDateTime> = store
        .get_key_times(timer_type, key)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to get key times: {e:?}"))?;

    // Remove each trigger
    for time in times {
        store
            .remove_trigger(key, time, timer_type)
            .await
            .map_err(|e| format!("Failed to remove trigger: {e:?}"))?;
    }

    Ok(())
}

/// Helper function to verify that the store matches our expected state
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn verify_store_state<S, H1, H2>(
    store: &S,
    segment: &Segment,
    expected_state: &HashMap<Key, StdHashSet<CompactDateTime, H2>, H1>,
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
    H1: BuildHasher,
    H2: BuildHasher,
{
    // Check all keys in our expected state
    for (key, expected_times) in expected_state {
        if expected_times.is_empty() {
            continue; // Skip keys with no times
        }

        // Get actual times for this key from the store
        let actual_times = get_key_triggers(store, &segment.id, key).await?;

        // Verify each key's times match what we expect (compare across hasher types)
        let times_match = actual_times.len() == expected_times.len()
            && actual_times.iter().all(|t| expected_times.contains(t));
        if !times_match {
            return Err(format!(
                "Key times mismatch for key {key:?}. Expected: {expected_times:?}, Got: \
                 {actual_times:?}"
            ));
        }

        // Verify slab consistency for each time
        for &time in expected_times {
            let slab_id = Slab::from_time(segment.slab_size, time).id();
            let slab_triggers = get_slab_triggers_all_types(store, slab_id).await?;

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
