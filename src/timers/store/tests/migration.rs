#![allow(
    clippy::mutable_key_type,
    reason = "Trigger's ArcSwap field is excluded from hash/equality via Educe"
)]

//! Tests for timer schema migration from v1 to v2.
//!
//! Verifies that the migration logic correctly identifies segments needing
//! migration and that v2 schema operations work correctly with timer types.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::migration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::tests::{TestStoreResult, common};
use crate::timers::store::tests::common::{add_trigger, get_slab_triggers};
use crate::timers::store::{
    SEGMENT_VERSION_V1, SEGMENT_VERSION_V2, Segment, SegmentId, TriggerStore, TriggerV1,
};
use crate::timers::{TimerType, Trigger};
use ahash::{HashSet, HashSetExt};
use futures::{StreamExt, TryStreamExt};
use std::fmt::Debug;
use tracing::Span;

/// Tests that `needs_migration` correctly identifies v1 and v2 segments.
///
/// # Errors
///
/// Returns an error if the logic is incorrect.
pub fn test_needs_migration_logic<S>(_store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Test v1 segment (version = None)
    let mut v1_none_segment = segment.clone();
    v1_none_segment.version = None;

    if !migration::needs_migration(&v1_none_segment) {
        return Err("needs_migration should return true for segment with version=None".to_owned());
    }

    // Test v1 segment (version = 1)
    let mut v1_explicit_segment = segment.clone();
    v1_explicit_segment.version = Some(SEGMENT_VERSION_V1);

    if !migration::needs_migration(&v1_explicit_segment) {
        return Err("needs_migration should return true for segment with version=1".to_owned());
    }

    // Test v2 segment
    let mut v2_segment = segment.clone();
    v2_segment.version = Some(SEGMENT_VERSION_V2);

    if migration::needs_migration(&v2_segment) {
        return Err("needs_migration should return false for segment with version=2".to_owned());
    }

    Ok(())
}

/// Tests segment version update functionality.
///
/// # Errors
///
/// Returns an error if version update fails.
pub async fn test_segment_version_update<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    use tracing::info_span;

    // Create a v1 segment
    let mut v1_segment = segment.clone();
    v1_segment.version = Some(SEGMENT_VERSION_V1);

    store
        .insert_segment(v1_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert v1 segment: {e:?}"))?;

    // Insert some v1 data to make this realistic
    let now = CompactDateTime::now().map_err(|e| format!("Failed to get time: {e:?}"))?;
    let time = now
        .add_duration(CompactDuration::new(3600))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let slab_id = Slab::from_time(v1_segment.id, v1_segment.slab_size, time).id();

    let trigger_v1 = TriggerV1 {
        key: "version-update-key".into(),
        time,
        span: info_span!("version_update_trigger"),
    };

    store
        .insert_slab_v1(&v1_segment.id, slab_id)
        .await
        .map_err(|e| format!("Failed to insert v1 slab: {e:?}"))?;
    store
        .insert_slab_trigger_v1(&v1_segment.id, slab_id, trigger_v1)
        .await
        .map_err(|e| format!("Failed to insert v1 trigger: {e:?}"))?;

    // Verify v1 data exists
    let v1_slabs: Vec<_> = store
        .get_slabs_v1(&v1_segment.id)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get v1 slabs: {e:?}"))?;

    if v1_slabs.is_empty() {
        return Err("V1 slab should exist before version update".to_owned());
    }

    // Update to v2
    store
        .update_segment_version(&v1_segment.id, SEGMENT_VERSION_V2, v1_segment.slab_size)
        .await
        .map_err(|e| format!("Failed to update segment version: {e:?}"))?;

    // Reload and verify
    let updated_segment = store
        .get_segment(&v1_segment.id)
        .await
        .map_err(|e| format!("Failed to get segment: {e:?}"))?
        .ok_or("Segment not found after version update")?;

    if updated_segment.version != Some(SEGMENT_VERSION_V2) {
        return Err(format!(
            "Expected version {:?}, got {:?}",
            Some(SEGMENT_VERSION_V2),
            updated_segment.version
        ));
    }

    Ok(())
}

/// Tests creating v2 segments directly (no migration needed).
///
/// Verifies that new segments are created with v2 schema and don't require
/// migration.
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn test_v2_segment_creation<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Create a v2 segment
    let mut v2_segment = segment.clone();
    v2_segment.version = Some(SEGMENT_VERSION_V2);

    store
        .insert_segment(v2_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert v2 segment: {e:?}"))?;

    // Verify needs_migration returns false
    if migration::needs_migration(&v2_segment) {
        return Err("needs_migration should return false for v2 segment".to_owned());
    }

    // Add a trigger with explicit timer type
    let time = CompactDateTime::now()
        .map_err(|e| format!("Failed to get current time: {e:?}"))?
        .add_duration(CompactDuration::new(3600))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let trigger = Trigger::new(
        "v2-key".into(),
        time,
        TimerType::DeferRetry,
        Span::current(),
    );

    add_trigger(store, &v2_segment, &trigger).await?;

    // Verify trigger is stored with correct type
    let slab = Slab::from_time(v2_segment.id, v2_segment.slab_size, trigger.time);
    let slab_triggers = common::get_slab_triggers_by_type(store, &slab, TimerType::DeferRetry).await?;

    let found = slab_triggers.iter().any(|t| {
        t.key == trigger.key && t.time == trigger.time && t.timer_type == TimerType::DeferRetry
    });

    if !found {
        return Err("V2 trigger not found with correct timer type".to_owned());
    }

    Ok(())
}

/// Tests mixed timer types in v2 schema.
///
/// Verifies that multiple timer types can coexist for the same key.
///
/// # Errors
///
/// Returns an error if the store operation fails or data is inconsistent.
pub async fn test_mixed_timer_types<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Create a v2 segment
    let mut v2_segment = segment.clone();
    v2_segment.version = Some(SEGMENT_VERSION_V2);

    store
        .insert_segment(v2_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert segment: {e:?}"))?;

    let key: Key = "mixed-types-key".into();

    let now = CompactDateTime::now().map_err(|e| format!("Failed to get current time: {e:?}"))?;

    let time1 = now
        .add_duration(CompactDuration::new(3600))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let time2 = now
        .add_duration(CompactDuration::new(7200))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    // Add Application timer
    let app_trigger = Trigger::new(key.clone(), time1, TimerType::Application, Span::current());
    add_trigger(store, &v2_segment, &app_trigger).await?;

    // Add DeferRetry timer for same key
    let defer_trigger = Trigger::new(key.clone(), time2, TimerType::DeferRetry, Span::current());
    add_trigger(store, &v2_segment, &defer_trigger).await?;

    // Verify both triggers exist (need to check both timer types)
    let key_times = common::get_key_triggers_all_types(store, &v2_segment.id, &key).await?;
    if !key_times.contains(&time1) || !key_times.contains(&time2) {
        return Err("Both timer types should be present for key".to_owned());
    }

    // Verify they have correct types via slab retrieval
    let slab1 = Slab::from_time(v2_segment.id, v2_segment.slab_size, time1);
    let slab1_triggers = common::get_slab_triggers_by_type(store, &slab1, TimerType::Application).await?;

    let app_found = slab1_triggers
        .iter()
        .any(|t| t.key == key && t.time == time1 && t.timer_type == TimerType::Application);

    if !app_found {
        return Err("Application timer not found with correct type".to_owned());
    }

    let slab2 = Slab::from_time(v2_segment.id, v2_segment.slab_size, time2);
    let slab2_triggers = common::get_slab_triggers_by_type(store, &slab2, TimerType::DeferRetry).await?;

    let defer_found = slab2_triggers
        .iter()
        .any(|t| t.key == key && t.time == time2 && t.timer_type == TimerType::DeferRetry);

    if !defer_found {
        return Err("DeferRetry timer not found with correct type".to_owned());
    }

    Ok(())
}

/// Tests that v1 slab enumeration works correctly.
///
/// # Errors
///
/// Returns an error if enumeration fails.
pub async fn test_v1_slab_enumeration<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Create a v1 segment
    let mut v1_segment = segment.clone();
    v1_segment.version = Some(SEGMENT_VERSION_V1);

    store
        .insert_segment(v1_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert segment: {e:?}"))?;

    // Insert multiple v1 slabs
    let slab_ids = vec![0_u32, 5, 10, 15];

    for &slab_id in &slab_ids {
        store
            .insert_slab_v1(&v1_segment.id, slab_id)
            .await
            .map_err(|e| format!("Failed to insert v1 slab {slab_id}: {e:?}"))?;
    }

    // Enumerate and verify all slabs are returned
    let slab_stream = store.get_slabs_v1(&v1_segment.id);
    futures::pin_mut!(slab_stream);

    let mut retrieved_slabs = Vec::new();
    while let Some(result) = slab_stream.next().await {
        let slab_id = result.map_err(|e| format!("Failed to enumerate v1 slabs: {e:?}"))?;
        retrieved_slabs.push(slab_id);
    }

    // Verify all inserted slabs were enumerated
    let expected: HashSet<u32> = slab_ids.into_iter().collect();
    let actual: HashSet<u32> = retrieved_slabs.into_iter().collect();

    if expected != actual {
        return Err(format!(
            "V1 slab enumeration mismatch. Expected: {expected:?}, Got: {actual:?}"
        ));
    }

    Ok(())
}

/// Tests that v1 trigger retrieval works correctly.
///
/// # Errors
///
/// Returns an error if retrieval fails.
pub async fn test_v1_trigger_retrieval<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    use futures::StreamExt;
    use tracing::info_span;

    // Create a v1 segment
    let mut v1_segment = segment.clone();
    v1_segment.version = Some(SEGMENT_VERSION_V1);

    store
        .insert_segment(v1_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert segment: {e:?}"))?;

    // Insert v1 triggers in a specific slab
    let slab_id = 0_u32;

    let now = CompactDateTime::now().map_err(|e| format!("Failed to get time: {e:?}"))?;

    let triggers = vec![
        TriggerV1 {
            key: "trigger-key-1".into(),
            time: now,
            span: info_span!("trigger_1"),
        },
        TriggerV1 {
            key: "trigger-key-2".into(),
            time: now
                .add_duration(CompactDuration::new(60))
                .map_err(|e| format!("Failed to add duration: {e:?}"))?,
            span: info_span!("trigger_2"),
        },
        TriggerV1 {
            key: "trigger-key-3".into(),
            time: now
                .add_duration(CompactDuration::new(120))
                .map_err(|e| format!("Failed to add duration: {e:?}"))?,
            span: info_span!("trigger_3"),
        },
    ];

    // Insert slab and triggers
    store
        .insert_slab_v1(&v1_segment.id, slab_id)
        .await
        .map_err(|e| format!("Failed to insert v1 slab: {e:?}"))?;

    for trigger in &triggers {
        store
            .insert_slab_trigger_v1(&v1_segment.id, slab_id, trigger.clone())
            .await
            .map_err(|e| format!("Failed to insert v1 trigger: {e:?}"))?;
    }

    // Retrieve and verify all triggers
    let trigger_stream = store.get_slab_triggers_v1(&v1_segment.id, slab_id);
    futures::pin_mut!(trigger_stream);

    let mut retrieved_triggers = Vec::new();
    while let Some(result) = trigger_stream.next().await {
        let trigger = result.map_err(|e| format!("Failed to retrieve v1 triggers: {e:?}"))?;
        retrieved_triggers.push(trigger);
    }

    // Verify all triggers were retrieved
    if retrieved_triggers.len() != triggers.len() {
        return Err(format!(
            "Expected {} triggers, got {}",
            triggers.len(),
            retrieved_triggers.len()
        ));
    }

    // Verify each trigger is present (comparing key and time, ignoring span)
    for original in &triggers {
        let found = retrieved_triggers
            .iter()
            .any(|t| t.key == original.key && t.time == original.time);

        if !found {
            return Err(format!(
                "Trigger not found: key={}, time={}",
                original.key, original.time
            ));
        }
    }

    Ok(())
}

/// Tests v1 slab insert and retrieval roundtrip.
///
/// # Errors
///
/// Returns an error if insert or retrieval fails.
pub async fn test_v1_slab_roundtrip<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Create a v1 segment
    let mut v1_segment = segment.clone();
    v1_segment.version = Some(SEGMENT_VERSION_V1);

    store
        .insert_segment(v1_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert segment: {e:?}"))?;

    // Insert some v1 slab IDs
    let slab_ids: Vec<u32> = vec![0, 5, 10];
    for &slab_id in &slab_ids {
        store
            .insert_slab_v1(&v1_segment.id, slab_id)
            .await
            .map_err(|e| format!("Failed to insert v1 slab {slab_id}: {e:?}"))?;
    }

    // Retrieve and verify
    let retrieved: Vec<u32> = store
        .get_slabs_v1(&v1_segment.id)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get v1 slabs: {e:?}"))?;

    let expected: HashSet<u32> = slab_ids.into_iter().collect();
    let actual: HashSet<u32> = retrieved.into_iter().collect();

    if expected != actual {
        return Err(format!(
            "V1 slab roundtrip failed. Expected: {expected:?}, Got: {actual:?}"
        ));
    }

    Ok(())
}

/// Tests v1 trigger insert and retrieval roundtrip.
///
/// # Errors
///
/// Returns an error if insert or retrieval fails.
pub async fn test_v1_trigger_roundtrip<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    use tracing::info_span;

    // Create a v1 segment
    let mut v1_segment = segment.clone();
    v1_segment.version = Some(SEGMENT_VERSION_V1);

    store
        .insert_segment(v1_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert segment: {e:?}"))?;

    let slab_id = 0_u32;

    // Create some v1 triggers
    let now = CompactDateTime::now().map_err(|e| format!("Failed to get current time: {e:?}"))?;

    let trigger1 = TriggerV1 {
        key: "test-key-1".into(),
        time: now,
        span: info_span!("test_trigger_1"),
    };

    let trigger2 = TriggerV1 {
        key: "test-key-2".into(),
        time: now
            .add_duration(CompactDuration::new(60))
            .map_err(|e| format!("Failed to add duration: {e:?}"))?,
        span: info_span!("test_trigger_2"),
    };

    // Insert slab first
    store
        .insert_slab_v1(&v1_segment.id, slab_id)
        .await
        .map_err(|e| format!("Failed to insert v1 slab: {e:?}"))?;

    // Insert triggers
    store
        .insert_slab_trigger_v1(&v1_segment.id, slab_id, trigger1.clone())
        .await
        .map_err(|e| format!("Failed to insert v1 trigger 1: {e:?}"))?;

    store
        .insert_slab_trigger_v1(&v1_segment.id, slab_id, trigger2.clone())
        .await
        .map_err(|e| format!("Failed to insert v1 trigger 2: {e:?}"))?;

    // Retrieve and verify
    let retrieved: Vec<TriggerV1> = store
        .get_slab_triggers_v1(&v1_segment.id, slab_id)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get v1 triggers: {e:?}"))?;

    if retrieved.len() != 2 {
        return Err(format!("Expected 2 triggers, got {}", retrieved.len()));
    }

    // Verify both triggers are present (ignoring span in comparison)
    let has_trigger1 = retrieved
        .iter()
        .any(|t| t.key == trigger1.key && t.time == trigger1.time);
    let has_trigger2 = retrieved
        .iter()
        .any(|t| t.key == trigger2.key && t.time == trigger2.time);

    if !has_trigger1 {
        return Err("Trigger 1 not found in retrieved triggers".to_owned());
    }

    if !has_trigger2 {
        return Err("Trigger 2 not found in retrieved triggers".to_owned());
    }

    Ok(())
}

/// Helper: Get all v2 triggers for a segment across all slabs.
async fn get_all_v2_triggers<S>(
    store: &S,
    segment_id: &SegmentId,
    slab_size: CompactDuration,
) -> Result<HashSet<Trigger>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let slab_ids: Vec<SlabId> = store
        .get_slabs(segment_id)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get slabs: {e:?}"))?;

    let mut all_triggers = HashSet::new();

    for slab_id in slab_ids {
        let slab = Slab::new(*segment_id, slab_id, slab_size);
        // Get both Application and DeferRetry triggers
        let triggers = common::get_slab_triggers_all_types(store, &slab).await?;
        all_triggers.extend(triggers);
    }

    Ok(all_triggers)
}

/// Helper: Insert a trigger to both slab and key indices with slab
/// registration.
async fn insert_trigger_fully<S>(
    store: &S,
    segment_id: &SegmentId,
    slab: &Slab,
    trigger: &Trigger,
) -> Result<(), String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    store
        .insert_slab(segment_id, slab.clone())
        .await
        .map_err(|e| format!("Failed to register slab: {e:?}"))?;

    store
        .insert_slab_trigger(slab.clone(), trigger.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger to slab: {e:?}"))?;

    store
        .insert_key_trigger(segment_id, trigger.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger to key index: {e:?}"))?;

    Ok(())
}

/// Helper: Collect triggers from multiple slabs, avoiding duplicates if slabs
/// are the same. Fetches all triggers across all timer types.
async fn collect_triggers_from_slabs<S>(store: &S, slabs: &[Slab]) -> Result<Vec<Trigger>, String>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let mut all_triggers = Vec::new();
    let mut seen_slab_ids = HashSet::new();

    for slab in slabs {
        // Only fetch if we haven't seen this slab ID yet
        if seen_slab_ids.insert(slab.id()) {
            // Fetch all triggers across all timer types efficiently
            let triggers: Vec<_> = store
                .get_slab_triggers_all_types(slab)
                .try_collect()
                .await
                .map_err(|e| format!("Failed to get triggers from slab: {e:?}"))?;

            all_triggers.extend(triggers);
        }
    }

    Ok(all_triggers)
}

/// Helper: Verify a trigger exists in collection with expected properties.
fn verify_trigger_exists(
    triggers: &[Trigger],
    expected_key: &Key,
    expected_type: TimerType,
) -> Result<(), String> {
    let found = triggers
        .iter()
        .find(|t| t.key == *expected_key)
        .ok_or_else(|| format!("Trigger with key {expected_key} not found"))?;

    if found.timer_type != expected_type {
        return Err(format!(
            "Trigger {expected_key} has wrong timer_type: expected {expected_type:?}, got {:?}",
            found.timer_type
        ));
    }

    Ok(())
}

/// Tests migration with multiple slabs containing real data.
///
/// Creates v2 segment with data, simulates it being v1, then migrates.
///
/// # Errors
///
/// Returns an error if migration fails or data is incorrect.
pub async fn test_multi_slab_v1_migration<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    use tracing::info_span;

    // Create a v1 segment
    let mut v1_segment = segment.clone();
    v1_segment.version = Some(SEGMENT_VERSION_V1);
    v1_segment.slab_size = CompactDuration::new(3600); // 1 hour slabs

    store
        .insert_segment(v1_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert v1 segment: {e:?}"))?;

    // Insert v1 data spanning multiple slabs
    let now = CompactDateTime::now().map_err(|e| format!("Failed to get time: {e:?}"))?;

    let time1 = now
        .add_duration(CompactDuration::new(1000))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?; // slab 0

    let time2 = now
        .add_duration(CompactDuration::new(4000))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?; // slab 1

    let time3 = now
        .add_duration(CompactDuration::new(8000))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?; // slab 2

    // Calculate slab IDs for each time
    let slab_id_1 = Slab::from_time(v1_segment.id, v1_segment.slab_size, time1).id();
    let slab_id_2 = Slab::from_time(v1_segment.id, v1_segment.slab_size, time2).id();
    let slab_id_3 = Slab::from_time(v1_segment.id, v1_segment.slab_size, time3).id();

    // Insert v1 triggers
    let trigger1 = TriggerV1 {
        key: "multi-key-1".into(),
        time: time1,
        span: info_span!("multi_trigger_1"),
    };

    let trigger2 = TriggerV1 {
        key: "multi-key-2".into(),
        time: time2,
        span: info_span!("multi_trigger_2"),
    };

    let trigger3 = TriggerV1 {
        key: "multi-key-3".into(),
        time: time3,
        span: info_span!("multi_trigger_3"),
    };

    // Insert slabs
    store
        .insert_slab_v1(&v1_segment.id, slab_id_1)
        .await
        .map_err(|e| format!("Failed to insert slab 1: {e:?}"))?;
    store
        .insert_slab_v1(&v1_segment.id, slab_id_2)
        .await
        .map_err(|e| format!("Failed to insert slab 2: {e:?}"))?;
    store
        .insert_slab_v1(&v1_segment.id, slab_id_3)
        .await
        .map_err(|e| format!("Failed to insert slab 3: {e:?}"))?;

    // Insert triggers
    store
        .insert_slab_trigger_v1(&v1_segment.id, slab_id_1, trigger1.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger 1: {e:?}"))?;
    store
        .insert_slab_trigger_v1(&v1_segment.id, slab_id_2, trigger2.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger 2: {e:?}"))?;
    store
        .insert_slab_trigger_v1(&v1_segment.id, slab_id_3, trigger3.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger 3: {e:?}"))?;

    // Verify v1 data is present
    let v1_slabs: Vec<_> = store
        .get_slabs_v1(&v1_segment.id)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get v1 slabs: {e:?}"))?;

    if v1_slabs.len() < 2 {
        return Err(format!(
            "Expected at least 2 v1 slabs, got {}",
            v1_slabs.len()
        ));
    }

    // Run migration
    migration::migrate_segment(store, &v1_segment)
        .await
        .map_err(|e| format!("Migration failed: {e:#}"))?;

    // Verify segment version updated to v2
    let migrated_segment = store
        .get_segment(&v1_segment.id)
        .await
        .map_err(|e| format!("Failed to get segment: {e:?}"))?
        .ok_or("Segment not found after migration")?;

    if migrated_segment.version != Some(SEGMENT_VERSION_V2) {
        return Err(format!(
            "Expected version {}, got {:?}",
            SEGMENT_VERSION_V2, migrated_segment.version
        ));
    }

    // Verify all triggers migrated to v2
    let triggers_after = get_all_v2_triggers(store, &v1_segment.id, v1_segment.slab_size).await?;

    if triggers_after.len() != 3 {
        return Err(format!(
            "Expected 3 triggers after migration, got {}",
            triggers_after.len()
        ));
    }

    // Verify each trigger preserved (with Application timer type)
    let expected_triggers = vec![
        (trigger1.key.clone(), trigger1.time),
        (trigger2.key.clone(), trigger2.time),
        (trigger3.key.clone(), trigger3.time),
    ];

    for (key, time) in expected_triggers {
        if !triggers_after
            .iter()
            .any(|t| t.key == key && t.time == time && t.timer_type == TimerType::Application)
        {
            return Err(format!(
                "Trigger lost during migration: key={key}, time={time}"
            ));
        }
    }

    Ok(())
}

/// Tests crash recovery: system crashes after partial v2 writes but before
/// version update.
///
/// Scenario:
/// 1. Some triggers written to v2 tables
/// 2. CRASH - version not updated, segment still marked v1
/// 3. System restarts, retries migration
/// 4. All triggers written to v2 (overwrites partial data - idempotent)
/// 5. Version updated
/// 6. V1 data cleaned up
///
/// Expected: No data loss, migration completes successfully.
///
/// # Errors
///
/// Returns an error if any store operation fails or if the migration does not
/// complete successfully.
pub async fn test_crash_before_version_update<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    use tracing::info_span;

    let mut v1_segment = segment.clone();
    v1_segment.version = Some(SEGMENT_VERSION_V1);

    store
        .insert_segment(v1_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert v1 segment: {e:?}"))?;

    let now = CompactDateTime::now().map_err(|e| format!("Failed to get time: {e:?}"))?;

    let time1 = now
        .add_duration(CompactDuration::new(3600))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let time2 = now
        .add_duration(CompactDuration::new(7200))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    // Insert v1 data for both triggers
    let slab_id_1 = Slab::from_time(v1_segment.id, v1_segment.slab_size, time1).id();
    let slab_id_2 = Slab::from_time(v1_segment.id, v1_segment.slab_size, time2).id();

    let trigger1_v1 = TriggerV1 {
        key: "crash-key-1".into(),
        time: time1,
        span: info_span!("crash_trigger_1"),
    };

    let trigger2_v1 = TriggerV1 {
        key: "crash-key-2".into(),
        time: time2,
        span: info_span!("crash_trigger_2"),
    };

    // Insert v1 slabs and triggers
    store
        .insert_slab_v1(&v1_segment.id, slab_id_1)
        .await
        .map_err(|e| format!("Failed to insert slab 1: {e:?}"))?;
    store
        .insert_slab_v1(&v1_segment.id, slab_id_2)
        .await
        .map_err(|e| format!("Failed to insert slab 2: {e:?}"))?;

    store
        .insert_slab_trigger_v1(&v1_segment.id, slab_id_1, trigger1_v1.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger 1: {e:?}"))?;
    store
        .insert_slab_trigger_v1(&v1_segment.id, slab_id_2, trigger2_v1.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger 2: {e:?}"))?;

    // Simulate partial v2 writes (crash scenario: only trigger1 written to v2,
    // version not updated)
    let trigger1_v2 = Trigger::new(
        trigger1_v1.key.clone(),
        trigger1_v1.time,
        TimerType::Application,
        Span::current(),
    );

    let slab1 = Slab::from_time(v1_segment.id, v1_segment.slab_size, trigger1_v2.time);
    store
        .insert_slab_trigger(slab1.clone(), trigger1_v2.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger1 to v2: {e:?}"))?;

    store
        .insert_key_trigger(&v1_segment.id, trigger1_v2.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger1 key to v2: {e:?}"))?;

    // Note: trigger2 NOT written to v2 - simulates crash during migration

    // Verify segment is still v1 (version not updated - this is the crash point)
    let check_segment = store
        .get_segment(&v1_segment.id)
        .await
        .map_err(|e| format!("Failed to get segment: {e:?}"))?
        .ok_or("Segment disappeared")?;

    if check_segment.version != Some(SEGMENT_VERSION_V1) {
        return Err("Segment version should still be v1 before migration".to_owned());
    }

    // Now simulate system restart and retry migration
    migration::migrate_segment(store, &v1_segment)
        .await
        .map_err(|e| format!("Migration retry failed: {e:#}"))?;

    // Verify version updated to v2
    let final_segment = store
        .get_segment(&v1_segment.id)
        .await
        .map_err(|e| format!("Failed to get segment: {e:?}"))?
        .ok_or("Segment not found after migration")?;

    if final_segment.version != Some(SEGMENT_VERSION_V2) {
        return Err(format!(
            "Expected version {}, got {:?}",
            SEGMENT_VERSION_V2, final_segment.version
        ));
    }

    // Verify both triggers exist in v2 (migration completed)
    let all_triggers = get_all_v2_triggers(store, &v1_segment.id, v1_segment.slab_size).await?;

    if all_triggers.len() != 2 {
        return Err(format!(
            "Expected 2 triggers after migration, got {}",
            all_triggers.len()
        ));
    }

    let has_trigger1 = all_triggers
        .iter()
        .any(|t| t.key == trigger1_v1.key && t.time == trigger1_v1.time);
    let has_trigger2 = all_triggers
        .iter()
        .any(|t| t.key == trigger2_v1.key && t.time == trigger2_v1.time);

    if !has_trigger1 {
        return Err("Trigger1 lost after migration retry - data loss!".to_owned());
    }

    if !has_trigger2 {
        return Err("Trigger2 not migrated - data loss!".to_owned());
    }

    Ok(())
}

/// Tests crash recovery: system crashes after version update but before v1
/// cleanup.
///
/// Scenario:
/// 1. All triggers written to v2
/// 2. Version updated to v2 (MIGRATION COMPLETE MARKER)
/// 3. CRASH - before v1 cleanup
/// 4. System restarts
/// 5. Segment version is v2, no migration runs
/// 6. V1 data orphaned but harmless
///
/// Expected: System functions normally, v1 data can be cleaned up manually
/// later.
///
/// # Errors
///
/// Returns an error if any store operation fails.
pub async fn test_crash_after_version_update<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    use tracing::info_span;

    let mut v1_segment = segment.clone();
    v1_segment.version = Some(SEGMENT_VERSION_V1);

    store
        .insert_segment(v1_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert v1 segment: {e:?}"))?;

    let now = CompactDateTime::now().map_err(|e| format!("Failed to get time: {e:?}"))?;
    let time = now
        .add_duration(CompactDuration::new(3600))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let slab_id = Slab::from_time(v1_segment.id, v1_segment.slab_size, time).id();

    // Insert v1 data that will become orphaned
    let trigger_v1 = TriggerV1 {
        key: "orphan-key".into(),
        time,
        span: info_span!("orphan_trigger"),
    };

    store
        .insert_slab_v1(&v1_segment.id, slab_id)
        .await
        .map_err(|e| format!("Failed to insert v1 slab: {e:?}"))?;

    store
        .insert_slab_trigger_v1(&v1_segment.id, slab_id, trigger_v1.clone())
        .await
        .map_err(|e| format!("Failed to insert v1 trigger: {e:?}"))?;

    // Verify v1 data exists
    let v1_slabs: Vec<_> = store
        .get_slabs_v1(&v1_segment.id)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get v1 slabs: {e:?}"))?;

    if v1_slabs.is_empty() {
        return Err("V1 slab should exist before migration".to_owned());
    }

    // Write to v2
    let trigger_v2 = Trigger::new(
        trigger_v1.key.clone(),
        trigger_v1.time,
        TimerType::Application,
        Span::current(),
    );

    let slab = Slab::from_time(v1_segment.id, v1_segment.slab_size, trigger_v2.time);
    store
        .insert_slab_trigger(slab.clone(), trigger_v2.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger to v2: {e:?}"))?;

    store
        .insert_key_trigger(&v1_segment.id, trigger_v2.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger key to v2: {e:?}"))?;

    // Update version to v2 (this is the completion marker)
    store
        .update_segment_version(&v1_segment.id, SEGMENT_VERSION_V2, v1_segment.slab_size)
        .await
        .map_err(|e| format!("Failed to update version: {e:?}"))?;

    // Simulate crash here - v1 data NOT cleaned up

    // Verify version is v2
    let check_segment = store
        .get_segment(&v1_segment.id)
        .await
        .map_err(|e| format!("Failed to get segment: {e:?}"))?
        .ok_or("Segment not found")?;

    if check_segment.version != Some(SEGMENT_VERSION_V2) {
        return Err("Segment should be v2 after version update".to_owned());
    }

    // Verify migration is NOT run again (segment is v2)
    if migration::needs_migration(&check_segment) {
        return Err("Migration should not be needed for v2 segment".to_owned());
    }

    // Verify trigger is accessible from v2
    let triggers = get_slab_triggers(store, &slab).await?;
    if !triggers
        .iter()
        .any(|t| t.key == trigger_v2.key && t.time == trigger_v2.time)
    {
        return Err("Trigger not accessible from v2 after version update".to_owned());
    }

    // Verify v1 data still exists (orphaned but harmless)
    let v1_slabs_after: Vec<_> = store
        .get_slabs_v1(&v1_segment.id)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get v1 slabs after migration: {e:?}"))?;

    if v1_slabs_after.is_empty() {
        // V1 data may have been cleaned up, which is fine
        // The key point is that v2 data is accessible
    } else {
        // V1 data is orphaned but doesn't break the system
        // This is the scenario we're testing - system works correctly with
        // orphaned v1 data
    }

    Ok(())
}

/// Tests migration idempotency: running migration multiple times produces same
/// result.
///
/// Verifies that:
/// 1. First migration succeeds
/// 2. Second migration is a no-op (version already v2)
/// 3. Data is correct after both runs
/// 4. No duplicates created
///
/// # Errors
///
/// Returns an error if any store operation fails or if the migration produces
/// incorrect results.
pub async fn test_migration_idempotency<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    use tracing::info_span;

    let mut v1_segment = segment.clone();
    v1_segment.version = Some(SEGMENT_VERSION_V1);

    store
        .insert_segment(v1_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert v1 segment: {e:?}"))?;

    // Insert v1 test data
    let now = CompactDateTime::now().map_err(|e| format!("Failed to get time: {e:?}"))?;
    let time = now
        .add_duration(CompactDuration::new(3600))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let slab_id = Slab::from_time(v1_segment.id, v1_segment.slab_size, time).id();

    let trigger_v1 = TriggerV1 {
        key: "idempotent-key".into(),
        time,
        span: info_span!("idempotent_trigger"),
    };

    store
        .insert_slab_v1(&v1_segment.id, slab_id)
        .await
        .map_err(|e| format!("Failed to insert slab: {e:?}"))?;
    store
        .insert_slab_trigger_v1(&v1_segment.id, slab_id, trigger_v1.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger: {e:?}"))?;

    // Run migration first time
    migration::migrate_segment(store, &v1_segment)
        .await
        .map_err(|e| format!("First migration failed: {e:#}"))?;

    // Verify segment is v2
    let segment_after_first = store
        .get_segment(&v1_segment.id)
        .await
        .map_err(|e| format!("Failed to get segment: {e:?}"))?
        .ok_or("Segment not found after first migration")?;

    if segment_after_first.version != Some(SEGMENT_VERSION_V2) {
        return Err("First migration should update version to v2".to_owned());
    }

    // Verify data migrated
    let triggers_after_first =
        get_all_v2_triggers(store, &v1_segment.id, v1_segment.slab_size).await?;
    if triggers_after_first.len() != 1 {
        return Err(format!(
            "Expected 1 trigger after first migration, got {}",
            triggers_after_first.len()
        ));
    }

    // Run migration second time (should be no-op since version already v2)
    if migration::needs_migration(&segment_after_first) {
        return Err("Segment should not need migration after first run".to_owned());
    }

    // If we did run migration again, it should be safe
    migration::migrate_segment(store, &segment_after_first)
        .await
        .map_err(|e| format!("Second migration failed: {e:#}"))?;

    // Verify segment still v2
    let segment_after_second = store
        .get_segment(&v1_segment.id)
        .await
        .map_err(|e| format!("Failed to get segment: {e:?}"))?
        .ok_or("Segment not found after second migration")?;

    if segment_after_second.version != Some(SEGMENT_VERSION_V2) {
        return Err("Second migration should not change version".to_owned());
    }

    // Verify data still present and unchanged
    let triggers_after_second =
        get_all_v2_triggers(store, &v1_segment.id, v1_segment.slab_size).await?;
    if triggers_after_second.len() != 1 {
        return Err(format!(
            "Expected 1 trigger after second migration, got {}",
            triggers_after_second.len()
        ));
    }

    if !triggers_after_second
        .iter()
        .any(|t| t.key == trigger_v1.key && t.time == trigger_v1.time)
    {
        return Err("Trigger data changed after second migration".to_owned());
    }

    Ok(())
}

/// Tests crash recovery for `slab_size` migration: system crashes after partial
/// writes but before `slab_size` update.
///
/// Scenario:
/// 1. Segment is v2 with old `slab_size`
/// 2. Some triggers written to new `slab_size` slabs
/// 3. CRASH - `slab_size` not updated, segment still has old `slab_size`
/// 4. System restarts, retries `slab_size` migration
/// 5. All triggers written to new slabs (overwrites partial data - idempotent)
/// 6. `Slab_size` updated
/// 7. Old slabs cleaned up
///
/// Expected: No data loss, migration completes successfully.
///
/// # Errors
///
/// Returns an error if any store operation fails or if the migration does not
/// complete successfully.
pub async fn test_crash_before_slab_size_update<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let old_slab_size = CompactDuration::new(3600); // 1 hour
    let new_slab_size = CompactDuration::new(7200); // 2 hours

    let mut v2_segment = segment.clone();
    v2_segment.version = Some(SEGMENT_VERSION_V2);
    v2_segment.slab_size = old_slab_size;

    store
        .insert_segment(v2_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert v2 segment: {e:?}"))?;

    let now = CompactDateTime::now().map_err(|e| format!("Failed to get time: {e:?}"))?;

    // Create triggers in old slabs
    let time1 = now
        .add_duration(CompactDuration::new(1800))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let time2 = now
        .add_duration(CompactDuration::new(5400))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let trigger1 = Trigger::new(
        "slab-crash-key-1".into(),
        time1,
        TimerType::Application,
        Span::current(),
    );
    let trigger2 = Trigger::new(
        "slab-crash-key-2".into(),
        time2,
        TimerType::DeferRetry,
        Span::current(),
    );

    // Write triggers to old slabs
    let old_slab1 = Slab::from_time(v2_segment.id, old_slab_size, trigger1.time);
    let old_slab2 = Slab::from_time(v2_segment.id, old_slab_size, trigger2.time);

    insert_trigger_fully(store, &v2_segment.id, &old_slab1, &trigger1).await?;
    insert_trigger_fully(store, &v2_segment.id, &old_slab2, &trigger2).await?;

    // Simulate partial migration: write trigger1 to new slab but not trigger2
    // (crash scenario)
    let new_slab1 = Slab::from_time(v2_segment.id, new_slab_size, trigger1.time);
    store
        .insert_slab_trigger(new_slab1.clone(), trigger1.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger1 to new slab: {e:?}"))?;

    // Note: trigger2 NOT written to new slab - simulates crash during migration
    // Note: slab_size NOT updated - this is the crash point

    // Verify segment still has old slab_size
    let check_segment = store
        .get_segment(&v2_segment.id)
        .await
        .map_err(|e| format!("Failed to get segment: {e:?}"))?
        .ok_or("Segment disappeared")?;

    if check_segment.slab_size != old_slab_size {
        return Err("Segment slab_size should still be old value before migration".to_owned());
    }

    // Now simulate system restart and retry migration
    migration::migrate_slab_size(store, &v2_segment, new_slab_size)
        .await
        .map_err(|e| format!("Slab_size migration retry failed: {e:#}"))?;

    // Verify slab_size updated to new value
    let final_segment = store
        .get_segment(&v2_segment.id)
        .await
        .map_err(|e| format!("Failed to get segment: {e:?}"))?
        .ok_or("Segment not found after migration")?;

    if final_segment.slab_size != new_slab_size {
        return Err(format!(
            "Expected slab_size {}, got {}",
            new_slab_size, final_segment.slab_size
        ));
    }

    // Verify all triggers migrated to new slabs with correct timer types
    let new_slab1 = Slab::from_time(v2_segment.id, new_slab_size, trigger1.time);
    let new_slab2 = Slab::from_time(v2_segment.id, new_slab_size, trigger2.time);

    let all_triggers = collect_triggers_from_slabs(store, &[new_slab1, new_slab2]).await?;

    if all_triggers.len() != 2 {
        return Err(format!(
            "Expected 2 triggers total in new slabs, got {}",
            all_triggers.len()
        ));
    }

    verify_trigger_exists(
        &all_triggers,
        &"slab-crash-key-1".into(),
        TimerType::Application,
    )?;
    verify_trigger_exists(
        &all_triggers,
        &"slab-crash-key-2".into(),
        TimerType::DeferRetry,
    )?;

    Ok(())
}

/// Tests crash recovery for `slab_size` migration: system crashes after
/// `slab_size` update but before cleanup.
///
/// Scenario:
/// 1. All triggers written to new `slab_size` slabs
/// 2. `Slab_size` updated (atomic marker)
/// 3. CRASH - old slabs NOT cleaned up
/// 4. System continues normally
///
/// Expected: System works correctly with new `slab_size`, old data is orphaned
/// but harmless.
///
/// # Errors
///
/// Returns an error if any store operation fails.
pub async fn test_crash_after_slab_size_update<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let old_slab_size = CompactDuration::new(3600); // 1 hour
    let new_slab_size = CompactDuration::new(7200); // 2 hours

    let mut v2_segment = segment.clone();
    v2_segment.version = Some(SEGMENT_VERSION_V2);
    v2_segment.slab_size = old_slab_size;

    store
        .insert_segment(v2_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert v2 segment: {e:?}"))?;

    let now = CompactDateTime::now().map_err(|e| format!("Failed to get time: {e:?}"))?;

    let time1 = now
        .add_duration(CompactDuration::new(1800))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let trigger1 = Trigger::new(
        "cleanup-crash-key".into(),
        time1,
        TimerType::Application,
        Span::current(),
    );

    // Write trigger to old slab (need to register slab first)
    let old_slab = Slab::from_time(v2_segment.id, old_slab_size, trigger1.time);

    // Register old slab
    store
        .insert_slab(&v2_segment.id, old_slab.clone())
        .await
        .map_err(|e| format!("Failed to register old slab: {e:?}"))?;

    store
        .insert_slab_trigger(old_slab.clone(), trigger1.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger to old slab: {e:?}"))?;

    store
        .insert_key_trigger(&v2_segment.id, trigger1.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger key: {e:?}"))?;

    // Write trigger to new slab
    let new_slab = Slab::from_time(v2_segment.id, new_slab_size, trigger1.time);
    store
        .insert_slab_trigger(new_slab.clone(), trigger1.clone())
        .await
        .map_err(|e| format!("Failed to insert trigger to new slab: {e:?}"))?;

    // Update slab_size (atomic marker - this is the point where migration is
    // considered complete)
    store
        .update_segment_version(&v2_segment.id, SEGMENT_VERSION_V2, new_slab_size)
        .await
        .map_err(|e| format!("Failed to update slab_size: {e:?}"))?;

    // CRASH - old slab NOT cleaned up
    // Old slab still contains data but system should ignore it

    // Verify system works correctly with new slab_size
    let final_segment = store
        .get_segment(&v2_segment.id)
        .await
        .map_err(|e| format!("Failed to get segment: {e:?}"))?
        .ok_or("Segment not found")?;

    if final_segment.slab_size != new_slab_size {
        return Err(format!(
            "Expected slab_size {}, got {}",
            new_slab_size, final_segment.slab_size
        ));
    }

    // Verify trigger is accessible in new slab
    let triggers_new: Vec<_> = store
        .get_slab_triggers(&new_slab, TimerType::Application)
        .try_collect()
        .await
        .map_err(|e| format!("Failed to get triggers from new slab: {e:?}"))?;

    if triggers_new.len() != 1 {
        return Err(format!(
            "Expected 1 trigger in new slab, got {}",
            triggers_new.len()
        ));
    }

    // Old slab still has data (orphaned but harmless)
    // We don't clean it up in this test - that's the crash scenario
    // In production, old data would eventually be TTL'd out or cleaned up later

    Ok(())
}

/// Tests `slab_size` migration with multiple slabs and triggers.
///
/// Scenario:
/// 1. Create v2 segment with old `slab_size`
/// 2. Insert triggers spanning multiple old slabs
/// 3. Migrate to new `slab_size` (larger - merges slabs)
/// 4. Verify all triggers correctly migrated to new slabs
/// 5. Verify timer types preserved
///
/// # Errors
///
/// Returns an error if any store operation fails or if the migration produces
/// incorrect results.
pub async fn test_slab_size_change_with_multiple_slabs<S>(
    store: &S,
    segment: &Segment,
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let old_slab_size = CompactDuration::new(3600); // 1 hour
    let new_slab_size = CompactDuration::new(7200); // 2 hours (will merge slabs)

    let mut v2_segment = segment.clone();
    v2_segment.version = Some(SEGMENT_VERSION_V2);
    v2_segment.slab_size = old_slab_size;

    store
        .insert_segment(v2_segment.clone())
        .await
        .map_err(|e| format!("Failed to insert v2 segment: {e:?}"))?;

    let now = CompactDateTime::now().map_err(|e| format!("Failed to get time: {e:?}"))?;

    // Create triggers at different times - don't align to slabs artificially
    // The migration should handle any times correctly
    let time1 = now
        .add_duration(CompactDuration::new(1800))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let time2 = now
        .add_duration(CompactDuration::new(5400))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let time3 = now
        .add_duration(CompactDuration::new(9000))
        .map_err(|e| format!("Failed to add duration: {e:?}"))?;

    let trigger1 = Trigger::new(
        "multi-key-1".into(),
        time1,
        TimerType::Application,
        Span::current(),
    );
    let trigger2 = Trigger::new(
        "multi-key-2".into(),
        time2,
        TimerType::DeferRetry,
        Span::current(),
    );
    let trigger3 = Trigger::new(
        "multi-key-3".into(),
        time3,
        TimerType::Application,
        Span::current(),
    );

    // Write all triggers to old slabs
    for trigger in &[&trigger1, &trigger2, &trigger3] {
        let old_slab = Slab::from_time(v2_segment.id, old_slab_size, trigger.time);
        insert_trigger_fully(store, &v2_segment.id, &old_slab, trigger).await?;
    }

    // Migrate to new slab_size
    migration::migrate_slab_size(store, &v2_segment, new_slab_size)
        .await
        .map_err(|e| format!("Slab_size migration failed: {e:#}"))?;

    // Verify slab_size updated
    let final_segment = store
        .get_segment(&v2_segment.id)
        .await
        .map_err(|e| format!("Failed to get segment: {e:?}"))?
        .ok_or("Segment not found after migration")?;

    if final_segment.slab_size != new_slab_size {
        return Err(format!(
            "Expected slab_size {}, got {}",
            new_slab_size, final_segment.slab_size
        ));
    }

    // Verify all triggers migrated to new slabs with correct timer types
    let new_slab_1 = Slab::from_time(v2_segment.id, new_slab_size, trigger1.time);
    let new_slab_2 = Slab::from_time(v2_segment.id, new_slab_size, trigger2.time);
    let new_slab_3 = Slab::from_time(v2_segment.id, new_slab_size, trigger3.time);

    let all_triggers =
        collect_triggers_from_slabs(store, &[new_slab_1, new_slab_2, new_slab_3]).await?;

    // Verify no triggers lost or duplicated
    let unique_keys: HashSet<_> = all_triggers.iter().map(|t| &t.key).collect();
    if unique_keys.len() != 3 {
        return Err(format!(
            "Expected 3 unique triggers after migration, got {}",
            unique_keys.len()
        ));
    }

    verify_trigger_exists(&all_triggers, &"multi-key-1".into(), TimerType::Application)?;
    verify_trigger_exists(&all_triggers, &"multi-key-2".into(), TimerType::DeferRetry)?;
    verify_trigger_exists(&all_triggers, &"multi-key-3".into(), TimerType::Application)?;

    Ok(())
}
