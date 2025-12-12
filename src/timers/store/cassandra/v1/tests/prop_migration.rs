//! Property-based tests for timer store migration.
//!
//! Tests V1→V2 schema migration and V2→V2 slab size migration using
//! random scenarios to verify all migration invariants.

use crate::Key;
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::v1::V1Operations;
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentId, SegmentVersion, TriggerStore, TriggerV1};
use crate::timers::{TimerType, Trigger};
use ahash::{HashMap, HashSet};
use futures::TryStreamExt;
use quickcheck::{Arbitrary, Gen};
use tracing::Span;
use uuid::Uuid;

/// Test input containing a migration scenario.
///
/// Each test trial uses a randomly generated segment ID for isolation.
#[derive(Clone, Debug)]
pub struct MigrationTestInput {
    /// Unique segment ID for this trial (`UUIDv4`).
    pub segment_id: SegmentId,
    /// Random segment name.
    pub segment_name: String,
    /// Initial version (V1 or V2).
    pub initial_version: SegmentVersion,
    /// Initial slab size.
    pub initial_slab_size: CompactDuration,
    /// Target slab size after migration.
    pub target_slab_size: CompactDuration,
    /// Triggers to insert before migration.
    pub triggers: Vec<MigrationTriggerData>,
}

/// Trigger data for migration tests.
#[derive(Clone, Debug)]
pub struct MigrationTriggerData {
    /// The key for this trigger.
    pub key: Key,
    /// The time when this trigger should fire.
    pub time: CompactDateTime,
    /// The timer type (only used for V2 setup).
    pub timer_type: TimerType,
}

impl Arbitrary for MigrationTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate unique segment ID for this trial
        let segment_id = Uuid::new_v4();

        // Generate random segment name
        let segment_name = format!("segment-{}", u8::arbitrary(g) % 100);

        // Randomly choose initial version (50% V1, 50% V2)
        let initial_version = if bool::arbitrary(g) {
            SegmentVersion::V1
        } else {
            SegmentVersion::V2
        };

        // Generate slab sizes (1 second to 7 days to avoid TTL overflow)
        // Clamp to 604_800 seconds (7 days)
        let initial_slab_size = CompactDuration::new(u32::arbitrary(g).clamp(1, 604_800));
        let target_slab_size = CompactDuration::new(u32::arbitrary(g).clamp(1, 604_800));

        // Generate 0-50 triggers (empty segments are valid test cases)
        let trigger_count = usize::arbitrary(g) % 51;
        let mut triggers = Vec::with_capacity(trigger_count);

        // Use small key pool for better collision coverage
        let key_pool: Vec<Key> = (0_i32..5_i32).map(|i| format!("key-{i}").into()).collect();

        for _ in 0..trigger_count {
            let key_idx = usize::arbitrary(g) % key_pool.len();
            let key = key_pool[key_idx].clone();

            // Generate random time using arbitrary
            let time = CompactDateTime::arbitrary(g);

            // Generate random timer type
            let timer_type = if bool::arbitrary(g) {
                TimerType::Application
            } else {
                TimerType::DeferRetry
            };

            triggers.push(MigrationTriggerData {
                key,
                time,
                timer_type,
            });
        }

        Self {
            segment_id,
            segment_name,
            initial_version,
            initial_slab_size,
            target_slab_size,
            triggers,
        }
    }
}

/// Reference model for expected post-migration state.
#[derive(Clone, Debug)]
pub struct MigrationModel {
    /// Expected segment metadata after migration.
    pub segment: Segment,
    /// Expected triggers after migration: (key, time, `timer_type`).
    /// V1→V2: All triggers become Application type.
    /// V2→V2: Timer types preserved.
    pub triggers: HashSet<(Key, CompactDateTime, TimerType)>,
}

impl MigrationModel {
    /// Builds the reference model from test input.
    ///
    /// Applies migration transformation rules:
    /// - V1→V2: All triggers become Application type
    /// - V2→V2: Timer types preserved
    #[must_use]
    pub fn from_input(input: &MigrationTestInput) -> Self {
        let segment = Segment {
            id: input.segment_id,
            name: input.segment_name.clone(),
            version: SegmentVersion::V2,
            slab_size: input.target_slab_size,
        };

        let mut triggers = HashSet::default();

        for trigger_data in &input.triggers {
            // Apply timer type transformation based on initial version
            let timer_type = match input.initial_version {
                SegmentVersion::V1 => TimerType::Application, // V1 has no timer_type
                SegmentVersion::V2 => trigger_data.timer_type, // V2 preserves timer_type
            };

            triggers.insert((trigger_data.key.clone(), trigger_data.time, timer_type));
        }

        Self { segment, triggers }
    }

    /// Gets all unique slab IDs that triggers should be in.
    #[must_use]
    pub fn expected_slab_ids(&self) -> Vec<SlabId> {
        let mut slab_ids: Vec<SlabId> = self
            .triggers
            .iter()
            .map(|(_, time, _)| {
                Slab::from_time(self.segment.id, self.segment.slab_size, *time).id()
            })
            .collect();
        slab_ids.sort_unstable();
        slab_ids.dedup();
        slab_ids
    }
}

/// Sets up V1 initial state using `V1Operations`.
///
/// V1 setup requires:
/// 1. Initialize segment with metadata (name, `slab_size`) using
///    `insert_segment_v1()`
/// 2. Register slabs with `insert_slab()`
/// 3. Add triggers with `add_trigger()` (no `timer_type`)
///
/// # Errors
///
/// Returns error if V1 operations fail.
async fn setup_v1_state(
    operations: &V1Operations,
    input: &MigrationTestInput,
) -> color_eyre::Result<()> {
    // Step 1: Initialize segment with V1 metadata (version=NULL)
    operations
        .insert_segment_v1(
            &input.segment_id,
            &input.segment_name,
            input.initial_slab_size,
        )
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to insert V1 segment: {e:?}"))?;

    // Step 2: Calculate unique slab IDs from triggers
    let mut slab_ids = HashSet::default();
    for trigger_data in &input.triggers {
        let slab_id =
            Slab::from_time(input.segment_id, input.initial_slab_size, trigger_data.time).id();
        slab_ids.insert(slab_id);
    }

    // Step 3: Register all slabs (V1 requirement)
    for slab_id in &slab_ids {
        operations
            .insert_slab(&input.segment_id, *slab_id)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to insert V1 slab: {e:?}"))?;
    }

    // Step 4: Add triggers (V1 has no timer_type field)
    for trigger_data in &input.triggers {
        let v1_trigger = TriggerV1 {
            key: trigger_data.key.clone(),
            time: trigger_data.time,
            span: Span::current(),
        };

        operations
            .add_trigger(
                &input.segment_id,
                Slab::from_time(input.segment_id, input.initial_slab_size, trigger_data.time).id(),
                v1_trigger,
            )
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to add V1 trigger: {e:?}"))?;
    }

    Ok(())
}

/// Sets up V2 initial state using `TableAdapter`.
///
/// V2 setup:
/// 1. Create segment with full metadata
/// 2. Use `add_trigger()` for coordinated writes
///
/// # Errors
///
/// Returns error if V2 operations fail.
async fn setup_v2_state(
    store: &TableAdapter<CassandraTriggerStore>,
    input: &MigrationTestInput,
) -> color_eyre::Result<()> {
    // Create segment with full metadata
    let segment = Segment {
        id: input.segment_id,
        name: input.segment_name.clone(),
        version: SegmentVersion::V2,
        slab_size: input.initial_slab_size,
    };

    store
        .insert_segment(segment.clone())
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to insert V2 segment: {e:?}"))?;

    // Add triggers with timer_type
    for trigger_data in &input.triggers {
        let trigger = Trigger::new(
            trigger_data.key.clone(),
            trigger_data.time,
            trigger_data.timer_type,
            Span::current(),
        );

        let slab = Slab::from_time(input.segment_id, input.initial_slab_size, trigger_data.time);

        store
            .add_trigger(&segment, slab, trigger)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to add V2 trigger: {e:?}"))?;
    }

    Ok(())
}

/// Verifies invariant 1: Segment metadata is correct after migration.
///
/// Expected: version=V2, `slab_size=target_slab_size`, name preserved (for V2
/// only).
///
/// # Errors
///
/// Returns error if segment metadata doesn't match expectations.
async fn verify_segment_metadata(
    store: &TableAdapter<CassandraTriggerStore>,
    model: &MigrationModel,
    initial_version: SegmentVersion,
) -> color_eyre::Result<()> {
    let segment = store
        .get_segment(&model.segment.id)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to get segment: {e:?}"))?
        .ok_or_else(|| color_eyre::eyre::eyre!("Segment {} not found", model.segment.id))?;

    if segment.version != SegmentVersion::V2 {
        return Err(color_eyre::eyre::eyre!(
            "Segment version mismatch: expected V2, got {:?}",
            segment.version
        ));
    }

    if segment.slab_size != model.segment.slab_size {
        return Err(color_eyre::eyre::eyre!(
            "Segment slab_size mismatch: expected {:?}, got {:?}",
            model.segment.slab_size,
            segment.slab_size
        ));
    }

    // Only check name preservation for V2→V2 migrations
    // V1→V2 migrations don't preserve name (V1 segments have NULL name)
    if initial_version == SegmentVersion::V2 && segment.name != model.segment.name {
        return Err(color_eyre::eyre::eyre!(
            "Segment name mismatch for V2→V2 migration: expected {:?}, got {:?}",
            model.segment.name,
            segment.name
        ));
    }

    Ok(())
}

/// Verifies invariant 2: All triggers are preserved with correct `timer_type`.
///
/// Builds actual trigger set from store and compares to model.
///
/// # Errors
///
/// Returns error if triggers don't match model.
async fn verify_data_preservation(
    store: &TableAdapter<CassandraTriggerStore>,
    model: &MigrationModel,
) -> color_eyre::Result<()> {
    let mut actual_triggers = HashSet::default();

    // Collect all triggers from key index (authoritative)
    let mut seen_keys = HashSet::default();

    for (key, ..) in &model.triggers {
        if seen_keys.contains(key) {
            continue;
        }
        seen_keys.insert(key.clone());

        // Get triggers for both timer types
        for timer_type in [TimerType::Application, TimerType::DeferRetry] {
            let triggers: Vec<Trigger> = store
                .get_key_triggers(&model.segment.id, timer_type, key)
                .try_collect()
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!(
                        "Failed to get key triggers for {} {:?}: {e:?}",
                        key,
                        timer_type
                    )
                })?;

            for trigger in triggers {
                actual_triggers.insert((trigger.key.clone(), trigger.time, trigger.timer_type));
            }
        }
    }

    if actual_triggers != model.triggers {
        let missing: Vec<_> = model.triggers.difference(&actual_triggers).collect();
        let extra: Vec<_> = actual_triggers.difference(&model.triggers).collect();

        return Err(color_eyre::eyre::eyre!(
            "Trigger mismatch:\n  Missing: {missing:?}\n  Extra: {extra:?}"
        ));
    }

    Ok(())
}

/// Verifies invariant 3: Triggers are in correct slabs based on target slab
/// size.
///
/// # Errors
///
/// Returns error if triggers are in wrong slabs.
async fn verify_correct_indexing(
    store: &TableAdapter<CassandraTriggerStore>,
    model: &MigrationModel,
) -> color_eyre::Result<()> {
    // Build map of slab_id -> expected triggers
    let mut expected_by_slab: HashMap<SlabId, HashSet<(Key, CompactDateTime, TimerType)>> =
        HashMap::default();

    for (key, time, timer_type) in &model.triggers {
        let slab = Slab::from_time(model.segment.id, model.segment.slab_size, *time);
        expected_by_slab
            .entry(slab.id())
            .or_default()
            .insert((key.clone(), *time, *timer_type));
    }

    // Verify each slab has correct triggers
    for (slab_id, expected) in &expected_by_slab {
        let slab = Slab::new(model.segment.id, *slab_id, model.segment.slab_size);

        let actual_triggers: Vec<Trigger> = store
            .get_slab_triggers_all_types(&slab)
            .try_collect()
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!("Failed to get slab triggers for {slab_id}: {e:?}")
            })?;

        let actual: HashSet<(Key, CompactDateTime, TimerType)> = actual_triggers
            .into_iter()
            .map(|t| (t.key, t.time, t.timer_type))
            .collect();

        if &actual != expected {
            let missing: Vec<_> = expected.difference(&actual).collect();
            let extra: Vec<_> = actual.difference(expected).collect();

            return Err(color_eyre::eyre::eyre!(
                "Slab {slab_id} indexing mismatch:\n  Missing: {missing:?}\n  Extra: {extra:?}"
            ));
        }
    }

    Ok(())
}

/// Verifies invariant 4: Slab index matches key index exactly.
///
/// Both indices must contain the same triggers (dual-index consistency).
///
/// # Errors
///
/// Returns error if indices don't match.
async fn verify_dual_index_consistency(
    store: &TableAdapter<CassandraTriggerStore>,
    model: &MigrationModel,
) -> color_eyre::Result<()> {
    // Collect from slab index
    let mut slab_triggers = HashSet::default();
    let slab_ids = model.expected_slab_ids();

    for slab_id in slab_ids {
        let slab = Slab::new(model.segment.id, slab_id, model.segment.slab_size);
        let triggers: Vec<Trigger> = store
            .get_slab_triggers_all_types(&slab)
            .try_collect()
            .await
            .map_err(|e| {
                color_eyre::eyre::eyre!("Failed to get slab triggers for dual-index check: {e:?}")
            })?;

        for trigger in triggers {
            slab_triggers.insert((trigger.key.clone(), trigger.time, trigger.timer_type));
        }
    }

    // Collect from key index
    let mut key_triggers = HashSet::default();
    let mut seen_keys = HashSet::default();

    for (key, ..) in &model.triggers {
        if seen_keys.contains(key) {
            continue;
        }
        seen_keys.insert(key.clone());

        for timer_type in [TimerType::Application, TimerType::DeferRetry] {
            let triggers: Vec<Trigger> = store
                .get_key_triggers(&model.segment.id, timer_type, key)
                .try_collect()
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!(
                        "Failed to get key triggers for dual-index check: {e:?}"
                    )
                })?;

            for trigger in triggers {
                key_triggers.insert((trigger.key.clone(), trigger.time, trigger.timer_type));
            }
        }
    }

    if slab_triggers != key_triggers {
        let missing_in_key: Vec<_> = slab_triggers.difference(&key_triggers).collect();
        let missing_in_slab: Vec<_> = key_triggers.difference(&slab_triggers).collect();

        return Err(color_eyre::eyre::eyre!(
            "Dual-index consistency violation:\n  In slab but not key: {missing_in_key:?}\n  In \
             key but not slab: {missing_in_slab:?}"
        ));
    }

    Ok(())
}

/// Verifies that ONLY expected slabs exist (no extra slabs).
///
/// # Errors
///
/// Returns error if extra slabs are found.
async fn verify_no_extra_slabs(
    operations: &CassandraTriggerStore,
    model: &MigrationModel,
) -> color_eyre::Result<()> {
    // Get ALL slabs for this segment
    let all_slabs: Vec<SlabId> = operations
        .get_slabs(&model.segment.id)
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to get all slabs: {e:?}"))?;

    let expected_slabs: HashSet<SlabId> = model.expected_slab_ids().into_iter().collect();

    let actual_slabs: HashSet<SlabId> = all_slabs.into_iter().collect();

    // Check for extra slabs (in actual but not expected)
    let extra_slabs: Vec<_> = actual_slabs.difference(&expected_slabs).collect();
    if !extra_slabs.is_empty() {
        return Err(color_eyre::eyre::eyre!(
            "Extra slabs found: {extra_slabs:?} (expected only: {expected_slabs:?})"
        ));
    }

    // Check for missing slabs (in expected but not actual)
    let missing_slabs: Vec<_> = expected_slabs.difference(&actual_slabs).collect();
    if !missing_slabs.is_empty() {
        return Err(color_eyre::eyre::eyre!("Missing slabs: {missing_slabs:?}"));
    }

    Ok(())
}

/// Verifies invariant 5: Old V1 data and obsolete slabs are cleaned up.
///
/// # Errors
///
/// Returns error if cleanup didn't happen.
async fn verify_cleanup(
    v1_operations: &V1Operations,
    operations: &CassandraTriggerStore,
    input: &MigrationTestInput,
    model: &MigrationModel,
) -> color_eyre::Result<()> {
    // If initial version was V1, verify ALL V1 data for this segment is gone
    if input.initial_version == SegmentVersion::V1 {
        // Get ALL V1 slabs for this segment (not just ones we created)
        let all_v1_slabs: Vec<SlabId> = v1_operations
            .get_slabs(&input.segment_id)
            .try_collect()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to get V1 slabs: {e:?}"))?;

        // Check each V1 slab has no triggers remaining
        for slab_id in &all_v1_slabs {
            let v1_triggers: Vec<_> = v1_operations
                .get_slab_triggers(&input.segment_id, *slab_id)
                .try_collect()
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Failed to check V1 trigger cleanup: {e:?}")
                })?;

            if !v1_triggers.is_empty() {
                return Err(color_eyre::eyre::eyre!(
                    "V1 cleanup failed: {} triggers remain in slab {}",
                    v1_triggers.len(),
                    slab_id
                ));
            }
        }
    }

    // If slab size changed, verify old slabs are removed
    if input.initial_slab_size != input.target_slab_size {
        // Calculate old slab IDs
        let mut old_slab_ids = HashSet::default();
        for trigger_data in &input.triggers {
            let old_slab_id =
                Slab::from_time(input.segment_id, input.initial_slab_size, trigger_data.time).id();
            old_slab_ids.insert(old_slab_id);
        }

        // Get expected new slab IDs
        let new_slab_ids: HashSet<SlabId> = model.expected_slab_ids().into_iter().collect();

        // Check that obsolete slabs are removed
        let obsolete_slabs: Vec<_> = old_slab_ids.difference(&new_slab_ids).collect();

        for old_slab_id in obsolete_slabs {
            // Check if old slab still exists in metadata
            let all_slabs: Vec<SlabId> = operations
                .get_slabs(&input.segment_id)
                .try_collect()
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Failed to check slab cleanup: {e:?}"))?;

            if all_slabs.contains(old_slab_id) {
                return Err(color_eyre::eyre::eyre!(
                    "Obsolete slab {} not cleaned up",
                    old_slab_id
                ));
            }
        }
    }

    Ok(())
}

/// Creates a test Cassandra configuration.
fn test_cassandra_config(keyspace: &str) -> CassandraConfiguration {
    use std::time::Duration;

    CassandraConfiguration {
        nodes: vec!["127.0.0.1:9042".to_owned()],
        keyspace: keyspace.to_owned(),
        datacenter: Some("datacenter1".to_owned()),
        rack: None,
        user: None,
        password: None,
        retention: Duration::from_secs(86400),
    }
}

/// Property test: migration preserves all invariants.
///
/// # Test Strategy
///
/// 1. Setup: Create initial state (V1 or V2) with random triggers
/// 2. Migration: Create new store with `target_slab_size` and call
///    `get_segment()`
/// 3. Verification: Build reference model and verify all 5 invariants
/// 4. Cleanup: Delete segment
///
/// # Errors
///
/// Returns error if any invariant is violated.
pub async fn prop_migration_invariants(
    v1_operations: &V1Operations,
    input: MigrationTestInput,
) -> color_eyre::Result<()> {
    // Setup phase: Create initial state
    match input.initial_version {
        SegmentVersion::V1 => {
            setup_v1_state(v1_operations, &input).await?;
        }
        SegmentVersion::V2 => {
            let config = test_cassandra_config("prosody_test");
            let cassandra_base = CassandraStore::new(&config).await?;
            let cassandra_store = CassandraTriggerStore::with_store(
                cassandra_base,
                &config.keyspace,
                input.initial_slab_size,
            )
            .await?;
            let store = TableAdapter::new(cassandra_store);
            setup_v2_state(&store, &input).await?;
        }
    }

    // Migration phase: Create store with target_slab_size
    let config = test_cassandra_config("prosody_test");
    let cassandra_base = CassandraStore::new(&config).await?;
    let cassandra_store =
        CassandraTriggerStore::with_store(cassandra_base, &config.keyspace, input.target_slab_size)
            .await?;
    let store = TableAdapter::new(cassandra_store);

    // Trigger migration by calling get_segment()
    // Note: Empty V1 segments (no triggers) don't exist in the database
    let segment_opt = store
        .get_segment(&input.segment_id)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to trigger migration: {e:?}"))?;

    // Build reference model
    let model = MigrationModel::from_input(&input);

    // Empty V1 segments don't exist - skip verification if no segment and no
    // triggers
    if segment_opt.is_none() && input.triggers.is_empty() {
        // This is expected for empty V1 segments - no segment row was created
        return Ok(());
    }

    // Verification phase: Check all invariants
    verify_segment_metadata(&store, &model, input.initial_version).await?;
    verify_data_preservation(&store, &model).await?;
    verify_correct_indexing(&store, &model).await?;
    verify_dual_index_consistency(&store, &model).await?;
    verify_no_extra_slabs(store.operations(), &model).await?;
    verify_cleanup(v1_operations, store.operations(), &input, &model).await?;

    // Cleanup phase: Delete segment (success or failure)
    store
        .operations()
        .delete_segment(&input.segment_id)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to cleanup segment: {e:?}"))?;

    Ok(())
}
