//! Property-based tests for timer store migration.
//!
//! Tests V1→V2 schema migration, V2→V3 key state backfill, and slab size
//! migration using random scenarios to verify all migration invariants.

use super::test_cassandra_config;
use crate::Key;
use crate::cassandra::CassandraStore;
use crate::test_util::TEST_KEYSPACE;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::v1::{V1Operations, coordinated};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentId, SegmentVersion, TriggerStore, TriggerV1};
use crate::timers::{TimerType, Trigger};
use ahash::HashSet;
use quickcheck::{Arbitrary, Gen};
use strum::VariantArray;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

mod verify;
use verify::{
    verify_cleanup, verify_correct_indexing, verify_data_preservation,
    verify_dual_index_consistency, verify_key_state_invariant, verify_no_extra_slabs,
    verify_segment_metadata,
};

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

        // Randomly choose initial version (33% V1, 33% V2, 33% V3)
        let initial_version = match u8::arbitrary(g) % 3 {
            0 => SegmentVersion::V1,
            1 => SegmentVersion::V2,
            _ => SegmentVersion::V3,
        };

        // Generate slab sizes (1 second to 7 days to avoid TTL overflow)
        // Clamp to 604_800 seconds (7 days)
        let initial_slab_size = CompactDuration::new(u32::arbitrary(g).clamp(1, 604_800));
        let target_slab_size = CompactDuration::new(u32::arbitrary(g).clamp(1, 604_800));

        // Generate 0-50 triggers (empty segments are valid test cases)
        let trigger_count = usize::arbitrary(g) % 51;
        let mut triggers = Vec::with_capacity(trigger_count);

        // Use small key pool for better collision density
        let key_pool: Vec<Key> = (0_i32..5_i32).map(|i| format!("key-{i}").into()).collect();

        for _ in 0..trigger_count {
            let key_idx = usize::arbitrary(g) % key_pool.len();
            let key = key_pool[key_idx].clone();

            // Generate random time using arbitrary
            let time = CompactDateTime::arbitrary(g);

            // Generate random timer type, drawing from every persisted variant
            // (V2/V3 preserve it; V1 collapses to Application in the model).
            let timer_type =
                TimerType::VARIANTS[usize::from(u8::arbitrary(g)) % TimerType::VARIANTS.len()];

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
    /// V2→V2/V3: Timer types preserved.
    pub triggers: HashSet<(Key, CompactDateTime, TimerType)>,
}

impl MigrationModel {
    /// Builds the reference model from test input.
    ///
    /// Applies migration transformation rules:
    /// - V1→V2: All triggers become Application type
    /// - V2→V3: Timer types preserved, segment version bumped to V3
    #[must_use]
    pub fn from_input(input: &MigrationTestInput) -> Self {
        // V1 goes V1→V2→V3 (both phases run sequentially).
        // V2 goes V2→V3. V3 stays V3 (no migration needed).
        let expected_version = SegmentVersion::V4;

        let segment = Segment {
            id: input.segment_id,
            name: input.segment_name.clone(),
            version: expected_version,
            slab_size: input.target_slab_size,
        };

        let mut triggers = HashSet::default();

        for trigger_data in &input.triggers {
            // Apply timer type transformation based on initial version
            let timer_type = match input.initial_version {
                SegmentVersion::V1 => TimerType::Application, // V1 has no timer_type
                SegmentVersion::V2 | SegmentVersion::V3 | SegmentVersion::V4 => {
                    trigger_data.timer_type
                } // V2/V3 preserve timer_type
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
            .map(|(_, time, _)| Slab::from_time(self.segment.slab_size, *time).id())
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
        let slab_id = Slab::from_time(input.initial_slab_size, trigger_data.time).id();
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
            context: Span::current().context(),
        };

        coordinated::add_trigger(
            operations,
            &input.segment_id,
            Slab::from_time(input.initial_slab_size, trigger_data.time).id(),
            v1_trigger,
        )
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to add V1 trigger: {e:?}"))?;
    }

    Ok(())
}

/// Sets up V2 initial state: segment row + slab entries + clustering key rows,
/// with **no state MAP entries**.
///
/// This faithfully reproduces the on-disk layout of a real V2 segment before
/// V2→V3 migration runs. The state column is left absent so that
/// `migrate_key_states` / `backfill_key_state` must populate it from scratch.
async fn setup_v2_state(
    store: &CassandraTriggerStore,
    input: &MigrationTestInput,
) -> color_eyre::Result<()> {
    // Insert segment metadata at version V2.
    store
        .insert_segment()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to insert V2 segment: {e:?}"))?;

    // Register each unique slab in the slab index.
    let mut registered_slabs = HashSet::default();
    for trigger_data in &input.triggers {
        let slab = Slab::from_time(input.initial_slab_size, trigger_data.time);
        if registered_slabs.insert(slab.id()) {
            store
                .insert_slab(slab)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Failed to insert slab: {e:?}"))?;
        }
    }

    // Write each trigger to the slab index and the key clustering index, but
    // deliberately omit any write to the state MAP column. This is the exact
    // layout produced by old V2 write paths before state backfill existed.
    for trigger_data in &input.triggers {
        let slab = Slab::from_time(input.initial_slab_size, trigger_data.time);
        let trigger = Trigger::for_testing(
            trigger_data.key.clone(),
            trigger_data.time,
            trigger_data.timer_type,
        );

        store
            .insert_slab_trigger(slab, trigger.clone())
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to insert slab trigger: {e:?}"))?;

        store
            .add_key_trigger_clustering(&input.segment_id, trigger)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to add key trigger clustering: {e:?}"))?;
    }

    Ok(())
}

/// Sets up V3 initial state: segment row + triggers written through the full
/// state-aware path so that state MAP entries are populated on creation.
async fn setup_v3_state(
    store: &TableAdapter<CassandraTriggerStore>,
    input: &MigrationTestInput,
) -> color_eyre::Result<()> {
    store
        .insert_segment()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to insert V3 segment: {e:?}"))?;

    for trigger_data in &input.triggers {
        let slab = Slab::from_time(input.initial_slab_size, trigger_data.time);
        // Slab metadata is normally written by the scheduler actor; this
        // fixture writes it directly so the migration test sees the slabs.
        store
            .insert_slab(slab)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to insert V3 slab: {e:?}"))?;

        let trigger = Trigger::for_testing(
            trigger_data.key.clone(),
            trigger_data.time,
            trigger_data.timer_type,
        );
        store
            .add_trigger(trigger)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to add V3 trigger: {e:?}"))?;
    }

    Ok(())
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
            // Write true V2 layout: clustering rows + slab entries, no state
            // MAP entries.  backfill_key_state must populate state from scratch.
            let config = test_cassandra_config(TEST_KEYSPACE);
            let cassandra_base = CassandraStore::new(&config).await?;
            let segment = Segment {
                id: input.segment_id,
                name: input.segment_name.clone(),
                slab_size: input.initial_slab_size,
                version: SegmentVersion::V2,
            };
            let cassandra_store =
                CassandraTriggerStore::with_store(cassandra_base, &config.keyspace, segment)
                    .await?;
            setup_v2_state(&cassandra_store, &input).await?;
        }
        SegmentVersion::V3 | SegmentVersion::V4 => {
            // Write V3 layout: triggers go through the full state-aware path.
            let config = test_cassandra_config(TEST_KEYSPACE);
            let cassandra_base = CassandraStore::new(&config).await?;
            let segment = Segment {
                id: input.segment_id,
                name: input.segment_name.clone(),
                slab_size: input.initial_slab_size,
                version: SegmentVersion::V3,
            };
            let cassandra_store =
                CassandraTriggerStore::with_store(cassandra_base, &config.keyspace, segment)
                    .await?;
            let store = TableAdapter::new(cassandra_store);
            setup_v3_state(&store, &input).await?;
        }
    }

    // Migration phase: Create store with target_slab_size
    let config = test_cassandra_config(TEST_KEYSPACE);
    let cassandra_base = CassandraStore::new(&config).await?;
    let segment = Segment {
        id: input.segment_id,
        name: String::new(),
        slab_size: input.target_slab_size,
        version: SegmentVersion::V3,
    };
    let cassandra_store =
        CassandraTriggerStore::with_store(cassandra_base, &config.keyspace, segment).await?;
    let store = TableAdapter::new(cassandra_store);

    // Trigger migration by calling get_segment()
    // Note: Empty V1 segments (no triggers) don't exist in the database
    let segment_opt = store
        .get_segment()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to trigger migration: {e:?}"))?;

    // Build reference model
    let model = MigrationModel::from_input(&input);

    // Empty V1 segments don't exist - skip verification if no segment and no
    // triggers. V2/V3 segments always have a row (inserted in setup).
    if segment_opt.is_none()
        && input.triggers.is_empty()
        && input.initial_version == SegmentVersion::V1
    {
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
    verify_key_state_invariant(&store, &model, input.initial_version).await?;

    // Cleanup phase: Delete segment (success or failure)
    store
        .operations()
        .delete_segment()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to cleanup segment: {e:?}"))?;

    Ok(())
}
