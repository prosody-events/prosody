//! Post-migration checks against the migration model.

use super::{MigrationModel, MigrationTestInput};
use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::cassandra::v1::V1Operations;
use crate::timers::store::cassandra::{CassandraTriggerStore, TimerState};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{SegmentVersion, TriggerStore};
use crate::timers::{TimerType, Trigger};
use ahash::{HashMap, HashSet};
use futures::TryStreamExt;
use strum::VariantArray;

/// Verifies that segment metadata is correct after migration.
///
/// Expected: version=V2, `slab_size=target_slab_size`, name preserved (for V2
/// only).
pub(super) async fn verify_segment_metadata(
    store: &TableAdapter<CassandraTriggerStore>,
    model: &MigrationModel,
    initial_version: SegmentVersion,
) -> color_eyre::Result<()> {
    let segment = store
        .get_segment()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to get segment: {e:?}"))?
        .ok_or_else(|| color_eyre::eyre::eyre!("Segment {} not found", model.segment.id))?;

    if segment.version != model.segment.version {
        return Err(color_eyre::eyre::eyre!(
            "Segment version mismatch: expected {:?}, got {:?}",
            model.segment.version,
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

    // Only check name preservation for V2→V3 migrations (and V2→V2 slab-size).
    // V1→V2 migrations don't preserve name (V1 segments have NULL name).
    if initial_version != SegmentVersion::V1 && segment.name != model.segment.name {
        return Err(color_eyre::eyre::eyre!(
            "Segment name mismatch for V2→V3 migration: expected {:?}, got {:?}",
            model.segment.name,
            segment.name
        ));
    }

    Ok(())
}

/// Collects every trigger the key index holds for each distinct key in the
/// model, flattened into `(key, time, timer_type)` tuples.
///
/// The key index is the authoritative surface after migration; both the
/// data-preservation and dual-index checks read it the same way.
pub(super) async fn collect_key_index_triggers(
    store: &TableAdapter<CassandraTriggerStore>,
    model: &MigrationModel,
) -> color_eyre::Result<HashSet<(Key, CompactDateTime, TimerType)>> {
    let mut triggers = HashSet::default();
    let mut seen_keys = HashSet::default();

    for (key, ..) in &model.triggers {
        if !seen_keys.insert(key.clone()) {
            continue;
        }

        for &timer_type in TimerType::VARIANTS {
            let found: Vec<Trigger> = store
                .get_key_triggers(timer_type, key)
                .try_collect()
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!(
                        "Failed to get key triggers for {key} {timer_type:?}: {e:?}"
                    )
                })?;

            for trigger in found {
                triggers.insert((trigger.key.clone(), trigger.time, trigger.timer_type));
            }
        }
    }

    Ok(triggers)
}

/// Verifies that all triggers are preserved with correct `timer_type`.
///
/// Builds actual trigger set from store and compares to model.
pub(super) async fn verify_data_preservation(
    store: &TableAdapter<CassandraTriggerStore>,
    model: &MigrationModel,
) -> color_eyre::Result<()> {
    let actual_triggers = collect_key_index_triggers(store, model).await?;

    if actual_triggers != model.triggers {
        let missing: Vec<_> = model.triggers.difference(&actual_triggers).collect();
        let extra: Vec<_> = actual_triggers.difference(&model.triggers).collect();

        return Err(color_eyre::eyre::eyre!(
            "Trigger mismatch:\n  Missing: {missing:?}\n  Extra: {extra:?}"
        ));
    }

    Ok(())
}

/// Verifies that triggers are in correct slabs based on target slab
/// size.
pub(super) async fn verify_correct_indexing(
    store: &TableAdapter<CassandraTriggerStore>,
    model: &MigrationModel,
) -> color_eyre::Result<()> {
    // Build map of slab_id -> expected triggers
    let mut expected_by_slab: HashMap<SlabId, HashSet<(Key, CompactDateTime, TimerType)>> =
        HashMap::default();

    for (key, time, timer_type) in &model.triggers {
        let slab = Slab::from_time(model.segment.slab_size, *time);
        expected_by_slab
            .entry(slab.id())
            .or_default()
            .insert((key.clone(), *time, *timer_type));
    }

    // Verify each slab has correct triggers
    for (slab_id, expected) in &expected_by_slab {
        let actual_triggers: Vec<Trigger> = store
            .get_slab_triggers_all_types(*slab_id)
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

/// Verifies that the slab index matches the key index exactly.
///
/// Both indices must contain the same triggers (dual-index consistency).
pub(super) async fn verify_dual_index_consistency(
    store: &TableAdapter<CassandraTriggerStore>,
    model: &MigrationModel,
) -> color_eyre::Result<()> {
    // Collect from slab index
    let mut slab_triggers = HashSet::default();
    let slab_ids = model.expected_slab_ids();

    for slab_id in slab_ids {
        let triggers: Vec<Trigger> = store
            .get_slab_triggers_all_types(slab_id)
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
    let key_triggers = collect_key_index_triggers(store, model).await?;

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
pub(super) async fn verify_no_extra_slabs(
    operations: &CassandraTriggerStore,
    model: &MigrationModel,
) -> color_eyre::Result<()> {
    // Get ALL slabs for this segment
    let all_slabs: Vec<SlabId> = operations
        .get_slabs()
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

/// Verifies that old V1 data and obsolete slabs are cleaned up.
pub(super) async fn verify_cleanup(
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
            let old_slab_id = Slab::from_time(input.initial_slab_size, trigger_data.time).id();
            old_slab_ids.insert(old_slab_id);
        }

        // Get expected new slab IDs
        let new_slab_ids: HashSet<SlabId> = model.expected_slab_ids().into_iter().collect();

        // Check that obsolete slabs are removed
        let obsolete_slabs: Vec<_> = old_slab_ids.difference(&new_slab_ids).collect();

        for old_slab_id in obsolete_slabs {
            // Check if old slab still exists in metadata
            let all_slabs: Vec<SlabId> = operations
                .get_slabs()
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

/// Verifies that after migration to V3, all keys with triggers have
/// correct state entries and correct clustering row counts.
///
/// For each `(key, timer_type)` pair:
/// - 1 trigger → state is `Inline`, and the clustering row has been deleted
///   (data moved into the state MAP column).
/// - ≥2 triggers → state is `Overflow`, and clustering rows remain.
///
/// Applies to both V1-initial (V1→V2→V3) and V2-initial (V2→V3) segments.
/// V3-initial segments already have correct state; they are skipped here since
/// the state column was populated by the original write path, not by backfill.
pub(super) async fn verify_key_state_invariant(
    store: &TableAdapter<CassandraTriggerStore>,
    model: &MigrationModel,
    initial_version: SegmentVersion,
) -> color_eyre::Result<()> {
    // Only verify for segments that went through backfill (V1 and V2 initials).
    if initial_version == SegmentVersion::V3 {
        return Ok(());
    }

    // Count expected timers per (key, timer_type).
    let mut counts: HashMap<(Key, TimerType), usize> = HashMap::default();
    for (key, _, timer_type) in &model.triggers {
        *counts.entry((key.clone(), *timer_type)).or_default() += 1;
    }

    for ((key, timer_type), count) in &counts {
        let state = store
            .operations()
            .fetch_state(&model.segment.id, key, *timer_type)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to get timer state: {e:?}"))?;

        match count {
            1 => {
                if !matches!(state, TimerState::Inline(_)) {
                    return Err(color_eyre::eyre::eyre!(
                        "Key state invariant violated: ({}, {:?}) has 1 trigger but state is \
                         {state:?}, expected Inline",
                        key,
                        timer_type
                    ));
                }

                // For singleton normalization: the clustering row must have been
                // deleted — the trigger lives exclusively in the state MAP now.
                let clustering_rows = store
                    .operations()
                    .peek_trigger_times(&model.segment.id, key, *timer_type)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Failed to count clustering rows: {e:?}")
                    })?;

                if !clustering_rows.is_empty() {
                    return Err(color_eyre::eyre::eyre!(
                        "Singleton normalization incomplete: ({}, {:?}) has Inline state but {} \
                         clustering row(s) still exist",
                        key,
                        timer_type,
                        clustering_rows.len()
                    ));
                }
            }
            n if *n >= 2 && !matches!(state, TimerState::Overflow) => {
                return Err(color_eyre::eyre::eyre!(
                    "Key state invariant violated: ({}, {:?}) has {n} triggers but state is \
                     {state:?}, expected Overflow",
                    key,
                    timer_type
                ));
            }
            _ => {}
        }
    }

    Ok(())
}
