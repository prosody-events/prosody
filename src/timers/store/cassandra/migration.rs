//! Timer schema migration from v1 to v2 and slab size changes.
//!
//! This module handles two types of migrations:
//!
//! ## Phase 1: Version Migration (v1 → v2)
//!
//! Migrates timer data from v1 schema (without timer types) to v2 schema (with
//! timer types). Migration is performed on-demand per segment when first
//! accessed after deployment.
//!
//! **Strategy:**
//! - **Lazy Migration**: Segments are migrated only when accessed, not all at
//!   once
//! - **Atomic Per-Segment**: Each segment migration is atomic and independent
//! - **Default Timer Type**: All v1 timers are migrated as
//!   `TimerType::Application`
//! - **Version Tracking**: Segment version is updated to v2 after successful
//!   migration
//! - **V1 Data Cleanup**: V1 data is deleted after successful migration to v2
//!
//! **Process:**
//! 1. Check segment version
//! 2. If v1 (None or Some(1)): a. Read all v1 triggers from v1 tables b.
//!    Convert to v2 triggers with `TimerType::Application` c. Write v2 triggers
//!    to v2 tables d. Update segment version to v2 (atomic marker) e. Delete v1
//!    data (cleanup, can fail safely)
//! 3. If v2 (Some(2)): No version migration needed
//!
//! ## Phase 2: Slab Size Migration (v2 → v2)
//!
//! Changes the slab size for a v2 segment, recalculating slab IDs for all
//! triggers.
//!
//! **Strategy:**
//! - **Preserves Timer Types**: All `timer_type` values are preserved during
//!   migration
//! - **Recalculates Slab IDs**: Each trigger's slab ID is recalculated based on
//!   new slab size
//! - **Slab Size Tracking**: Segment `slab_size` is updated after successful
//!   migration
//! - **Old Slab Cleanup**: Old v2 slabs are deleted after successful migration
//!
//! **Process:**
//! 1. Check segment `slab_size` vs `desired_slab_size`
//! 2. If different: a. Read all triggers from old v2 slabs b. Recalculate slab
//!    ID for each trigger: `floor(time / new_slab_size)` c. Write triggers to
//!    new v2 slabs d. Update segment `slab_size` (atomic marker) e. Delete old
//!    slabs (cleanup, can fail safely)
//! 3. If same: No slab size migration needed

use crate::timers::duration::CompactDuration;
use crate::timers::error::TimerManagerError;
use crate::timers::slab::Slab;
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentId, SegmentVersion, SlabId, TriggerV1};
use crate::timers::{TimerType, Trigger};
use ahash::HashSet;
use futures::TryStreamExt;
use thiserror::Error;
use tokio::try_join;
use tracing::{debug, info, instrument, warn};

use super::{CassandraTriggerStore, CassandraTriggerStoreError};

/// Checks if a segment needs migration from v1 to v2.
///
/// # Arguments
///
/// * `segment` - The segment to check
///
/// # Returns
///
/// `true` if the segment is v1 and needs migration, `false` otherwise.
#[must_use]
pub fn needs_migration(segment: &Segment) -> bool {
    matches!(segment.version, SegmentVersion::V1)
}

/// Migrates a segment from v1 to v2 schema.
///
/// Reads all v1 triggers, converts them to v2 with `TimerType::Application`,
/// writes to v2 tables using coordinated high-level operations, updates the
/// segment version, and cleans up v1 data.
///
/// **Note**: This function is Cassandra-specific since V1 schema only exists
/// for Cassandra storage.
///
/// # Arguments
///
/// * `store` - The Cassandra trigger store wrapped in `TableAdapter`
/// * `segment` - The segment to migrate
///
/// # Errors
///
/// Returns [`TimerManagerError`] if:
/// - Reading v1 triggers fails
/// - Writing v2 triggers fails
/// - Updating segment version fails
/// - Cleaning up v1 data fails
#[instrument(level = "info", skip(store), err)]
pub async fn migrate_segment(
    store: &CassandraTriggerStore,
    segment: &Segment,
) -> Result<(), TimerManagerError<CassandraTriggerStoreError>> {
    let segment_id = segment.id;
    info!("Starting v1 to v2 migration for segment {segment_id}");

    // Create V1 operations on-demand for migration
    let v1_ops = store.v1();

    // Phase 1: Collect all v1 slabs (non-destructive)
    let slab_ids: Vec<_> = v1_ops
        .get_slabs(&segment_id)
        .try_collect()
        .await
        .map_err(TimerManagerError::Store)?;

    debug!("Found {} v1 slabs to migrate", slab_ids.len());

    let mut total_triggers = 0_usize;

    // Phase 2: Read all v1 data and write to v2 (idempotent, no deletes)
    // This phase can be retried completely if it fails at any point
    for slab_id in &slab_ids {
        // Read v1 triggers for this slab
        let v1_triggers: Vec<TriggerV1> = v1_ops
            .get_slab_triggers(&segment_id, *slab_id)
            .try_collect()
            .await
            .map_err(TimerManagerError::Store)?;

        debug!(
            "Migrating {} triggers from slab {slab_id}",
            v1_triggers.len()
        );

        // Convert each v1 trigger to v2 and write using coordinated high-level
        // operation
        for v1_trigger in v1_triggers {
            let v2_trigger = Trigger::new(
                v1_trigger.key.clone(),
                v1_trigger.time,
                TimerType::Application,
                v1_trigger.span,
            );

            // Recalculate slab based on trigger time and segment slab_size.
            // Since slab_size is unchanged during version migration, this will produce
            // the same slab_id as V1, so the slab metadata row already exists.
            let target_slab = Slab::from_time(segment_id, segment.slab_size, v2_trigger.time);

            // Write to V2 tables (slab and key indices).
            // Note: We don't call insert_slab here because the slab metadata row
            // already exists in the segments table from V1 operations (with TTL).
            // Since slab_size is unchanged, V1 and V2 use the same slab_ids.
            try_join!(
                store.insert_slab_trigger(target_slab, v2_trigger.clone()),
                store.insert_key_trigger(&segment_id, v2_trigger),
            )
            .map_err(TimerManagerError::Store)?;

            total_triggers += 1;
        }
    }

    // Phase 3: Update segment version (atomic marker indicating migration complete)
    // This is the critical point - after this, the system uses v2 tables
    store
        .update_segment_version(&segment_id, SegmentVersion::V2, segment.slab_size)
        .await
        .map_err(TimerManagerError::Store)?;

    info!(
        "Successfully migrated {total_triggers} triggers from segment {segment_id}, version \
         updated to v2"
    );

    // Phase 4: Clean up V1 trigger data (can fail safely - data is orphaned but
    // harmless)
    cleanup_v1_data(&v1_ops, &segment_id, &slab_ids, segment.slab_size).await;

    Ok(())
}

/// Cleans up V1 trigger data and obsolete slab metadata after migration.
///
/// This function removes V1 data from `timer_slabs` and `timer_keys` tables,
/// and deletes slab metadata for slabs that are not reused by V2.
///
/// # Arguments
///
/// * `v1_ops` - V1 operations interface
/// * `segment_id` - The segment being migrated
/// * `slab_ids` - List of V1 slab IDs to clean up
/// * `target_slab_size` - The V2 slab size (used to calculate which slabs are
///   reused)
async fn cleanup_v1_data(
    v1_ops: &super::v1::V1Operations,
    segment_id: &SegmentId,
    slab_ids: &[SlabId],
    target_slab_size: CompactDuration,
) {
    // Calculate which V1 slabs are obsolete (not used by V2)
    // If slab size didn't change, V2 uses the same slab IDs - don't delete those!
    let mut v2_slab_ids = HashSet::default();
    for slab_id in slab_ids {
        // Get triggers from this old V1 slab and calculate their new V2 slab IDs
        let v1_triggers: Vec<TriggerV1> = match v1_ops
            .get_slab_triggers(segment_id, *slab_id)
            .try_collect()
            .await
        {
            Ok(triggers) => triggers,
            Err(error) => {
                warn!("Failed to get v1 triggers for cleanup calculation: {error:#}");
                continue;
            }
        };

        for trigger in &v1_triggers {
            let v2_slab_id = Slab::from_time(*segment_id, target_slab_size, trigger.time).id();
            v2_slab_ids.insert(v2_slab_id);
        }
    }

    // Now clean up V1 data
    for slab_id in slab_ids {
        // Get all V1 triggers to delete from key index
        let v1_triggers: Vec<_> = match v1_ops
            .get_slab_triggers(segment_id, *slab_id)
            .try_collect()
            .await
        {
            Ok(triggers) => triggers,
            Err(error) => {
                warn!(
                    "Failed to get v1 triggers for cleanup of slab {slab_id} in segment \
                     {segment_id}: {error:#}; v1 data is orphaned but migration completed \
                     successfully"
                );
                continue;
            }
        };

        // Delete each trigger from V1 key index
        for trigger in v1_triggers {
            if let Err(error) = v1_ops
                .delete_key_trigger(segment_id, &trigger.key, trigger.time)
                .await
            {
                warn!(
                    "Failed to delete v1 key trigger during cleanup: {error:#}; orphaned data \
                     remains"
                );
            }
        }

        // Clear all V1 slab triggers (timer_slabs table)
        if let Err(error) = v1_ops.clear_slab_triggers(segment_id, *slab_id).await {
            warn!(
                "Failed to clear v1 slab triggers for {slab_id} in segment {segment_id}: \
                 {error:#}; v1 data is orphaned but migration completed successfully"
            );
        }

        // Delete slab metadata ONLY for obsolete slabs (not reused by V2)
        // During version migration, slab_size is unchanged, so V2 reuses all V1
        // slab_ids and we won't delete any slab metadata rows here (all are in
        // v2_slab_ids). Note: If a slab_id is needed in the future,
        // add_trigger() will recreate it.
        if !v2_slab_ids.contains(slab_id)
            && let Err(error) = v1_ops.delete_slab_metadata(segment_id, *slab_id).await
        {
            warn!(
                "Failed to delete obsolete v1 slab metadata for {slab_id} in segment \
                 {segment_id}: {error:#}; orphaned metadata remains"
            );
        }
    }

    debug!(
        "Cleaned up V1 data for {} slabs in segment {segment_id}",
        slab_ids.len()
    );
}

/// Checks if a segment needs slab size migration.
///
/// # Arguments
///
/// * `segment` - The segment to check
/// * `desired_slab_size` - The target slab size
///
/// # Returns
///
/// `true` if the segment's `slab_size` differs from `desired_slab_size`,
/// `false` otherwise.
#[must_use]
pub fn needs_slab_size_migration(segment: &Segment, desired_slab_size: CompactDuration) -> bool {
    segment.slab_size != desired_slab_size
}

/// Migrates a v2 segment to a new slab size.
///
/// Reads all triggers from slabs with the old slab size, recalculates their
/// slab IDs based on the new slab size, writes them to new slabs using
/// coordinated high-level operations, updates the segment `slab_size`, and
/// cleans up old slabs.
///
/// **IMPORTANT:** This function requires the segment to be v2. Phase 1 version
/// migration must be completed before calling this function.
///
/// **Note**: This function is Cassandra-specific.
///
/// # Arguments
///
/// * `store` - The Cassandra trigger store wrapped in `TableAdapter`
/// * `segment` - The v2 segment to migrate
/// * `desired_slab_size` - The target slab size
///
/// # Errors
///
/// Returns [`TimerManagerError`] if:
/// - Segment is not v2 (must run version migration first)
/// - Reading old triggers fails
/// - Writing new triggers fails
/// - Updating segment `slab_size` fails
/// - Cleaning up old slabs fails
#[instrument(level = "info", skip(store), err)]
pub async fn migrate_slab_size(
    store: &CassandraTriggerStore,
    segment: &Segment,
    desired_slab_size: CompactDuration,
) -> Result<(), TimerManagerError<CassandraTriggerStoreError>> {
    let segment_id = segment.id;
    let old_slab_size = segment.slab_size;

    // Verify segment is v2
    if segment.version != SegmentVersion::V2 {
        warn!(
            "Cannot migrate slab_size for segment {segment_id}: segment is not v2 (version = \
             {:?}). Run version migration first.",
            segment.version
        );
        return Ok(());
    }

    info!(
        "Starting slab_size migration for segment {segment_id}: {old_slab_size} → \
         {desired_slab_size}"
    );

    // Phase 1: Collect all slabs with old slab_size (non-destructive)
    let old_slab_ids: Vec<_> = store
        .get_slabs(&segment_id)
        .try_collect()
        .await
        .map_err(TimerManagerError::Store)?;

    debug!(
        "Found {} slabs to migrate for segment {segment_id}",
        old_slab_ids.len()
    );

    let mut total_triggers = 0_usize;
    let mut registered_slabs = HashSet::default();

    // Phase 2: Read all triggers from old slabs and write to new slabs
    // (idempotent, no deletes) This phase can be retried completely if it fails at
    // any point
    for old_slab_id in &old_slab_ids {
        let old_slab = Slab::new(segment_id, *old_slab_id, old_slab_size);

        // Read all triggers from old slab across all timer types
        let triggers = store
            .get_slab_triggers_all_types(&old_slab)
            .try_collect::<Vec<_>>()
            .await
            .map_err(TimerManagerError::Store)?;

        debug!(
            "Migrating {} triggers from old slab {old_slab_id}",
            triggers.len()
        );

        // Write each trigger to its new slab
        // NOTE: We only write to slab tables, NOT key tables
        // Key tables are indexed by (segment_id, key, time, type) which doesn't
        // include slab_id, so they don't need to change when slab_size changes
        for trigger in triggers {
            // Recalculate slab ID based on new slab size
            let new_slab = Slab::from_time(segment_id, desired_slab_size, trigger.time);

            // Register the new slab metadata if not already registered
            // Note: Cassandra INSERT is an upsert, so this is idempotent
            if registered_slabs.insert(new_slab.id()) {
                store
                    .insert_slab(&segment_id, new_slab.clone())
                    .await
                    .map_err(TimerManagerError::Store)?;
            }

            // Write to new v2 slab index
            store
                .insert_slab_trigger(new_slab.clone(), trigger.clone())
                .await
                .map_err(TimerManagerError::Store)?;

            total_triggers += 1;
        }
    }

    // Phase 3: Update segment slab_size (atomic marker indicating migration
    // complete) This is the critical point - after this, the system uses new
    // slab_size
    store
        .update_segment_version(&segment_id, SegmentVersion::V2, desired_slab_size)
        .await
        .map_err(TimerManagerError::Store)?;

    info!(
        "Successfully migrated {total_triggers} triggers for segment {segment_id}, slab_size \
         updated to {desired_slab_size}"
    );

    // Phase 4: Clean up old slabs (can fail safely - data is orphaned but harmless)
    // If cleanup fails, we can retry it later or ignore it
    for old_slab_id in &old_slab_ids {
        // Delete the old slab registration
        if let Err(error) = store.delete_slab(&segment_id, *old_slab_id).await {
            warn!(
                "Failed to delete old slab {old_slab_id} for segment {segment_id}: {error:#}; old \
                 slab data is orphaned but migration completed successfully"
            );
            continue;
        }

        // Clear the old slab triggers
        let old_slab = Slab::new(segment_id, *old_slab_id, old_slab_size);
        if let Err(error) = store.clear_slab_triggers(&old_slab).await {
            warn!(
                "Failed to clear triggers for old slab {old_slab_id} in segment {segment_id}: \
                 {error:#}; old slab data is orphaned but migration completed successfully"
            );
        }
    }

    debug!(
        "Cleaned up {} old slabs for segment {segment_id}",
        old_slab_ids.len()
    );

    Ok(())
}

/// Errors that can occur during migration operations.
#[derive(Debug, Error)]
pub enum MigrationError {
    /// Segment disappeared after V1→V2 schema migration completed.
    #[error("Segment {0} disappeared after V1→V2 migration")]
    SegmentDisappearedAfterV1Migration(SegmentId),

    /// Segment disappeared after slab size migration completed.
    #[error("Segment {0} disappeared after slab size migration")]
    SegmentDisappearedAfterSlabSizeMigration(SegmentId),
}
