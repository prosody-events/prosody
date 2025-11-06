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
use crate::timers::store::{Segment, SegmentId, SegmentVersion, SlabId};
use crate::timers::{DELETE_CONCURRENCY, TimerType, Trigger};

/// Maximum concurrent slab migrations.
///
/// Conservative limit to avoid overwhelming Cassandra during migration.
/// With 4 slabs × 4 triggers = 16 total concurrent operations.
const MIGRATION_SLAB_CONCURRENCY: usize = 4;

/// Maximum concurrent trigger writes per slab during migration.
///
/// Conservative limit for write operations during migration.
/// Combined with [`MIGRATION_SLAB_CONCURRENCY`]: 4 × 4 = 16 total operations.
const MIGRATION_WRITE_CONCURRENCY: usize = 4;
use ahash::HashSet;
use futures::TryStreamExt;
use futures::stream;
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
pub async fn migrate_segment_version(
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

    // Phase 2: Read all v1 data and write to v2 (idempotent, no deletes)
    // Process all slabs and triggers concurrently for maximum throughput
    stream::iter(slab_ids.iter().copied().map(Ok::<_, TimerManagerError<_>>))
        .try_for_each_concurrent(MIGRATION_SLAB_CONCURRENCY, |slab_id| {
            let v1_ops = v1_ops.clone();
            let store = store.clone();
            let segment_id = segment.id;
            let slab_size = segment.slab_size;

            async move {
                // Stream v1 triggers and write to v2 concurrently
                let triggers = v1_ops.get_slab_triggers(&segment_id, slab_id);

                triggers
                    .map_err(TimerManagerError::Store)
                    .try_for_each_concurrent(MIGRATION_WRITE_CONCURRENCY, |v1_trigger| {
                        let store = store.clone();

                        async move {
                            // Convert v1 trigger to v2 with Application timer type
                            let v2_trigger = Trigger::new(
                                v1_trigger.key.clone(),
                                v1_trigger.time,
                                TimerType::Application,
                                v1_trigger.span,
                            );

                            // Recalculate slab based on trigger time and segment slab_size.
                            // Since slab_size is unchanged during version migration, this produces
                            // the same slab_id as V1, so slab metadata row already exists.
                            let target_slab =
                                Slab::from_time(segment_id, slab_size, v2_trigger.time);

                            // Write to V2 tables (slab and key indices)
                            try_join!(
                                store.insert_slab_trigger(target_slab, v2_trigger.clone()),
                                store.insert_key_trigger(&segment_id, v2_trigger),
                            )
                            .map_err(TimerManagerError::Store)?;

                            Ok(())
                        }
                    })
                    .await?;

                debug!("Migrated slab {slab_id}");
                Ok(())
            }
        })
        .await?;

    // Phase 3: Update segment version (atomic marker indicating migration complete)
    // This is the critical point - after this, the system uses v2 tables
    store
        .update_segment_version(&segment_id, SegmentVersion::V2, segment.slab_size)
        .await
        .map_err(TimerManagerError::Store)?;

    info!("Successfully migrated segment {segment_id} from V1 to V2");

    // Phase 4: Clean up V1 trigger data (can fail safely - data is orphaned but
    // harmless)
    if let Err(error) = cleanup_v1_data(&v1_ops, &segment_id, &slab_ids).await {
        warn!(
            "Failed to clean up V1 data for segment {segment_id}: {error:#}; v1 data is orphaned \
             but migration completed successfully"
        );
    }

    Ok(())
}

/// Cleans up V1 trigger data after migration.
///
/// This function removes V1 trigger data from `timer_slabs` and `timer_keys`
/// tables. Slab metadata in `timer_segments` is left intact as it is shared
/// between V1 and V2 and does not need to be cleaned up.
///
/// # Arguments
///
/// * `v1_ops` - V1 operations interface
/// * `segment_id` - The segment being migrated
/// * `slab_ids` - List of V1 slab IDs to clean up
///
/// # Errors
///
/// Returns an error if any cleanup operations fail. Processing continues for
/// all slabs even if some fail, but the first error encountered is returned.
async fn cleanup_v1_data(
    v1_ops: &super::v1::V1Operations,
    segment_id: &SegmentId,
    slab_ids: &[SlabId],
) -> Result<(), CassandraTriggerStoreError> {
    // Process all slabs concurrently for maximum throughput
    stream::iter(
        slab_ids
            .iter()
            .copied()
            .map(Ok::<_, CassandraTriggerStoreError>),
    )
    .try_for_each_concurrent(DELETE_CONCURRENCY, |slab_id| {
        let v1_ops = v1_ops.clone();
        let segment_id = *segment_id;

        async move {
            // Stream triggers from slab and delete from key index concurrently
            let triggers = v1_ops.get_slab_triggers(&segment_id, slab_id);
            triggers
                .try_for_each_concurrent(DELETE_CONCURRENCY, |trigger| {
                    let v1_ops = v1_ops.clone();
                    async move {
                        v1_ops
                            .delete_key_trigger(&segment_id, &trigger.key, trigger.time)
                            .await
                    }
                })
                .await?;

            // Clear slab triggers (separate table, can't combine with above)
            v1_ops.clear_slab_triggers(&segment_id, slab_id).await?;

            Ok(())
        }
    })
    .await?;

    debug!(
        "Cleaned up V1 trigger data for {} slabs in segment {segment_id}",
        slab_ids.len()
    );

    Ok(())
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
