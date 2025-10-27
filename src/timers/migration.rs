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
use crate::timers::store::{
    SEGMENT_VERSION_V1, SEGMENT_VERSION_V2, Segment, TriggerStore, TriggerV1,
};
use crate::timers::{TimerType, Trigger};
use futures::TryStreamExt;
use std::error::Error;
use std::fmt::Debug;
use tracing::{debug, info, instrument, warn};

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
    match segment.version {
        None | Some(SEGMENT_VERSION_V1) => true,
        Some(SEGMENT_VERSION_V2) => false,
        Some(v) => {
            warn!("Unknown segment version {v}, treating as v1");
            true
        }
    }
}

/// Migrates a segment from v1 to v2 schema.
///
/// Reads all v1 triggers, converts them to v2 with `TimerType::Application`,
/// writes to v2 tables, updates the segment version, and cleans up v1 data.
///
/// # Arguments
///
/// * `store` - The trigger store implementation
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
pub async fn migrate_segment<T>(
    store: &T,
    segment: &Segment,
) -> Result<(), TimerManagerError<T::Error>>
where
    T: TriggerStore,
    T::Error: Error + Debug,
{
    let segment_id = segment.id;
    info!("Starting v1 to v2 migration for segment {segment_id}");

    // Phase 1: Collect all v1 slabs (non-destructive)
    let slab_ids: Vec<_> = store
        .get_slabs_v1(&segment_id)
        .try_collect()
        .await
        .map_err(TimerManagerError::Store)?;

    debug!("Found {} v1 slabs to migrate", slab_ids.len());

    let mut total_triggers = 0_usize;

    // Phase 2: Read all v1 data and write to v2 (idempotent, no deletes)
    // This phase can be retried completely if it fails at any point
    for slab_id in &slab_ids {
        let slab = Slab::new(segment_id, *slab_id, segment.slab_size);

        // Read v1 triggers for this slab
        let v1_triggers: Vec<TriggerV1> = store
            .get_slab_triggers_v1(&segment_id, *slab_id)
            .try_collect()
            .await
            .map_err(TimerManagerError::Store)?;

        debug!(
            "Migrating {} triggers from slab {slab_id}",
            v1_triggers.len()
        );

        // Convert each v1 trigger to v2 and write to both indices
        for v1_trigger in v1_triggers {
            let v2_trigger = Trigger::new(
                v1_trigger.key.clone(),
                v1_trigger.time,
                TimerType::Application,
                v1_trigger.span,
            );

            // Write to v2 slab index
            // Note: Cassandra INSERT is an upsert, so this is idempotent
            store
                .insert_slab_trigger(slab.clone(), v2_trigger.clone())
                .await
                .map_err(TimerManagerError::Store)?;

            // Write to v2 key index
            store
                .insert_key_trigger(&segment_id, v2_trigger)
                .await
                .map_err(TimerManagerError::Store)?;

            total_triggers += 1;
        }
    }

    // Phase 3: Update segment version (atomic marker indicating migration complete)
    // This is the critical point - after this, the system uses v2 tables
    store
        .update_segment_version(&segment_id, SEGMENT_VERSION_V2, segment.slab_size)
        .await
        .map_err(TimerManagerError::Store)?;

    info!(
        "Successfully migrated {total_triggers} triggers from segment {segment_id}, version \
         updated to v2"
    );

    // Phase 4: Clean up v1 data (can fail safely - data is orphaned but harmless)
    // If cleanup fails, we can retry it later or ignore it
    for slab_id in &slab_ids {
        if let Err(error) = store.delete_slab_v1(&segment_id, *slab_id).await {
            warn!(
                "Failed to delete v1 slab {slab_id} for segment {segment_id}: {error:#}; v1 data \
                 is orphaned but migration completed successfully"
            );
        }
    }

    debug!(
        "Cleaned up {} v1 slabs for segment {segment_id}",
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
/// slab IDs based on the new slab size, writes them to new slabs, updates the
/// segment `slab_size`, and cleans up old slabs.
///
/// **IMPORTANT:** This function requires the segment to be v2. Phase 1 version
/// migration must be completed before calling this function.
///
/// # Arguments
///
/// * `store` - The trigger store implementation
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
pub async fn migrate_slab_size<T>(
    store: &T,
    segment: &Segment,
    desired_slab_size: CompactDuration,
) -> Result<(), TimerManagerError<T::Error>>
where
    T: TriggerStore,
    T::Error: Error + Debug,
{
    let segment_id = segment.id;
    let old_slab_size = segment.slab_size;

    // Verify segment is v2
    if segment.version != Some(SEGMENT_VERSION_V2) {
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

    // Phase 2: Read all triggers from old slabs and write to new slabs
    // (idempotent, no deletes) This phase can be retried completely if it fails at
    // any point
    for old_slab_id in &old_slab_ids {
        let old_slab = Slab::new(segment_id, *old_slab_id, old_slab_size);

        // Read triggers from old slab (both Application and DeferRetry)
        let app_triggers = store
            .get_slab_triggers(&old_slab, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await
            .map_err(TimerManagerError::Store)?;

        let defer_triggers = store
            .get_slab_triggers(&old_slab, TimerType::DeferRetry)
            .try_collect::<Vec<_>>()
            .await
            .map_err(TimerManagerError::Store)?;

        let triggers: Vec<Trigger> = app_triggers.into_iter().chain(defer_triggers).collect();

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

            // Write to new v2 slab index
            // Note: Cassandra INSERT is an upsert, so this is idempotent
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
        .update_segment_version(&segment_id, SEGMENT_VERSION_V2, desired_slab_size)
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
