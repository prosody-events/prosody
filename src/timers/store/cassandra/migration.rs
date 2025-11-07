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
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use tokio::task::coop::cooperative;
use tokio::try_join;
use tracing::{debug, info, instrument, warn};

use super::{CassandraTriggerStore, CassandraTriggerStoreError};

/// Migrates a segment if necessary, performing both version and slab size
/// migrations.
///
/// This function orchestrates the two-phase migration process:
/// 1. **Phase 1**: V1→V2 schema migration (if needed)
/// 2. **Phase 2**: Slab size migration (if needed)
///
/// Each phase is performed atomically and independently. If no migrations are
/// needed, the segment is returned unchanged.
///
/// # Arguments
///
/// * `store` - The Cassandra trigger store
/// * `segment` - The segment to potentially migrate
/// * `desired_slab_size` - The target slab size for Phase 2 migration
///
/// # Returns
///
/// The segment with updated version and/or `slab_size` if migrations were
/// performed.
///
/// # Errors
///
/// Returns [`TimerManagerError`] if any migration phase fails.
#[instrument(level = "debug", skip(store), err)]
pub async fn migrate_segment_if_needed(
    store: &CassandraTriggerStore,
    mut segment: Segment,
    desired_slab_size: CompactDuration,
) -> Result<Segment, TimerManagerError<CassandraTriggerStoreError>> {
    // Phase 1: V1→V2 schema migration
    if needs_migration(&segment) {
        segment = migrate_segment_version(store, segment).await?;
    }

    // Phase 2: Slab size migration
    if needs_slab_size_migration(&segment, desired_slab_size) {
        segment = migrate_slab_size(store, segment, desired_slab_size).await?;
    }

    Ok(segment)
}

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
    mut segment: Segment,
) -> Result<Segment, TimerManagerError<CassandraTriggerStoreError>> {
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

    // Update segment version and return
    segment.version = SegmentVersion::V2;
    Ok(segment)
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

/// Migrates triggers from old slabs to new slabs with recalculated slab IDs.
///
/// Processes slabs concurrently (bounded by [`MIGRATION_SLAB_CONCURRENCY`]),
/// streaming triggers within each slab sequentially. Returns the set of new
/// slab IDs created during migration.
///
/// # Arguments
///
/// * `store` - The Cassandra trigger store
/// * `segment` - The segment being migrated
/// * `old_slab_ids` - Slab IDs with the old `slab_size`
/// * `desired_slab_size` - Target slab size for recalculation
///
/// # Returns
///
/// Set of all new slab IDs created during migration
///
/// # Errors
///
/// Returns [`TimerManagerError`] if reading or writing triggers fails
async fn migrate_triggers_to_new_slabs(
    store: &CassandraTriggerStore,
    segment: &Segment,
    old_slab_ids: &[SlabId],
    desired_slab_size: CompactDuration,
) -> Result<HashSet<SlabId>, TimerManagerError<CassandraTriggerStoreError>> {
    // Process slabs concurrently, streaming triggers within each slab
    let new_slab_id_sets: Vec<HashSet<SlabId>> = stream::iter(old_slab_ids.iter().copied())
        .map(|old_slab_id| {
            let store = store.clone();

            async move {
                let segment_id = segment.id;
                let old_slab_size = segment.slab_size;
                let old_slab = Slab::new(segment_id, old_slab_id, old_slab_size);
                let mut new_slab_ids = HashSet::default();
                let trigger_stream = store.get_slab_triggers_all_types(&old_slab);
                tokio::pin!(trigger_stream);

                while let Some(trigger) = cooperative(trigger_stream.try_next())
                    .await
                    .map_err(TimerManagerError::Store)?
                {
                    let new_slab = Slab::from_time(segment_id, desired_slab_size, trigger.time);

                    // Register new slab metadata if first time seeing this ID
                    if new_slab_ids.insert(new_slab.id()) {
                        store
                            .insert_slab(&segment_id, new_slab.clone())
                            .await
                            .map_err(TimerManagerError::Store)?;
                    }

                    // Write trigger to new slab (key table unchanged)
                    store
                        .insert_slab_trigger(new_slab, trigger)
                        .await
                        .map_err(TimerManagerError::Store)?;
                }

                debug!(
                    "Migrated old slab {old_slab_id} to {} new unique slabs",
                    new_slab_ids.len()
                );

                Ok::<_, TimerManagerError<CassandraTriggerStoreError>>(new_slab_ids)
            }
        })
        .buffer_unordered(MIGRATION_SLAB_CONCURRENCY)
        .try_collect()
        .await?;

    // Merge all new slab IDs from all old slabs
    let new_slab_ids: HashSet<SlabId> = new_slab_id_sets.into_iter().flatten().collect();

    debug!(
        "Migrated {} old slabs → {} new unique slab IDs",
        old_slab_ids.len(),
        new_slab_ids.len()
    );

    Ok(new_slab_ids)
}

/// Cleans up old slabs with schema-aware overlap protection.
///
/// **Critical schema insight:** `timer_typed_slabs` has partition key
/// `(segment_id, slab_size, id)`, meaning old and new triggers with the SAME
/// `slab_id` but DIFFERENT `slab_size` are in SEPARATE partitions.
///
/// **Cleanup strategy:**
/// - Metadata (`timer_segments`): Skip deletion for reused slab IDs (shared
///   row)
/// - Triggers (`timer_typed_slabs`): ALWAYS delete old partition (separate from
///   new)
///
/// **Example with reused `slab_id=2`:**
/// - Old partition: `(segment_id, slab_size=100, id=2)` ← DELETE
/// - New partition: `(segment_id, slab_size=30, id=2)` ← Keep
/// - Metadata row: `(segment_id, slab_id=2)` ← Shared, don't delete
///
/// # Arguments
///
/// * `store` - The Cassandra trigger store
/// * `segment` - The segment being migrated
/// * `old_slab_ids` - Original slab IDs before migration
/// * `new_slab_ids` - New slab IDs created during migration
async fn cleanup_old_slabs_with_overlap_protection(
    store: &CassandraTriggerStore,
    segment: &Segment,
    old_slab_ids: Vec<SlabId>,
    new_slab_ids: &HashSet<SlabId>,
) {
    let old_slab_size = segment.slab_size;
    let old_slab_count = old_slab_ids.len();
    let reused_count = old_slab_ids
        .iter()
        .filter(|id| new_slab_ids.contains(id))
        .count();

    if reused_count > 0 {
        debug!(
            "{} slab IDs reused by new slab_size (will skip metadata deletion, but clear triggers)",
            reused_count
        );
    }

    debug!(
        "Cleaning up {} old slabs for segment {}",
        old_slab_count, segment.id
    );

    // Process ALL old slab IDs: conditionally delete metadata, always clear
    // triggers
    stream::iter(old_slab_ids)
        .for_each_concurrent(DELETE_CONCURRENCY, |slab_id| {
            let store = store.clone();
            let is_reused = new_slab_ids.contains(&slab_id);

            async move {
                let segment_id = segment.id;

                // Delete metadata ONLY if slab_id is not reused (shared row in timer_segments)
                if !is_reused && let Err(error) = store.delete_slab(&segment_id, slab_id).await {
                    warn!(
                        "Failed to delete metadata for old slab {slab_id} (segment {segment_id}): \
                         {error:#}"
                    );
                    // Continue to clear triggers even if metadata deletion
                    // fails
                }

                // ALWAYS clear old triggers - they're in a separate partition due to slab_size
                // in partition key: (segment_id, OLD_slab_size, id) != (segment_id,
                // NEW_slab_size, id)
                let old_slab = Slab::new(segment_id, slab_id, old_slab_size);
                if let Err(error) = store.clear_slab_triggers(&old_slab).await {
                    warn!(
                        "Failed to clear triggers for old slab {slab_id} (segment {segment_id}): \
                         {error:#}"
                    );
                }
            }
        })
        .await;

    debug!(
        "Cleaned up {} old slabs for segment {} ({} metadata deletions skipped for reused IDs)",
        old_slab_count, segment.id, reused_count
    );
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
    mut segment: Segment,
    desired_slab_size: CompactDuration,
) -> Result<Segment, TimerManagerError<CassandraTriggerStoreError>> {
    let segment_id = segment.id;
    let old_slab_size = segment.slab_size;

    // Verify segment is v2
    if segment.version != SegmentVersion::V2 {
        warn!(
            "Cannot migrate slab_size for segment {segment_id}: segment is not v2 (version = \
             {:?}). Run version migration first.",
            segment.version
        );
        return Ok(segment);
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

    // Phase 2: Migrate triggers to new slabs with recalculated slab IDs
    let new_slab_ids =
        migrate_triggers_to_new_slabs(store, &segment, &old_slab_ids, desired_slab_size).await?;

    // Phase 3: Update segment slab_size (atomic marker indicating migration
    // complete)
    store
        .update_segment_version(&segment_id, SegmentVersion::V2, desired_slab_size)
        .await
        .map_err(TimerManagerError::Store)?;

    info!(
        "Successfully migrated segment {segment_id}, slab_size updated from {old_slab_size} to \
         {desired_slab_size}"
    );

    // Phase 4: Clean up old slabs with overlap protection (best-effort)
    cleanup_old_slabs_with_overlap_protection(store, &segment, old_slab_ids, &new_slab_ids).await;

    // Update segment slab_size and return
    segment.slab_size = desired_slab_size;
    Ok(segment)
}
