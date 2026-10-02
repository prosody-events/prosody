//! V1 operations that keep the slab and key indices consistent. Each writes
//! both indices through one [`V1Operations`].

use super::V1Operations;
use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::SlabId;
use crate::timers::store::cassandra::CassandraTriggerStoreError;
use crate::timers::store::{SegmentId, TriggerV1};
use tracing::instrument;

/// Adds a v1 trigger to both slab and key indices.
///
/// High-level coordinated operation that maintains dual-index consistency
/// by inserting the trigger into both:
/// 1. The slab index (`timer_slabs` table) - for time-based queries
/// 2. The key index (`timer_keys` table) - for entity-based queries
///
/// Both operations execute in parallel using `try_join!` for efficiency,
/// so if one insertion fails the other may still have succeeded, leaving
/// the indices inconsistent until the caller retries.
#[instrument(level = "debug", skip(ops, trigger), err)]
pub(super) async fn add_trigger(
    ops: &V1Operations,
    segment_id: &SegmentId,
    slab_id: SlabId,
    trigger: TriggerV1,
) -> Result<(), CassandraTriggerStoreError> {
    use tokio::try_join;

    // Insert into both indices in parallel
    try_join!(
        ops.insert_slab_trigger(segment_id, slab_id, trigger.clone()),
        ops.insert_key_trigger(segment_id, trigger)
    )?;

    Ok(())
}

/// Removes a v1 trigger from both slab and key indices.
///
/// High-level coordinated operation that maintains dual-index consistency
/// by deleting the trigger from both:
/// 1. The slab index (`timer_slabs` table)
/// 2. The key index (`timer_keys` table)
///
/// Both operations execute in parallel using `try_join!` for efficiency,
/// so if one deletion fails the other may still have succeeded, leaving
/// the indices inconsistent until the caller retries.
#[instrument(level = "debug", skip(ops), err)]
pub(super) async fn remove_trigger(
    ops: &V1Operations,
    segment_id: &SegmentId,
    slab_id: SlabId,
    key: &Key,
    time: CompactDateTime,
) -> Result<(), CassandraTriggerStoreError> {
    use tokio::try_join;

    // Delete from both indices in parallel
    try_join!(
        ops.delete_slab_trigger(segment_id, slab_id, key, time),
        ops.delete_key_trigger(segment_id, key, time)
    )?;

    Ok(())
}

/// Clears all v1 triggers for a key from both slab and key indices.
///
/// High-level coordinated operation that maintains dual-index consistency
/// by:
/// 1. Querying all triggers for the key from the key index
/// 2. Calculating the slab ID for each trigger using the provided slab size
/// 3. Deleting each trigger from its respective slab index (concurrently)
/// 4. Clearing all triggers from the key index
///
/// If an error interrupts processing partway through, some triggers may
/// already be deleted from one index but not the other.
#[instrument(level = "debug", skip(ops), err)]
pub(super) async fn clear_triggers_for_key(
    ops: &V1Operations,
    segment_id: &SegmentId,
    key: &Key,
    slab_size: CompactDuration,
) -> Result<(), CassandraTriggerStoreError> {
    use crate::timers::DELETE_CONCURRENCY;
    use crate::timers::slab::Slab;
    use futures::TryStreamExt;

    let segment_id_copy = *segment_id;
    let stream = ops.get_key_triggers(segment_id, key);

    // Delete each trigger from its slab index
    stream
        .try_for_each_concurrent(DELETE_CONCURRENCY, move |trigger| {
            let ops_clone = ops.clone();
            async move {
                // Calculate which slab this trigger belongs to
                let slab = Slab::from_time(slab_size, trigger.time);
                ops_clone
                    .delete_slab_trigger(&segment_id_copy, slab.id(), &trigger.key, trigger.time)
                    .await
            }
        })
        .await?;

    // Clear all triggers from key index
    ops.clear_key_triggers(segment_id, key).await?;

    Ok(())
}

/// Deletes a v1 slab and all its triggers from both slab and key indices.
///
/// High-level coordinated operation that maintains dual-index consistency
/// by:
/// 1. Getting all triggers in the slab
/// 2. Deleting each trigger from the key index
/// 3. Clearing all triggers from the slab index
/// 4. Deleting the slab metadata
///
/// If an error interrupts processing partway through, some steps may
/// already be complete while others are not.
#[instrument(level = "debug", skip(ops), err)]
pub(super) async fn delete_slab(
    ops: &V1Operations,
    segment_id: &SegmentId,
    slab_id: SlabId,
) -> Result<(), CassandraTriggerStoreError> {
    use crate::timers::DELETE_CONCURRENCY;
    use futures::TryStreamExt;
    use tokio::try_join;

    let segment_id_copy = *segment_id;
    let stream = ops.get_slab_triggers(segment_id, slab_id);

    // Delete each trigger from key index
    stream
        .try_for_each_concurrent(DELETE_CONCURRENCY, move |trigger| {
            let ops_clone = ops.clone();
            async move {
                ops_clone
                    .delete_key_trigger(&segment_id_copy, &trigger.key, trigger.time)
                    .await
            }
        })
        .await?;

    // Clear slab triggers and metadata in parallel
    try_join!(
        ops.clear_slab_triggers(&segment_id_copy, slab_id),
        ops.delete_slab_metadata(&segment_id_copy, slab_id)
    )?;

    Ok(())
}
