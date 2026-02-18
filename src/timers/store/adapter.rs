//! Adapts `TriggerOperations` to implement `TriggerStore`.
//!
//! This module provides the `TableAdapter` struct that wraps a type
//! implementing `TriggerOperations` (20 primitive methods) and provides the
//! public `TriggerStore` interface (9 methods) with coordinated dual-table
//! operations.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentId, SlotState, TimerSlot, TriggerStore};
use crate::timers::{TimerType, Trigger};
use futures::Stream;
use futures::future::try_join_all;
use std::collections::HashMap;
use std::future::{Future, ready};
use std::hash::BuildHasher;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::try_join;
use tracing::{debug, instrument};

/// Determines the slot state for a timer type from a singleton map column
/// value.
///
/// Interprets the `singleton_timers` CQL map column into the three possible
/// slot states:
///
/// - **Absent**: Column is NULL, or map has no entry for this type (legacy data
///   or empty).
/// - **Singleton**: Map entry has a value (timer stored in static slot).
/// - **Overflow**: Map entry is NULL (multiple timers, scan clustering
///   columns).
///
/// # Arguments
///
/// * `singleton_map` - The deserialized `singleton_timers` static column.
///   `None` if the column itself is NULL (legacy partition without the column).
/// * `timer_type` - The timer type to look up in the map.
#[must_use]
pub fn determine_slot_state<S: BuildHasher>(
    singleton_map: Option<&HashMap<i8, Option<TimerSlot>, S>>,
    timer_type: TimerType,
) -> SlotState {
    let Some(map) = singleton_map else {
        return SlotState::Absent;
    };

    match map.get(&i8::from(timer_type)) {
        None => SlotState::Absent,
        Some(None) => SlotState::Overflow,
        Some(Some(slot)) => SlotState::Singleton(slot.clone()),
    }
}

/// Adapts `TriggerOperations` to implement `TriggerStore`.
///
/// This struct wraps a type implementing `TriggerOperations` (20 primitive
/// methods) and provides the public `TriggerStore` interface (9 methods)
/// with coordinated dual-table operations.
///
/// Uses `Arc` for cheap cloning and best-effort consistency via `try_join!`.
///
/// **Visibility**: `pub` but not re-exported from `store/mod.rs`.
/// Used by factory functions in implementation modules (cassandra/mod.rs,
/// memory.rs) which return concrete `TableAdapter<T>` types.
///
/// # Example
///
/// ```rust,ignore
/// // Factory functions return concrete TableAdapter:
/// pub fn cassandra_store(...) -> TableAdapter<CassandraTriggerStore> {
///     let cassandra = CassandraTriggerStore::new(...);
///     TableAdapter::new(cassandra)
/// }
///
/// // Consumer usage:
/// let store = cassandra_store(...);  // TableAdapter<CassandraTriggerStore>: TriggerStore
/// let manager = TimerManager::new(..., store);
/// ```
#[derive(Clone)]
pub struct TableAdapter<T> {
    operations: Arc<T>,
}

impl<T> TableAdapter<T> {
    /// Creates a new `TableAdapter` wrapping the given operations.
    pub fn new(operations: T) -> Self {
        Self {
            operations: Arc::new(operations),
        }
    }

    /// Returns a reference to the underlying operations.
    ///
    /// Provides access to low-level `TriggerOperations` methods for cases
    /// where direct primitive access is needed (e.g., migration, internal
    /// maintenance operations).
    #[must_use]
    pub fn operations(&self) -> &T {
        self.operations.as_ref()
    }
}

/// Implements the public `TriggerStore` interface using internal
/// `TriggerOperations`.
impl<T> TriggerStore for TableAdapter<T>
where
    T: TriggerOperations,
{
    type Error = T::Error;

    // ===================================================================
    // Pass-through methods (7 methods): Delegate directly to operations
    // ===================================================================

    fn get_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send {
        self.operations.get_segment(segment_id)
    }

    fn insert_segment(
        &self,
        segment: Segment,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations.insert_segment(segment)
    }

    fn get_slab_range(
        &self,
        segment_id: &SegmentId,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
        self.operations.get_slab_range(segment_id, range)
    }

    fn get_slab_triggers_all_types(
        &self,
        slab: &Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        self.operations.get_slab_triggers_all_types(slab)
    }

    fn delete_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations.delete_slab(segment_id, slab_id)
    }

    fn get_key_times(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send {
        self.operations.get_key_times(segment_id, timer_type, key)
    }

    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        self.operations
            .get_key_triggers(segment_id, timer_type, key)
    }

    // ===================================================================
    // Coordinated writes (2 methods): Use try_join! for best-effort dual-table
    // consistency
    // ===================================================================

    #[instrument(level = "debug", skip(self, segment, slab, trigger), err)]
    async fn add_trigger(
        &self,
        segment: &Segment,
        slab: Slab,
        trigger: Trigger,
    ) -> Result<(), Self::Error> {
        // Coordinate: slab metadata + slab trigger + key trigger
        try_join!(
            self.operations.insert_slab(&segment.id, slab.clone()),
            self.operations.insert_slab_trigger(slab, trigger.clone()),
            self.operations.insert_key_trigger(&segment.id, trigger),
        )?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self, segment, slab), err)]
    async fn remove_trigger(
        &self,
        segment: &Segment,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        // Coordinate: delete from both tables
        try_join!(
            self.operations
                .delete_slab_trigger(slab, timer_type, key, time),
            self.operations
                .delete_key_trigger(&segment.id, timer_type, key, time),
        )?;
        Ok(())
    }

    #[instrument(
        level = "debug",
        skip(self, segment, new_slab, new_trigger, old_slabs),
        err
    )]
    async fn clear_and_schedule(
        &self,
        segment: &Segment,
        new_slab: Slab,
        new_trigger: Trigger,
        old_slabs: Vec<Slab>,
    ) -> Result<(), Self::Error> {
        // INVARIANT (at-least-once delivery): Write new timer FIRST, then
        // delete old entries. If crash occurs after writing new but before
        // deleting old, both timers exist (may fire twice). If we deleted
        // first and crashed, timer would be lost (never fires). The ordering
        // below enforces: Step 2 (write new) completes before Step 3 (delete
        // old).

        let key = new_trigger.key.clone();
        let timer_type = new_trigger.timer_type;

        // Step 1: Collect old trigger times from slab index before any writes.
        // Each slab is queried concurrently since they target different
        // partitions and have no ordering dependency.
        let slab_read_futures = old_slabs.iter().map(|old_slab| {
            let ops = &self.operations;
            let key = &key;
            async move {
                use futures::TryStreamExt;
                let triggers: Vec<_> = ops
                    .get_slab_triggers(old_slab, timer_type)
                    .try_filter(|t| ready(t.key == *key))
                    .try_collect()
                    .await?;
                Ok::<_, Self::Error>(
                    triggers
                        .into_iter()
                        .map(|t| (old_slab.clone(), t.time))
                        .collect::<Vec<_>>(),
                )
            }
        });
        let old_times: Vec<_> = try_join_all(slab_read_futures)
            .await?
            .into_iter()
            .flatten()
            .collect();

        debug!(
            key = %key,
            timer_type = ?timer_type,
            new_time = ?new_trigger.time,
            new_slab_id = ?new_slab.id(),
            old_entry_count = old_times.len(),
            "clear_and_schedule: writing new timer to both indices before deleting old entries"
        );

        // Step 2 (DUAL-INDEX WRITE): Write new timer to slab index AND
        // atomically clear+schedule in key index. Both indices are written
        // concurrently via try_join!, ensuring the new timer exists in BOTH
        // timer_typed_slabs and timer_typed_keys before any old data is
        // removed. The key index operation uses singleton slot optimization
        // (for Cassandra):
        // - Atomically DELETEs all clustering rows for this key/type
        // - UPDATEs the static singleton slot with the new timer
        // For singleton→singleton replacement, no tombstones are created.
        try_join!(
            self.operations.insert_slab(&segment.id, new_slab.clone()),
            self.operations
                .insert_slab_trigger(new_slab, new_trigger.clone()),
            self.operations
                .clear_and_schedule_key(&segment.id, new_trigger),
        )?;

        debug!(
            key = %key,
            timer_type = ?timer_type,
            "clear_and_schedule: new timer written to slab + key indices; proceeding to delete old slab entries"
        );

        // Step 3: Delete old entries from slab index only, grouped by slab
        // and parallelized across slabs.
        // Key index was already cleared atomically in Step 2.
        // INVARIANT (at-least-once): This runs AFTER Step 2 completes, so
        // the new timer is guaranteed to exist before old entries are removed.
        //
        // Group entries by slab_id so deletes targeting the same slab
        // partition run sequentially (avoiding contention), while different
        // slabs are deleted concurrently.
        let mut grouped: HashMap<_, Vec<_>> = HashMap::new();
        for (old_slab, old_time) in &old_times {
            grouped
                .entry(old_slab.id())
                .or_default()
                .push((old_slab, old_time));
        }

        let delete_futures = grouped.values().map(|entries| {
            let ops = &self.operations;
            let key = &key;
            async move {
                for &(old_slab, old_time) in entries {
                    debug!(
                        key = %key,
                        timer_type = ?timer_type,
                        old_time = ?old_time,
                        old_slab_id = ?old_slab.id(),
                        "clear_and_schedule: deleting old slab entry"
                    );
                    ops.delete_slab_trigger(old_slab, timer_type, key, *old_time)
                        .await?;
                }
                Ok::<_, Self::Error>(())
            }
        });
        try_join_all(delete_futures).await?;

        debug!(
            key = %key,
            timer_type = ?timer_type,
            deleted_count = old_times.len(),
            "clear_and_schedule: complete"
        );

        Ok(())
    }
}
