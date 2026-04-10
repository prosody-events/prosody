//! Adapts `TriggerOperations` to implement `TriggerStore`.
//!
//! This module provides the `TableAdapter` struct that wraps a type
//! implementing `TriggerOperations` (22 primitive methods) and provides the
//! public `TriggerStore` interface (13 methods) with coordinated dual-table
//! operations.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use crate::timers::{TimerType, Trigger};
use futures::Stream;
use futures::future::try_join_all;
use std::future::{Future, ready};
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::try_join;
use tracing::{debug, instrument};

/// Adapts `TriggerOperations` to implement `TriggerStore`.
///
/// This struct wraps a type implementing `TriggerOperations` (22 primitive
/// methods) and provides the public `TriggerStore` interface (13 methods)
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
    // Segment accessors
    // ===================================================================

    fn segment(&self) -> Segment {
        self.operations.segment().clone()
    }

    fn segment_id(&self) -> SegmentId {
        self.operations.segment().id
    }

    fn slab_size(&self) -> CompactDuration {
        self.operations.segment().slab_size
    }

    // ===================================================================
    // Pass-through methods: Delegate directly to operations
    // ===================================================================

    fn get_segment(&self) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send {
        self.operations.get_segment()
    }

    fn insert_segment(&self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations.insert_segment()
    }

    fn get_slab_range(
        &self,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
        self.operations.get_slab_range(range)
    }

    fn get_slab_triggers_all_types(
        &self,
        slab_id: SlabId,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let slab = Slab::new(slab_id, self.slab_size());
        self.operations.get_slab_triggers_all_types(slab)
    }

    fn delete_slab(&self, slab_id: SlabId) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations.delete_slab(slab_id)
    }

    fn get_key_times(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send {
        self.operations.get_key_times(timer_type, key)
    }

    fn get_key_triggers(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        self.operations.get_key_triggers(timer_type, key)
    }

    // ===================================================================
    // Coordinated writes: Use try_join! for best-effort dual-table
    // consistency
    // ===================================================================

    #[instrument(level = "debug", skip(self, trigger), err)]
    async fn add_trigger(&self, trigger: Trigger) -> Result<(), Self::Error> {
        let slab = Slab::from_time(self.slab_size(), trigger.time);
        // Coordinate: slab metadata + slab trigger + key trigger
        try_join!(
            self.operations.insert_slab(slab.clone()),
            self.operations.insert_slab_trigger(slab, trigger.clone()),
            self.operations.insert_key_trigger(trigger),
        )?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn remove_trigger(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        let slab = Slab::from_time(self.slab_size(), time);
        // Coordinate: delete from both tables
        try_join!(
            self.operations
                .delete_slab_trigger(&slab, timer_type, key, time),
            self.operations.delete_key_trigger(timer_type, key, time),
        )?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self, trigger), err)]
    async fn clear_and_schedule(&self, trigger: Trigger) -> Result<(), Self::Error> {
        // INVARIANT (at-least-once delivery): Write new timer FIRST, then
        // delete old entries. If crash occurs after writing new but before
        // deleting old, both timers exist (may fire twice). If we deleted
        // first and crashed, timer would be lost (never fires). The ordering
        // below enforces: Step 2 (write new) completes before Step 3 (delete
        // old).

        let slab_size = self.slab_size();
        let new_slab = Slab::from_time(slab_size, trigger.time);
        let key = trigger.key.clone();
        let timer_type = trigger.timer_type;

        // Step 1: Collect old trigger times from key index before any writes.
        // The key index is partitioned by key — no client-side filtering needed.
        // Slab derivation is deferred to Step 3.
        let old_times: Vec<CompactDateTime> = {
            use futures::TryStreamExt;
            self.operations
                .get_key_triggers(timer_type, &key)
                .try_filter(|t| ready(t.time != trigger.time))
                .map_ok(|t| t.time)
                .try_collect()
                .await?
        };

        debug!(
            key = %key,
            timer_type = ?timer_type,
            new_time = ?trigger.time,
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
            self.operations.insert_slab(new_slab.clone()),
            self.operations
                .insert_slab_trigger(new_slab, trigger.clone()),
            self.operations.clear_and_schedule_key(trigger),
        )?;

        debug!(
            key = %key,
            timer_type = ?timer_type,
            "clear_and_schedule: new timer written to slab + key indices; proceeding to delete old slab entries"
        );

        // Step 3: Delete old entries from slab index only.
        // Key index was already cleared atomically in Step 2.
        // INVARIANT (at-least-once): This runs AFTER Step 2 completes, so
        // the new timer is guaranteed to exist before old entries are removed.
        // Each delete targets a different clustering row, so concurrent deletes
        // to the same partition are safe.
        let delete_futures = old_times.iter().map(|&old_time| {
            let old_slab = Slab::from_time(slab_size, old_time);
            let ops = &self.operations;
            let key = &key;
            async move {
                debug!(
                    key = %key,
                    timer_type = ?timer_type,
                    old_time = ?old_time,
                    old_slab_id = ?old_slab.id(),
                    "clear_and_schedule: deleting old slab entry"
                );
                ops.delete_slab_trigger(&old_slab, timer_type, key, old_time)
                    .await
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
