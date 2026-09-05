use crate::Key;
use crate::error::ClassifyError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, SegmentVersion};
use crate::timers::{TimerType, Trigger};
use futures::Stream;
use smallvec::SmallVec;
use std::error::Error;
use std::future::Future;
use std::ops::RangeInclusive;

/// The timer store trait for individual table operations.
///
/// [`CassandraTriggerStore`] and [`InMemoryTriggerStore`] implement this trait.
/// Store clones must observe every completed write to their shared rows.
///
/// [`CassandraTriggerStore`]: super::cassandra::CassandraTriggerStore
/// [`InMemoryTriggerStore`]: super::memory::InMemoryTriggerStore
pub trait TriggerStore: Clone + Send + Sync + 'static {
    /// Test only: a store over the same rows with an empty cache.
    #[cfg(test)]
    #[must_use]
    fn cold(&self) -> Self {
        self.clone()
    }

    /// Error type for storage operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Returns the segment this store is scoped to.
    fn segment(&self) -> &Segment;

    // =========================================================================
    // Segment Operations (3 methods)
    // =========================================================================

    /// Persists this store's segment configuration.
    fn insert_segment(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Retrieves this store's segment from persistent storage.
    fn get_segment(&self) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send;

    /// Deletes this store's segment and all associated metadata.
    fn delete_segment(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // =========================================================================
    // Slab Metadata Operations (4 methods)
    // =========================================================================

    /// Lists all slab IDs in this store's segment.
    fn get_slabs(&self) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send;

    /// Lists slab IDs in a specified inclusive range within this store's
    /// segment.
    fn get_slab_range(
        &self,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send;

    /// Registers (inserts) a slab ID under this store's segment.
    fn insert_slab(&self, slab: Slab) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unregisters (deletes) a slab ID from this store's segment.
    fn delete_slab(&self, slab_id: SlabId) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Reads the persisted `slab_watermark` for this segment.
    ///
    /// `None` = pre-migration / fresh segment → callers should treat as
    /// "scan from slab 0". When `Some(w)`, every slab clustering row in this
    /// segment has `slab_id > w` (invariant I1).
    fn get_slab_watermark(
        &self,
    ) -> impl Future<Output = Result<Option<SlabId>, Self::Error>> + Send;

    /// Persists `slab_watermark` for this segment as a single UPDATE.
    ///
    /// Used by the cleanup path to *raise* the watermark when older slabs
    /// have been deleted. The plain `set` path never lowers the watermark
    /// during cleanup — that combined write goes through
    /// [`Self::batch_insert_slab_with_watermark`].
    fn set_slab_watermark(
        &self,
        watermark: Option<SlabId>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Atomically inserts a slab clustering row **and** lowers
    /// `slab_watermark` in one UNLOGGED BATCH on the segment partition.
    ///
    /// Used on the past-time insert path — `slab.id() <= current_watermark`.
    /// Atomicity guarantees I1 across crashes: after this returns, either
    /// both writes are visible or neither is.
    fn batch_insert_slab_with_watermark(
        &self,
        slab: Slab,
        watermark: Option<SlabId>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // =========================================================================
    // Slab Trigger Operations (4 methods)
    // =========================================================================

    /// Streams all triggers of a specific type within a slab's time range.
    fn get_slab_triggers(
        &self,
        slab: &Slab,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    /// Streams ALL triggers within a slab across all timer types.
    fn get_slab_triggers_all_types(
        &self,
        slab: Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    /// Inserts a trigger into the slab index.
    fn insert_slab_trigger(
        &self,
        slab: Slab,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes a specific trigger from a slab's index.
    fn delete_slab_trigger(
        &self,
        slab: &Slab,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Clears all triggers from a slab's index across ALL timer types.
    fn clear_slab_triggers(
        &self,
        slab: &Slab,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // =========================================================================
    // Key Trigger Operations (7 methods)
    // =========================================================================

    /// Streams all scheduled times for a given key and timer type.
    fn get_key_times(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<(CompactDateTime, i32), Self::Error>> + Send;

    /// Streams all triggers for a given key and timer type.
    fn get_key_triggers(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    /// Streams ALL triggers for a given key across all timer types.
    fn get_key_triggers_all_types(
        &self,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    /// Upserts a trigger into the key-based index.
    ///
    /// The logical identity is `(key, time, timer_type)`. If that identity is
    /// already present, implementations must replace mutable metadata such as
    /// `tag` and span context rather than treating the call as a distinct
    /// timer.
    fn upsert_key_trigger(
        &self,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Replaces one key entry. Other coordinates keep their entries.
    fn replace_key_trigger(
        &self,
        old: &Trigger,
        new: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes a specific trigger from the key-based index.
    fn delete_key_trigger(
        &self,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Clears all triggers for a specific key and timer type.
    fn clear_key_triggers(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Atomically clears existing timers and schedules a new one in the key
    /// index.
    ///
    /// This is the key-index-only primitive for tombstone-free singleton
    /// overwrites. For Cassandra, this uses a BATCH to atomically DELETE
    /// clustering rows and UPDATE the static singleton slot. For in-memory
    /// stores, this clears and inserts.
    ///
    /// Returns the old trigger times that were cleared (excluding the new
    /// trigger's own time). Callers use these to clean up the slab index
    /// without a separate pre-read.
    fn clear_and_schedule_key(
        &self,
        trigger: Trigger,
    ) -> impl Future<Output = Result<SmallVec<[CompactDateTime; 1]>, Self::Error>> + Send;

    /// Clears all triggers from the key index for a given key, across ALL timer
    /// types.
    fn clear_key_triggers_all_types(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // =========================================================================
    // Tag Operations (2 methods)
    // =========================================================================

    /// Updates the key row tag. The slab row keeps its original tag.
    /// The caller must first confirm that the key row exists.
    fn update_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
        new_tag: i32,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Reads the `tag` from a key-index clustering row.
    ///
    /// Returns `None` if the row is absent ("committed" in oracle terms).
    /// Returns `Some(0)` for rows with a `NULL` tag (pre-migration rows).
    fn current_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Option<i32>, Self::Error>> + Send;

    // =========================================================================
    // Version Management (1 method)
    // =========================================================================

    /// Updates the schema version and slab size for this store's segment.
    fn update_segment_version(
        &self,
        new_version: SegmentVersion,
        new_slab_size: CompactDuration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
