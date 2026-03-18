//! Internal trait for primitive storage operations.
//!
//! This module defines the `TriggerOperations` trait used by Cassandra and
//! Memory implementations. It provides 22 primitive methods that operate on
//! individual tables without coordinating across tables.
//!
//! **Not part of the public API.** Use `TriggerStore` instead.

use crate::Key;
use crate::error::ClassifyError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, SegmentVersion};
use crate::timers::{TimerType, Trigger};
use futures::Stream;
use std::error::Error;
use std::future::Future;
use std::ops::RangeInclusive;

/// Internal trait for primitive storage operations.
///
/// This trait provides 22 primitive methods that operate on individual
/// tables without coordinating across tables. It is the trait bound for
/// `TableAdapter<T>`, which is part of the public API.
///
/// **Users should not implement this trait directly.** Use `TriggerStore`
/// instead.
///
/// # Used by
///
/// - `CassandraTriggerStore` implementation
/// - `InMemoryTriggerStore` implementation
/// - `TableAdapter` to implement `TriggerStore`
///
/// # Visibility
///
/// This trait is `pub` to satisfy Rust's visibility rules (used in public
/// `TableAdapter`), but is not re-exported from `store/mod.rs`, keeping it
/// effectively internal.
pub trait TriggerOperations: Clone + Send + Sync + 'static {
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

    // =========================================================================
    // Slab Trigger Operations (5 methods)
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
        slab: &Slab,
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
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send;

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

    /// Inserts a trigger into the key-based index.
    fn insert_key_trigger(
        &self,
        trigger: Trigger,
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
    /// # Arguments
    ///
    /// * `trigger` - The new trigger to schedule (replaces all existing for
    ///   key/type)
    fn clear_and_schedule_key(
        &self,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Clears all triggers from the key index for a given key, across ALL timer
    /// types.
    fn clear_key_triggers_all_types(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

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
