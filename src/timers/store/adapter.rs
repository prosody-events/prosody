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
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use crate::timers::{TimerType, Trigger};
use futures::Stream;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::try_join;

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
    /// PRIVATE - DO NOT MAKE PUBLIC OR PUB(CRATE) OR PUB(SUPER).
    /// Tests must NEVER call this method.
    ///
    /// Test organization:
    /// - High-level tests: Use TriggerStore trait methods only
    /// - Low-level tests: Construct TriggerOperations implementations directly
    ///   (not via TableAdapter)
    fn operations(&self) -> &T {
        &self.operations
    }
}

/// Implements the public `TriggerStore` interface using internal
/// `TriggerOperations`.
impl<T: TriggerOperations> TriggerStore for TableAdapter<T> {
    type Error = T::Error;

    // ===================================================================
    // Pass-through methods (7 methods): Delegate directly to operations
    // ===================================================================

    fn get_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl std::future::Future<Output = Result<Option<Segment>, Self::Error>> + Send {
        self.operations.get_segment(segment_id)
    }

    fn insert_segment(
        &self,
        segment: Segment,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
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
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
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

    fn add_trigger(
        &self,
        segment: &Segment,
        slab: Slab,
        trigger: Trigger,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let ops = Arc::clone(&self.operations);
        let segment_id = segment.id; // Extract segment ID
        async move {
            // Coordinate: slab metadata + slab trigger + key trigger
            try_join!(
                ops.insert_slab(&segment_id, slab.clone()),
                ops.insert_slab_trigger(slab, trigger.clone()),
                ops.insert_key_trigger(&segment_id, trigger),
            )?;
            Ok(())
        }
    }

    fn remove_trigger(
        &self,
        segment: &Segment,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let ops = Arc::clone(&self.operations);
        let segment_id = segment.id; // Extract segment ID
        async move {
            // Coordinate: delete from both tables
            try_join!(
                ops.delete_slab_trigger(slab, timer_type, key, time),
                ops.delete_key_trigger(&segment_id, timer_type, key, time),
            )?;
            Ok(())
        }
    }
}
