//! Persistent storage abstraction for timer data.
//!
//! This module provides the [`TriggerStore`] trait and related types for
//! persisting timer data across application restarts. The storage system is
//! designed to handle large volumes of timer events efficiently while providing
//! strong consistency guarantees.
//!
//! ## Architecture Overview
//!
//! The storage system uses a hierarchical organization:
//!
//! ```text
//! Segment (partition of timers by consumer group)
//!   ├── Slabs (time-based partitions for efficient queries)
//!   │   └── Triggers (individual timer events)
//!   └── Key Index (fast lookup by key)
//!       └── Times (scheduled times for each key)
//! ```
//!
//! ## Key Concepts
//!
//! - **Segments**: Top-level partitions that isolate timers by consumer group
//! - **Slabs**: Time-based partitions within segments for efficient range
//!   operations
//! - **Triggers**: Individual timer events with key, time, and metadata
//! - **Dual Indexing**: Both slab-based (time-oriented) and key-based
//!   (entity-oriented) indices
//!
//! ## Storage Implementations
//!
//! The crate provides several storage implementations:
//!
//! - [`memory::InMemoryTriggerStore`]: In-memory storage for testing and
//!   development
//! - Future implementations may include persistent stores like `ScyllaDB`,
//!   `PostgreSQL`, etc.
//!
//! ## Usage Patterns
//!
//! The [`TriggerStore`] trait supports several key usage patterns:
//!
//! ### Adding Timers
//! ```rust,no_run
//! use prosody::timers::store::TriggerStore;
//! # async fn example<T: TriggerStore>(store: &T) {
//! # use prosody::timers::store::{Segment, SegmentId};
//! # use prosody::timers::Trigger;
//! # use prosody::timers::duration::CompactDuration;
//! # use prosody::timers::datetime::CompactDateTime;
//! # use prosody::Key;
//! # use uuid::Uuid;
//! # use tracing::Span;
//! # let segment_id = SegmentId::new_v4();
//! # let segment = Segment {
//! #     id: segment_id,
//! #     name: "test".to_string(),
//! #     slab_size: CompactDuration::new(60),
//! # };
//! # let trigger = Trigger {
//! #     key: "test".into(),
//! #     time: CompactDateTime::now().unwrap(),
//! #     span: Span::none(),
//! # };
//! // Note: This is a simplified example - actual usage would involve slabs
//! // store.add_trigger(&segment, slab, trigger).await.unwrap();
//! # }
//! ```
//!
//! ### Querying by Time Range
//! ```rust,no_run
//! use futures::StreamExt;
//! use prosody::timers::store::TriggerStore;
//! # async fn example<T: TriggerStore>(store: &T) {
//! # use prosody::timers::store::SegmentId;
//! # use uuid::Uuid;
//! # let segment_id = SegmentId::new_v4();
//! let slab_ids: Vec<_> = store.get_slab_range(&segment_id, 0..100).collect().await;
//! # }
//! ```
//!
//! ### Querying by Key
//! ```rust,no_run
//! use futures::StreamExt;
//! use prosody::timers::store::TriggerStore;
//! # async fn example<T: TriggerStore>(store: &T) {
//! # use prosody::timers::store::SegmentId;
//! # use prosody::Key;
//! # use uuid::Uuid;
//! # let segment_id = SegmentId::new_v4();
//! # let key: Key = "test".into();
//! let times: Vec<_> = store.get_key_triggers(&segment_id, &key).collect().await;
//! # }
//! ```

use crate::Key;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::manager::DELETE_CONCURRENCY;
use crate::timers::slab::{Slab, SlabId};
use futures::{Stream, TryStreamExt};
use std::error::Error;
use std::ops::RangeBounds;
use tokio::try_join;
use uuid::Uuid;

/// In-memory storage implementation for testing and development.
pub mod memory;

#[cfg(test)]
/// Comprehensive test suite for [`TriggerStore`] implementations.
pub mod tests;

/// Unique identifier for timer segments.
///
/// Each segment represents a logical partition of timers, typically
/// corresponding to a consumer group or application instance. Using [`Uuid`]
/// ensures global uniqueness across distributed deployments.
pub type SegmentId = Uuid;

/// Represents a logical partition of timers with time-based organization.
///
/// A [`Segment`] defines how timers are partitioned and organized within the
/// storage system. Each segment has a unique identifier, human-readable name,
/// and configuration for time-based slab organization.
///
/// ## Purpose
///
/// Segments serve several purposes:
/// - **Isolation**: Separate timer data between different consumer groups
/// - **Organization**: Provide time-based partitioning via slab configuration
/// - **Scalability**: Enable horizontal scaling by distributing segments
///
/// ## Slab Organization
///
/// The `slab_size` determines how timers are partitioned by time:
/// - Smaller slabs: More precise time ranges, more metadata overhead
/// - Larger slabs: Broader time ranges, less metadata overhead
/// - Typical values: 5-60 minutes depending on timer density
#[derive(Clone, Debug)]
pub struct Segment {
    /// Unique identifier for this segment.
    pub id: SegmentId,

    /// Human-readable name for debugging and monitoring.
    pub name: String,

    /// Time span for each slab within this segment.
    ///
    /// Determines how timers are partitioned by time for efficient
    /// range queries and storage organization.
    pub slab_size: CompactDuration,
}

/// Persistent storage interface for timer data.
///
/// [`TriggerStore`] defines the interface for storing and retrieving timer data
/// with strong consistency guarantees. Implementations must provide efficient
/// operations for both time-based and key-based access patterns.
///
/// ## Design Principles
///
/// - **Dual Indexing**: Support both time-oriented (slab) and entity-oriented
///   (key) access
/// - **Consistency**: Maintain consistency between multiple indices
/// - **Efficiency**: Optimize for high-volume timer operations
/// - **Durability**: Survive application restarts and failures
///
/// ## Implementation Requirements
///
/// Implementations must ensure:
/// - **Atomicity**: Operations that modify multiple indices must be atomic
/// - **Consistency**: All indices must reflect the same logical state
/// - **Durability**: Data must persist across application restarts
/// - **Performance**: Operations should scale with data volume
///
/// ## Error Handling
///
/// All operations return [`Result`] types to handle storage errors gracefully.
/// Implementations should distinguish between transient errors (network issues)
/// and permanent errors (constraint violations).
pub trait TriggerStore: Clone + Send + Sync + 'static {
    /// Error type for storage operations.
    ///
    /// Should provide detailed information about the failure mode to enable
    /// appropriate error handling and retry logic.
    type Error: Error + Send + Sync + 'static;

    // Segment management operations

    /// Creates a new segment in the store.
    ///
    /// # Arguments
    ///
    /// * `segment` - The [`Segment`] configuration to store
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if successful, or an error if
    /// the segment could not be created.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A segment with the same ID already exists
    /// - The storage system is unavailable
    /// - The segment configuration is invalid
    fn insert_segment(
        &self,
        segment: Segment,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Retrieves a segment by its identifier.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The unique identifier of the segment to retrieve
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to:
    /// - `Ok(Some(segment))` if the segment exists
    /// - `Ok(None)` if the segment does not exist
    /// - `Err(error)` if the operation failed
    ///
    /// # Errors
    ///
    /// Returns an error if the storage system is unavailable or the operation
    /// fails.
    fn get_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send;

    /// Deletes a segment and all associated data.
    ///
    /// This operation removes the segment and all slabs, triggers, and indices
    /// associated with it. Use with caution as this operation is typically
    /// irreversible.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The unique identifier of the segment to delete
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if successful, or an error if
    /// the deletion failed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The storage system is unavailable
    /// - The deletion operation fails partway through
    fn delete_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // Slab management operations

    /// Returns all slab identifiers for a segment.
    ///
    /// This provides a way to discover which time ranges have active timers
    /// within a segment. Useful for cleanup operations and monitoring.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment to query for slabs
    ///
    /// # Returns
    ///
    /// A [`Stream`] of slab identifiers. The stream may be empty if no
    /// slabs exist for the segment.
    ///
    /// # Errors
    ///
    /// Stream items are [`Result`] types that may contain errors if:
    /// - The storage system becomes unavailable during iteration
    /// - Individual slab records are corrupted
    fn get_slabs(
        &self,
        segment_id: &SegmentId,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send;

    /// Returns slab identifiers within a specified range for a segment.
    ///
    /// This enables efficient querying of timers within specific time ranges
    /// by first identifying which slabs overlap with the desired time window.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment to query for slabs
    /// * `range` - The range of slab IDs to include (supports any
    ///   [`RangeBounds`])
    ///
    /// # Returns
    ///
    /// A [`Stream`] of slab identifiers within the specified range.
    ///
    /// # Errors
    ///
    /// Stream items are [`Result`] types that may contain errors if:
    /// - The storage system becomes unavailable during iteration
    /// - Individual slab records are corrupted
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use prosody::timers::store::TriggerStore;
    /// # async fn example<T: TriggerStore>(store: &T, segment_id: &prosody::timers::store::SegmentId) {
    /// use futures::StreamExt;
    ///
    /// // Get slabs 10 through 20
    /// let slabs: Vec<_> = store.get_slab_range(segment_id, 10..=20)
    ///     .collect().await;
    /// # }
    /// ```
    fn get_slab_range<B>(
        &self,
        segment_id: &SegmentId,
        range: B,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send
    where
        B: RangeBounds<SlabId> + Send;

    /// Registers a slab identifier for a segment.
    ///
    /// This operation tracks which time ranges (slabs) contain active timers.
    /// It's typically called when the first timer is added to a new time range.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment that owns this slab
    /// * `slab_id` - The slab identifier to register
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if successful.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The storage system is unavailable
    /// - The segment does not exist
    fn insert_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unregisters a slab identifier from a segment.
    ///
    /// This operation removes the slab from the segment's slab index but does
    /// NOT automatically delete the slab's trigger data. Use
    /// [`clear_slab_triggers`] to remove the actual timer data.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment that owns this slab
    /// * `slab_id` - The slab identifier to unregister
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage system is unavailable.
    fn delete_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // Slab trigger operations (time-based index)

    /// Returns all triggers within a specific slab.
    ///
    /// This provides time-range-based access to timers, enabling efficient
    /// queries for timers that should execute within a specific time window.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab specification (segment, ID, and size)
    ///
    /// # Returns
    ///
    /// A [`Stream`] of triggers within the slab's time range.
    ///
    /// # Errors
    ///
    /// Stream items are [`Result`] types that may contain errors if:
    /// - The storage system becomes unavailable during iteration
    /// - Individual trigger records are corrupted
    fn get_slab_triggers(
        &self,
        slab: &Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    /// Adds a trigger to a slab's time-based index.
    ///
    /// This operation maintains the time-oriented view of timers, enabling
    /// efficient queries by time range. Typically used in conjunction with
    /// [`insert_key_trigger`] to maintain dual indexing.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab to add the trigger to
    /// * `trigger` - The trigger to add
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if successful.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The storage system is unavailable
    /// - The trigger data is invalid
    fn insert_slab_trigger(
        &self,
        slab: Slab,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes a specific trigger from a slab's time-based index.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab to remove the trigger from
    /// * `key` - The key of the trigger to remove
    /// * `time` - The time of the trigger to remove
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage system is unavailable.
    fn delete_slab_trigger(
        &self,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes all triggers from a slab's time-based index.
    ///
    /// This is typically used during cleanup operations when a slab's
    /// time range has passed and all timers have been processed.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab to clear
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage system is unavailable.
    fn clear_slab_triggers(
        &self,
        slab: &Slab,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // Key trigger operations (entity-based index)

    /// Returns all scheduled times for a specific key within a segment.
    ///
    /// This provides entity-oriented access to timers, enabling efficient
    /// queries for all timers associated with a particular logical entity
    /// (e.g., user, order, session).
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment to search within
    /// * `key` - The key to find scheduled times for
    ///
    /// # Returns
    ///
    /// A [`Stream`] of [`CompactDateTime`] values representing when
    /// timers are scheduled for the specified key.
    ///
    /// # Errors
    ///
    /// Stream items are [`Result`] types that may contain errors if:
    /// - The storage system becomes unavailable during iteration
    /// - Individual time records are corrupted
    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send;

    /// Adds a trigger to the key-based index.
    ///
    /// This operation maintains the entity-oriented view of timers, enabling
    /// efficient queries by key. Typically used in conjunction with
    /// [`insert_slab_trigger`] to maintain dual indexing.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment containing the trigger
    /// * `trigger` - The trigger to index by key
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if successful.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The storage system is unavailable
    /// - The trigger data is invalid
    fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes a specific trigger from the key-based index.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment containing the trigger
    /// * `key` - The key of the trigger to remove
    /// * `time` - The time of the trigger to remove
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage system is unavailable.
    fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes all triggers for a specific key from the key-based index.
    ///
    /// This operation removes all scheduled times for a particular entity,
    /// effectively canceling all timers associated with that key.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment containing the triggers
    /// * `key` - The key to clear all triggers for
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage system is unavailable.
    fn clear_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // High-level composite operations

    /// Adds a trigger to all relevant indices atomically.
    ///
    /// This is the primary method for adding new timers to the store. It
    /// ensures that the trigger is properly indexed in both the time-based
    /// (slab) and entity-based (key) indices, maintaining consistency
    /// across the storage system.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment configuration
    /// * `slab` - The slab containing the trigger
    /// * `trigger` - The trigger to add
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if all operations succeed.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the indexing operations fail. Implementations
    /// should ensure atomicity - either all indices are updated or none are.
    fn add_trigger(
        &self,
        segment: &Segment,
        slab: Slab,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let segment_id = segment.id;
        let slab_id = slab.id();
        async move {
            try_join!(
                self.insert_slab(&segment_id, slab_id),
                self.insert_slab_trigger(slab, trigger.clone()),
                self.insert_key_trigger(&segment_id, trigger),
            )?;
            Ok(())
        }
    }

    /// Removes a trigger from all relevant indices atomically.
    ///
    /// This operation removes the specified trigger from both the time-based
    /// and entity-based indices, maintaining consistency across the storage
    /// system.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment configuration
    /// * `slab` - The slab containing the trigger
    /// * `key` - The key of the trigger to remove
    /// * `time` - The time of the trigger to remove
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if all operations succeed.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the removal operations fail. Implementations
    /// should ensure atomicity - either all indices are updated or none are.
    fn remove_trigger(
        &self,
        segment: &Segment,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            try_join!(
                self.delete_slab_trigger(slab, key, time),
                self.delete_key_trigger(&segment.id, key, time)
            )?;
            Ok(())
        }
    }

    /// Removes all triggers for a specific key from all indices.
    ///
    /// This operation efficiently removes all timers associated with a
    /// particular entity by first querying the key index, then removing
    /// from both the slab and key indices. This is useful for cleanup
    /// operations when an entity is deleted or when canceling all timers
    /// for an entity.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment containing the triggers
    /// * `key` - The key to clear all triggers for
    /// * `slab_size` - The slab size configuration for calculating slab
    ///   locations
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to `Ok(())` if all operations succeed.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the removal operations fail. The operation
    /// may partially complete if some triggers are removed before an error
    /// occurs.
    fn clear_triggers_for_key(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        slab_size: CompactDuration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        // pull the stream, then delete per time, then final clear
        let segment_id_copy = *segment_id;
        let key_clone = key.clone();
        let stream = self.get_key_triggers(segment_id, key);

        async move {
            stream
                .try_for_each_concurrent(DELETE_CONCURRENCY, move |time| {
                    let key_clone = key.clone();
                    let slab = Slab::from_time(segment_id_copy, slab_size, time);
                    async move { self.delete_slab_trigger(&slab, &key_clone, time).await }
                })
                .await?;

            self.clear_key_triggers(&segment_id_copy, &key_clone).await
        }
    }
}
