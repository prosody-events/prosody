//! Persistent storage abstraction for timer data.
//!
//! This module defines the [`TriggerStore`] trait and supporting types for
//! persisting and querying timer events. Timer data is organized into
//! segments and time-based slabs, and indexed both by time and by key,
//! enabling efficient range and entity lookups.
//!
//! # Architecture
//!
//! ```text
//! Segment (partition of timers by consumer group)
//! ├── Slabs (time-based partitions for efficient queries)
//! │   └── Triggers (individual timer events)
//! └── Key Index (fast lookup by key)
//!     └── Times (scheduled times for each key)
//! ```
//!
//! The default in-memory implementation [`memory::InMemoryTriggerStore`] is
//! suitable for testing and development. Production storage backends can
//! implement the same trait to provide durability.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::{DELETE_CONCURRENCY, TimerType, Trigger};
use educe::Educe;
use futures::{Stream, TryStreamExt};
use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::ops::RangeInclusive;
use tokio::try_join;
use tracing::Span;
use uuid::Uuid;

/// Cassandra-based persistent storage implementation.
pub mod cassandra;
pub mod memory;

#[cfg(test)]
/// Comprehensive test suite for [`TriggerStore`] implementations.
pub mod tests;

/// Segment schema version.
///
/// Determines which Cassandra table schema is used for storing triggers.
/// - V1: Legacy schema without `timer_type` field
/// - V2: Current schema with `timer_type` field for Application vs `DeferRetry`
#[repr(i8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentVersion {
    /// V1 schema without `timer_type` field.
    V1 = 1,
    /// V2 schema with `timer_type` field.
    V2 = 2,
}

impl Default for SegmentVersion {
    fn default() -> Self {
        Self::V1
    }
}

impl From<SegmentVersion> for i8 {
    fn from(version: SegmentVersion) -> Self {
        version as i8
    }
}

impl TryFrom<i8> for SegmentVersion {
    type Error = InvalidSegmentVersionError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            _ => Err(InvalidSegmentVersionError(value)),
        }
    }
}

/// Error returned when trying to convert an invalid i8 value to
/// [`SegmentVersion`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSegmentVersionError(i8);

impl fmt::Display for InvalidSegmentVersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Invalid segment version: {}. Expected 1 (V1) or 2 (V2)",
            self.0
        )
    }
}

impl Error for InvalidSegmentVersionError {}

/// V1 trigger representation without timer type field.
///
/// Used during migration to represent triggers from v1 schema tables.
/// V1 triggers are identified solely by (key, time) without a type field.
/// This is a simple data bag for temporary migration use.
#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct TriggerV1 {
    /// Entity key identifying what this timer belongs to.
    pub key: Key,

    /// When this timer should execute.
    pub time: CompactDateTime,

    /// Tracing span for distributed observability context.
    #[educe(PartialEq(ignore), Hash(ignore))]
    pub span: Span,
}

impl PartialOrd for TriggerV1 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TriggerV1 {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare by (key, time) tuple, ignoring span
        (&self.key, &self.time).cmp(&(&other.key, &other.time))
    }
}

/// Unique identifier for a timer segment.
///
/// Segments partition timers by logical grouping (for example, by consumer
/// group or application instance). Each segment has its own time-based slab
/// configuration.
pub type SegmentId = Uuid;

/// Configuration for a timer segment.
///
/// Each segment has:
/// - `id`: its unique [`Uuid`]
/// - `name`: a human-readable identifier
/// - `slab_size`: the duration of each time-based partition (slab)
/// - `version`: schema version (V1 or V2)
#[derive(Clone, Debug)]
pub struct Segment {
    /// Unique segment identifier.
    pub id: SegmentId,

    /// Human-readable name for monitoring and debugging.
    pub name: String,

    /// Duration of a time-based slab in this segment.
    pub slab_size: CompactDuration,

    /// Schema version determining the table schema.
    pub version: SegmentVersion,
}

/// Persistent storage interface for timer data.
///
/// Implementations group timer triggers in two ways:
/// 1. **Time-based**: grouped into slabs for efficient range queries.
/// 2. **Key-based**: grouped by entity key for efficient lookup and deletion.
///
/// # Consistency
///
/// Trigger addition and removal involve two indices (slab and key).
/// Implementations should strive to perform both updates such that the two
/// indices remain in sync. In some storage backends this may be best-effort (no
/// cross-index transaction).
///
/// # Performance
///
/// Storage backends should scale to high volumes of timer events and handle
/// concurrent access.
pub trait TriggerStore: Clone + Send + Sync + 'static {
    /// Error type for storage operations.
    ///
    /// Should convey I/O and consistency failures.
    type Error: Error + Send + Sync + 'static;

    // =========================================================================
    // Foundational Operations - Segment Configuration
    // =========================================================================

    /// Creates a new segment configuration.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment metadata to insert.
    ///
    /// # Errors
    ///
    /// Returns an error if insertion fails or if a segment with the same ID
    /// already exists.
    fn insert_segment(
        &self,
        segment: Segment,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Retrieves a segment by its identifier.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier to look up.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(segment))` if found.
    /// * `Ok(None)` if not found.
    /// * `Err` on failure.
    fn get_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send;

    /// Deletes a segment and all associated metadata.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier to delete.
    ///
    /// # Errors
    ///
    /// Returns an error if deletion of the segment or its indices fails.
    fn delete_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // =========================================================================
    // Low-Level Primitives - Single-Index Operations
    // =========================================================================
    //
    // These methods operate on a single index (either slab or key) and must be
    // implemented by each storage backend. They provide the building blocks for
    // higher-level composite operations.

    // --- Slab Metadata Operations ---

    /// Lists all slab IDs in a segment.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    ///
    /// # Returns
    ///
    /// A stream of slab identifiers. Each item may be an error.
    fn get_slabs(
        &self,
        segment_id: &SegmentId,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send;

    /// Lists slab IDs in a specified inclusive range.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `range` - The inclusive range of slab IDs to query.
    ///
    /// # Returns
    ///
    /// A stream of slab identifiers within the range.
    fn get_slab_range(
        &self,
        segment_id: &SegmentId,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send;

    /// Registers (inserts) a slab ID under a segment.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `slab` - The slab to insert.
    ///
    /// # Errors
    ///
    /// Returns an error if registration fails.
    fn insert_slab(
        &self,
        segment_id: &SegmentId,
        slab: Slab,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unregisters (deletes) a slab ID from a segment.
    ///
    /// Does **not** delete the actual triggers in that slab; use
    /// [`Self::clear_slab_triggers`] to remove those.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `slab_id` - The slab ID to remove.
    ///
    /// # Errors
    ///
    /// Returns an error if removal fails.
    fn delete_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // --- Slab Trigger Operations (Time-Based Index) ---

    /// Streams all triggers of a specific type within a slab's time range.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab descriptor (contains `segment_id`, `slab_size`,
    ///   `slab_id`).
    /// * `timer_type` - The timer type to query (Application or `DeferRetry`).
    ///
    /// # Returns
    ///
    /// A stream of triggers of the specified type in that slab.
    fn get_slab_triggers(
        &self,
        slab: &Slab,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    /// Inserts a trigger into the slab index.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab descriptor.
    /// * `trigger` - The trigger to insert.
    ///
    /// # Errors
    ///
    /// Returns an error if insertion fails.
    fn insert_slab_trigger(
        &self,
        slab: Slab,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes a specific trigger from a slab's index.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab descriptor.
    /// * `timer_type` - The timer type (Application or `DeferRetry`).
    /// * `key` - The trigger's entity key.
    /// * `time` - The trigger's scheduled time.
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    fn delete_slab_trigger(
        &self,
        slab: &Slab,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Clears all triggers from a slab's index across ALL timer types.
    ///
    /// This method clears both Application and `DeferRetry` timers in the slab.
    /// Used for `slab_size` migration and cleanup operations.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error if clearing fails.
    fn clear_slab_triggers(
        &self,
        slab: &Slab,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Streams ALL triggers within a slab across all timer types.
    ///
    /// This is the primary method for querying slab triggers, matching
    /// Cassandra's ability to query an entire partition. More efficient than
    /// separately querying each timer type.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab descriptor.
    ///
    /// # Returns
    ///
    /// A stream of all triggers in the slab, regardless of timer type.
    fn get_slab_triggers_all_types(
        &self,
        slab: &Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    // --- Key Trigger Operations (Entity-Based Index) ---

    /// Streams all scheduled times for a given key and timer type.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `timer_type` - The timer type to query (Application or `DeferRetry`).
    /// * `key` - The entity key.
    ///
    /// # Returns
    ///
    /// A stream of times when the key has triggers of the specified type
    /// scheduled.
    fn get_key_times(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send;

    /// Streams all triggers for a given key and timer type.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `timer_type` - The timer type to query (Application or `DeferRetry`).
    /// * `key` - The entity key.
    ///
    /// # Returns
    ///
    /// A stream of full trigger records for the key and type.
    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    /// Inserts a trigger into the key-based index.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `trigger` - The trigger to insert.
    ///
    /// # Errors
    ///
    /// Returns an error if insertion fails.
    fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes a specific trigger from the key-based index.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `timer_type` - The timer type (Application or `DeferRetry`).
    /// * `key` - The entity key.
    /// * `time` - The scheduled time.
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Clears all triggers for a specific key and timer type.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `timer_type` - The timer type to clear (Application or `DeferRetry`).
    /// * `key` - The entity key.
    ///
    /// # Errors
    ///
    /// Returns an error if clearing fails.
    fn clear_key_triggers(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Streams ALL triggers for a given key across all timer types.
    ///
    /// This is the primary method for querying key triggers, matching
    /// Cassandra's ability to query an entire partition. More efficient than
    /// separately querying each timer type.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `key` - The entity key.
    ///
    /// # Returns
    ///
    /// A stream of all triggers for the key, regardless of timer type.
    fn get_key_triggers_all_types(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    /// Clears all triggers from the key index for a given key, across ALL timer
    /// types.
    ///
    /// This is a low-level primitive that only operates on the key index
    /// partition. For coordinated operations that maintain dual indices,
    /// use [`Self::clear_all_triggers_for_key`] instead.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `key` - The entity key.
    ///
    /// # Errors
    ///
    /// Returns an error if the clear operation fails.
    fn clear_key_triggers_all_types(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // =========================================================================
    // High-Level Composite Operations - Dual-Index Coordination
    // =========================================================================
    //
    // These methods provide default implementations that coordinate updates
    // across both slab and key indices to maintain consistency. They use the
    // low-level primitives defined above.

    /// Adds a trigger to both slab and key indices.
    ///
    /// Implementations should attempt to keep both indices updated so they
    /// remain consistent; failures may still leave partial state in some
    /// backends.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment configuration.
    /// * `slab` - The slab descriptor.
    /// * `trigger` - The trigger to add.
    ///
    /// # Errors
    ///
    /// Returns an error if any index insertion fails.
    fn add_trigger(
        &self,
        segment: &Segment,
        slab: Slab,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let segment_id = segment.id;
        async move {
            try_join!(
                self.insert_slab(&segment_id, slab.clone()),
                self.insert_slab_trigger(slab, trigger.clone()),
                self.insert_key_trigger(&segment_id, trigger),
            )?;
            Ok(())
        }
    }

    /// Removes a trigger from both slab and key indices.
    ///
    /// Implementations should attempt to keep both indices updated so they
    /// remain consistent; failures may still leave partial state in some
    /// backends.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment configuration.
    /// * `slab` - The slab descriptor.
    /// * `key` - The trigger's entity key.
    /// * `time` - The trigger's scheduled time.
    /// * `timer_type` - The timer type (Application or `DeferRetry`).
    ///
    /// # Errors
    ///
    /// Returns an error if any index removal fails.
    fn remove_trigger(
        &self,
        segment: &Segment,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            try_join!(
                self.delete_slab_trigger(slab, timer_type, key, time),
                self.delete_key_trigger(&segment.id, timer_type, key, time),
            )?;
            Ok(())
        }
    }

    /// Removes all triggers for a key and timer type, clearing both slab and
    /// key indices.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `timer_type` - The timer type to clear (Application or `DeferRetry`).
    /// * `key` - The entity key.
    /// * `slab_size` - Slab duration used to locate each time's slab.
    ///
    /// # Errors
    ///
    /// Returns an error if any removal fails. Partial completion may occur
    /// if an error interrupts processing.
    fn clear_triggers_for_key(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
        slab_size: CompactDuration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        // pull the stream, then delete per time, then final clear
        let segment_id_copy = *segment_id;
        let key_clone = key.clone();
        let stream = self.get_key_times(segment_id, timer_type, key);

        async move {
            stream
                .try_for_each_concurrent(DELETE_CONCURRENCY, move |time| {
                    let key_clone = key.clone();
                    let slab = Slab::from_time(segment_id_copy, slab_size, time);
                    async move {
                        self.delete_slab_trigger(&slab, timer_type, &key_clone, time)
                            .await
                    }
                })
                .await?;

            self.clear_key_triggers(&segment_id_copy, timer_type, &key_clone)
                .await
        }
    }

    /// Removes all triggers for a key across ALL timer types, clearing both
    /// slab and key indices.
    ///
    /// This is a high-level coordinated operation that maintains the dual-index
    /// invariant by efficiently querying all timer types at once and clearing
    /// from both indices.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier.
    /// * `key` - The entity key.
    /// * `slab_size` - Slab duration used to locate each time's slab.
    ///
    /// # Errors
    ///
    /// Returns an error if any deletion fails. Partial completion may occur
    /// if an error interrupts processing.
    fn clear_all_triggers_for_key(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        slab_size: CompactDuration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let segment_id_copy = *segment_id;
        let key_clone = key.clone();

        // Use the efficient all-types query method
        let stream = self.get_key_triggers_all_types(segment_id, key);

        async move {
            // Delete each trigger from slab index
            stream
                .try_for_each_concurrent(DELETE_CONCURRENCY, move |trigger| {
                    let slab = Slab::from_time(segment_id_copy, slab_size, trigger.time);
                    async move {
                        self.delete_slab_trigger(
                            &slab,
                            trigger.timer_type,
                            &trigger.key,
                            trigger.time,
                        )
                        .await
                    }
                })
                .await?;

            // Clear key index for all timer types in one operation
            self.clear_key_triggers_all_types(&segment_id_copy, &key_clone)
                .await
        }
    }

    // =========================================================================
    // V1 Schema Migration Support
    // =========================================================================
    //
    // These methods support migration from V1 schema (without timer_type) to
    // V2 schema (with timer_type). Used by the migration module.

    /// Updates the schema version and slab size for a segment.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment to update
    /// * `new_version` - The target schema version (`SegmentVersion::V2`)
    /// * `new_slab_size` - The new slab size (may be same as current)
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    fn update_segment_version(
        &self,
        segment_id: &SegmentId,
        new_version: SegmentVersion,
        new_slab_size: CompactDuration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Registers a slab ID in v1 tables for testing purposes.
    ///
    /// This method exists solely to enable testing of v1 migration logic.
    /// Production code should never call this method.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `slab_id` - The slab identifier to register
    ///
    /// # Errors
    ///
    /// Returns an error if insertion fails.
    fn insert_slab_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Inserts a v1 trigger for testing purposes.
    ///
    /// This method exists solely to enable testing of v1 migration logic.
    /// Production code should never call this method.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `slab_id` - The slab identifier
    /// * `trigger` - The v1 trigger to insert (without `timer_type`)
    ///
    /// # Errors
    ///
    /// Returns an error if insertion fails.
    fn insert_slab_trigger_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
        trigger: TriggerV1,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Lists all slab IDs in a v1 segment.
    ///
    /// V1 has no separate segment tracking, so implementations may return
    /// empty.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    ///
    /// # Returns
    ///
    /// A stream of slab identifiers from v1 tables.
    fn get_slabs_v1(
        &self,
        segment_id: &SegmentId,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send;

    /// Retrieves all triggers in a v1 slab.
    ///
    /// Queries v1 `timer_slabs` table with PK `((segment_id, id), key, time)`.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `slab_id` - The slab identifier
    ///
    /// # Returns
    ///
    /// A stream of v1 triggers (without `timer_type` field).
    fn get_slab_triggers_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Stream<Item = Result<TriggerV1, Self::Error>> + Send;

    /// Deletes a v1 slab metadata entry from the segments table.
    ///
    /// Low-level method that removes the slab ID from the segments table
    /// clustering columns. Does NOT delete triggers.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `slab_id` - The slab ID to remove from metadata
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    fn delete_slab_metadata_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes a specific v1 trigger from a slab's index.
    ///
    /// Low-level method that removes a single trigger identified by
    /// `(segment_id, slab_id, key, time)` from the `timer_slabs` table.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `slab_id` - The slab identifier
    /// * `key` - The trigger's entity key
    /// * `time` - The trigger's scheduled time
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    fn delete_slab_trigger_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes all v1 triggers for a slab from the `timer_slabs` table.
    ///
    /// Low-level method that removes all triggers with the given
    /// `(segment_id, slab_id)` partition key. Does NOT update segments table.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `slab_id` - The slab whose triggers should be deleted
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    fn clear_slab_triggers_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes a v1 slab and all its triggers.
    ///
    /// High-level method that deletes both the slab metadata from the segments
    /// table and all triggers from the `timer_slabs` table.
    ///
    /// Default implementation calls [`delete_slab_metadata_v1`] and
    /// [`clear_slab_triggers_v1`].
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `slab_id` - The slab to delete
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    fn delete_slab_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Sized,
    {
        async move {
            self.delete_slab_metadata_v1(segment_id, slab_id).await?;
            self.clear_slab_triggers_v1(segment_id, slab_id).await?;
            Ok(())
        }
    }

    /// Inserts a trigger into the v1 key-based index.
    ///
    /// Inserts into v1 `timer_keys` table using partition key (`segment_id`,
    /// key). V1 triggers do not have [`TimerType`] field.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `trigger` - The v1 trigger to insert
    ///
    /// # Errors
    ///
    /// Returns an error if insertion fails.
    fn insert_key_trigger_v1(
        &self,
        segment_id: &SegmentId,
        trigger: TriggerV1,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Retrieves all triggers for a key from v1 key-based index.
    ///
    /// Queries v1 `timer_keys` table using partition key (`segment_id`, key).
    /// Returns triggers ordered by time.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `key` - The key to query
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    fn get_key_triggers_v1(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<TriggerV1, Self::Error>> + Send;

    /// Deletes a specific v1 trigger from the key-based index.
    ///
    /// Low-level method that removes a single trigger identified by
    /// `(segment_id, key, time)` from the `timer_keys` table.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `key` - The entity key
    /// * `time` - The trigger's scheduled time
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    fn delete_key_trigger_v1(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Clears all triggers for a key from v1 tables.
    ///
    /// Removes from v1 `timer_keys` table using partition key (`segment_id`,
    /// key).
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `key` - The key whose triggers should be cleared
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    fn clear_key_triggers_v1(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
