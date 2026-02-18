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
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::{TimerType, Trigger};
use educe::Educe;
use futures::Stream;
use scylla::{DeserializeValue, SerializeValue};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::ops::RangeInclusive;
use tracing::Span;
use uuid::Uuid;

/// Cassandra-based persistent storage implementation.
pub mod cassandra;
pub mod memory;

/// Internal primitive operations trait (20 methods).
///
/// The trait itself is `pub` to satisfy Rust's visibility rules (used in public
/// `TableAdapter`), but is not re-exported, keeping it effectively internal.
pub mod operations;

/// TableAdapter struct for composing TriggerOperations into TriggerStore.
///
/// This module is public to allow returning concrete `TableAdapter<T>` types
/// from factory functions, but it's not re-exported at the crate root.
pub mod adapter;

/// Write-through cache wrapper for [`TriggerStore`] implementations.
pub mod cached;

#[cfg(test)]
/// Comprehensive test suite for [`TriggerStore`] implementations.
pub mod tests;

/// Segment schema version.
///
/// Determines which Cassandra table schema is used for storing triggers.
/// - V1: Legacy schema without `timer_type` field
/// - V2: Current schema with `timer_type` field for Application vs
///   `DeferredMessage`
#[repr(i8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentVersion {
    /// V1 schema without `timer_type` field.
    V1 = 1,
    /// V2 schema with `timer_type` field.
    V2 = 2,
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

impl ClassifyError for InvalidSegmentVersionError {
    fn classify_error(&self) -> ErrorCategory {
        // Invalid segment version value (not 1 or 2). Indicates data corruption or
        // incompatible schema version in database. Not recoverable by retry.
        ErrorCategory::Permanent
    }
}

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

/// Singleton timer data stored in the static map column.
///
/// Maps to the Cassandra UDT `timer_slot`:
/// ```cql
/// CREATE TYPE IF NOT EXISTS timer_slot (
///     time int,
///     span frozen<map<text, text>>
/// );
/// ```
///
/// Used for tombstone-free singleton timer storage in the `singleton_timers`
/// static column of `timer_typed_keys`.
#[derive(Clone, Debug, PartialEq, Eq, DeserializeValue, SerializeValue)]
pub struct TimerSlot {
    /// Timer trigger time.
    ///
    /// Stored as i32 in Cassandra but serialized/deserialized via
    /// `CompactDateTime`'s scylla trait implementations.
    pub time: CompactDateTime,
    /// OpenTelemetry span context for trace continuity.
    pub span: HashMap<String, String>,
}

/// State of a timer type's singleton slot.
///
/// Determined by querying the `singleton_timers` static map column:
/// - Map entry absent → `Absent` (legacy data or never written)
/// - Map entry present with value → `Singleton` (use static slot data)
/// - Map entry present with NULL → `Overflow` (scan clustering columns)
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotState {
    /// No map entry for this timer type.
    ///
    /// Legacy data or empty partition. Fall back to clustering column reads.
    Absent,
    /// Valid singleton timer in static slot.
    ///
    /// Contains the timer data; no clustering column scan needed.
    Singleton(TimerSlot),
    /// Map entry was deleted (NULL value).
    ///
    /// Multiple timers exist; scan clustering columns only.
    Overflow,
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

/// Public trigger storage interface.
///
/// Provides both primitive read operations and coordinated write operations
/// for the V2 schema (with `timer_type` field).
///
/// # Implementation Guide
///
/// Storage backends can implement this trait in two ways:
///
/// 1. **Via `TableAdapter`** (recommended for most backends):
///    - Implement internal `TriggerOperations` trait (20 primitive methods)
///    - Wrap in `TableAdapter<T>` which implements `TriggerStore`
///    - Best-effort consistency using parallel execution
///
/// 2. **Direct implementation** (for transactional backends):
///    - Implement `TriggerStore` directly (9 methods)
///    - Use database transactions for atomic dual-table operations
///    - Provides ACID guarantees
///
/// # Used By
///
/// - `TimerManager` (coordinated writes + key queries)
/// - Slab Loader (segment management + slab queries)
pub trait TriggerStore: Clone + Send + Sync + 'static {
    /// Error type for storage operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    // ===================================================================
    // Segment Operations (2 methods) - Used by Loader
    // ===================================================================

    /// Retrieves segment metadata by ID.
    fn get_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send;

    /// Creates a new segment with metadata.
    fn insert_segment(
        &self,
        segment: Segment,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // ===================================================================
    // Slab Query Operations (2 methods) - Used by Loader
    // ===================================================================

    /// Streams slab IDs within a time range for a segment.
    fn get_slab_range(
        &self,
        segment_id: &SegmentId,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send;

    /// Streams all triggers in a slab across all timer types.
    fn get_slab_triggers_all_types(
        &self,
        slab: &Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    // ===================================================================
    // Slab Cleanup (1 method) - Used by Loader
    // ===================================================================

    /// Deletes slab metadata (does not delete triggers).
    fn delete_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // ===================================================================
    // Key Query Operations (2 methods) - Used by TimerManager
    // ===================================================================

    /// Streams scheduled times for a key and timer type.
    ///
    /// Returns only timestamps without full trigger metadata.
    /// More efficient than `get_key_triggers` when span data not needed.
    fn get_key_times(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send;

    /// Streams full trigger objects for a key and timer type.
    ///
    /// Includes all metadata (key, time, `timer_type`, span).
    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    // ===================================================================
    // Coordinated Write Operations (3 methods) - Used by TimerManager
    // ===================================================================

    /// Adds a trigger to both slab and key tables.
    ///
    /// Implementations should attempt to keep both tables in sync.
    /// Transactional backends can provide ACID guarantees.
    fn add_trigger(
        &self,
        segment: &Segment,
        slab: Slab,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes a trigger from both slab and key tables.
    fn remove_trigger(
        &self,
        segment: &Segment,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Atomically clears existing timers and schedules a new one.
    ///
    /// This is the core primitive for tombstone-free singleton timer
    /// overwrites. It performs the following operations atomically:
    ///
    /// 1. Writes the new timer to the slab index
    /// 2. Writes the new timer to the singleton slot (static column)
    /// 3. Deletes any existing clustering rows for this key/type
    /// 4. Cleans up old slab index entries
    ///
    /// # Write Ordering
    ///
    /// New timer is written FIRST, then old entries are deleted. This ensures
    /// at-least-once delivery: if a crash occurs, both timers may exist
    /// temporarily, but the timer will never be lost.
    ///
    /// # Parameters
    ///
    /// * `segment` - The segment containing this timer
    /// * `new_slab` - The slab for the new timer
    /// * `new_trigger` - The new timer to schedule
    /// * `old_slabs` - Slabs containing old timers to remove from slab index
    fn clear_and_schedule(
        &self,
        segment: &Segment,
        new_slab: Slab,
        new_trigger: Trigger,
        old_slabs: Vec<Slab>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
