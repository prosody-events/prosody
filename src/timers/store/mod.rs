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

use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::{TimerType, Trigger};
use crate::{Key, Partition, Topic};
use educe::Educe;
use futures::Stream;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::ops::RangeInclusive;
use tracing::Span;
use uuid::Uuid;

/// Cassandra-based persistent storage implementation.
pub mod cassandra;
pub mod memory;

/// Internal primitive operations trait (22 methods).
///
/// The trait itself is `pub` to satisfy Rust's visibility rules (used in public
/// `TableAdapter`), but is not re-exported, keeping it effectively internal.
pub mod operations;

/// TableAdapter struct for composing TriggerOperations into TriggerStore.
///
/// This module is public to allow returning concrete `TableAdapter<T>` types
/// from factory functions, but it's not re-exported at the crate root.
pub mod adapter;

#[cfg(test)]
/// Comprehensive test suite for [`TriggerStore`] implementations.
pub mod tests;

/// Segment schema version.
///
/// Determines which Cassandra table schema is used for storing triggers.
/// - V1: Legacy schema without `timer_type` field
/// - V2: Schema with `timer_type` field; `state` MAP may be absent for
///   pre-migration keys (ambiguous: 0 timers or clustering-only data)
/// - V3: Post-migration schema; `state` MAP is always populated, so NULL
///   unambiguously means 0 timers
#[repr(i8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentVersion {
    /// V1 schema without `timer_type` field.
    V1 = 1,
    /// V2 schema with `timer_type` field.
    V2 = 2,
    /// V3 schema: all keys have state entries backfilled; NULL = new key.
    V3 = 3,
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
            3 => Ok(Self::V3),
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
            "Invalid segment version: {}. Expected 1 (V1), 2 (V2), or 3 (V3)",
            self.0
        )
    }
}

impl Error for InvalidSegmentVersionError {}

impl ClassifyError for InvalidSegmentVersionError {
    fn classify_error(&self) -> ErrorCategory {
        // Invalid segment version value (not 1, 2, or 3). Indicates data corruption or
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

impl Segment {
    /// Canonical per-Kafka-partition segment: id derived from
    /// `{group}:{topic}/{partition}`, schema V3.
    ///
    /// Single source of the formula so every reader of a partition's timer
    /// rows — the partition loop and the keyed-state commit oracle — names
    /// the same segment.
    #[must_use]
    pub fn for_partition(
        group_id: &str,
        topic: Topic,
        partition: Partition,
        slab_size: CompactDuration,
    ) -> Self {
        let name = format!("{group_id}:{topic}/{partition}");
        Self {
            id: Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes()),
            name,
            slab_size,
            version: SegmentVersion::V3,
        }
    }
}

/// Factory for segment-scoped [`TriggerStore`] instances.
///
/// Holds shared resources and creates per-segment stores; store creation is
/// synchronous. For Cassandra the shared resources are the session and
/// prepared statements; for the in-memory store they are the shared maps —
/// the durable substrate memory mode keeps across store mints, so stores
/// created for the same segment observe the same rows. The keyed-state
/// commit oracle never mints its own store here: it receives a clone of the
/// partition's store handle (see
/// [`StateBackendFactory::for_partition`](crate::state::StateBackendFactory::for_partition)).
pub trait TriggerStoreProvider: Clone + Send + Sync + 'static {
    /// The store type created by this provider.
    type Store: TriggerStore;

    /// Creates a store scoped to the specified segment (synchronous, no I/O).
    fn create_store(&self, segment: Segment) -> Self::Store;
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
///    - Implement internal `TriggerOperations` trait (22 primitive methods)
///    - Wrap in `TableAdapter<T>` which implements `TriggerStore`
///    - Best-effort consistency using parallel execution
///
/// 2. **Direct implementation** (for transactional backends):
///    - Implement `TriggerStore` directly (13 methods)
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
    // Segment Accessors
    // ===================================================================

    /// Returns the segment this store is scoped to.
    fn segment(&self) -> Segment;

    /// Returns the segment ID this store is scoped to.
    fn segment_id(&self) -> SegmentId;

    /// Returns the slab size for this store's segment.
    fn slab_size(&self) -> CompactDuration;

    // ===================================================================
    // Segment Operations (2 methods) - Used by Loader
    // ===================================================================

    /// Retrieves this store's segment metadata from persistent storage.
    fn get_segment(&self) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send;

    /// Persists this store's segment metadata.
    fn insert_segment(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // ===================================================================
    // Slab Query Operations (2 methods) - Used by Loader
    // ===================================================================

    /// Streams slab IDs within a time range for this store's segment.
    fn get_slab_range(
        &self,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send;

    /// Streams all triggers in a slab across all timer types.
    fn get_slab_triggers_all_types(
        &self,
        slab_id: SlabId,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    // ===================================================================
    // Slab Metadata Writes (2 methods) - Used by SchedulerActor
    // ===================================================================

    /// Inserts slab metadata (the `(id, slab_id)` clustering row). Used by
    /// the scheduler actor when it observes a slab it has not registered yet
    /// and the slab is above `slab_watermark`. Past-time slabs route through
    /// [`Self::batch_insert_slab_with_watermark`] instead so the slab row
    /// and the watermark lower together atomically.
    fn insert_slab(&self, slab: Slab) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes slab metadata (does not delete triggers).
    fn delete_slab(&self, slab_id: SlabId) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // ===================================================================
    // Slab Watermark Operations (3 methods) - Used by SchedulerActor
    // ===================================================================

    /// Reads the persisted `slab_watermark` for this segment.
    ///
    /// `None` = pre-migration / fresh segment → callers should treat as
    /// "scan from slab 0". When `Some(w)`, every slab clustering row in this
    /// segment has `slab_id > w`.
    fn get_slab_watermark(
        &self,
    ) -> impl Future<Output = Result<Option<SlabId>, Self::Error>> + Send;

    /// Persists `slab_watermark` for this segment.
    fn set_slab_watermark(
        &self,
        watermark: Option<SlabId>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Atomically inserts a slab clustering row and lowers
    /// `slab_watermark` in one UNLOGGED BATCH on the segment partition.
    fn batch_insert_slab_with_watermark(
        &self,
        slab: Slab,
        watermark: Option<SlabId>,
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
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send;

    /// Streams full trigger objects for a key and timer type.
    ///
    /// Includes all metadata (key, time, `timer_type`, span).
    fn get_key_triggers(
        &self,
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
    fn add_trigger(&self, trigger: Trigger)
    -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes a trigger from both slab and key tables.
    fn remove_trigger(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Atomically clears existing timers for a key/type and schedules a new
    /// one.
    ///
    /// This is the core primitive for tombstone-free singleton timer
    /// overwrites. Reads existing trigger times from the key index, writes
    /// the new timer to both indexes (new slab + singleton key slot), then
    /// deletes old slab entries.
    ///
    /// # Write Ordering
    ///
    /// New timer is written FIRST, then old entries are deleted. This ensures
    /// at-least-once delivery: if a crash occurs, both timers may exist
    /// temporarily, but the timer will never be lost.
    fn clear_and_schedule(
        &self,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // ===================================================================
    // Tag Operations (2 methods) - Used by TimerManager commit oracle
    // ===================================================================

    /// Updates the `tag` on both persisted timer indices.
    ///
    /// No-op if the row is absent. Used by
    /// `complete()`-from-`FiringRescheduled` to rotate the tag so the
    /// commit oracle can detect the round-trip after in-memory operation and
    /// after slab reloads.
    fn update_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
        new_tag: i32,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Reads the `tag` from a key-index row.
    ///
    /// Returns `None` if the row is absent (commit oracle: "committed").
    /// Returns `Some(0)` for rows with a `NULL` tag (pre-migration rows).
    ///
    /// **Contract: the answer must reflect every write performed through
    /// this store and its clones.** The keyed-state commit oracle holds a
    /// clone of the partition's writing store (handle passing — see
    /// [`StateBackendFactory::for_partition`](crate::state::StateBackendFactory::for_partition)),
    /// so a per-instance cache is fine as long as clones share it; a stale
    /// answer flips a recovery decision (rolling back a committed write, or
    /// promoting an abandoned one).
    fn current_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Option<i32>, Self::Error>> + Send;
}
