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
use crate::{Key, Partition, Topic};
use educe::Educe;
use opentelemetry::Context;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use uuid::Uuid;

/// Cassandra-based persistent storage implementation.
pub mod cassandra;
pub mod memory;

mod operations;
pub use operations::TriggerStore;

pub(crate) mod adapter;

/// Selects whether a replacement keeps the old slab row for recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetainOldSlab {
    /// Keep the old source until state promotion finishes.
    Yes,
    /// Remove the old source after the replacement write.
    No,
}

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

    /// Scheduling-time trace context for distributed observability.
    #[educe(PartialEq(ignore), Hash(ignore))]
    pub context: Context,
}

impl PartialOrd for TriggerV1 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TriggerV1 {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare by (key, time) tuple, ignoring the trace context
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
    /// Single source of the formula; the partition loop calls it once per
    /// acquisition, and the keyed-state commit oracle shares the resulting
    /// segment by holding a clone of the same store handle (never by
    /// re-deriving the id).
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
/// synchronous. Implementations must ensure that stores minted for the same
/// segment observe each other's durable writes — see each provider's own
/// doc for how (e.g.
/// [`InMemoryTriggerStoreProvider`](memory::InMemoryTriggerStoreProvider),
/// [`CassandraTriggerStoreProvider`](cassandra::CassandraTriggerStoreProvider)).
pub trait TriggerStoreProvider: Clone + Send + Sync + 'static {
    /// The store type created by this provider.
    type Store: TriggerStore;

    /// Creates a store scoped to the specified segment (synchronous, no I/O).
    fn create_store(&self, segment: Segment) -> Self::Store;
}
