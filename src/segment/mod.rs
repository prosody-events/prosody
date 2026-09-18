//! Canonical per-Kafka-partition segment id: the type and its derivation,
//! owned by neither the defer nor the keyed-state subsystem.

use crate::{Partition, Topic};
use uuid::Uuid;

/// `UUIDv5` naming the Cassandra rows a single Kafka partition owns.
pub type SegmentId = Uuid;

/// Canonical segment id: `UUIDv5(NAMESPACE_OID,
/// "{topic}/{partition}:{group}")`.
///
/// Single source of this derivation, shared by the defer stores
/// ([`crate::consumer::middleware::defer::segment::Segment`]) and the
/// keyed-state segment so both name a partition with the same id.
///
/// **Invariant (frozen on-disk contract):** released defer data — and
/// keyed-state data once released — is keyed by this output. The namespace
/// (`NAMESPACE_OID`) and format MUST NOT change, or all persisted rows are
/// orphaned. Pinned by `defer_segment_id_frozen`.
///
/// Timers keep a second, older derivation ([`timer_segment_id`]) and their own
/// `SegmentId` alias. A later change migrates timer data onto this id and folds
/// that alias into this one.
#[must_use]
pub(crate) fn partition_segment_id(topic: Topic, partition: Partition, group: &str) -> SegmentId {
    let name = format!("{topic}/{partition}:{group}");
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}

/// The name a timer segment is known by: `"{group}:{topic}/{partition}"`.
///
/// **Invariant (frozen on-disk contract):** this string is both the
/// `timer_segments.name` column and the input to [`timer_segment_id`]. The two
/// must come from one place. A second copy of the format lets the name and the
/// id drift apart. Pinned by `timer_segment_id_frozen`.
#[must_use]
pub(crate) fn timer_segment_name(group: &str, topic: Topic, partition: Partition) -> String {
    format!("{group}:{topic}/{partition}")
}

/// Timer segment id: `UUIDv5(NAMESPACE_URL, name)` for a name that
/// [`timer_segment_name`] built.
///
/// **Invariant (frozen on-disk contract):** released timer data is keyed by
/// this output. The namespace and the name format MUST NOT change, or every
/// persisted trigger is orphaned. Pinned by `timer_segment_id_frozen`.
#[must_use]
pub(crate) fn timer_segment_id(name: &str) -> SegmentId {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes())
}

#[cfg(test)]
mod tests;
