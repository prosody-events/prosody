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
/// **Invariant (frozen on-disk contract):** persisted defer and keyed-state
/// rows are keyed by this output. A change to the namespace (`NAMESPACE_OID`)
/// or the format orphans every row. `segment_ids_frozen` checks this output.
///
/// TODO: timers use an older formula (`timers::store::Segment::for_partition`).
/// Move timer data to this id with `migrate_segment_if_needed`, then remove
/// the timer `SegmentId` alias.
#[must_use]
pub(crate) fn partition_segment_id(topic: Topic, partition: Partition, group: &str) -> SegmentId {
    let name = format!("{topic}/{partition}:{group}");
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}

#[cfg(test)]
mod tests;
