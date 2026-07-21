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
/// TODO(follow-up PR): timers still derive their segment id with a separate
/// legacy formula (`timers::store::Segment::for_partition`, `NAMESPACE_URL`)
/// and keep their own `timers::store::SegmentId` alias. A later PR migrates
/// timer data onto this id — reusing
/// `timers::store::cassandra::migration::migrate_segment_if_needed` — and folds
/// that alias into this one.
#[must_use]
pub(crate) fn partition_segment_id(topic: Topic, partition: Partition, group: &str) -> SegmentId {
    let name = format!("{topic}/{partition}:{group}");
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}

#[cfg(test)]
mod tests;
