//! Tests for the canonical per-Kafka-partition segment-id derivation.

use super::*;
use quickcheck_macros::quickcheck;
use std::sync::Arc;

/// Frozen on-disk contract: released defer data is keyed by this id, derived as
/// `UUIDv5(NAMESPACE_OID, "{topic}/{partition}:{group}")`. The literal was
/// captured before relocating the derivation out of the defer module. If this
/// assertion ever changes it orphans every persisted row — never "update" it to
/// match new output; treat a failure as a corrupted formula.
#[test]
fn defer_segment_id_frozen() -> color_eyre::Result<()> {
    assert_eq!(
        partition_segment_id(Topic::from("topic"), 0, "group"),
        Uuid::parse_str("8e49149d-48f6-5bf4-a392-f16c57fe3059")?,
    );
    Ok(())
}

/// The derivation is a pure function: the same `(topic, partition, group)`
/// always yields the same id (stable across restarts).
#[quickcheck]
fn prop_deterministic(topic: String, partition: Partition, group: String) -> bool {
    let topic = Topic::from(topic.into_boxed_str());
    let group: Arc<str> = group.into();
    partition_segment_id(topic, partition, &group) == partition_segment_id(topic, partition, &group)
}

/// Every field enters the formula: changing the topic, the partition, or the
/// group each yields a different id. Folds the three "differs by …" examples
/// into one invariant.
#[quickcheck]
fn prop_all_fields_participate(topic: String, partition: Partition, group: String) -> bool {
    let other_topic = Topic::from(format!("{topic}x").into_boxed_str());
    let other_group = format!("{group}x");
    // `wrapping_add(1)` is guaranteed to differ from `partition` for any i32.
    let other_partition = partition.wrapping_add(1);
    let base_topic = Topic::from(topic.into_boxed_str());
    let group: Arc<str> = group.into();

    let base = partition_segment_id(base_topic, partition, &group);
    base != partition_segment_id(other_topic, partition, &group)
        && base != partition_segment_id(base_topic, other_partition, &group)
        && base != partition_segment_id(base_topic, partition, &other_group)
}
