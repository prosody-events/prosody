//! Tests for the canonical per-Kafka-partition segment-id derivation.

use super::*;
use crate::timers::duration::CompactDuration;
use crate::timers::store::Segment;
use quickcheck_macros::quickcheck;
use std::sync::Arc;

/// Frozen on-disk contracts: persisted rows store both segment ids and the
/// timer segment name. Never change a literal to match new output.
#[test]
fn segment_ids_frozen() -> color_eyre::Result<()> {
    assert_eq!(
        partition_segment_id(Topic::from("topic"), 0, "group"),
        Uuid::parse_str("8e49149d-48f6-5bf4-a392-f16c57fe3059")?,
    );

    let timer = Segment::for_partition("group", Topic::from("topic"), 0, CompactDuration::new(60));
    assert_eq!(timer.name, "group:topic/0");
    assert_eq!(
        timer.id,
        Uuid::parse_str("d3436a33-e5de-5ec4-b122-927bab3808c2")?
    );
    Ok(())
}

/// Every field enters the formula: a different topic, partition, or group
/// gives a different id.
#[quickcheck]
fn prop_all_fields_participate(topic: String, partition: Partition, group: String) -> bool {
    let other_topic = Topic::from(format!("{topic}x").into_boxed_str());
    let other_group = format!("{group}x");
    // `wrapping_add(1)` differs from `partition` for every i32.
    let other_partition = partition.wrapping_add(1);
    let base_topic = Topic::from(topic.into_boxed_str());
    let group: Arc<str> = group.into();

    let base = partition_segment_id(base_topic, partition, &group);
    base != partition_segment_id(other_topic, partition, &group)
        && base != partition_segment_id(base_topic, other_partition, &group)
        && base != partition_segment_id(base_topic, partition, &other_group)
}
