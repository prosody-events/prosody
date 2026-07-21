use super::*;
use quickcheck_macros::quickcheck;

/// `Segment::new` stores each field verbatim and derives `id` via the
/// canonical `partition_segment_id` formula. Folds the fixed-input id and
/// accessor examples into one property over arbitrary topic/partition/group;
/// the formula's own correctness (determinism, frozen bytes, field
/// participation) is owned by `src/segment/tests.rs`.
#[quickcheck]
fn prop_new_stores_fields_and_derives_id(
    topic: String,
    partition: Partition,
    group: String,
) -> bool {
    let topic = Topic::from(topic.into_boxed_str());
    let consumer_group: ConsumerGroup = group.into();
    let expected_id = partition_segment_id(topic, partition, &consumer_group);

    let segment = Segment::new(topic, partition, consumer_group.clone());

    segment.id() == expected_id
        && segment.topic() == &topic
        && segment.partition() == partition
        && segment.consumer_group() == &consumer_group
}
