use super::*;
use std::sync::Arc;

#[test]
fn test_segment_new_computes_correct_id() {
    let topic = Topic::from("test-topic");
    let partition = Partition::from(0_i32);
    let consumer_group: ConsumerGroup = Arc::from("test-group");

    let segment = Segment::new(topic, partition, consumer_group.clone());
    let expected_id = partition_segment_id(topic, partition, &consumer_group);

    assert_eq!(segment.id(), expected_id);
}

#[test]
fn test_segment_accessors() {
    let topic = Topic::from("test-topic");
    let partition = Partition::from(42_i32);
    let consumer_group: ConsumerGroup = Arc::from("test-group");

    let segment = Segment::new(topic, partition, consumer_group.clone());

    assert_eq!(segment.topic(), &topic);
    assert_eq!(segment.partition(), partition);
    assert_eq!(segment.consumer_group(), &consumer_group);
}
