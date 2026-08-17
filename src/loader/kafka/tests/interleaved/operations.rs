use super::*;

impl Arbitrary for PartitionSpec {
    fn arbitrary(g: &mut Gen) -> Self {
        let message_count = (usize::arbitrary(g) % 41) + 20; // 20..=60
        let lso = usize::arbitrary(g) % message_count; // 0..message_count (0 = no deletion)
        PartitionSpec { message_count, lso }
    }
}

impl Arbitrary for TopicSpec {
    fn arbitrary(g: &mut Gen) -> Self {
        let partition_count = (usize::arbitrary(g) % 16) + 1; // 1..=16
        TopicSpec {
            partitions: (0..partition_count)
                .map(|_| PartitionSpec::arbitrary(g))
                .collect(),
        }
    }
}

/// Produce messages to all partitions of a single topic concurrently.
pub(super) async fn produce_all_partitions(
    producer: &FutureProducer,
    topic_name: &str,
    specs: &[PartitionSpec],
) -> color_eyre::Result<Vec<Vec<i64>>> {
    join_all(specs.iter().enumerate().map(|(p, spec)| {
        produce_messages(producer, topic_name, p as Partition, spec.message_count)
    }))
    .await
    .into_iter()
    .collect()
}

/// Delete all scenario prefixes in one broker request.
pub(super) async fn delete_scenario_records(
    topics: &[Topic],
    specs: &[TopicSpec],
    offsets: &[Vec<Vec<i64>>],
) -> color_eyre::Result<()> {
    let deletions: Vec<(Topic, Partition, Offset)> = specs
        .iter()
        .enumerate()
        .flat_map(|(topic, spec)| {
            spec.partitions
                .iter()
                .enumerate()
                .filter(|(_, partition)| partition.lso > 0)
                .map(move |(partition, spec)| {
                    (
                        topics[topic],
                        partition as Partition,
                        offsets[topic][partition][spec.lso],
                    )
                })
        })
        .collect();
    if deletions.is_empty() {
        return Ok(());
    }

    admin()?.delete_records(deletions.iter().copied()).await?;
    let consumer = watermark_consumer()?;
    join_all(deletions.into_iter().map(|(topic, partition, offset)| {
        wait_for_lso(Arc::clone(&consumer), topic.to_string(), partition, offset)
    }))
    .await
    .into_iter()
    .collect::<color_eyre::Result<Vec<()>>>()?;
    Ok(())
}

/// Assert the outcome of one load request against the expected deleted/valid
/// boundary.
pub(super) fn assert_load_result(
    result: Result<ConsumerRecord<serde_json::Value>, KafkaLoaderError>,
    topic: usize,
    partition: Partition,
    offset_idx: usize,
    expected_offset: Offset,
    lso: usize,
) -> color_eyre::Result<()> {
    if offset_idx < lso {
        let Err(KafkaLoaderError::OffsetDeleted {
            partition: got_partition,
            requested_offset: got_offset,
            ..
        }) = result
        else {
            color_eyre::eyre::bail!(
                "topic {topic} partition {partition} offset_idx {offset_idx} (offset \
                 {expected_offset}) expected OffsetDeleted (lso_idx={lso}), got: {result:?}"
            );
        };
        assert_eq!(got_partition, partition);
        assert_eq!(got_offset, expected_offset);
    } else {
        let Ok(ConsumerRecord::Message(msg)) = result else {
            color_eyre::eyre::bail!(
                "topic {topic} partition {partition} offset_idx {offset_idx} (offset \
                 {expected_offset}) expected Ok (lso_idx={lso}), got: {result:?}"
            );
        };
        assert_eq!(msg.offset(), expected_offset);
        assert_eq!(msg.partition(), partition);
    }
    Ok(())
}
