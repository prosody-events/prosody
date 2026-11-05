//! Contract tests for the librdkafka behavior that the partition reader
//! depends on. They run against the local broker, so an rdkafka upgrade that
//! changes the behavior fails them.

use crate::codec::JsonCodec;
use crate::consumer::decode::IgnoreRequests;
use crate::consumer::source::Reader;
use crate::otel::SpanRelation;
use crate::test_util::TEST_RUNTIME;
use crate::test_util::kafka::{BOOTSTRAP, with_topic};
use color_eyre::eyre::{OptionExt, Result, bail, ensure};
use futures::StreamExt;
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::spawn_blocking;
use tokio::time::timeout;
use uuid::Uuid;

/// The records each test produces.
const RECORDS: i64 = 50;

/// The hang-guard for each read and each produce.
const DEADLINE: Duration = Duration::from_secs(30);

/// A split queue that stays assigned receives every record of its partition,
/// and the consumer queue receives none. The split holds after the partition
/// is unassigned and assigned again, with no new split.
///
/// Falsify: remove the split. The consumer queue then receives the records.
#[test]
fn split_before_assign_survives_reassignment() -> Result<()> {
    TEST_RUNTIME.block_on(with_topic(&topic_name("split"), 1, async |topic| {
        produce(topic).await?;
        let topic = topic.to_owned();
        spawn_blocking(move || {
            let consumer = consumer()?;
            let queue = consumer
                .split_partition_queue(&topic, 0)
                .ok_or_eyre("no partition queue")?;

            consumer.incremental_assign(&assignment(&topic)?)?;
            read_split(&consumer, &queue)?;

            consumer.incremental_unassign(&assignment(&topic)?)?;
            consumer.incremental_assign(&assignment(&topic)?)?;
            read_split(&consumer, &queue)
        })
        .await?
    }))
}

/// The reader wakes for each record that reaches its empty queue. The
/// producer sends one record at a time, so the queue empties between most
/// arrivals, and the librdkafka non-empty callback fires many times.
///
/// Falsify: remove the callback in [`Reader::read`]. The reader then misses
/// the first record that reaches its empty queue, and the read stops at its
/// deadline. The race between the empty poll and the wait is too short to
/// reach here; `prop_admit_reads_each_record_once_in_order` covers it.
#[test]
fn reader_wakes_for_each_arrival() -> Result<()> {
    TEST_RUNTIME.block_on(with_topic(&topic_name("wake"), 1, async |topic| {
        let consumer = consumer()?;
        let queue = consumer
            .split_partition_queue(topic, 0)
            .ok_or_eyre("no partition queue")?;
        consumer.incremental_assign(&assignment(topic)?)?;

        let reader = Reader::<JsonCodec, _>::new(1, IgnoreRequests, SpanRelation::default());
        let offsets = reader
            .read(queue)
            .map(|record| record.offset())
            .take(usize::try_from(RECORDS)?)
            .collect::<Vec<_>>();
        let (offsets, produced) = tokio::join!(timeout(DEADLINE, offsets), produce(topic));

        // The client blocks in its close, so it drops off the runtime workers.
        spawn_blocking(move || drop(consumer)).await?;
        produced?;
        let Ok(offsets) = offsets else {
            bail!("the reader stopped before it read every record");
        };
        ensure!(
            offsets == (0..RECORDS).collect::<Vec<_>>(),
            "out of order: {offsets:?}"
        );
        Ok(())
    }))
}

fn topic_name(name: &str) -> String {
    format!("source_test_{name}_{}", Uuid::new_v4())
}

fn consumer() -> Result<Arc<BaseConsumer>> {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP)
        .set("group.id", Uuid::new_v4().to_string())
        .set("enable.auto.commit", "false")
        .create()?;
    Ok(Arc::new(consumer))
}

/// Partition 0 of `topic`, from its first offset.
fn assignment(topic: &str) -> Result<TopicPartitionList> {
    let mut list = TopicPartitionList::with_capacity(1);
    list.add_partition_offset(topic, 0, Offset::Beginning)?;
    Ok(list)
}

/// Sends [`RECORDS`] keyed records to partition 0 of `topic`, one at a time.
async fn produce(topic: &str) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP)
        .create()?;
    for _ in 0..RECORDS {
        let record = FutureRecord::to(topic)
            .partition(0)
            .key("key")
            .payload("{}");
        if let Err((error, _)) = producer.send(record, Timeout::After(DEADLINE)).await {
            bail!("produce failed: {error:#}");
        }
    }
    Ok(())
}

/// Reads every record from `queue` in offset order, and fails if the consumer
/// queue receives a record first.
fn read_split(
    consumer: &BaseConsumer,
    queue: &PartitionQueue<DefaultConsumerContext>,
) -> Result<()> {
    let deadline = Instant::now() + DEADLINE;
    let mut next = 0;
    while next < RECORDS {
        ensure!(
            Instant::now() < deadline,
            "read {next} of {RECORDS} records from the partition queue"
        );
        if let Some(stray) = consumer.poll(Duration::ZERO) {
            bail!(
                "the consumer queue received {:?}",
                stray.map(|message| message.offset())
            );
        }
        if let Some(message) = queue.poll(Duration::from_millis(100)) {
            let offset = message?.offset();
            ensure!(offset == next, "read offset {offset}, expected {next}");
            next += 1;
        }
    }
    Ok(())
}
