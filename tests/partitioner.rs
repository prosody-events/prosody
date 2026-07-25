//! Live-Kafka test proving [`prosody::state_reader::partition_for_key`]
//! reproduces the partition Kafka's `consistent_random` partitioner assigns.
//!
//! The test produces random keys to a topic with a prime number of
//! partitions. For each key, the partition Kafka actually delivers the message
//! to must equal the partition `partition_for_key` computes for that key. This
//! is the property that proves `crc32fast` matches librdkafka's CRC. Unit tests
//! with fixed golden vectors only catch drift after this match is established;
//! they cannot prove the match holds in the first place.

#![recursion_limit = "256"]

use std::collections::HashMap;
use std::time::Duration;

use color_eyre::eyre::{Result, ensure};
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::UncommittedMessage;
use prosody::consumer::middleware::CloneProvider;
use prosody::consumer::{
    ConsumerConfiguration, DemandType, EventHandler, Keyed, KeyedStateConfiguration,
    ProsodyConsumer, Uncommitted,
};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::state_reader::{PartitionCount, partition_for_key};
use prosody::telemetry::Telemetry;
use prosody::timers::UncommittedTimer;
use prosody::tracing::init_test_logging;
use prosody::{JsonCodec, Partition};
use serde_json::{Value, json};
use tokio::sync::mpsc::{self, Sender};
use tracing::error;
use uuid::Uuid;

mod common;

/// Per-recv hang-guard: bounds a stuck consume, never asserts on timing.
const RECV_HANG_GUARD: Duration = Duration::from_secs(30);

/// Prime partition count: avoids power-of-two low-bit masking artifacts.
const PRIME_PARTITIONS: u16 = 31;

/// Test handler that forwards each message's key and partition to a channel,
/// then commits. `msg.partition()` is Kafka's ground truth for where the
/// record landed, not anything the reader computed.
#[derive(Clone)]
struct PartitionCaptureHandler {
    tx: Sender<(String, Partition)>,
}

impl EventHandler for PartitionCaptureHandler {
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage<Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        let (msg, uncommitted) = message.into_inner();
        let observed = (msg.key().to_string(), msg.partition());
        if let Err(error) = self.tx.send(observed).await {
            error!("failed to forward captured partition: {error:#}");
        }
        uncommitted.commit().await;
    }

    async fn on_timer<C, U>(&self, _context: C, _timer: U, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        U: UncommittedTimer,
    {
    }

    async fn shutdown(self) {}
}

/// The partition Kafka delivers each produced key to always equals
/// `partition_for_key(key, count)`. This proves the producer's partitioner
/// and the reader's partition lookup agree.
#[tokio::test]
async fn consumed_partition_matches_partition_for_key() -> Result<()> {
    init_test_logging();

    let (topic, admin) = common::create_topic_with_partitions(PRIME_PARTITIONS).await?;
    let count = PartitionCount::try_from(i32::from(PRIME_PARTITIONS))?;
    let bootstrap = vec!["localhost:9094".to_owned()];

    let (tx, mut rx) = mpsc::channel::<(String, Partition)>(256);

    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id(Uuid::new_v4().to_string())
        .subscribed_topics(&[topic.to_string()])
        .build()?;
    let consumer: ProsodyConsumer<JsonCodec> = ProsodyConsumer::new(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        KeyedStateConfiguration::builder().build()?,
        CloneProvider::new(PartitionCaptureHandler { tx }),
        Telemetry::new(),
    )
    .await?;

    // Single consumer owns all partitions; wait before producing so no record
    // is missed by an unassigned partition.
    common::wait_for_assignment(&consumer, u32::from(PRIME_PARTITIONS)).await?;

    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-partitioner")
        .build()?;
    let producer = ProsodyProducer::<JsonCodec>::new(&producer_config, Telemetry::new().sender())?;

    let outcome: Result<()> = async {
        let n = usize::try_from(common::integration_test_count())?;
        let mut expected: HashMap<String, Partition> = HashMap::with_capacity(n);
        for _ in 0..n {
            let key = format!("k-{}", Uuid::new_v4());
            let partition = partition_for_key(key.as_bytes(), count)?;
            producer.send([], topic, &key, json!({})).await?;
            expected.insert(key, partition);
        }

        for _ in 0..n {
            let (key, observed) = common::expect_event(&mut rx, RECV_HANG_GUARD).await?;
            let want = *expected
                .get(&key)
                .ok_or_else(|| color_eyre::eyre::eyre!("consumed unknown key {key}"))?;
            ensure!(
                observed == want,
                "key {key}: consumed partition {observed} != computed {want}",
            );
        }
        // A dropped message would surface as a hang above, never a false pass.
        // This check confirms exactly `n` messages arrived, with no extras.
        common::expect_no_event(&mut rx, Duration::from_millis(500)).await?;
        Ok(())
    }
    .await;

    consumer.shutdown().await;
    admin.delete_topic(&topic).await?;
    outcome
}
