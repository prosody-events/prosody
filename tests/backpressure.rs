//! This module tests backpressure in the Prosody message-passing system.
//!
//! It simulates a scenario where a producer sends a large number of messages
//! while the consumer processes them slowly, demonstrating the backpressure
//! handling capabilities of the system. It also checks that backpressure never
//! strands a partition at a log segment that ends without a deliverable record.

#![recursion_limit = "512"]

use std::time::Duration;

use crate::common::handler::ChannelHandler;
use color_eyre::eyre::{Result, bail, eyre};
use prosody::high_level::config::TriggerStoreConfiguration;
use prosody::tracing::init_test_logging;
use prosody::{
    JsonCodec,
    consumer::middleware::CloneProvider,
    consumer::{ConsumerConfiguration, KeyedStateConfiguration, ProsodyConsumer},
    producer::{ProducerConfiguration, ProsodyProducer},
    telemetry::Telemetry,
};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde_json::{Value, json};
use tokio::spawn;
use tokio::sync::mpsc::{Receiver, channel};
use tokio::task::spawn_blocking;
use tokio::time::{Instant, timeout};
use tracing::{error, info};
use uuid::Uuid;

mod common;

/// The broker the stall test connects to.
const BOOTSTRAP: &str = "localhost:9094";

/// The log segment size of the stall test topic. Kafka accepts no smaller
/// `segment.bytes`.
const SEGMENT_BYTES: usize = 1 << 20;

/// The number of records in partition 1's backlog.
const BACKLOG: usize = 400;

/// The hang-guard for each broker call and each received message.
const DEADLINE: Duration = Duration::from_secs(60);

/// Demonstrates backpressure in the message processing system by
/// simulating a setup where messages are produced at a high rate while the
/// consumer processes them at a slower rate.
///
/// # Errors
///
/// Returns a `Result` error if there are issues setting up the topic,
/// producer, consumer, or message channels.
#[tokio::test]
async fn test_backpressure() -> Result<()> {
    // Initialize the logger.
    init_test_logging();

    // Create a unique four-partition topic for the test
    let (topic, admin_client) = common::kafka::create_topic_with_partitions(4).await?;
    let bootstrap: Vec<String> = vec!["localhost:9094".to_owned()];

    // Use a channel with a buffer capacity to accommodate slow processing
    let (messages_tx, mut messages_rx) = channel(64);

    // Configure the consumer with a slow message handler
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id(Uuid::new_v4().to_string().as_str())
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    // Set up the producer configuration. Every fallible step sits before the
    // consumer starts, so nothing between its start and its shutdown can
    // return early and leak the consumer or the topic.
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;

    let producer = ProsodyProducer::<JsonCodec>::new(&producer_config, Telemetry::new().sender())?;

    let slow_handler = ChannelHandler::with_delay(messages_tx, Duration::from_secs(1));
    let consumer: ProsodyConsumer<JsonCodec> = ProsodyConsumer::new(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        KeyedStateConfiguration::builder().build()?,
        CloneProvider::new(slow_handler),
        Telemetry::new(),
    )
    .await?;

    // Produce a large number of messages
    let total = 1_000u32;

    // Start production of messages in a separate task
    spawn(async move {
        for i in 0..total {
            let payload = json!({ "seq": i });
            if let Err(e) = producer.send([], topic, &i.to_string(), payload).await {
                error!("Failed to send message: {e}");
            }
        }
    });

    // Counter for the number of messages processed by the consumer
    let mut count = 0_u32;
    let start_time = Instant::now();

    // Process messages as they are received by the slow consumer
    while messages_rx.recv().await.is_some() {
        count += 1;
        if count.is_multiple_of(100) {
            info!("Received {count} messages so far");
        }
        if count == total {
            break;
        }
    }

    // Log the time taken to process all messages
    let total_elapsed = start_time.elapsed();
    info!("Total messages processed: {count}");
    info!("Total processing time: {total_elapsed:?}");

    // Shutdown the consumer and clean up resources
    consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;
    Ok(())
}

/// Proves that backpressure never strands a partition at a log segment that
/// ends without a deliverable record.
///
/// Partition 0 holds a committed transaction whose commit marker ends a
/// segment, and one more record after it. A fetch at the marker returns no
/// record, so librdkafka needs a second fetch to reach the next record.
/// Partition 1 holds a backlog, and a slow handler keeps every in-flight permit
/// busy. A pause between the two fetches resets partition 0 to the marker,
/// and partition 0 then waits there for many seconds. A partition that gets
/// past the marker delivers its next record after a few backlog records.
#[tokio::test]
async fn backpressure_never_strands_a_partition_at_an_empty_segment_tail() -> Result<()> {
    init_test_logging();

    let topic = Uuid::new_v4().to_string();
    let admin: AdminClient<DefaultClientContext> = client_config().create()?;
    let created = admin
        .create_topics(
            &[NewTopic::new(&topic, 2, TopicReplication::Fixed(1))
                .set("segment.bytes", &SEGMENT_BYTES.to_string())],
            &AdminOptions::new(),
        )
        .await?;
    for result in created {
        result.map_err(|(topic, code)| eyre!("failed to create {topic}: {code}"))?;
    }

    let outcome = backlog_before_segment_tail(&topic).await;

    for result in admin.delete_topics(&[&topic], &AdminOptions::new()).await? {
        result.map_err(|(topic, code)| eyre!("failed to delete {topic}: {code}"))?;
    }

    let backlog = outcome?;
    info!("{backlog} of {BACKLOG} backlog records arrived before the segment tail");
    assert!(
        backlog < BACKLOG / 8,
        "partition 0 waited at its empty segment tail ({backlog} of {BACKLOG} backlog records \
         arrived first)"
    );
    Ok(())
}

/// Writes the layout to `topic`, consumes it with a slow handler, and returns
/// the number of backlog records that arrive before the `after` record.
async fn backlog_before_segment_tail(topic: &str) -> Result<usize> {
    let layout = topic.to_owned();
    spawn_blocking(move || produce_layout(&layout)).await??;

    let (messages_tx, mut messages_rx) = channel(BACKLOG + 2);
    let consumer: ProsodyConsumer<JsonCodec> = ProsodyConsumer::new(
        &ConsumerConfiguration::builder()
            .bootstrap_servers(vec![BOOTSTRAP.to_owned()])
            .group_id(Uuid::new_v4().to_string().as_str())
            .probe_port(None)
            .subscribed_topics(&[topic.to_owned()])
            .max_uncommitted(4_usize)
            .build()?,
        &TriggerStoreConfiguration::InMemory,
        KeyedStateConfiguration::builder().build()?,
        CloneProvider::new(ChannelHandler::with_delay(
            messages_tx,
            Duration::from_millis(50),
        )),
        Telemetry::new(),
    )
    .await?;

    let backlog = receive_until_after(&mut messages_rx).await;
    consumer.shutdown().await;
    backlog
}

/// Writes a commit marker at the end of partition 0's first log segment.
///
/// The first transaction holds one record that fills most of the segment, so
/// its commit marker still fits. The second transaction's `after` record does
/// not fit, so it starts the next segment. Partition 1 gets the backlog.
fn produce_layout(topic: &str) -> Result<()> {
    let producer: BaseProducer = client_config().set("transactional.id", topic).create()?;
    producer.init_transactions(DEADLINE)?;
    for (key, padding) in [("gap", SEGMENT_BYTES * 7 / 8), ("after", SEGMENT_BYTES / 4)] {
        producer.begin_transaction()?;
        let payload = json!({ "padding": "x".repeat(padding) }).to_string();
        producer
            .send(
                BaseRecord::to(topic)
                    .partition(0)
                    .key(key)
                    .payload(&payload),
            )
            .map_err(|(error, _)| error)?;
        producer.commit_transaction(DEADLINE)?;
    }

    let backlog: BaseProducer = client_config().create()?;
    for seq in 0..BACKLOG {
        let key = format!("backlog-{seq}");
        let payload = json!({ "seq": seq }).to_string();
        backlog
            .send(
                BaseRecord::to(topic)
                    .partition(1)
                    .key(&key)
                    .payload(&payload),
            )
            .map_err(|(error, _)| error)?;
    }
    backlog.flush(DEADLINE)?;
    Ok(())
}

/// Receives records until the `after` record arrives, and returns the number
/// of backlog records that arrived first.
async fn receive_until_after(messages: &mut Receiver<(String, Value)>) -> Result<usize> {
    let mut backlog = 0;
    loop {
        let Some((key, _)) = timeout(DEADLINE, messages.recv()).await? else {
            bail!("the handler channel closed before the after record arrived");
        };
        match key.as_str() {
            "after" => return Ok(backlog),
            "gap" => {}
            _ => backlog += 1,
        }
    }
}

/// A client configuration for the stall test broker.
fn client_config() -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", BOOTSTRAP);
    config
}
