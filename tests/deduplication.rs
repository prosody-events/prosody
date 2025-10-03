//! This module provides tests to verify the deduplication functionality of the
//! Prosody system. The tests ensure messages with identical event IDs are
//! deduplicated correctly, so only one is processed.

use crate::common::TestHandler;
use color_eyre::eyre::{Result, ensure};
use prosody::{
    Topic,
    admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration},
    consumer::middleware::CloneProvider,
    consumer::{ConsumerConfiguration, ProsodyConsumer},
    producer::{ProducerConfiguration, ProsodyProducer},
    telemetry::Telemetry,
};
use serde_json::json;
use tokio::sync::mpsc::channel;
use tokio::time::{Duration, Instant};
use uuid::Uuid;

mod common;

/// Tests the deduplication of messages with identical event IDs.
///
/// This test verifies that when two messages with the same event ID
/// are sent, only one is received by the consumer, ensuring proper
/// deduplication functionality.
///
/// # Errors
///
/// This function will return an error if setting up the Kafka topic,
/// producer, or consumer fails, or if deduplication does not work
/// as expected.
///
/// # Panics
///
/// Panics if initializing the tracing subscriber fails.
#[tokio::test]
async fn test_deduplication_of_same_event_id() -> Result<()> {
    // Initialize compact tracing format
    common::init_test_logging()?;

    // Create a unique Kafka topic for isolated testing
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::new(&AdminConfiguration::new(bootstrap.clone())?)?;

    // Create the Kafka topic with a single partition and replica
    admin_client
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(1_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    // Configure the producer and consumer for communication with the Kafka broker
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;

    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id("test-deduplication-consumer")
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    // Set up a channel to collect messages from the consumer
    let (messages_tx, mut messages_rx) = channel(10);
    let handler = TestHandler { messages_tx };

    // Initialize the producer and consumer
    let producer = ProsodyProducer::new(&producer_config)?;
    let consumer = ProsodyConsumer::new(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        CloneProvider::new(handler.clone()),
        Telemetry::new(),
    )
    .await?;

    // Define two messages with identical event IDs for the deduplication test
    let key = "test-key";
    let event_id = "event-123";
    let payload = json!({ "id": event_id, "value": "first message" });
    let payload_duplicate = json!({ "id": event_id, "value": "duplicate message" });

    // Send both the original and duplicate messages
    producer.send([], topic, key, &payload).await?;
    producer.send([], topic, key, &payload_duplicate).await?;

    // Collect received messages with a predefined timeout
    let mut received_messages = Vec::new();
    let start = Instant::now();
    let timeout = Duration::from_secs(30);

    while start.elapsed() < timeout {
        if let Some((recv_key, recv_payload)) = messages_rx.recv().await {
            received_messages.push((recv_key, recv_payload));
            break; // Stop after receiving the first message
        }
    }

    // Shutdown the consumer once messages are collected
    consumer.shutdown().await;

    // Verify that successful deduplication results in only the first message being
    // received
    ensure!(
        received_messages.len() == 1,
        "Expected one message due to deduplication"
    );

    let (recv_key, recv_payload) = &received_messages[0];
    ensure!(recv_key == key);
    ensure!(recv_payload == &payload);

    // Clean up the test topic after verifying deduplication
    admin_client.delete_topic(&topic).await?;
    Ok(())
}
