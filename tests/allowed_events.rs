//! This module provides a test suite to verify the functionality of event
//! filtering in the Prosody application. It ensures that the consumer can
//! correctly filter out disallowed events and only process allowed ones.

use crate::common::TestHandler;
use color_eyre::eyre::{Result, ensure, eyre};
use prosody::{
    Topic,
    admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration},
    consumer::{ConsumerConfiguration, ProsodyConsumer},
    consumer::middleware::CloneProvider,
    producer::{ProducerConfiguration, ProsodyProducer},
};
use serde_json::json;
use tokio::sync::mpsc::channel;
use tokio::time::{Duration, timeout};
use uuid::Uuid;

mod common;

/// Tests the event filtering functionality within Prosody consumers.
///
/// This async test verifies that only events with allowed types are processed
/// by the consumer, while disallowed types are filtered out.
///
/// # Errors
///
/// Returns an error if topic creation, sending or receiving messages, or
/// consumer shutdown fails.
#[tokio::test]
async fn test_allowed_events_filtering() -> Result<()> {
    // Initialize logging
    common::init_test_logging()?;

    // Create a unique topic to isolate the test environment
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap = vec!["localhost:9094".to_owned()];

    // Create a Kafka topic using the admin client for testing
    let admin_client = ProsodyAdminClient::new(&AdminConfiguration::new(bootstrap.clone())?)?;
    admin_client
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(1_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    // Configure the consumer to filter allowed events only
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id("test-allowed-events-consumer")
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .allowed_events(vec!["allowed".to_owned()])
        .build()?;

    // Configure the producer for sending test messages
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;

    // Set up a channel for consumer messages
    let (messages_tx, mut messages_rx) = channel(10);

    // Initialize consumer and producer
    let consumer = ProsodyConsumer::new(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        CloneProvider::new(TestHandler { messages_tx }),
    )
    .await?;
    let producer = ProsodyProducer::new(&producer_config)?;

    let key = "test-key";

    // Create test payloads: one to be filtered, one to be allowed
    let payload_filtered = json!({
        "type": "disallowed",
        "content": "this message should be filtered"
    });
    let payload_allowed = json!({
        "type": "allowed",
        "content": "this message should be delivered"
    });

    // Send both disallowed and allowed messages
    producer.send([], topic, key, &payload_filtered).await?;
    producer.send([], topic, key, &payload_allowed).await?;

    // Validate receipt of only the allowed message
    let received = timeout(Duration::from_secs(30), messages_rx.recv()).await?;
    let (received_key, received_payload) =
        received.ok_or_else(|| eyre!("Timeout waiting for a delivered message"))?;

    // Shut down the consumer and assert the filtering behavior
    consumer.shutdown().await;

    ensure!(received_key == key);
    ensure!(received_payload == payload_allowed);

    // Clean up by deleting the test topic
    admin_client.delete_topic(&topic).await?;
    Ok(())
}
