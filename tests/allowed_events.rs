//! This module provides a test suite to verify the functionality of event
//! filtering in the Prosody application. It ensures that the consumer can
//! correctly filter out disallowed events and only process allowed ones.

#![recursion_limit = "256"]

use crate::common::handler::ChannelHandler;
use color_eyre::eyre::{Result, ensure, eyre};
use prosody::tracing::init_test_logging;
use prosody::{
    JsonCodec,
    consumer::middleware::CloneProvider,
    consumer::{ConsumerConfiguration, KeyedStateConfiguration, ProsodyConsumer},
    producer::{ProducerConfiguration, ProsodyProducer},
    telemetry::Telemetry,
};
use serde_json::json;
use tokio::sync::mpsc::channel;
use tokio::time::{Duration, timeout};

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
    init_test_logging();

    // Create a unique single-partition topic to isolate the test environment
    let (topic, admin_client) = common::kafka::create_topic_with_partitions(1).await?;
    let bootstrap = vec!["localhost:9094".to_owned()];

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
    let consumer: ProsodyConsumer<JsonCodec> = ProsodyConsumer::new(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        KeyedStateConfiguration::builder().build()?,
        CloneProvider::new(ChannelHandler::new(messages_tx)),
        Telemetry::new(),
    )
    .await?;
    let key = "test-key";

    let outcome: Result<()> = async {
        let producer =
            ProsodyProducer::<JsonCodec>::new(&producer_config, Telemetry::new().sender())?;

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
        producer.send([], topic, key, payload_filtered).await?;
        producer
            .send([], topic, key, payload_allowed.clone())
            .await?;

        // Validate receipt of only the allowed message. The wait is a
        // hang-guard for an event that will arrive, sized generously so cluster
        // slowness never trips it; the assertions below on key/payload are what
        // prove correctness.
        let received = timeout(Duration::from_mins(1), messages_rx.recv()).await?;
        let (received_key, received_payload) =
            received.ok_or_else(|| eyre!("Timeout waiting for a delivered message"))?;

        ensure!(received_key == key);
        ensure!(received_payload == payload_allowed);
        Ok(())
    }
    .await;

    // Shut down the consumer and delete the test topic on every path.
    consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;
    outcome
}
