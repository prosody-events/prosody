//! This module contains tests that verify the functionality of `source system
//! filtering` in the Prosody application.
//!
//! The tests ensure that messages are correctly filtered based on their `source
//! system` and consumer group configurations. The Prosody library enables
//! message handling via Kafka, and this module tests specific subscription
//! scenarios to validate that only expected messages are received.

use crate::common::TestHandler;
use color_eyre::eyre::{Result, ensure, eyre};
use prosody::{
    Topic,
    admin::ProsodyAdminClient,
    consumer::{ConsumerConfiguration, ProsodyConsumer},
    high_level::config::TriggerStoreConfiguration,
    producer::{ProducerConfiguration, ProsodyProducer},
};
use serde_json::{Value, json};
use tokio::sync::mpsc::channel;
use tokio::time::{Duration, timeout};
use tracing_subscriber::fmt;
use uuid::Uuid;

#[path = "common.rs"]
mod common;

/// Tests the filtering functionality of the source system.
#[tokio::test]
async fn test_source_system_filtering() -> Result<()> {
    let _ = fmt().compact().try_init();
    let timeout_duration = Duration::from_secs(5);

    // Scenario 1: Both source system and group ID are the same, so no messages
    // should be expected.
    run_scenario("filter-test", "filter-test", false, "1", timeout_duration).await?;

    // Scenario 2: Consumer group is different, so messages should be delivered.
    run_scenario(
        "filter-test",
        "different-group",
        true,
        "2",
        timeout_duration,
    )
    .await?;

    Ok(())
}

/// Runs a test scenario where messages are sent and their reception is
/// verified.
///
/// # Arguments
///
/// * `source_system`: The source system identifier for the producer.
/// * `group_id`: The consumer group identifier.
/// * `expect_messages`: A boolean indicating whether the messages should be
///   received.
/// * `event_suffix`: A suffix to uniquely identify the event.
/// * `timeout_duration`: The duration to wait for message receipt.
///
/// # Errors
///
/// This function returns an error if the test setup, execution, or verification
/// fails.
async fn run_scenario(
    source_system: &'static str,
    group_id: &'static str,
    expect_messages: bool,
    event_suffix: &'static str,
    timeout_duration: Duration,
) -> Result<()> {
    // Create a unique Kafka topic for the test.
    let topic_string = Uuid::new_v4().to_string();
    let topic: Topic = topic_string.as_str().into();
    let bootstrap = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::new(&bootstrap)?;
    admin_client.create_topic(&topic, 1, 1).await?;

    // Build producer and consumer configurations.
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system(source_system)
        .build()?;

    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id(group_id)
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    // Set up a channel to communicate received messages.
    let (tx, mut rx) = channel(10);
    let consumer = ProsodyConsumer::new::<TestHandler>(
        &consumer_config,
        &TriggerStoreConfiguration::InMemory,
        TestHandler { messages_tx: tx },
    )
    .await?;
    let producer = ProsodyProducer::new(&producer_config)?;

    // Send a test message and an end-of-stream marker.
    let key = "test-key";
    let event_id = format!("unique-event-{event_suffix}");
    let payload = json!({ "id": event_id, "value": "test message" });

    producer.send([], topic, key, &payload).await?;
    producer.send([], topic, key, &Value::Null).await?;

    // Check whether the messages are received or not, based on `expect_messages`.
    if expect_messages {
        // Ensure that the message is received.
        let recv_result = timeout(timeout_duration, rx.recv()).await?;
        let (recv_key, recv_payload) =
            recv_result.ok_or_else(|| eyre!("Did not receive the regular message"))?;

        ensure!(recv_key == key, "keys did not match");
        ensure!(recv_payload == payload, "payloads did not match");

        // Verify the reception of the end-of-stream marker.
        let end_mark_result = timeout(timeout_duration, rx.recv()).await?;
        match end_mark_result {
            Some((_k, Value::Null)) => {} // Expected outcome.
            _ => return Err(eyre!("Did not receive expected end-of-stream marker")),
        }
    } else {
        // Validate that no message was received.
        let recv = timeout(timeout_duration, rx.recv()).await;
        if let Ok(Some((k, v))) = recv {
            consumer.shutdown().await;
            admin_client.delete_topic(&topic).await?;
            return Err(eyre!("Unexpected message received: key: {k}, payload: {v}"));
        }
    }

    // Shutdown the consumer once complete and clean up the test topic.
    consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;
    Ok(())
}
