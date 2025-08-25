//! Integration tests for regex topic subscription functionality.
//!
//! This module verifies that Prosody correctly handles regex-based topic
//! subscriptions (topics prefixed with "^") and that consumers can dynamically
//! subscribe to topics matching patterns.

use crate::common::TestHandler;
use color_eyre::eyre::{Result, ensure, eyre};
use prosody::{
    Topic,
    admin::ProsodyAdminClient,
    consumer::{ConsumerConfiguration, ProsodyConsumer},
    producer::{ProducerConfiguration, ProsodyProducer},
};
use serde_json::json;
use std::collections::HashSet;
use tokio::sync::mpsc::channel;
use tokio::time::{Duration, timeout};
use uuid::Uuid;

mod common;

/// Tests that regex topic subscription works with pattern matching.
///
/// This test verifies that a consumer subscribed to a regex pattern
/// (^prefix_.*) can receive messages from multiple topics that match the
/// pattern.
///
/// # Errors
///
/// Returns an error if topic creation, sending or receiving messages, or
/// consumer shutdown fails.
#[tokio::test]
async fn test_regex_topic_subscription() -> Result<()> {
    common::init_test_logging()?;

    let uuid = Uuid::new_v4().to_string();
    let topic_prefix = format!("regex_test_{}", &uuid[0..8]);

    // Create topics that should match our regex pattern
    let topic1: Topic = format!("{topic_prefix}_events").as_str().into();
    let topic2: Topic = format!("{topic_prefix}_logs").as_str().into();
    let topic3: Topic = format!("other_{topic_prefix}_data").as_str().into(); // Won't match

    let bootstrap = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::new(&bootstrap)?;

    // Create all test topics
    admin_client.create_topic(&topic1, 1, 1).await?;
    admin_client.create_topic(&topic2, 1, 1).await?;
    admin_client.create_topic(&topic3, 1, 1).await?;

    // Configure consumer with regex subscription pattern
    let regex_pattern = format!("^{topic_prefix}_.*");
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id(format!("regex-test-consumer-{uuid}"))
        .probe_port(None)
        .subscribed_topics(vec![regex_pattern])
        .build()?;

    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("regex-test-producer")
        .build()?;

    let (messages_tx, mut messages_rx) = channel(10);

    let consumer = ProsodyConsumer::new::<TestHandler>(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        TestHandler { messages_tx },
    )
    .await?;

    let producer = ProsodyProducer::new(&producer_config)?;

    // Send messages to all topics
    let test_key = "test-key";
    let payload1 = json!({"source": "topic1", "data": "from events topic"});
    let payload2 = json!({"source": "topic2", "data": "from logs topic"});
    let payload3 = json!({"source": "topic3", "data": "from other topic"});

    producer.send([], topic1, test_key, &payload1).await?;
    producer.send([], topic2, test_key, &payload2).await?;
    producer.send([], topic3, test_key, &payload3).await?; // Should not be received

    // Use proper synchronization: collect exactly 2 messages with timeout
    let mut received_messages = Vec::new();

    // First message
    let first_msg = timeout(Duration::from_secs(30), messages_rx.recv())
        .await?
        .ok_or_else(|| eyre!("Timeout waiting for first message"))?;
    received_messages.push(first_msg);

    // Second message
    let second_msg = timeout(Duration::from_secs(30), messages_rx.recv())
        .await?
        .ok_or_else(|| eyre!("Timeout waiting for second message"))?;
    received_messages.push(second_msg);

    // Verify no third message arrives (with short timeout)
    let no_third_msg = timeout(Duration::from_secs(2), messages_rx.recv()).await;
    if let Ok(Some(_)) = no_third_msg {
        return Err(eyre!("Unexpected third message received"));
    }

    consumer.shutdown().await;

    // Verify we received exactly 2 messages (from topic1 and topic2 only)
    ensure!(
        received_messages.len() == 2,
        "Expected 2 messages from regex subscription, got {}",
        received_messages.len()
    );

    // Verify all messages have correct key
    for (key, _payload) in &received_messages {
        ensure!(key == test_key, "Message key mismatch");
    }

    // Verify we got messages from both matching topics
    let sources: HashSet<String> = received_messages
        .iter()
        .filter_map(|(_key, payload)| payload.get("source")?.as_str().map(ToString::to_string))
        .collect();

    ensure!(sources.contains("topic1"), "Missing message from topic1");
    ensure!(sources.contains("topic2"), "Missing message from topic2");
    ensure!(
        !sources.contains("topic3"),
        "Unexpected message from topic3 (should not match pattern)"
    );

    // Cleanup
    admin_client.delete_topic(&topic1).await?;
    admin_client.delete_topic(&topic2).await?;
    admin_client.delete_topic(&topic3).await?;

    Ok(())
}
