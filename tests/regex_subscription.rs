//! Integration tests for regex topic subscription functionality.
//!
//! This module verifies that Prosody correctly handles regex-based topic
//! subscriptions (topics prefixed with "^") and that consumers can dynamically
//! subscribe to topics matching patterns.

use crate::common::{FallibleTestHandler, collect_messages_with_timeout};
use color_eyre::eyre::{Result, ensure};
use prosody::{
    Topic,
    admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration},
    cassandra::config::CassandraConfigurationBuilder,
    consumer::ConsumerConfigurationBuilder,
    consumer::failure::{
        retry::RetryConfigurationBuilder, topic::FailureTopicConfigurationBuilder,
    },
    high_level::{HighLevelClient, HighLevelClientError, mode::Mode},
    producer::ProducerConfigurationBuilder,
};
use serde_json::{Value, json};
use std::collections::HashSet;
use tokio::sync::mpsc::channel;
use uuid::Uuid;

mod common;

/// Test configuration constants.
const MESSAGE_TIMEOUT_SECS: u64 = 30;
const TEST_KEY: &str = "test-key";
const BOOTSTRAP_SERVER: &str = "localhost:9094";
const CASSANDRA_HOST: &str = "localhost:9042";

/// Represents a test topic with metadata.
#[derive(Debug)]
struct TestTopic {
    name: Topic,
    source_id: &'static str,
    should_match_pattern: bool,
}

impl TestTopic {
    fn new(name: Topic, source_id: &'static str, should_match_pattern: bool) -> Self {
        Self {
            name,
            source_id,
            should_match_pattern,
        }
    }
}

/// Creates test topics for regex subscription testing.
async fn create_test_topics(topic_prefix: &str) -> Result<(Vec<TestTopic>, ProsodyAdminClient)> {
    let topics = vec![
        TestTopic::new(
            format!("{topic_prefix}_events").as_str().into(),
            "topic1",
            true,
        ),
        TestTopic::new(
            format!("{topic_prefix}_logs").as_str().into(),
            "topic2",
            true,
        ),
        TestTopic::new(
            format!("other_{topic_prefix}_data").as_str().into(),
            "topic3",
            false,
        ),
    ];

    let bootstrap = vec![BOOTSTRAP_SERVER.to_owned()];
    let admin_client = ProsodyAdminClient::new(&AdminConfiguration::new(bootstrap)?)?;

    // Create all test topics with 1 partition and 1 replica
    for topic in &topics {
        admin_client
            .create_topic(
                &TopicConfiguration::builder()
                    .name(topic.name.to_string())
                    .partition_count(1_u16)
                    .replication_factor(1_u16)
                    .build()?,
            )
            .await?;
    }

    Ok((topics, admin_client))
}

/// Creates a configured `HighLevelClient` for regex subscription testing.
fn create_high_level_client(
    regex_pattern: String,
    consumer_group: String,
) -> Result<HighLevelClient<FallibleTestHandler>, HighLevelClientError> {
    let bootstrap = vec![BOOTSTRAP_SERVER.to_owned()];

    let mut producer_builder = ProducerConfigurationBuilder::default();
    producer_builder
        .bootstrap_servers(bootstrap.clone())
        .source_system("regex-test-producer");

    let mut consumer_builder = ConsumerConfigurationBuilder::default();
    consumer_builder
        .bootstrap_servers(bootstrap)
        .group_id(consumer_group)
        .probe_port(None)
        .subscribed_topics(vec![regex_pattern]);

    let retry_builder = RetryConfigurationBuilder::default();
    let failure_topic_builder = FailureTopicConfigurationBuilder::default();
    let mut cassandra_builder = CassandraConfigurationBuilder::default();
    cassandra_builder.nodes(vec![CASSANDRA_HOST.to_owned()]);

    HighLevelClient::new(
        Mode::BestEffort,
        &mut producer_builder,
        &consumer_builder,
        &retry_builder,
        &failure_topic_builder,
        &cassandra_builder,
    )
}

/// Sends test messages to all topics and returns expected matching count.
async fn send_test_messages(
    client: &HighLevelClient<FallibleTestHandler>,
    topics: &[TestTopic],
) -> Result<usize> {
    let mut expected_messages = 0;

    for topic in topics {
        let payload = json!({
            "source": topic.source_id,
            "data": format!("from {} topic", topic.source_id)
        });

        client.send(topic.name, TEST_KEY, &payload).await?;

        if topic.should_match_pattern {
            expected_messages += 1;
        }
    }

    Ok(expected_messages)
}

/// Verifies that received messages match expectations.
fn verify_messages(messages: &[(String, Value)], topics: &[TestTopic]) -> Result<()> {
    let expected_count = topics.iter().filter(|t| t.should_match_pattern).count();

    ensure!(
        messages.len() == expected_count,
        "Expected {} messages from regex subscription, got {}",
        expected_count,
        messages.len()
    );

    // Verify all messages have correct key
    for (key, _) in messages {
        ensure!(
            key == TEST_KEY,
            "Message key mismatch: expected {}, got {}",
            TEST_KEY,
            key
        );
    }

    // Verify we got messages from all matching topics and none from non-matching
    let received_sources: HashSet<String> = messages
        .iter()
        .filter_map(|(_, payload)| {
            payload
                .as_object()?
                .get("source")?
                .as_str()
                .map(ToString::to_string)
        })
        .collect();

    for topic in topics {
        if topic.should_match_pattern {
            ensure!(
                received_sources.contains(topic.source_id),
                "Missing message from {}",
                topic.source_id
            );
        } else {
            ensure!(
                !received_sources.contains(topic.source_id),
                "Unexpected message from {} (should not match pattern)",
                topic.source_id
            );
        }
    }

    Ok(())
}

/// Cleans up test topics.
async fn cleanup_topics(admin_client: &ProsodyAdminClient, topics: &[TestTopic]) -> Result<()> {
    for topic in topics {
        admin_client.delete_topic(&topic.name).await?;
    }
    Ok(())
}

/// Tests that regex topic subscription works with pattern matching through
/// `HighLevelClient`.
///
/// This test verifies that a `HighLevelClient` consumer subscribed to a regex
/// pattern (^prefix_.*) can receive messages from multiple topics that match
/// the pattern, while filtering out topics that don't match.
///
/// # Test Scenario
///
/// 1. Creates topics: `{prefix}_events`, `{prefix}_logs` (should match) and
///    `other_{prefix}_data` (should not match)
/// 2. Subscribes to pattern `^{prefix}_.*`
/// 3. Sends messages to all topics
/// 4. Verifies only matching topics receive messages
///
/// # Errors
///
/// Returns an error if topic creation, client configuration, message
/// sending/receiving, or cleanup fails.
#[tokio::test]
async fn test_regex_topic_subscription() -> Result<()> {
    common::init_test_logging()?;

    let uuid = Uuid::new_v4().to_string();
    let topic_prefix = format!("regex_test_{}", &uuid[0..8]);
    let regex_pattern = format!("^{topic_prefix}_.*");
    let consumer_group = format!("regex-test-consumer-{uuid}");

    // Setup test topics and admin client
    let (topics, admin_client) = create_test_topics(&topic_prefix).await?;

    // Setup message collection channel
    let (messages_tx, mut messages_rx) = channel(10);

    // Create and configure high-level client
    let client = create_high_level_client(regex_pattern, consumer_group)?;
    client
        .subscribe(FallibleTestHandler { messages_tx })
        .await?;

    // Send test messages and get expected count
    let expected_message_count = send_test_messages(&client, &topics).await?;

    // Collect messages with timeout
    let received_messages = collect_messages_with_timeout(
        &mut messages_rx,
        expected_message_count,
        MESSAGE_TIMEOUT_SECS,
    )
    .await?;

    // Cleanup consumer before verification
    client.unsubscribe().await?;

    // Verify messages match expectations
    verify_messages(&received_messages, &topics)?;

    // Cleanup test topics
    cleanup_topics(&admin_client, &topics).await?;

    Ok(())
}
