//! Tests for the producer deduplication functionality in Prosody.
//!
//! This module verifies that the Prosody messaging system correctly handles
//! message deduplication based on event IDs within message payloads.

use color_eyre::eyre::{self, ensure};
use eyre::Result;
use prosody::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::UncommittedMessage;
use prosody::consumer::{ConsumerConfiguration, EventHandler, Keyed, ProsodyConsumer};
use prosody::consumer::middleware::CloneProvider;
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::timers::UncommittedTimer;
use prosody::{Payload, Topic};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::timeout;
use uuid::Uuid;

mod common;

/// Handler that forwards messages to a channel for test verification.
#[derive(Clone)]
struct TestHandler {
    tx: Sender<(String, Value)>,
}

impl EventHandler for TestHandler {
    async fn on_message<C>(&self, _ctx: C, msg: UncommittedMessage)
    where
        C: EventContext,
    {
        let (inner, uncommitted) = msg.into_inner();
        let key = inner.key().to_string();
        let payload = inner.payload().clone();
        let _ = self.tx.send((key, payload)).await;
        uncommitted.commit();
    }

    async fn on_timer<C, U>(&self, _context: C, _timer: U)
    where
        C: EventContext,
        U: UncommittedTimer,
    {
    }

    async fn shutdown(self) {}
}

/// Asserts that a message with the expected key and payload is received.
async fn expect_message(
    rx: &mut Receiver<(String, Value)>,
    timeout_dur: Duration,
    expected_key: &str,
    expected_payload: &Value,
) -> Result<()> {
    let (key, payload) = timeout(timeout_dur, rx.recv())
        .await?
        .ok_or_else(|| eyre::eyre!("Timed out waiting for message for key {}", expected_key))?;
    ensure!(
        key == expected_key,
        "expected key {}, got {}",
        expected_key,
        key
    );
    ensure!(
        &payload == expected_payload,
        "expected payload {:?}, got {:?}",
        expected_payload,
        payload
    );
    Ok(())
}

/// Tests that messages with duplicate IDs but the same key are deduplicated.
async fn case_duplicate_id_same_key(
    producer: &ProsodyProducer,
    topic: &Topic,
    rx: &mut Receiver<(String, Value)>,
    receive_timeout: Duration,
    no_message_timeout: Duration,
) -> Result<()> {
    let key = "dup-key";
    let event_id = "E";
    let first = Payload::from(json!({"id": event_id, "value": "first"}));
    let second = Payload::from(json!({"id": event_id, "value": "second"}));

    producer.send([], *topic, key, &first).await?;
    producer.send([], *topic, key, &second).await?;

    // Only the first should be emitted
    expect_message(
        rx,
        receive_timeout,
        key,
        &json!({"id": event_id, "value": "first"}),
    )
    .await?;
    ensure!(
        timeout(no_message_timeout, rx.recv()).await.is_err(),
        "duplicate message was unexpectedly received"
    );
    Ok(())
}

/// Tests that messages with the same ID but different keys are not
/// deduplicated.
async fn case_same_id_different_keys(
    producer: &ProsodyProducer,
    topic: &Topic,
    rx: &mut Receiver<(String, Value)>,
    receive_timeout: Duration,
) -> Result<()> {
    let event_id = "E";
    let payload = Payload::from(json!({"id": event_id, "value": "payload"}));
    let keys = ["key-A", "key-B"];
    for &k in &keys {
        producer.send([], *topic, k, &payload).await?;
    }

    let mut seen = Vec::new();
    for _ in 0..2u8 {
        let (k, p) = timeout(receive_timeout, rx.recv())
            .await?
            .ok_or_else(|| eyre::eyre!("Expected message for case_same_id_different_keys"))?;
        seen.push((k, p));
    }
    for &k in &keys {
        ensure!(
            seen.iter()
                .any(|(key, pl)| key == k && pl == &json!({"id": event_id, "value": "payload"})),
            "missing message for key {}",
            k
        );
    }
    Ok(())
}

/// Tests that messages with different IDs but the same key are all delivered.
async fn case_distinct_ids_same_key(
    producer: &ProsodyProducer,
    topic: &Topic,
    rx: &mut Receiver<(String, Value)>,
    receive_timeout: Duration,
) -> Result<()> {
    let key = "same-key";
    let one = Payload::from(json!({"id": "e1", "value": "one"}));
    let two = Payload::from(json!({"id": "e2", "value": "two"}));

    producer.send([], *topic, key, &one).await?;
    producer.send([], *topic, key, &two).await?;

    expect_message(
        rx,
        receive_timeout,
        key,
        &json!({"id": "e1", "value": "one"}),
    )
    .await?;
    expect_message(
        rx,
        receive_timeout,
        key,
        &json!({"id": "e2", "value": "two"}),
    )
    .await?;
    Ok(())
}

/// Tests that the deduplication state resets after a message without an ID.
async fn case_reset_after_none(
    producer: &ProsodyProducer,
    topic: &Topic,
    rx: &mut Receiver<(String, Value)>,
    receive_timeout: Duration,
    no_message_timeout: Duration,
) -> Result<()> {
    let key = "reset-key";
    let event_id = "X";
    let a = Payload::from(json!({"id": event_id, "value": "first"}));
    let none = Payload::from(json!({"value": "second"}));
    let b = Payload::from(json!({"id": event_id, "value": "third"}));

    producer.send([], *topic, key, &a).await?;
    producer.send([], *topic, key, &none).await?;
    producer.send([], *topic, key, &b).await?;

    expect_message(
        rx,
        receive_timeout,
        key,
        &json!({"id": event_id, "value": "first"}),
    )
    .await?;
    expect_message(rx, receive_timeout, key, &json!({"value": "second"})).await?;
    expect_message(
        rx,
        receive_timeout,
        key,
        &json!({"id": event_id, "value": "third"}),
    )
    .await?;

    ensure!(
        timeout(no_message_timeout, rx.recv()).await.is_err(),
        "unexpected extra message after reset"
    );
    Ok(())
}

/// Tests that after switching IDs, a subsequent message with the original ID
/// is delivered (i.e., cache updated on different ID). This catches the bug
/// where the cache wasn’t updated when encountering a new event ID.
async fn case_return_to_original_id(
    producer: &ProsodyProducer,
    topic: &Topic,
    rx: &mut Receiver<(String, Value)>,
    receive_timeout: Duration,
) -> Result<()> {
    let key = "return-key";
    let id1 = "first_id";
    let id2 = "second_id";
    let m1 = Payload::from(json!({"id": id1, "value": "one"}));
    let m2 = Payload::from(json!({"id": id2, "value": "two"}));
    let m3 = Payload::from(json!({"id": id1, "value": "three"}));

    producer.send([], *topic, key, &m1).await?;
    producer.send([], *topic, key, &m2).await?;
    producer.send([], *topic, key, &m3).await?;

    expect_message(
        rx,
        receive_timeout,
        key,
        &json!({"id": id1, "value": "one"}),
    )
    .await?;
    expect_message(
        rx,
        receive_timeout,
        key,
        &json!({"id": id2, "value": "two"}),
    )
    .await?;
    expect_message(
        rx,
        receive_timeout,
        key,
        &json!({"id": id1, "value": "three"}),
    )
    .await?;
    Ok(())
}

/// Tests the message deduplication behavior of the Prosody producer.
#[tokio::test]
async fn test_producer_deduplication() -> Result<()> {
    // Initialize tracing for easier debugging
    common::init_test_logging()?;

    // Setup test environment with Kafka broker
    let brokers = vec!["localhost:9094".to_owned()];
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let admin = ProsodyAdminClient::new(&AdminConfiguration::new(brokers.clone())?)?;
    admin
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(1_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    // Configure producer with idempotence cache
    let producer = ProsodyProducer::new(
        &ProducerConfiguration::builder()
            .bootstrap_servers(brokers.clone())
            .source_system("test-producer")
            .idempotence_cache_size(2usize)
            .build()?,
    )?;

    // Set up consumer with test handler
    let consumer = {
        let cfg = ConsumerConfiguration::builder()
            .bootstrap_servers(brokers.clone())
            .group_id("test-dedup-consumer")
            .subscribed_topics(&[topic.to_string()])
            .commit_interval(Duration::from_secs(1))
            .stall_threshold(Duration::from_secs(60))
            .probe_port(None)
            .build()?;
        let (tx, rx) = channel(16);
        let handler = TestHandler { tx };
        (
            ProsodyConsumer::new(
                &cfg,
                &common::create_cassandra_trigger_store_config(),
                CloneProvider::new(handler),
            )
            .await?,
            rx,
        )
    };
    let (consumer_client, mut rx) = consumer;

    // Common timeouts for test cases
    let receive_timeout = Duration::from_secs(30);
    let no_message_timeout = Duration::from_secs(10);

    // Execute test cases
    case_duplicate_id_same_key(
        &producer,
        &topic,
        &mut rx,
        receive_timeout,
        no_message_timeout,
    )
    .await?;
    case_same_id_different_keys(&producer, &topic, &mut rx, receive_timeout).await?;
    case_distinct_ids_same_key(&producer, &topic, &mut rx, receive_timeout).await?;
    case_reset_after_none(
        &producer,
        &topic,
        &mut rx,
        receive_timeout,
        no_message_timeout,
    )
    .await?;
    case_return_to_original_id(&producer, &topic, &mut rx, receive_timeout).await?;

    // Teardown
    consumer_client.shutdown().await;
    admin.delete_topic(&topic).await?;
    Ok(())
}
