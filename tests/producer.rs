//! Tests for the producer deduplication functionality in Prosody.
//!
//! This module verifies that the Prosody messaging system correctly handles
//! message deduplication based on event IDs within message payloads.

use color_eyre::eyre::{self, ensure};
use eyre::Result;
use prosody::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::UncommittedMessage;
use prosody::consumer::middleware::CloneProvider;
use prosody::consumer::{ConsumerConfiguration, DemandType, EventHandler, Keyed, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::telemetry::Telemetry;
use prosody::timers::UncommittedTimer;
use prosody::tracing::init_test_logging;
use prosody::{JsonCodec, Topic};
use serde_json::{Value, json};

type Payload = Value;
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
    type Payload = Value;

    async fn on_message<C>(&self, _ctx: C, msg: UncommittedMessage<Value>, _demand_type: DemandType)
    where
        C: EventContext,
    {
        let (inner, uncommitted) = msg.into_inner();
        let key = inner.key().to_string();
        let payload = inner.payload().clone();
        let _ = self.tx.send((key, payload)).await;
        uncommitted.commit();
    }

    async fn on_timer<C, U>(&self, _context: C, _timer: U, _demand_type: DemandType)
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
        .ok_or_else(|| eyre::eyre!("Channel closed waiting for key {}", expected_key))?;
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
    producer: &ProsodyProducer<JsonCodec>,
    topic: &Topic,
    rx: &mut Receiver<(String, Value)>,
    receive_timeout: Duration,
) -> Result<()> {
    let key = "dup-key";
    let event_id = "E";
    let first = Payload::from(json!({"id": event_id, "value": "first"}));
    let second = Payload::from(json!({"id": event_id, "value": "second"}));
    let eof = Payload::from(json!({"eof": true}));

    producer.send([], *topic, key, &first).await?;
    producer.send([], *topic, key, &second).await?;
    producer.send([], *topic, key, &eof).await?;

    // Only the first should be emitted; second is deduped; EOF proves it was
    // skipped
    expect_message(
        rx,
        receive_timeout,
        key,
        &json!({"id": event_id, "value": "first"}),
    )
    .await?;
    expect_message(rx, receive_timeout, key, &json!({"eof": true})).await?;
    Ok(())
}

/// Tests that messages with the same ID but different keys are not
/// deduplicated.
async fn case_same_id_different_keys(
    producer: &ProsodyProducer<JsonCodec>,
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
    producer: &ProsodyProducer<JsonCodec>,
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

/// Tests that a no-ID message does not clear dedup state.
/// Sending id=X, no-id, id=X delivers only the first two; the third is
/// deduplicated because the hash of (topic, key, X) is still in the cache.
async fn case_reset_after_none(
    producer: &ProsodyProducer<JsonCodec>,
    topic: &Topic,
    rx: &mut Receiver<(String, Value)>,
    receive_timeout: Duration,
) -> Result<()> {
    let key = "reset-key";
    let event_id = "X";
    let a = Payload::from(json!({"id": event_id, "value": "first"}));
    let none = Payload::from(json!({"value": "second"}));
    let b = Payload::from(json!({"id": event_id, "value": "third"}));
    let eof = Payload::from(json!({"eof": true}));

    producer.send([], *topic, key, &a).await?;
    producer.send([], *topic, key, &none).await?;
    producer.send([], *topic, key, &b).await?;
    producer.send([], *topic, key, &eof).await?;

    // First message delivered
    expect_message(
        rx,
        receive_timeout,
        key,
        &json!({"id": event_id, "value": "first"}),
    )
    .await?;
    // No-ID message delivered (skips cache entirely)
    expect_message(rx, receive_timeout, key, &json!({"value": "second"})).await?;
    // Third message (same id=X) is deduplicated; EOF proves it was skipped
    expect_message(rx, receive_timeout, key, &json!({"eof": true})).await?;
    Ok(())
}

/// Tests that sending id1→id2→id1 on the same key deduplicates the third
/// message, because the hash-based cache remembers all seen (topic, key, id)
/// triples — not just the most recent.
async fn case_return_to_original_id(
    producer: &ProsodyProducer<JsonCodec>,
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
    let eof = Payload::from(json!({"eof": true}));

    producer.send([], *topic, key, &m1).await?;
    producer.send([], *topic, key, &m2).await?;
    producer.send([], *topic, key, &m3).await?;
    producer.send([], *topic, key, &eof).await?;

    // First two delivered (distinct IDs)
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
    // Third (id1 again) is deduplicated; EOF proves it was skipped
    expect_message(rx, receive_timeout, key, &json!({"eof": true})).await?;
    Ok(())
}

/// Tests the message deduplication behavior of the Prosody producer.
#[tokio::test]
async fn test_producer_deduplication() -> Result<()> {
    init_test_logging();

    let brokers = vec!["localhost:9094".to_owned()];
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(brokers.clone())?)?;
    admin
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(1_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    let producer = ProsodyProducer::<JsonCodec>::new(
        &ProducerConfiguration::builder()
            .bootstrap_servers(brokers.clone())
            .source_system("test-producer")
            .idempotence_cache_size(20usize)
            .build()?,
        Telemetry::new().sender(),
    )?;

    let (consumer_client, mut rx) = {
        let cfg = ConsumerConfiguration::builder()
            .bootstrap_servers(brokers.clone())
            .group_id("test-dedup-consumer")
            .subscribed_topics(&[topic.to_string()])
            .commit_interval(Duration::from_secs(1))
            .stall_threshold(Duration::from_mins(1))
            .probe_port(None)
            .build()?;
        let (tx, rx) = channel(16);
        let consumer: ProsodyConsumer<JsonCodec> = ProsodyConsumer::new(
            &cfg,
            &common::create_cassandra_trigger_store_config(),
            CloneProvider::new(TestHandler { tx }),
            Telemetry::new(),
        )
        .await?;
        (consumer, rx)
    };

    let receive_timeout = Duration::from_secs(10);

    // Capture result so consumer is always shut down, even on failure.
    // Dropping the consumer without shutdown causes a blocking hang in Drop.
    let result = timeout(Duration::from_secs(20), async {
        case_duplicate_id_same_key(&producer, &topic, &mut rx, receive_timeout).await?;
        case_same_id_different_keys(&producer, &topic, &mut rx, receive_timeout).await?;
        case_distinct_ids_same_key(&producer, &topic, &mut rx, receive_timeout).await?;
        case_reset_after_none(&producer, &topic, &mut rx, receive_timeout).await?;
        case_return_to_original_id(&producer, &topic, &mut rx, receive_timeout).await?;
        Ok::<(), eyre::Report>(())
    })
    .await
    .map_err(|_| eyre::eyre!("test timed out after 20 seconds"))
    .and_then(|r| r);

    consumer_client.shutdown().await;
    admin.delete_topic(&topic).await?;
    result
}
