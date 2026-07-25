//! Integration tests for the deduplication middleware.
//!
//! Verifies that messages with identical event IDs are deduplicated when
//! processed through the pipeline consumer with the deduplication middleware.

#![recursion_limit = "256"]

use crate::common::{FallibleTestHandler, collect_messages_with_timeout};
use color_eyre::eyre::{Result, ensure};
use prosody::consumer::middleware::deduplication::DeduplicationConfigurationBuilder;
use prosody::consumer::middleware::defer::DeferConfigurationBuilder;
use prosody::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
use prosody::consumer::middleware::retry::RetryConfigurationBuilder;
use prosody::consumer::middleware::scheduler::SchedulerConfigurationBuilder;
use prosody::consumer::middleware::timeout::TimeoutConfigurationBuilder;
use prosody::consumer::{
    CommonConfiguration, ConsumerConfiguration, ConsumerSetup, KeyedStateConfiguration,
    PipelineMiddlewareConfiguration, ProsodyConsumer,
};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::telemetry::Telemetry;
use prosody::tracing::init_test_logging;
use prosody::{
    JsonCodec, Topic,
    admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration},
};
use serde_json::json;
use tokio::sync::mpsc::channel;
use uuid::Uuid;

mod common;

/// Tests that two messages with the same event ID are deduplicated.
///
/// Sends two messages with the same key and `"id"` field through a pipeline
/// consumer with the deduplication middleware enabled. Only the first message
/// should be delivered to the handler.
///
/// # Errors
///
/// Returns an error if topic setup, producer/consumer initialization, or
/// message verification fails.
#[tokio::test]
async fn test_pipeline_deduplication_of_same_event_id() -> Result<()> {
    init_test_logging();

    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap.clone())?)?;

    admin_client
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(1_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;

    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap)
        .group_id(Uuid::new_v4().to_string())
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    let (messages_tx, mut messages_rx) = channel(10);
    let handler = FallibleTestHandler { messages_tx };

    let telemetry = Telemetry::new();
    let producer = ProsodyProducer::<JsonCodec>::new(&producer_config, telemetry.sender())?;

    let pipeline_config = PipelineMiddlewareConfiguration {
        retry: RetryConfigurationBuilder::default().build()?,
        monopolization: MonopolizationConfigurationBuilder::default().build()?,
        defer: DeferConfigurationBuilder::default().build()?,
    };

    let common_config = CommonConfiguration {
        scheduler: SchedulerConfigurationBuilder::default().build()?,
        timeout: TimeoutConfigurationBuilder::default().build()?,
        dedup: DeduplicationConfigurationBuilder::default().build()?,
        keyed_state: KeyedStateConfiguration::builder().build()?,
    };

    let consumer = ProsodyConsumer::<JsonCodec>::pipeline_consumer(
        ConsumerSetup {
            consumer: &consumer_config,
            trigger_store: &common::create_cassandra_trigger_store_config(),
            common: &common_config,
            deps: None,
        },
        pipeline_config,
        telemetry,
        handler,
    )
    .await?;

    // Send two messages with the same event ID
    let key = "test-key";
    let event_id = "event-123";
    let payload = json!({ "id": event_id, "value": "first message" });
    let payload_duplicate = json!({ "id": event_id, "value": "duplicate message" });

    producer.send([], topic, key, payload.clone()).await?;
    producer.send([], topic, key, payload_duplicate).await?;

    // Always shut the consumer down and delete the topic before propagating a
    // failure. An early return would leave the consumer's client threads alive
    // and hang the test binary, and it would leak the UUID-named topic in the
    // shared cluster.
    let outcome = async {
        // Only the first message should be processed.
        let received = collect_messages_with_timeout(&mut messages_rx, 1_usize, 30_u64).await?;
        ensure!(
            received.len() == 1,
            "Expected one message due to deduplication, got {}",
            received.len()
        );
        let (recv_key, recv_payload) = &received[0];
        ensure!(recv_key == key);
        ensure!(recv_payload == &payload);
        Ok(())
    }
    .await;
    consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;
    outcome
}
