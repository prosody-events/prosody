//! This module tests backpressure in the Prosody message-passing system.
//!
//! It simulates a scenario where a producer sends a large number of messages
//! while the consumer processes them slowly, demonstrating the backpressure
//! handling capabilities of the system.

use crate::common::SlowTestHandler;
use color_eyre::eyre::Result;
use prosody::tracing::init_test_logging;
use prosody::{
    Topic,
    admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration},
    consumer::middleware::CloneProvider,
    consumer::{ConsumerConfiguration, ProsodyConsumer},
    producer::{ProducerConfiguration, ProsodyProducer},
    telemetry::Telemetry,
};
use serde_json::json;
use tokio::spawn;
use tokio::sync::mpsc::channel;
use tokio::time::Instant;
use tracing::{error, info};
use uuid::Uuid;

mod common;

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

    // Create a unique topic for the test
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap: Vec<String> = vec!["localhost:9094".to_owned()];

    // Setup an admin client to manage the topic creation
    let admin_client = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap.clone())?)?;
    admin_client
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(4_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    // Use a channel with a buffer capacity to accommodate slow processing
    let (messages_tx, mut messages_rx) = channel(64);

    // Configure the consumer with a slow message handler
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id(Uuid::new_v4().to_string().as_str())
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    let slow_handler = SlowTestHandler { messages_tx };
    let consumer = ProsodyConsumer::new(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        CloneProvider::new(slow_handler),
        Telemetry::new(),
    )
    .await?;

    // Set up the producer configuration
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;

    let producer = ProsodyProducer::new(&producer_config)?;

    // Produce a large number of messages
    let total = 1_000u32;

    // Start production of messages in a separate task
    spawn(async move {
        for i in 0..total {
            let payload = json!({ "seq": i });
            if let Err(e) = producer.send([], topic, &i.to_string(), &payload).await {
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
