//! This module tests backpressure in the Prosody message-passing system.
//!
//! It simulates a scenario where a producer sends a large number of messages
//! while the consumer processes them slowly, demonstrating the backpressure
//! handling capabilities of the system.

#![recursion_limit = "256"]

use std::time::Duration;

use crate::common::handler::ChannelHandler;
use color_eyre::eyre::Result;
use prosody::tracing::init_test_logging;
use prosody::{
    JsonCodec,
    consumer::middleware::CloneProvider,
    consumer::{ConsumerConfiguration, KeyedStateConfiguration, ProsodyConsumer},
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

    // Create a unique four-partition topic for the test
    let (topic, admin_client) = common::kafka::create_topic_with_partitions(4).await?;
    let bootstrap: Vec<String> = vec!["localhost:9094".to_owned()];

    // Use a channel with a buffer capacity to accommodate slow processing
    let (messages_tx, mut messages_rx) = channel(64);

    // Configure the consumer with a slow message handler
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id(Uuid::new_v4().to_string().as_str())
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    // Set up the producer configuration. Every fallible step sits before the
    // consumer starts, so nothing between its start and its shutdown can
    // return early and leak the consumer or the topic.
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;

    let producer = ProsodyProducer::<JsonCodec>::new(&producer_config, Telemetry::new().sender())?;

    let slow_handler = ChannelHandler::with_delay(messages_tx, Duration::from_secs(1));
    let consumer: ProsodyConsumer<JsonCodec> = ProsodyConsumer::new(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        KeyedStateConfiguration::builder().build()?,
        CloneProvider::new(slow_handler),
        Telemetry::new(),
    )
    .await?;

    // Produce a large number of messages
    let total = 1_000u32;

    // Start production of messages in a separate task
    spawn(async move {
        for i in 0..total {
            let payload = json!({ "seq": i });
            if let Err(e) = producer.send([], topic, &i.to_string(), payload).await {
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
    let shutdown = consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;
    shutdown?;
    Ok(())
}
