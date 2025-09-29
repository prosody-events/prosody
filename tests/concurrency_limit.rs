//! This module tests the global concurrency limit in the Prosody system across
//! multiple partitions.
//!
//! The test, `test_global_concurrency_limit_multi_partition`, performs the
//! following steps:
//!
//! 1. **Topic and Consumer Configuration:**
//! - Creates a new Kafka topic with 3 partitions.
//! - Configures a consumer with a global concurrency limit (set to 3 in this
//!   test).
//! - Provides a custom event handler (`ConcurrencyTestHandler`) that tracks
//!   concurrent message processing.
//!
//! 2. **Message Processing Behavior:**
//! - For each received message, the handler increments a counter tracking
//!   currently processing tasks.
//! - It updates a maximum observed counter to track the highest concurrency
//!   level reached.
//! - The handler blocks until receiving a release signal through a watch
//!   channel.
//! - Once released, it commits the message and decrements the processing
//!   counter.
//!
//! 3. **Test Execution:**
//! - Produces 30 messages with 30 different keys to distribute them across
//!   partitions.
//! - Immediately signals release to unblock all waiting handlers.
//! - Waits for notification that all messages have been processed.
//! - Verifies that the maximum number of concurrently running handler tasks
//!   never exceeded the global limit.
//!
//! This test validates that the global concurrency limit works correctly by
//! ensuring that the maximum number of messages processed simultaneously stays
//! within the configured limit, even when messages are distributed across
//! multiple partitions.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use color_eyre::eyre::{Result, eyre};
use prosody::consumer::Uncommitted;
use prosody::consumer::event_context::EventContext;
use prosody::timers::UncommittedTimer;
use prosody::{
    Topic,
    admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration},
    consumer::ConsumerConfiguration,
    consumer::EventHandler,
    consumer::ProsodyConsumer,
    consumer::message::UncommittedMessage,
    consumer::middleware::CloneProvider,
    producer::ProducerConfiguration,
    producer::ProsodyProducer,
};
use serde_json::json;
use tokio::sync::{Notify, watch};
use uuid::Uuid;

mod common;

/// A custom event handler for testing the global concurrency limit enforcement.
///
/// This handler keeps track of the number of concurrently running handlers and
/// blocks each message handler until receiving a signal to proceed.
#[derive(Clone)]
struct ConcurrencyTestHandler {
    /// Total number of messages expected to process.
    total: usize,
    /// Counter of messages that have completed processing.
    processed: Arc<AtomicUsize>,
    /// Number of concurrently running handlers at any given time.
    current: Arc<AtomicUsize>,
    /// Maximum number of concurrently running handlers observed during the
    /// test.
    max_concurrent: Arc<AtomicUsize>,
    /// Watch receiver to coordinate when handlers should complete processing.
    release_rx: watch::Receiver<bool>,
    /// Notification mechanism to signal when all messages have been processed.
    notify: Arc<Notify>,
}

impl EventHandler for ConcurrencyTestHandler {
    async fn on_message<C>(&self, _context: C, message: UncommittedMessage)
    where
        C: EventContext,
    {
        // Increment the current processing count and update maximum observed
        // concurrency
        let current = self.current.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_concurrent.fetch_max(current, Ordering::AcqRel);

        // Block until receiving the release signal
        let mut rx = self.release_rx.clone();
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }

        // Mark processing as complete
        self.current.fetch_sub(1, Ordering::AcqRel);
        self.processed.fetch_add(1, Ordering::AcqRel);
        message.commit().await;

        // If all expected messages have been processed, notify waiters
        if self.processed.load(Ordering::Acquire) == self.total {
            self.notify.notify_waiters();
        }
    }

    async fn on_timer<C, U>(&self, _context: C, _timer: U)
    where
        C: EventContext,
        U: UncommittedTimer,
    {
    }

    async fn shutdown(self) {}
}

/// Produces test messages with varying keys to distribute them across
/// partitions.
///
/// # Arguments
/// * `producer` - Prosody producer instance to send messages
/// * `topic` - Destination topic for the messages
/// * `total` - Total number of messages to produce
/// * `num_keys` - Number of distinct keys to use (for partition distribution)
///
/// # Errors
/// Returns an error if message production fails.
async fn produce_messages(
    producer: &ProsodyProducer,
    topic: Topic,
    total: usize,
    num_keys: usize,
) -> Result<()> {
    for seq in 0..total {
        let key = format!("key-{}", seq % num_keys);
        let payload = json!({
            "seq": seq,
            "content": "test message"
        });
        producer.send([], topic, &key, &payload).await?;
    }
    Ok(())
}

/// Tests that the global concurrency limit is properly enforced across multiple
/// partitions.
///
/// This test creates a topic with three partitions and configures a consumer
/// with a global concurrency limit of three. It produces messages distributed
/// across partitions and verifies that the number of concurrently processed
/// messages never exceeds the configured limit.
///
/// # Errors
/// Returns an error if:
/// - Topic creation or deletion fails
/// - Consumer or producer creation fails
/// - Message production fails
/// - The concurrency limit is exceeded during the test
#[tokio::test]
async fn test_global_concurrency_limit_multi_partition() -> Result<()> {
    // Initialize logging
    common::init_test_logging()?;

    // Create a topic with 3 partitions
    let partitions = 3_u16;
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::new(&AdminConfiguration::new(bootstrap.clone())?)?;
    admin_client
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(partitions)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    // Configure test parameters
    let global_limit = 3;
    let total_messages = 30;
    let num_keys = 30;

    // Configure the consumer (concurrency limit will be set via environment
    // variable)
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id("test-global-concurrency-consumer-multi")
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    // Create a watch channel for controlling message handlers
    let (release_tx, release_rx) = watch::channel(false);

    // Create the custom test handler
    let handler = ConcurrencyTestHandler {
        total: total_messages,
        processed: Arc::new(AtomicUsize::new(0)),
        current: Arc::new(AtomicUsize::new(0)),
        max_concurrent: Arc::new(AtomicUsize::new(0)),
        release_rx,
        notify: Arc::new(Notify::new()),
    };

    // Create the consumer with the test handler
    let consumer: ProsodyConsumer = ProsodyConsumer::new(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        CloneProvider::new(handler.clone()),
    )
    .await?;

    // Configure and create the producer
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;
    let producer = ProsodyProducer::new(&producer_config)?;

    // Produce messages with varying keys to distribute across partitions
    produce_messages(&producer, topic, total_messages, num_keys).await?;

    // Signal all handlers to complete processing
    release_tx.send(true)?;

    // Wait for all messages to be processed
    handler.notify.notified().await;

    // Verify that the concurrency limit was respected
    let max_observed = handler.max_concurrent.load(Ordering::SeqCst);
    if max_observed > global_limit {
        return Err(eyre!(
            "Maximum concurrent tasks observed ({max_observed}) exceeded global limit \
             ({global_limit})"
        ));
    }

    // Clean up resources
    consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;
    Ok(())
}
