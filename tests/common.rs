//! Common utilities and types for Prosody tests.
//!
//! This module provides shared structures, functions, and test handlers used
//! across various test cases for the Prosody system. It includes utility
//! functions for setting up topics and configurations, managing producer and
//! consumer tasks, and verifying message integrity and order in property-based
//! tests.

#![allow(dead_code, clippy::implicit_hasher)]

use std::cmp::max;
use std::collections::{BTreeSet, HashSet};
use std::fmt::{Debug, Formatter};
use std::time::Duration;

use ahash::{HashMap, HashMapExt};
use color_eyre::eyre::{Result, eyre};
use derive_quickcheck_arbitrary::Arbitrary;
use itertools::Itertools;
use prosody::Topic;
use prosody::admin::ProsodyAdminClient;
use prosody::consumer::message::{MessageContext, UncommittedMessage};
use prosody::consumer::{ConsumerConfiguration, EventHandler, Keyed, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use quickcheck::{Arbitrary as QCArbitrary, Gen};
use serde_json::{Value, json};
use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{error, info, instrument};
use uuid::Uuid;

/// A small, non-zero count used in tests.
///
/// Provides a way to ensure small non-zero values are used within test cases,
/// particularly in property-based testing scenarios.
#[derive(Copy, Clone)]
pub struct SmallCount(u8);

impl SmallCount {
    /// Retrieves the underlying value of `SmallCount` as a `usize`.
    #[must_use]
    pub fn value(self) -> usize {
        self.0 as usize
    }
}

impl QCArbitrary for SmallCount {
    fn arbitrary(g: &mut Gen) -> Self {
        // Provide a constant array of sequential non-zero values and
        // ensure we always select at least 1 as a fallback.
        const VALUES: [u8; 12] = const_array();
        Self(*g.choose(&VALUES).unwrap_or(&1))
    }
}

impl Debug for SmallCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Generates a constant array of sequential u8 values.
const fn const_array<const N: usize>() -> [u8; N] {
    let mut arr = [0; N];
    let mut i = 0;
    while i < N {
        arr[i] = i as u8 + 1;
        i += 1;
    }
    arr
}

/// Represents test input parameters for property-based tests.
#[derive(Clone, Debug, Arbitrary)]
pub struct TestInput {
    /// A map from a key to a set of messages associated with the key.
    pub messages: HashMap<u64, BTreeSet<u64>>,

    /// The number of partitions each test topic should have.
    pub partition_count: SmallCount,

    /// The number of producers that should be spawned for the test.
    pub producer_count: SmallCount,

    /// The number of consumers that should be spawned for the test.
    pub consumer_count: SmallCount,

    /// The maximum number of messages that can be enqueued per key.
    pub max_enqueued_per_key: SmallCount,
}

/// Creates a test topic with a specified partition count.
///
/// # Arguments
///
/// * `partition_count` - The number of partitions for the test topic.
///
/// Returns a tuple containing the created topic and an admin client for cleanup
/// tasks.
///
/// # Errors
///
/// Returns an error if the topic creation fails.
pub async fn create_test_topic(partition_count: SmallCount) -> Result<(Topic, ProsodyAdminClient)> {
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let admin_client = ProsodyAdminClient::new(&["localhost:9094"])?;

    admin_client
        .create_topic(&topic, partition_count.value() as u16, 1)
        .await?;

    info!("created topic: {topic}");
    Ok((topic, admin_client))
}

/// Creates producer and consumer configurations for a specified topic.
///
/// # Arguments
///
/// * `topic` - The topic for which configurations are created.
/// * `max_enqueued_per_key` - Maximum messages to enqueue per key.
///
/// Returns producer and consumer configurations.
///
/// # Errors
///
/// Returns an error if configuration creation fails.
pub fn create_configs(
    topic: &Topic,
    max_enqueued_per_key: SmallCount,
) -> Result<(ProducerConfiguration, ConsumerConfiguration)> {
    let bootstrap: Vec<String> = vec!["localhost:9094".to_owned()];

    // Configure the producer settings
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;

    // Configure the consumer settings
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap)
        .group_id("test-consumer")
        .subscribed_topics(&[topic.to_string()])
        .max_enqueued_per_key(max_enqueued_per_key.value())
        .commit_interval(Duration::from_secs(1))
        .stall_threshold(Duration::from_secs(60))
        .probe_port(None)
        .build()?;

    Ok((producer_config, consumer_config))
}

/// Spawns producer tasks for property-based testing.
///
/// # Arguments
///
/// * `tasks` - A set of tasks to manage spawned producers.
/// * `input` - Contains the messages and number of producers to spawn.
/// * `producer_config` - Configuration used for the producer.
/// * `topic` - The topic to which producers send messages.
pub fn spawn_producers(
    tasks: &mut JoinSet<Result<()>>,
    input: &TestInput,
    producer_config: &ProducerConfiguration,
    topic: &Topic,
) {
    let message_count = input.messages.len();
    let producer_message_count = max(message_count / input.producer_count.value(), 1);

    for producer_messages in input
        .messages
        .clone()
        .into_iter()
        .chunks(producer_message_count)
        .into_iter()
        .map(Iterator::collect::<Vec<_>>)
    {
        let producer_config = producer_config.clone();
        let topic = *topic;

        tasks.spawn(async move {
            let producer = ProsodyProducer::new(&producer_config)?;
            for (key, messages) in producer_messages {
                let key = key.to_string();
                for message in messages {
                    producer.send([], topic, &key, &json!(message)).await?; // Send each message
                }
                producer.send([], topic, &key, &Value::Null).await?; // Send the end-of-stream marker
            }
            Ok(())
        });
    }
}

/// Spawns consumer tasks for property-based testing.
///
/// # Arguments
///
/// * `tasks` - A set of tasks to manage spawned consumers.
/// * `consumer_count` - Number of consumers to spawn.
/// * `consumer_config` - Configuration used for the consumer.
/// * `messages_tx` - Channel for transmitting received messages.
/// * `shutdown_rx` - Receiver for shutdown signals.
pub fn spawn_consumers(
    tasks: &mut JoinSet<Result<()>>,
    consumer_count: SmallCount,
    consumer_config: &ConsumerConfiguration,
    messages_tx: &Sender<(String, Value)>,
    shutdown_rx: &watch::Receiver<bool>,
) {
    for _ in 0..consumer_count.value() {
        let consumer_config = consumer_config.clone();
        let messages_tx = messages_tx.clone();
        let mut shutdown_rx = shutdown_rx.clone();

        let handler = TestHandler { messages_tx };

        tasks.spawn(async move {
            let consumer = ProsodyConsumer::new::<TestHandler>(&consumer_config, handler)?;
            shutdown_rx.wait_for(|is_shutdown| *is_shutdown).await?; // Wait for shutdown signal
            consumer.shutdown().await; // Shut down consumer gracefully
            Ok(())
        });
    }
}

/// Spawns a task to receive and verify messages for property-based testing.
///
/// # Arguments
///
/// * `tasks` - A set of tasks to manage message verification.
/// * `messages_rx` - Channel for receiving messages.
/// * `shutdown_tx` - Channel for sending shutdown signals.
/// * `expected_messages` - Map of expected key-value message pairs.
pub fn spawn_message_verifier(
    tasks: &mut JoinSet<Result<()>>,
    mut messages_rx: tokio::sync::mpsc::Receiver<(String, Value)>,
    shutdown_tx: watch::Sender<bool>,
    expected_messages: HashMap<u64, BTreeSet<u64>>,
) {
    tasks.spawn(async move {
        // Track keys and received messages to verify against expected results
        let mut keys: HashSet<String> = expected_messages.keys().map(ToString::to_string).collect();
        let mut received: HashMap<String, Vec<u64>> =
            HashMap::with_capacity(expected_messages.len());

        info!("receiving messages");

        // Receive messages and collect them
        while let Some((key, payload)) = messages_rx.recv().await {
            match payload {
                Value::Number(number) => {
                    let number = number.as_u64().ok_or_else(|| eyre!("invalid number"))?;
                    received.entry(key).or_default().push(number);
                }
                Value::Null => {
                    keys.remove(&key);
                    // Break loop if all keys are processed
                    if keys.is_empty() {
                        break;
                    }
                }
                _ => return Err(eyre!("unexpected payload type")),
            }
        }

        // Verify received messages
        verify_results(&expected_messages, received)?;

        info!("sending shutdown signal");
        shutdown_tx.send(true)?; // Send shutdown signal

        Ok(())
    });
}

/// Verifies that the received messages align with the expected ones.
///
/// # Arguments
///
/// * `expected` - Map of expected key-value message pairs.
/// * `received` - Map of actually received message data.
///
/// # Errors
///
/// Returns an error if the verification process reveals a mismatch.
pub fn verify_results(
    expected: &HashMap<u64, BTreeSet<u64>>,
    received: HashMap<String, Vec<u64>>,
) -> Result<()> {
    // Prepare actual data map from received messages for comparison
    let mut actual: HashMap<u64, BTreeSet<u64>> = HashMap::with_capacity(expected.len());

    for (key, received_messages) in received {
        let key: u64 = key.parse()?;
        let received_set: BTreeSet<u64> = received_messages.iter().copied().collect();
        actual.insert(key, received_set);

        // Verify order of messages
        let mut sorted = received_messages.clone();
        sorted.sort_unstable();

        if received_messages != sorted {
            return Err(eyre!(
                "invalid order for key {key}; expected: {sorted:?}, actual: {received_messages:?}"
            ));
        }
    }

    // Compare expected and actual results
    if *expected != actual {
        return Err(eyre!(
            "all messages were not received; expected: {expected:?}, actual: {actual:?}"
        ));
    }

    Ok(())
}

/// Executes the core logic for a property-based test.
///
/// # Arguments
///
/// * `input` - Test input containing message and configuration data.
///
/// # Errors
///
/// Returns an error if any part of the test setup, execution, or verification
/// fails.
pub async fn run_test(input: TestInput) -> Result<()> {
    // Create test topic and configuration settings
    let (topic, admin_client) = create_test_topic(input.partition_count).await?;
    let (producer_config, consumer_config) = create_configs(&topic, input.max_enqueued_per_key)?;

    // Setup channels and task management
    let (messages_tx, messages_rx) = channel(input.partition_count.value());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut tasks = JoinSet::new();

    // Spawn producers and consumers, and start message verification
    spawn_producers(&mut tasks, &input, &producer_config, &topic);
    spawn_consumers(
        &mut tasks,
        input.consumer_count,
        &consumer_config,
        &messages_tx,
        &shutdown_rx,
    );
    spawn_message_verifier(&mut tasks, messages_rx, shutdown_tx, input.messages);

    // Wait for all tasks to complete
    while let Some(result) = tasks.join_next().await {
        result??;
    }

    info!("test passed");

    // Clean up the test topic
    admin_client.delete_topic(&topic).await?;
    info!("deleted test topic: {topic}");

    Ok(())
}

/// A simple test implementation of the `EventHandler` trait that forwards
/// messages to a channel.
#[derive(Clone, Debug)]
pub struct TestHandler {
    /// A channel for transmitting received messages.
    pub messages_tx: Sender<(String, Value)>,
}

impl EventHandler for TestHandler {
    async fn on_message(&self, _context: MessageContext, message: UncommittedMessage) {
        let (msg, uncommitted) = message.into_inner();
        let message = msg.into_value();

        // Forward the message to the channel
        if let Err(error) = self
            .messages_tx
            .send((message.key.to_string(), message.payload))
            .await
        {
            error!("failed to send message: {error:#}");
        }

        uncommitted.commit(); // Commit message to mark as processed
    }

    async fn shutdown(self) {
        info!("TestHandler shutdown");
    }
}

/// A handler implementation that simulates backpressure by introducing a delay
/// in processing.
#[derive(Clone, Debug)]
pub struct SlowTestHandler {
    /// A channel for transmitting received messages.
    pub messages_tx: Sender<(String, Value)>,
}

impl EventHandler for SlowTestHandler {
    #[instrument(skip(self, _context))]
    async fn on_message(&self, _context: MessageContext, message: UncommittedMessage) {
        let (msg, uncommitted) = message.into_inner();
        let key = msg.key().to_string();
        let payload: Value = msg.payload().clone();

        // Simulate backpressure with a delay
        sleep(Duration::from_secs(1)).await;

        if let Err(e) = self.messages_tx.send((key.clone(), payload)).await {
            error!("failed to send message for key {}: {e:#}", key);
        }
        uncommitted.commit(); // Commit message to mark as processed
    }

    async fn shutdown(self) {
        info!("SlowTestHandler shutdown");
    }
}
