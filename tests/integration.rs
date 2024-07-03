//! Integration tests for the Prosody Kafka client library.
//!
//! This module contains a property-based integration test using `QuickCheck`
//! that verifies the correct functioning of the Prosody Kafka client library,
//! focusing on message ordering and completeness across multiple producers
//! and consumers.
//!
//! ## Test Overview
//!
//! The test is designed as a `QuickCheck` property, which means it's run
//! multiple times with randomly generated inputs. This approach helps to
//! explore a wide range of scenarios and increase confidence in the library's
//! robustness.
//!
//! For each run, the test simulates a scenario with multiple producers sending
//! messages to a Kafka topic and multiple consumers reading from that topic.
//! It verifies that:
//! 1. All messages are received.
//! 2. Messages for each key are received in the correct order.
//!
//! ## Test Parameters
//!
//! `QuickCheck` randomly generates the following parameters for each test run:
//! - `messages`: A map of keys to sets of message values.
//! - `partition_count`: The number of partitions in the Kafka topic.
//! - `producer_count`: The number of concurrent producers.
//! - `consumer_count`: The number of concurrent consumers.
//! - `max_enqueued_per_key`: Maximum number of messages to enqueue per key.
//!
//! ## Test Procedure
//!
//! 1. Create a Kafka topic with the specified number of partitions.
//! 2. Spawn multiple producer tasks, each sending a subset of the messages.
//! 3. Spawn multiple consumer tasks to read the messages.
//! 4. Collect and verify the received messages.
//!
//! ## Message Types
//!
//! The test uses two types of messages:
//! 1. Regular messages: JSON-encoded numbers.
//! 2. End-of-stream marker: A `null` value sent after all regular messages for
//!    a key.
//!
//! ## Completion Detection
//!
//! The test knows it's complete when it has received an end-of-stream marker
//! for every key in the input set. This ensures all messages have been
//! processed.
//!
//! ## Verification
//!
//! The test verifies correctness by:
//! 1. Checking that all expected messages are received.
//! 2. Ensuring messages for each key are received in the same order they were
//!    sent.
//!
//! ## Cleanup
//!
//! After each test run completes, it deletes the created Kafka topic to clean
//! up resources.
//!
//! This property-based test ensures that the Prosody library
//! correctly handles concurrent producers and consumers while maintaining
//! message ordering and completeness, which are crucial for many Kafka-based
//! applications. By using `QuickCheck`, the test can explore a wide range of
//! scenarios, increasing confidence in the library's correctness and
//! robustness.

use std::cmp::max;
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::time::Duration;

use ahash::{HashMap, HashMapExt, HashSet};
use color_eyre::eyre::{eyre, OptionExt, Result};
use color_eyre::Report;
use derive_quickcheck_arbitrary::Arbitrary;
use itertools::Itertools;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::FromClientConfig;
use rdkafka::ClientConfig;
use serde_json::{json, Value};
use tokio::runtime::Builder;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use tracing_subscriber::fmt;
use uuid::Uuid;

use prosody::consumer::message::{ConsumerMessage, MessageContext};
use prosody::consumer::{ConsumerConfiguration, MessageHandler, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::Topic;

/// Runs the main integration test to verify message ordering and completeness.
#[test]
fn receives_all_in_key_order() {
    // Try to load integration test count from environment variable
    let test_count = option_env!("INTEGRATION_TESTS")
        .and_then(|s| match s.parse::<u64>() {
            Ok(count) => Some(count),
            Err(error) => {
                warn!("invalid integration test count: {error:#}; using default");
                None
            }
        })
        .unwrap_or(3);

    // Initialize tracing for better test output
    fmt().compact().init();

    // Run the QuickCheck property-based test
    QuickCheck::new()
        .tests(test_count)
        .quickcheck(prop as fn(TestInput) -> TestResult);
}

/// Represents the input for each test run.
#[derive(Clone, Debug, Arbitrary)]
struct TestInput {
    messages: HashMap<u64, BTreeSet<u64>>,
    partition_count: SmallCount,
    producer_count: SmallCount,
    consumer_count: SmallCount,
    max_enqueued_per_key: SmallCount,
}

/// Defines the property to be tested.
fn prop(input: TestInput) -> TestResult {
    // Validate input data
    if input.messages.is_empty() || input.messages.values().any(BTreeSet::is_empty) {
        return TestResult::discard();
    }

    // Initialize the runtime for async operations
    let Ok(runtime) = Builder::new_multi_thread().enable_time().build() else {
        return TestResult::error("failed to initialize runtime");
    };

    // Run the test implementation
    match runtime.block_on(run_test(input)) {
        Ok(()) => TestResult::passed(),
        Err(e) => {
            error!("test failed with error: {e:#}");
            TestResult::error(e.to_string())
        }
    }
}

/// Runs the core logic of the integration test.
///
/// # Arguments
/// * `input` - The test input parameters.
///
/// # Errors
/// Returns an error if any part of the test setup or execution fails.
async fn run_test(input: TestInput) -> Result<()> {
    let (topic, admin_client) = create_test_topic(input.partition_count).await?;
    let (producer_config, consumer_config) = create_configs(&topic, input.max_enqueued_per_key)?;

    let (messages_tx, messages_rx) = channel(input.partition_count.value());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handler = TestHandler::new(messages_tx);

    let mut tasks = JoinSet::new();

    spawn_producers(&mut tasks, &input, &producer_config, &topic);
    spawn_consumers(
        &mut tasks,
        input.consumer_count,
        &consumer_config,
        &handler,
        &shutdown_rx,
    );
    spawn_message_verifier(&mut tasks, messages_rx, shutdown_tx, input.messages);

    // Wait for all tasks to complete
    while let Some(result) = tasks.join_next().await {
        result??;
    }

    info!("test passed");

    // Delete the test topic
    let admin_options = AdminOptions::default().operation_timeout(Some(Duration::from_secs(5)));
    admin_client
        .delete_topics(&[topic.as_ref()], &admin_options)
        .await?;

    info!("deleted test topic: {topic}");

    Ok(())
}

/// Creates a test topic and returns its name along with the admin client.
///
/// # Arguments
/// * `partition_count` - The number of partitions for the test topic.
///
/// # Errors
/// Returns an error if topic creation fails.
async fn create_test_topic(
    partition_count: SmallCount,
) -> Result<(Topic, AdminClient<DefaultClientContext>)> {
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", "localhost:9094");

    let admin_options = AdminOptions::default().operation_timeout(Some(Duration::from_secs(5)));
    let admin_client = AdminClient::from_config(&config)?;

    admin_client
        .create_topics(
            &[NewTopic::new(
                &topic,
                partition_count.value() as i32,
                TopicReplication::Fixed(1),
            )],
            &admin_options,
        )
        .await?;

    info!("created topic: {topic}");
    Ok((topic, admin_client))
}

/// Creates producer and consumer configurations.
///
/// # Arguments
/// * `topic` - The name of the test topic.
/// * `max_enqueued_per_key` - Maximum number of messages to enqueue per key.
///
/// # Errors
/// Returns an error if configuration creation fails.
fn create_configs(
    topic: &Topic,
    max_enqueued_per_key: SmallCount,
) -> Result<(ProducerConfiguration, ConsumerConfiguration)> {
    let bootstrap: Vec<String> = vec!["localhost:9094".to_owned()];
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .build()?;

    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap)
        .group_id("test-consumer")
        .subscribed_topics(&[topic.to_string()])
        .max_enqueued_per_key(max_enqueued_per_key.value())
        .commit_interval(Duration::from_secs(1))
        .partition_shutdown_timeout(Some(Duration::from_secs(60)))
        .build()?;

    Ok((producer_config, consumer_config))
}

/// Spawns producer tasks.
///
/// # Arguments
/// * `tasks` - The `JoinSet` to add tasks to.
/// * `input` - The test input parameters.
/// * `producer_config` - The producer configuration.
/// * `topic` - The name of the test topic.
fn spawn_producers(
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
                    producer.send(topic, &key, json!(message)).await?;
                }
                producer.send(topic, &key, Value::Null).await?;
            }
            Ok(())
        });
    }
}

/// Spawns consumer tasks.
///
/// # Arguments
/// * `tasks` - The `JoinSet` to add tasks to.
/// * `consumer_count` - The number of consumers to spawn.
/// * `consumer_config` - The consumer configuration.
/// * `handler` - The message handler.
/// * `shutdown_rx` - The shutdown signal receiver.
fn spawn_consumers(
    tasks: &mut JoinSet<Result<()>>,
    consumer_count: SmallCount,
    consumer_config: &ConsumerConfiguration,
    handler: &TestHandler,
    shutdown_rx: &watch::Receiver<bool>,
) {
    for _ in 0..consumer_count.value() {
        let consumer_config = consumer_config.clone();
        let handler = handler.clone();
        let mut shutdown_rx = shutdown_rx.clone();

        tasks.spawn(async move {
            let consumer = ProsodyConsumer::new(consumer_config, handler)?;
            shutdown_rx.wait_for(|is_shutdown| *is_shutdown).await?;
            consumer.shutdown().await;
            Ok(())
        });
    }
}

/// Spawns a task to receive and verify messages.
///
/// # Arguments
/// * `tasks` - The `JoinSet` to add the task to.
/// * `messages_rx` - The channel receiver for incoming messages.
/// * `shutdown_tx` - The shutdown signal sender.
/// * `expected_messages` - The expected messages to be received.
fn spawn_message_verifier(
    tasks: &mut JoinSet<Result<()>>,
    mut messages_rx: tokio::sync::mpsc::Receiver<(String, Value)>,
    shutdown_tx: watch::Sender<bool>,
    expected_messages: HashMap<u64, BTreeSet<u64>>,
) {
    tasks.spawn(async move {
        let mut keys: HashSet<String> = expected_messages.keys().map(ToString::to_string).collect();
        let mut received: HashMap<String, Vec<u64>> =
            HashMap::with_capacity(expected_messages.len());

        info!("receiving messages");
        while let Some((key, payload)) = messages_rx.recv().await {
            match payload {
                Value::Number(number) => {
                    let number = number.as_u64().ok_or_eyre("invalid number")?;
                    received.entry(key).or_default().push(number);
                }
                Value::Null => {
                    keys.remove(&key);
                    if keys.is_empty() {
                        break;
                    }
                }
                _ => return Err(eyre!("unexpected payload type")),
            }
        }

        verify_results(&expected_messages, received)?;

        info!("sending shutdown signal");
        shutdown_tx.send(true)?;

        Ok(())
    });
}

/// Verifies that received messages match expected messages.
///
/// # Arguments
/// * `expected` - The expected messages.
/// * `received` - The received messages.
///
/// # Errors
/// Returns an error if received messages don't match expected messages.
fn verify_results(
    expected: &HashMap<u64, BTreeSet<u64>>,
    received: HashMap<String, Vec<u64>>,
) -> Result<()> {
    let mut actual: HashMap<u64, BTreeSet<u64>> = HashMap::with_capacity(expected.len());

    for (key, received_messages) in received {
        let key: u64 = key.parse()?;
        let received_set: BTreeSet<u64> = received_messages.iter().copied().collect();
        actual.insert(key, received_set);

        let mut sorted = received_messages.clone();
        sorted.sort_unstable();

        if received_messages != sorted {
            return Err(eyre!(
                "invalid order for key {key}; expected: {sorted:?}, actual: {received_messages:?}"
            ));
        }
    }

    if *expected != actual {
        return Err(eyre!(
            "all messages were not received; expected: {expected:?}, actual: {actual:?}"
        ));
    }

    Ok(())
}

/// A test implementation of the `MessageHandler` trait.
#[derive(Clone, Debug)]
struct TestHandler {
    messages_tx: Sender<(String, Value)>,
}

impl TestHandler {
    /// Creates a new `TestHandler` instance.
    ///
    /// # Arguments
    /// * `messages_tx` - A channel sender for passing received messages.
    fn new(messages_tx: Sender<(String, Value)>) -> Self {
        Self { messages_tx }
    }
}

impl MessageHandler for TestHandler {
    type Error = Report;

    /// Handles a received message by sending it through the channel.
    ///
    /// # Arguments
    /// * `_context` - The message context (unused in this implementation).
    /// * `message` - The received consumer message.
    ///
    /// # Returns
    /// A Result indicating success or failure of the message handling.
    ///
    /// # Errors
    /// Returns an error if sending the message through the channel fails.
    async fn handle(
        &self,
        _context: &mut MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        let (key, payload, uncommitted) = message.into_inner();
        self.messages_tx.send((key.to_string(), payload)).await?;
        uncommitted.commit();
        Ok(())
    }
}

/// A wrapper for small, non-zero random numbers used in testing.
#[derive(Copy, Clone)]
struct SmallCount(u8);

impl SmallCount {
    /// Returns the wrapped value as a usize.
    fn value(self) -> usize {
        self.0 as usize
    }
}

impl Arbitrary for SmallCount {
    /// Generates a small, non-zero random number.
    fn arbitrary(g: &mut Gen) -> Self {
        const VALUES: [u8; 12] = const_array();
        Self(*max(&1, g.choose(&VALUES).unwrap_or(&1)))
    }
}

impl Debug for SmallCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Generates a constant array of sequential u8 values.
///
/// # Returns
/// An array of sequential u8 values from 1 to N.
const fn const_array<const N: usize>() -> [u8; N] {
    let mut arr = [0; N];
    let mut i = 0;
    while i < N {
        arr[i] = i as u8 + 1;
        i += 1;
    }
    arr
}
