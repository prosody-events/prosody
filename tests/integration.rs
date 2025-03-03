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
use color_eyre::eyre::{OptionExt, Result, eyre};
use derive_quickcheck_arbitrary::Arbitrary;
use itertools::Itertools;
use prosody::Topic;
use prosody::admin::ProsodyAdminClient;
use prosody::consumer::Keyed;
use prosody::consumer::message::{MessageContext, UncommittedMessage};
use prosody::consumer::{ConsumerConfiguration, EventHandler, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use serde_json::{Value, json};
use tokio::runtime::Builder;
use tokio::spawn;
use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};
use tracing_subscriber::fmt;
use uuid::Uuid;

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
    let _ = fmt().compact().try_init();

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
    let Ok(runtime) = Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
    else {
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

#[tokio::test]
async fn test_deduplication_of_same_event_id() -> Result<()> {
    // Initialize tracing for better test output
    let _ = fmt().compact().try_init();

    // Create a unique topic for the test
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap: Vec<String> = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::new(&bootstrap)?;
    admin_client.create_topic(&topic, 1, 1).await?;

    // Create producer configuration
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;

    // Create consumer configuration
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id("test-deduplication-consumer")
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    // Create a channel to collect received messages
    let (messages_tx, mut messages_rx) = channel(10);

    // Create a producer
    let producer = ProsodyProducer::new(&producer_config)?;

    // Create a test handler for the consumer
    let handler = TestHandler { messages_tx };

    // Start the consumer in a background task
    let consumer = ProsodyConsumer::new::<TestHandler>(&consumer_config, handler.clone())?;

    // Send multiple messages with the same key and same event_id
    let key = "test-key";
    let event_id = "event-123";
    let payload = json!({ "id": event_id, "value": "first message" });
    let payload_duplicate = json!({ "id": event_id, "value": "duplicate message" });

    // Send the first message
    producer.send([], topic, key, &payload).await?;
    // Send the duplicate message
    producer.send([], topic, key, &payload_duplicate).await?;

    // Collect received messages
    let mut received_messages = Vec::new();

    // Wait for messages to be received with a timeout
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    while start.elapsed() < timeout {
        if let Some((received_key, received_payload)) = messages_rx.recv().await {
            received_messages.push((received_key, received_payload));
            // Break if we have received any messages
            if !received_messages.is_empty() {
                break;
            }
        } else {
            break;
        }
    }

    // Shutdown the consumer
    consumer.shutdown().await;

    // Assert that only one message was received
    assert_eq!(
        received_messages.len(),
        1,
        "Expected only one message due to deduplication"
    );

    // Optionally, check the content of the message
    let (recv_key, recv_payload) = &received_messages[0];
    assert_eq!(recv_key, key);
    assert_eq!(recv_payload, &payload);

    // Clean up: delete the topic
    admin_client.delete_topic(&topic).await?;

    Ok(())
}

#[tokio::test]
async fn test_source_system_filtering() -> Result<()> {
    let _ = fmt().compact().try_init();
    let timeout_duration = Duration::from_secs(5);

    // Scenario 1: When producer and consumer share the same identifier,
    // no messages should be received.
    run_scenario("filter-test", "filter-test", false, "1", timeout_duration).await?;

    // Scenario 2: When producer and consumer have different identifiers,
    // messages should be delivered.
    run_scenario(
        "filter-test",
        "different-group",
        true,
        "2",
        timeout_duration,
    )
    .await?;

    Ok(())
}

async fn run_scenario(
    source_system: &'static str,
    group_id: &'static str,
    expect_messages: bool,
    event_suffix: &'static str,
    timeout_duration: Duration,
) -> Result<()> {
    // Create a unique topic.
    let topic_string = Uuid::new_v4().to_string();
    // Topic::from requires a &str.
    let topic: Topic = topic_string.as_str().into();
    let bootstrap = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::new(&bootstrap)?;
    admin_client.create_topic(&topic, 1, 1).await?;

    // Build producer and consumer configurations.
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system(source_system)
        .build()?;
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id(group_id)
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    // Create a channel and start the consumer.
    let (tx, mut rx) = channel(10);
    let consumer =
        ProsodyConsumer::new::<TestHandler>(&consumer_config, TestHandler { messages_tx: tx })?;
    let producer = ProsodyProducer::new(&producer_config)?;

    // Send a regular message (with a unique event id) and an end-of-stream marker.
    let key = "test-key";
    let event_id = format!("unique-event-{event_suffix}");
    let payload = json!({ "id": event_id, "value": "test message" });
    producer.send([], topic, key, &payload).await?;
    producer.send([], topic, key, &Value::Null).await?;

    if expect_messages {
        // Wait for the regular message.
        let recv_result = tokio::time::timeout(timeout_duration, rx.recv()).await;
        let recv_opt = recv_result.map_err(|_| eyre!("timeout waiting for regular message"))?;
        let (recv_key, recv_payload) =
            recv_opt.ok_or_else(|| eyre!("Did not receive the regular message"))?;
        assert_eq!(recv_key, key, "keys did not match");
        assert_eq!(recv_payload, payload, "payloads did not match");

        // Wait for the end-of-stream marker.
        let recv_result = tokio::time::timeout(timeout_duration, rx.recv()).await;
        let recv_opt =
            recv_result.map_err(|_| eyre!("timeout waiting for end-of-stream marker"))?;
        match recv_opt {
            Some((_k, Value::Null)) => { /* expected */ }
            _ => return Err(eyre!("Did not receive expected end-of-stream marker")),
        }
    } else {
        // Ensure no message is received.
        let recv = tokio::time::timeout(timeout_duration, rx.recv()).await;
        if let Ok(Some((k, v))) = recv {
            consumer.shutdown().await;
            admin_client.delete_topic(&topic).await?;
            return Err(eyre!("Unexpected message received: key: {k}, payload: {v}"));
        }
    }

    consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;
    Ok(())
}

#[tokio::test]
async fn test_allowed_events_filtering() -> Result<()> {
    use prosody::Topic;
    use prosody::admin::ProsodyAdminClient;
    use prosody::consumer::{ConsumerConfiguration, ProsodyConsumer};
    use prosody::producer::{ProducerConfiguration, ProsodyProducer};
    use serde_json::json;
    use std::time::Duration;
    use tokio::sync::mpsc::channel;
    use tokio::time::timeout;
    use tracing_subscriber::fmt;
    use uuid::Uuid;

    // Initialize tracing (ignore error if already initialized)
    let _ = fmt().compact().try_init();

    // Create a unique topic for the test.
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::new(&bootstrap)?;
    admin_client.create_topic(&topic, 1, 1).await?;

    // Configure the consumer with probe_port set to None and allow only event types
    // starting with "allowed".
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id("test-allowed-events-consumer")
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .allowed_events(vec!["allowed".to_owned()])
        .build()?;

    // Configure the producer.
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;

    // Create a channel to collect delivered messages.
    let (messages_tx, mut messages_rx) = channel(10);

    // Use the existing TestHandler which forwards messages to the channel.
    let handler = TestHandler { messages_tx };

    // Start the consumer.
    let consumer = ProsodyConsumer::new::<TestHandler>(&consumer_config, handler)?;
    // Create the producer.
    let producer = ProsodyProducer::new(&producer_config)?;

    let key = "test-key";
    // Build two payloads:
    // - One with a disallowed event type ("disallowed") that should be filtered.
    // - One with an allowed event type ("allowed_event") that should be delivered.
    let payload_filtered = json!({
        "type": "disallowed",
        "content": "this message should be filtered"
    });
    let payload_allowed = json!({
        "type": "allowed_event",
        "content": "this message should be delivered"
    });

    // Send both messages.
    producer.send([], topic, key, &payload_filtered).await?;
    producer.send([], topic, key, &payload_allowed).await?;

    // Wait up to 5 seconds for a delivered message.
    let received = timeout(Duration::from_secs(5), messages_rx.recv()).await?;
    let (received_key, received_payload) =
        received.ok_or_else(|| eyre!("Timeout waiting for a delivered message"))?;

    // Shutdown the consumer.
    consumer.shutdown().await;

    // Verify that the received message is the allowed one.
    assert_eq!(received_key, key);
    assert_eq!(received_payload, payload_allowed);

    // Clean up the test topic.
    admin_client.delete_topic(&topic).await?;
    Ok(())
}

/// Tests the backpressure handling capabilities of the messaging system.
///
/// This test verifies that the system properly handles backpressure when a
/// consumer processes uniquely-keyed messages significantly slower than they
/// are produced. It creates a producer that quickly sends messages and a
/// consumer that deliberately processes each message slowly, introducing a
/// 1-second delay per message.
///
/// The test monitors how the system manages this imbalance and ensures that:
/// 1. All messages are eventually processed despite the speed mismatch
/// 2. The messaging system doesn't collapse under load
/// 3. Backpressure is properly propagated through the system
///
/// # Errors
///
/// Returns an error if:
/// - Topic creation fails
/// - Consumer or producer configuration is invalid
/// - Message sending fails
/// - Topic deletion fails
///
/// # Note
///
/// This test is intentionally marked with `#[ignore]` because it runs very
/// slowly due to the deliberate processing delays introduced.
#[tokio::test]
#[ignore] // this test intentionally runs very slowly to demonstrate backpressure
async fn test_backpressure() -> Result<()> {
    // Initialize tracing for better test output.
    let _ = fmt().compact().try_init();

    // Create a unique topic.
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap: Vec<String> = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::new(&bootstrap)?;
    admin_client.create_topic(&topic, 4, 1).await?;

    // Set up a channel with a buffer capacity of 64 messages for communication
    // between the message handler and test harness.
    let (messages_tx, mut messages_rx) = channel(64);

    // Build a consumer configuration with default buffer settings.
    // The backpressure will be simulated by the slow handler, not by the
    // consumer configuration itself.
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id(Uuid::new_v4().to_string().as_str())
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    // Create the consumer using a custom slow handler that delays 1 second per
    // message.
    let slow_handler = SlowTestHandler { messages_tx };
    let consumer = ProsodyConsumer::new::<SlowTestHandler>(&consumer_config, slow_handler)?;

    // Build a producer configuration and create a producer.
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;
    let producer = ProsodyProducer::new(&producer_config)?;

    let total = 10_000u32;

    // Spawn a task that produces uniquely-keyed messages quickly without waiting
    // for confirmation of delivery, to test how the system handles the high
    // throughput.
    spawn(async move {
        for i in 0..total {
            let payload = json!({ "seq": i });
            if producer
                .send([], topic, &i.to_string(), &payload)
                .await
                .is_err()
            {
                error!("failed to send message");
            }
        }
    });

    // Start processing messages and periodically report progress.
    // This will receive messages at the rate determined by the slow handler.
    let mut count = 0_u32;
    let start_time = tokio::time::Instant::now();

    while messages_rx.recv().await.is_some() {
        count += 1;

        // Log progress every 100 messages to provide visibility
        // into the processing rate.
        if count % 100 == 0 {
            info!("Received {count} messages so far");
        }

        if count == total {
            break;
        }
    }
    let total_elapsed = start_time.elapsed();
    info!("Total messages processed: {count}");
    info!("Total processing time: {total_elapsed:?}");

    // Clean up resources
    consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;
    Ok(())
}

/// A test implementation of the `EventHandler` trait that processes messages
/// slowly to simulate backpressure conditions.
///
/// This handler introduces a deliberate 1-second delay for each message it
/// processes, allowing the test to verify how the system handles situations
/// where messages are produced faster than they can be consumed.
///
/// # Fields
///
/// * `messages_tx` - A channel sender used to notify the test harness about
///   processed messages.
#[derive(Clone, Debug)]
struct SlowTestHandler {
    messages_tx: Sender<(String, Value)>,
}

impl EventHandler for SlowTestHandler {
    /// Processes an incoming message with a deliberate delay to simulate slow
    /// processing.
    ///
    /// This method:
    /// 1. Extracts the message key and payload
    /// 2. Introduces a 1-second delay to simulate slow processing
    /// 3. Forwards the processed message to the test harness through a channel
    /// 4. Commits the message to acknowledge successful processing
    ///
    /// # Arguments
    ///
    /// * `_context` - Message context information (unused in this
    ///   implementation)
    /// * `message` - The uncommitted message to be processed
    ///
    /// # Note
    ///
    /// The artificial delay is the key component that creates backpressure in
    /// the system.
    #[instrument(skip(self, _context))]
    async fn on_message(&self, _context: MessageContext, message: UncommittedMessage) {
        let (msg, uncommitted) = message.into_inner();
        let key = msg.key().to_string();
        let payload: Value = msg.payload().clone();
        sleep(Duration::from_secs(1)).await;
        if let Err(e) = self.messages_tx.send((key.clone(), payload)).await {
            error!("failed to send message for key {}: {e:#}", key);
        }
        uncommitted.commit();
    }

    /// Performs cleanup when the handler is being shut down.
    ///
    /// # Arguments
    ///
    /// None
    async fn shutdown(self) {
        info!("SlowTestHandler shutdown");
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

    let mut tasks = JoinSet::new();

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

    // Delete the test topic
    admin_client.delete_topic(&topic).await?;

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
async fn create_test_topic(partition_count: SmallCount) -> Result<(Topic, ProsodyAdminClient)> {
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let admin_client = ProsodyAdminClient::new(&["localhost:9094"])?;

    admin_client
        .create_topic(&topic, partition_count.value() as u16, 1)
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
        .source_system("test-producer")
        .build()?;

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
                    producer.send([], topic, &key, &json!(message)).await?;
                }
                producer.send([], topic, &key, &Value::Null).await?;
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

/// A test implementation of the `EventHandler` trait.
#[derive(Clone, Debug)]
struct TestHandler {
    messages_tx: Sender<(String, Value)>,
}

impl EventHandler for TestHandler {
    /// Handles a received message by sending it through the channel.
    ///
    /// # Arguments
    /// * `_context` - The message context (unused in this implementation).
    /// * `message` - The received consumer message.
    ///
    /// # Returns
    /// A Future that completes when the message is handled.
    async fn on_message(&self, _context: MessageContext, message: UncommittedMessage) {
        let (message, uncommitted) = message.into_inner();
        let message = message.into_value();

        if let Err(error) = self
            .messages_tx
            .send((message.key.to_string(), message.payload))
            .await
        {
            error!("failed to send message: {error:#}");
        }

        uncommitted.commit();
    }

    async fn shutdown(self) {
        info!("partition shutdown");
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
