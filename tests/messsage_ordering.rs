//! This module tests message ordering in the Prosody system using
//! property-based testing with `QuickCheck`. It verifies that messages are
//! received in the order they were produced per key, utilizing integration
//! tests with Kafka, via the Prosody library.

#![recursion_limit = "256"]

use std::cmp::max;
use std::collections::{BTreeSet, HashSet};
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::LazyLock;
use std::time::Duration as StdDuration;

use ahash::{HashMap, HashMapExt};
use color_eyre::eyre::{Result, eyre};
use derive_quickcheck_arbitrary::Arbitrary;
use itertools::Itertools;
use parking_lot::Mutex;
use prosody::admin::{AdminConfiguration, ProsodyAdminClient};
use prosody::consumer::middleware::CloneProvider;
use prosody::consumer::{ConsumerConfiguration, KeyedStateConfiguration, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::telemetry::Telemetry;
use prosody::tracing::init_test_logging;
use prosody::{JsonCodec, Topic};
use quickcheck::{Arbitrary as QCArbitrary, Gen, QuickCheck, TestResult};
use serde_json::{Value, json};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::info;

mod common;
use common::handler::ChannelHandler;
use common::{TEST_RUNTIME, create_cassandra_trigger_store_config};
use uuid::Uuid;

/// Reusable per-partition-count environments: one topic + consumer group per
/// distinct generated partition count, created on first use and shared by
/// every iteration that draws the same count (the generated producer/consumer
/// counts — the property's actual subject — still vary freely per iteration).
///
/// Iterations isolate through run-scoped key prefixes: the pooled group's
/// committed offsets and any redelivered tail from a prior iteration are
/// filtered out by the verifier, so no per-iteration topics or consumer
/// groups are needed.
static ENV_POOL: LazyLock<Mutex<HashMap<usize, (Topic, String)>>> = LazyLock::new(Mutex::default);

/// A small, non-zero count used in tests.
///
/// Provides a way to ensure small non-zero values are used within test cases,
/// particularly in property-based testing scenarios.
#[derive(Copy, Clone)]
struct SmallCount(u8);

impl SmallCount {
    /// Retrieves the underlying value of `SmallCount` as a `usize`.
    fn value(self) -> usize {
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
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
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
struct TestInput {
    /// A map from a key to a set of messages associated with the key.
    messages: HashMap<u64, BTreeSet<u64>>,

    /// The number of partitions each test topic should have.
    partition_count: SmallCount,

    /// The number of producers that should be spawned for the test.
    producer_count: SmallCount,

    /// The number of consumers that should be spawned for the test.
    consumer_count: SmallCount,
}

/// Tests that messages are received in the order they were produced for each
/// key. This function leverages property-based testing using `QuickCheck`,
/// which generates various input scenarios to ensure correct order. It supports
/// integration testing with Kafka through the Prosody library.
#[test]
fn receives_all_in_key_order() -> Result<()> {
    // Start tracing for logging and debugging.
    init_test_logging();

    // Use QuickCheck to run property-based tests that validate message ordering.
    // Local default 3 (owner ruling): each iteration costs ~14s of intrinsic
    // multi-consumer rebalance coverage, so the default trades iterations,
    // never coverage; CI overrides via INTEGRATION_TESTS.
    QuickCheck::new()
        .tests(common::integration_test_count_or(3))
        .quickcheck(prop as fn(TestInput) -> TestResult);

    // Delete the pooled topics minted during the run.
    let slots: Vec<(Topic, String)> = ENV_POOL.lock().drain().map(|(_, slot)| slot).collect();
    TEST_RUNTIME.block_on(async {
        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(vec![
            "localhost:9094".to_owned(),
        ])?)?;
        for (topic, _group) in slots {
            admin.delete_topic(&topic).await?;
            info!("deleted pooled test topic: {topic}");
        }
        Ok(())
    })
}

/// Property function for `QuickCheck` to verify message ordering.
///
/// Uses the shared Tokio runtime to asynchronously run the `run_test` function
/// with generated test input to ensure the correct ordering of messages.
///
/// # Arguments
///
/// * `input` - The `TestInput` containing test parameters with messages and
///   configuration values for the test.
///
/// # Returns
///
/// * `TestResult::passed()` if the test succeeds.
/// * `TestResult::error()` with an error message if the test fails.
/// * `TestResult::discard()` if the input is invalid.
fn prop(input: TestInput) -> TestResult {
    // Discard test cases that have invalid configurations, such as empty messages.
    if input.messages.is_empty() || input.messages.values().any(BTreeSet::is_empty) {
        return TestResult::discard();
    }

    // Run the test within the shared runtime and return the outcome.
    match TEST_RUNTIME.block_on(run_test(input)) {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(e.to_string()),
    }
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
async fn run_test(input: TestInput) -> Result<()> {
    // Reuse (or lazily create) the pooled topic + group for this partition
    // count; a fresh run id scopes this iteration's keys.
    let (topic, group_id) = pooled_env(input.partition_count).await?;
    let (producer_config, consumer_config) = create_configs(&topic, &group_id)?;
    let run_id = Uuid::new_v4().to_string();

    // Setup channels and task management
    let (messages_tx, messages_rx) = channel(input.partition_count.value());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut tasks = JoinSet::new();

    // Spawn producers and consumers, and start message verification
    spawn_producers(&mut tasks, &input, &producer_config, &topic, &run_id);
    spawn_consumers(
        &mut tasks,
        input.consumer_count,
        &consumer_config,
        &messages_tx,
        &shutdown_rx,
    );
    spawn_message_verifier(&mut tasks, messages_rx, shutdown_tx, input.messages, run_id);

    // Wait for all tasks to complete
    while let Some(result) = tasks.join_next().await {
        result??;
    }

    info!("test passed");

    Ok(())
}

/// Returns the pooled topic and consumer group for `partition_count`,
/// creating them on first use. See [`ENV_POOL`].
///
/// # Errors
///
/// Returns an error if the topic creation fails.
async fn pooled_env(partition_count: SmallCount) -> Result<(Topic, String)> {
    if let Some(slot) = ENV_POOL.lock().get(&partition_count.value()) {
        return Ok(slot.clone());
    }

    let (topic, _admin) =
        common::kafka::create_topic_with_partitions(partition_count.value() as u16).await?;
    let group_id = format!("ordering-consumer-{}", Uuid::new_v4());
    ENV_POOL
        .lock()
        .insert(partition_count.value(), (topic, group_id.clone()));
    Ok((topic, group_id))
}

/// Creates producer and consumer configurations for a specified topic.
///
/// Returns producer and consumer configurations.
///
/// # Errors
///
/// Returns an error if configuration creation fails.
fn create_configs(
    topic: &Topic,
    group_id: &str,
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
        .group_id(group_id)
        .subscribed_topics(&[topic.to_string()])
        .commit_interval(StdDuration::from_secs(1))
        .stall_threshold(StdDuration::from_mins(1))
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
fn spawn_producers(
    tasks: &mut JoinSet<Result<()>>,
    input: &TestInput,
    producer_config: &ProducerConfiguration,
    topic: &Topic,
    run_id: &str,
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
        let run_id = run_id.to_owned();

        tasks.spawn(async move {
            let producer =
                ProsodyProducer::<JsonCodec>::new(&producer_config, Telemetry::new().sender())?;
            for (key, messages) in producer_messages {
                // Scope the key to this iteration's run so the shared
                // topic/group cannot leak traffic across iterations.
                let key = format!("{run_id}-{key}");
                for message in messages {
                    producer.send([], topic, &key, json!(message)).await?; // Send each message
                }
                producer.send([], topic, &key, Value::Null).await?; // Send the end-of-stream marker
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

        let handler = ChannelHandler::new(messages_tx);
        let handler_provider = CloneProvider::new(handler);

        tasks.spawn(async move {
            let consumer: ProsodyConsumer<JsonCodec> = ProsodyConsumer::new(
                &consumer_config,
                &create_cassandra_trigger_store_config(),
                KeyedStateConfiguration::builder().build()?,
                handler_provider,
                Telemetry::new(),
            )
            .await?;
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
/// * `run_id` - This iteration's key prefix; traffic without it is ignored.
fn spawn_message_verifier(
    tasks: &mut JoinSet<Result<()>>,
    mut messages_rx: Receiver<(String, Value)>,
    shutdown_tx: watch::Sender<bool>,
    expected_messages: HashMap<u64, BTreeSet<u64>>,
    run_id: String,
) {
    tasks.spawn(async move {
        // Track keys and received messages to verify against expected results
        let mut keys: HashSet<String> = expected_messages.keys().map(ToString::to_string).collect();
        let mut received: HashMap<String, Vec<u64>> =
            HashMap::with_capacity(expected_messages.len());
        let run_prefix = format!("{run_id}-");

        info!("receiving messages");

        // Receive messages and collect them
        while let Some((key, payload)) = messages_rx.recv().await {
            // Ignore traffic from other iterations sharing the pooled
            // topic/group (e.g. a redelivered tail from a previous run).
            let Some(key) = key.strip_prefix(&run_prefix).map(str::to_owned) else {
                continue;
            };
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
fn verify_results(
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
