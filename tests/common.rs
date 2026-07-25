//! Common utilities and types for Prosody tests.
//!
//! This module provides shared structures, functions, and test handlers used
//! across various test cases for the Prosody system. It includes utility
//! functions for creating topics and trigger-store configurations, test event
//! handlers, and helpers for collecting messages off a channel.

#![allow(
    dead_code,
    reason = "Shared test utilities: each tests/*.rs binary compiles this module separately, so a \
              helper used by only some binaries is dead in the rest"
)]

use std::env;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::LazyLock;
use std::time::Duration as StdDuration;

use std::fmt::Debug;

use color_eyre::eyre::{Result, bail, eyre};
use prosody::JsonCodec;
use prosody::Topic;
use prosody::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use prosody::cassandra::config::CassandraConfiguration;
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::{ConsumerMessage, UncommittedMessage};
use prosody::consumer::middleware::FallibleHandler;
use prosody::consumer::{
    ConsumerConfiguration, DemandType, EventHandler, HandlerProvider, Keyed,
    KeyedStateConfiguration, ProsodyConsumer, Uncommitted,
};
use prosody::error::{ClassifyError, ErrorCategory};
use prosody::high_level::config::TriggerStoreConfiguration;
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::telemetry::Telemetry;
use prosody::timers::{Trigger, UncommittedTimer};
use serde_json::Value;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, timeout};
use tracing::{error, info};
use uuid::Uuid;

/// The shared, pre-migrated keyspace every integration test runs against.
///
/// Tests never create per-test keyspaces — minting one per test leaks schema
/// (orphaned keyspaces bloat the cluster and eventually time out migration
/// tests). Isolation comes from per-test topics and consumer groups instead.
pub const TEST_KEYSPACE: &str = "prosody_test";

/// Shared multi-threaded runtime for all integration tests.
///
/// # Rationale for `expect`
///
/// `LazyLock` requires a non-fallible closure. Runtime creation failure is
/// unrecoverable in test infrastructure - tests cannot run without a runtime.
#[expect(
    clippy::expect_used,
    reason = "LazyLock requires non-fallible closure; test infra"
)]
pub static TEST_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .expect("Failed to create tokio runtime")
});

/// Number of times to repeat a property test against a live backend.
///
/// Read from `INTEGRATION_TESTS` (default 25, matching TESTING.md). CI cranks
/// this up; dev loops stay fast.
#[must_use]
pub fn integration_test_count() -> u64 {
    integration_test_count_or(25)
}

/// [`integration_test_count`] with a caller-chosen default for when
/// `INTEGRATION_TESTS` is unset.
///
/// For properties whose per-iteration cost is intrinsically heavy (multiple
/// seconds of live-broker protocol per iteration), a lower local default
/// keeps dev loops fast; the environment variable still overrides it.
#[must_use]
pub fn integration_test_count_or(default: u64) -> u64 {
    env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Creates a test topic with the given number of partitions.
///
/// Returns the created topic and an admin client for cleanup tasks.
///
/// # Errors
///
/// Returns an error if the topic creation fails.
pub async fn create_topic_with_partitions(
    partition_count: u16,
) -> Result<(Topic, &'static ProsodyAdminClient)> {
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap)?)?;

    admin_client
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(partition_count)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    info!("created topic: {topic}");
    Ok((topic, admin_client))
}

/// Creates a single-partition test topic.
///
/// Returns the created topic and an admin client for cleanup tasks.
///
/// # Errors
///
/// Returns an error if the topic creation fails.
pub async fn create_single_partition_topic() -> Result<(Topic, &'static ProsodyAdminClient)> {
    create_topic_with_partitions(1).await
}

/// Creates a Cassandra trigger store configuration for integration tests.
///
/// Uses the same configuration pattern as the Cassandra store unit tests,
/// connecting to localhost:9042 with a test keyspace.
///
/// # Returns
///
/// A `TriggerStoreConfiguration::Cassandra` configured for testing.
#[must_use]
pub fn create_cassandra_trigger_store_config() -> TriggerStoreConfiguration {
    TriggerStoreConfiguration::Cassandra(test_cassandra_config())
}

/// The Cassandra configuration shared by every integration test: the local
/// node and the pre-migrated [`TEST_KEYSPACE`]. Both
/// [`create_cassandra_trigger_store_config`] and the tests that open a
/// Cassandra store directly build from this one value.
#[must_use]
pub fn test_cassandra_config() -> CassandraConfiguration {
    CassandraConfiguration {
        datacenter: None,
        rack: None,
        nodes: vec!["localhost:9042".to_owned()],
        keyspace: TEST_KEYSPACE.to_owned(),
        user: None,
        password: None,
        retention: StdDuration::from_mins(10),
    }
}

/// Awaits the consumer's partition assignment under a generous hang-guard.
/// Fails with a clear error instead of hanging if the rebalance stalls or
/// only partially assigns.
///
/// # Errors
///
/// Returns an error if the consumer does not receive `count` partition
/// assignments within the hang-guard.
pub async fn wait_for_assignment(consumer: &ProsodyConsumer<JsonCodec>, count: u32) -> Result<()> {
    timeout(
        StdDuration::from_secs(30),
        consumer.wait_for_assigned_partitions(count),
    )
    .await
    .map_err(|_| eyre!("consumer did not receive a partition assignment in time"))?;
    Ok(())
}

/// The generic forward-to-channel [`EventHandler`]: sends every received
/// `(key, payload)` pair to a channel and commits.
///
/// This is the one plain-forwarder handler shared by the integration suite;
/// specialized handlers (error injection, timer scheduling, context capture)
/// stay file-local. An optional per-message `delay` simulates backpressure —
/// the only sanctioned use of `sleep` in tests.
#[derive(Clone, Debug)]
pub struct ChannelHandler {
    /// A channel for transmitting received messages.
    messages_tx: Sender<(String, Value)>,

    /// Per-message processing delay (backpressure simulation); zero for none.
    delay: StdDuration,
}

impl ChannelHandler {
    /// A handler that forwards immediately.
    #[must_use]
    pub fn new(messages_tx: Sender<(String, Value)>) -> Self {
        Self::with_delay(messages_tx, StdDuration::ZERO)
    }

    /// A handler that sleeps `delay` before forwarding, simulating a slow
    /// consumer for backpressure tests.
    #[must_use]
    pub fn with_delay(messages_tx: Sender<(String, Value)>, delay: StdDuration) -> Self {
        Self { messages_tx, delay }
    }
}

impl EventHandler for ChannelHandler {
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage<Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        let (msg, uncommitted) = message.into_inner();

        if !self.delay.is_zero() {
            // Simulate backpressure with a delay
            sleep(self.delay).await;
        }

        // Forward the message to the channel
        if let Err(error) = self
            .messages_tx
            .send((msg.key().to_string(), msg.payload().clone()))
            .await
        {
            error!("failed to send message: {error:#}");
        }

        uncommitted.commit().await; // Commit message to mark as processed
    }

    async fn on_timer<C, U>(&self, _context: C, _timer: U, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        U: UncommittedTimer,
    {
    }

    async fn shutdown(self) {
        info!("ChannelHandler shutdown");
    }
}

/// A test error type for `FallibleHandler` implementations that classifies as
/// permanent.
#[derive(Debug, Clone)]
pub struct TestError;

impl Display for TestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "test error")
    }
}

impl Error for TestError {}

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

/// A test error type for `FallibleHandler` implementations that classifies as
/// transient.
#[derive(Debug, Clone)]
pub struct TransientError;

impl Display for TransientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "transient test error")
    }
}

impl Error for TransientError {}

impl ClassifyError for TransientError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Transient
    }
}

/// A test handler that implements `FallibleHandler` for high-level client
/// testing.
///
/// This is a leaf handler (no inner): it relies on the default no-op
/// `after_commit` / `after_abort` apply hooks. The framework still guarantees
/// that exactly one of those hooks fires per `on_message` / `on_timer` call
/// (the rule-3 invariant), but with `type Output = ()` and no staged state to
/// finalise or unwind, this handler has nothing to do in either hook.
#[derive(Clone, Debug)]
pub struct FallibleTestHandler {
    /// Channel for transmitting received messages.
    pub messages_tx: Sender<(String, Value)>,
}

impl FallibleHandler for FallibleTestHandler {
    type Error = TestError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: ConsumerMessage<Value>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        // Forward the message to the channel, ignoring send errors for testing
        let _ = self
            .messages_tx
            .send((message.key().to_string(), message.payload().clone()))
            .await;
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        _timer: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(())
    }

    async fn shutdown(self) {
        // No cleanup needed for test handler
    }
}

/// A live single-partition consumer + producer pair with a dedicated topic.
///
/// Owns the shared per-test infrastructure every direct-`ProsodyConsumer`
/// integration harness used to duplicate: topic lifecycle (created in
/// [`ConsumerEnv::new`], deleted in [`ConsumerEnv::shutdown`]), a uniquely
/// named consumer group, the producer, and the wait for the consumer's
/// partition assignment. Per-file harnesses wrap this and add only their
/// handler, event channels, and assertions.
pub struct ConsumerEnv {
    topic: Topic,
    admin: &'static ProsodyAdminClient,
    consumer: ProsodyConsumer<JsonCodec>,
    producer: ProsodyProducer<JsonCodec>,
}

impl ConsumerEnv {
    /// Creates a fresh topic and consumer group named after `test_name`,
    /// builds the handler provider from the consumer configuration via
    /// `build_provider`, starts the consumer and producer, and waits for the
    /// partition assignment before returning — so a message sent immediately
    /// after `new` can never race the subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if any component fails to start or the consumer does
    /// not receive its partition assignment within the hang-guard.
    pub async fn new<P, F>(test_name: &str, build_provider: F) -> Result<Self>
    where
        P: HandlerProvider,
        P::Handler: EventHandler<Payload = Value>,
        F: AsyncFnOnce(&ConsumerConfiguration) -> Result<P>,
    {
        let (topic, admin) = create_single_partition_topic().await?;

        let consumer_config = ConsumerConfiguration::builder()
            .bootstrap_servers(vec!["localhost:9094".to_owned()])
            .group_id(format!("{test_name}-consumer-{}", Uuid::new_v4()))
            .subscribed_topics(&[topic.to_string()])
            .probe_port(None) // Disable probe server to allow parallel test execution
            .build()?;

        let handler_provider = build_provider(&consumer_config).await?;

        let consumer: ProsodyConsumer<JsonCodec> = ProsodyConsumer::new(
            &consumer_config,
            &create_cassandra_trigger_store_config(),
            KeyedStateConfiguration::builder().build()?,
            handler_provider,
            Telemetry::new(),
        )
        .await?;

        let producer = ProsodyProducer::<JsonCodec>::new(
            &ProducerConfiguration::builder()
                .bootstrap_servers(vec!["localhost:9094".to_owned()])
                .source_system(test_name.to_owned())
                .build()?,
            Telemetry::new().sender(),
        )?;

        // Wait until Kafka has assigned the consumer its partition before
        // producing — records sent before the subscription is live can be
        // missed. Awaits the assignment signal the rebalance callback
        // publishes (not a poll) under a generous hang-guard.
        wait_for_assignment(&consumer, 1).await?;

        Ok(Self {
            topic,
            admin,
            consumer,
            producer,
        })
    }

    /// The topic this environment produces to and consumes from.
    #[must_use]
    pub fn topic(&self) -> Topic {
        self.topic
    }

    /// Sends a message with the given key and payload to the test topic.
    ///
    /// # Errors
    ///
    /// Returns an error if the send fails.
    pub async fn send_message(&self, key: &str, payload: Value) -> Result<()> {
        self.producer.send([], self.topic, key, payload).await?;
        Ok(())
    }

    /// Shuts down the consumer, then deletes the test topic.
    ///
    /// Callers must invoke this before propagating a test failure — dropping
    /// a live consumer leaves rdkafka threads hanging.
    pub async fn shutdown(self) {
        self.consumer.shutdown().await;
        if let Err(e) = self.admin.delete_topic(&self.topic).await {
            error!("Failed to clean up topic {}: {e}", self.topic);
        }
    }
}

/// Receives the next event from `rx`, failing if none arrives within
/// `hang_guard`. The deadline is a hang-guard, never the assertion.
///
/// # Errors
///
/// Returns an error on timeout or if the channel closed.
pub async fn expect_event<T>(rx: &mut Receiver<T>, hang_guard: StdDuration) -> Result<T> {
    timeout(hang_guard, rx.recv())
        .await
        .map_err(|_| eyre!("timed out waiting for event after {hang_guard:?}"))?
        .ok_or_else(|| eyre!("event channel closed unexpectedly"))
}

/// Verifies that no event arrives on `rx` within `window`.
///
/// # Errors
///
/// Returns an error if an event arrives or the channel closed.
pub async fn expect_no_event<T: Debug>(rx: &mut Receiver<T>, window: StdDuration) -> Result<()> {
    match timeout(window, rx.recv()).await {
        Err(_) => Ok(()),
        Ok(Some(event)) => bail!("expected no event within {window:?} but received: {event:?}"),
        Ok(None) => bail!("event channel closed unexpectedly"),
    }
}

/// Collects exactly the expected number of messages within a timeout period.
///
/// # Arguments
///
/// * `receiver` - The channel receiver to collect messages from.
/// * `expected_count` - Number of messages expected.
/// * `timeout_secs` - Timeout in seconds for each message.
///
/// # Returns
///
/// A vector containing the collected messages.
///
/// # Errors
///
/// Returns an error if timeout occurs or unexpected messages are received.
pub async fn collect_messages_with_timeout(
    receiver: &mut Receiver<(String, Value)>,
    expected_count: usize,
    timeout_secs: u64,
) -> Result<Vec<(String, Value)>> {
    use tokio::time::{Duration, timeout};

    let mut messages = Vec::with_capacity(expected_count);
    let timeout_duration = Duration::from_secs(timeout_secs);

    // Collect expected messages
    for i in 0..expected_count {
        let message = timeout(timeout_duration, receiver.recv())
            .await
            .map_err(|_| eyre!("Timeout waiting for message {}", i + 1))?
            .ok_or_else(|| eyre!("Channel closed while waiting for message {}", i + 1))?;
        messages.push(message);
    }

    // Verify no additional messages arrive
    let no_extra_msg = timeout(Duration::from_secs(2), receiver.recv()).await;
    if let Ok(Some(_)) = no_extra_msg {
        return Err(eyre!("Unexpected extra message received"));
    }

    Ok(messages)
}
