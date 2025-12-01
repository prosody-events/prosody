//! Integration tests for defer middleware functionality.
//!
//! Tests the complete defer middleware flow including:
//! - Message deferral on transient failures
//! - Timer scheduling and retry logic
//! - Multi-message queuing per key
//! - Successful retry and completion
//! - Cache and Cassandra store integration
//!
//! # Running Tests
//!
//! These tests use real Kafka and Cassandra instances. While each test uses
//! unique resources (topics, keyspaces), Kafka consumer group coordination can
//! cause race conditions when tests run in parallel. For reliable results, run
//! sequentially:
//!
//! ```bash
//! cargo test --test defer_middleware -- --test-threads=1
//! ```

use color_eyre::eyre::{Result, ensure, eyre};
use prosody::cassandra::{CassandraConfiguration, CassandraStore};
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::ConsumerMessage;
use prosody::consumer::middleware::defer::store::cassandra::CassandraDeferStoreProvider;
use prosody::consumer::middleware::defer::{DeferConfiguration, DeferMiddleware};
use prosody::consumer::middleware::log::LogMiddleware;
use prosody::consumer::middleware::scheduler::SchedulerConfiguration;
use prosody::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, HandlerMiddleware,
};
use prosody::consumer::{ConsumerConfiguration, DemandType, Keyed, ProsodyConsumer};
use prosody::heartbeat::HeartbeatRegistry;
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::telemetry::Telemetry;
use prosody::timers::Trigger;
use prosody::tracing::init_test_logging;
use prosody::{
    Topic,
    admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration},
};
use quickcheck::{QuickCheck, TestResult};
use serde_json::{Value, json};
use std::env;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use thiserror::Error;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::timeout;
use tracing::{error, info};
use uuid::Uuid;

/// Shared runtime for integration tests.
#[allow(clippy::expect_used)]
static TEST_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .expect("Failed to create tokio runtime")
});

mod common;

/// Get the number of times to run each integration test from environment
/// variable.
///
/// Defaults to 1 if `INTEGRATION_TESTS` is not set or invalid.
fn integration_test_count() -> u64 {
    env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1)
}

/// Test error that can be classified.
#[derive(Debug, Error, Clone)]
enum TestError {
    #[error("transient failure")]
    Transient,

    #[error("permanent failure")]
    Permanent,
}

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            TestError::Transient => ErrorCategory::Transient,
            TestError::Permanent => ErrorCategory::Permanent,
        }
    }
}

/// Events reported by test handler.
#[derive(Debug, Clone, PartialEq)]
enum HandlerEvent {
    /// Message received and processed successfully.
    MessageSuccess { key: String, value: i64 },

    /// Message failed with transient error.
    MessageFailedTransient { key: String, value: i64 },
}

/// Test handler that can be configured to fail specific messages.
#[derive(Clone)]
struct DeferTestHandler {
    /// Messages that should fail with transient errors (will be deferred).
    fail_values: Arc<parking_lot::Mutex<Vec<i64>>>,

    /// Channel to report events.
    event_tx: Sender<HandlerEvent>,
}

impl FallibleHandler for DeferTestHandler {
    type Error = TestError;

    async fn on_message<C>(
        &self,
        _context: C,
        message: ConsumerMessage,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let key = message.key().to_string();
        let payload = message.payload();

        if let Some(value) = payload.get("value").and_then(Value::as_i64) {
            let should_fail = self.fail_values.lock().contains(&value);

            if should_fail {
                let _ = self
                    .event_tx
                    .send(HandlerEvent::MessageFailedTransient {
                        key: key.clone(),
                        value,
                    })
                    .await;
                info!("Handler failing message key={key} value={value}");
                return Err(TestError::Transient);
            }

            let _ = self
                .event_tx
                .send(HandlerEvent::MessageSuccess {
                    key: key.clone(),
                    value,
                })
                .await;
            info!("Handler succeeded for message key={key} value={value}");
            Ok(())
        } else {
            Ok(())
        }
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        _timer: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // DeferRetry timers are handled by defer middleware internally
        // and do NOT call this method. This would only be called for
        // non-defer timers, which we don't use in these tests.
        Ok(())
    }

    async fn shutdown(self) {}
}

/// Handler that wraps [`DeferTestHandler`] to return permanent errors for
/// specific values.
#[derive(Clone)]
struct PermanentErrorHandler {
    inner: DeferTestHandler,
    permanent_value: i64,
}

impl FallibleHandler for PermanentErrorHandler {
    type Error = TestError;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let payload = message.payload();
        if payload.get("value").and_then(Value::as_i64) == Some(self.permanent_value) {
            return Err(TestError::Permanent);
        }
        self.inner.on_message(context, message, demand_type).await
    }

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        self.inner.on_timer(context, timer, demand_type).await
    }

    async fn shutdown(self) {
        self.inner.shutdown().await;
    }
}

/// Test environment that encapsulates setup and provides helper methods.
struct DeferTestEnvironment {
    consumer: ProsodyConsumer,
    producer: ProsodyProducer,
    topic: Topic,
    event_rx: Receiver<HandlerEvent>,
    handler: DeferTestHandler,
}

impl DeferTestEnvironment {
    /// Create a new test environment with defer middleware.
    async fn new() -> Result<Self> {
        let topic = Self::setup_test_topic(1).await?;

        let (event_tx, event_rx) = channel(100);
        let handler = DeferTestHandler {
            fail_values: Arc::new(parking_lot::Mutex::new(vec![])),
            event_tx,
        };

        let defer_config = DeferConfiguration::builder()
            .base(Duration::from_secs(1))
            .failure_threshold(1.0_f64) // Never disable deferral in tests
            .build()?;

        let consumer_config = ConsumerConfiguration::builder()
            .group_id(format!("defer-test-{}", Uuid::new_v4()))
            .bootstrap_servers(vec!["localhost:9094".to_owned()])
            .subscribed_topics(&[topic.to_string()])
            .probe_port(None) // Disable probe server to allow parallel test execution
            .build()?;

        let scheduler_config = SchedulerConfiguration::builder().build()?;

        // Use unique keyspace per test to avoid interference
        let keyspace = format!("test_defer_{}", Uuid::new_v4().simple());
        let cassandra_config = CassandraConfiguration::builder()
            .nodes(vec!["localhost:9042".to_owned()])
            .keyspace(keyspace.clone())
            .build()?;

        let cassandra_store = CassandraStore::new(&cassandra_config).await?;
        let defer_provider =
            CassandraDeferStoreProvider::with_store(cassandra_store, &keyspace).await?;

        let telemetry = Telemetry::new();
        let heartbeats = HeartbeatRegistry::new("defer-test".to_owned(), Duration::from_secs(60));
        let defer_middleware = DeferMiddleware::new(
            defer_config,
            &consumer_config,
            &scheduler_config,
            defer_provider,
            &telemetry,
            &heartbeats,
        )?;

        let handler_provider = defer_middleware
            .layer(LogMiddleware)
            .into_provider(handler.clone());

        let consumer = ProsodyConsumer::new(
            &consumer_config,
            &common::create_cassandra_trigger_store_config(),
            handler_provider,
            Telemetry::new(),
        )
        .await?;

        let producer = ProsodyProducer::new(
            &ProducerConfiguration::builder()
                .bootstrap_servers(vec!["localhost:9094".to_owned()])
                .source_system("defer-integration-test".to_owned())
                .build()?,
        )?;

        Ok(Self {
            consumer,
            producer,
            topic,
            event_rx,
            handler,
        })
    }

    /// Create test environment with permanent error handler.
    async fn new_with_permanent_error_handler(permanent_value: i64) -> Result<Self> {
        let topic = Self::setup_test_topic(1).await?;

        let (event_tx, event_rx) = channel(100);
        let handler = DeferTestHandler {
            fail_values: Arc::new(parking_lot::Mutex::new(vec![])),
            event_tx,
        };

        let permanent_handler = PermanentErrorHandler {
            inner: handler.clone(),
            permanent_value,
        };

        let defer_config = DeferConfiguration::builder()
            .base(Duration::from_secs(1))
            .failure_threshold(1.0_f64)
            .build()?;

        let consumer_config = ConsumerConfiguration::builder()
            .group_id(format!("defer-test-{}", Uuid::new_v4()))
            .bootstrap_servers(vec!["localhost:9094".to_owned()])
            .subscribed_topics(&[topic.to_string()])
            .probe_port(None) // Disable probe server to allow parallel test execution
            .build()?;

        let scheduler_config = SchedulerConfiguration::builder().build()?;

        // Use unique keyspace per test
        let keyspace = format!("test_defer_{}", Uuid::new_v4().simple());
        let cassandra_config = CassandraConfiguration::builder()
            .nodes(vec!["localhost:9042".to_owned()])
            .keyspace(keyspace.clone())
            .build()?;

        let cassandra_store = CassandraStore::new(&cassandra_config).await?;
        let defer_provider =
            CassandraDeferStoreProvider::with_store(cassandra_store, &keyspace).await?;

        let telemetry = Telemetry::new();
        let heartbeats = HeartbeatRegistry::new("defer-test".to_owned(), Duration::from_secs(60));
        let defer_middleware = DeferMiddleware::new(
            defer_config,
            &consumer_config,
            &scheduler_config,
            defer_provider,
            &telemetry,
            &heartbeats,
        )?;

        let handler_provider = defer_middleware
            .layer(LogMiddleware)
            .into_provider(permanent_handler);

        let consumer = ProsodyConsumer::new(
            &consumer_config,
            &common::create_cassandra_trigger_store_config(),
            handler_provider,
            Telemetry::new(),
        )
        .await?;

        let producer = ProsodyProducer::new(
            &ProducerConfiguration::builder()
                .bootstrap_servers(vec!["localhost:9094".to_owned()])
                .source_system("defer-integration-test".to_owned())
                .build()?,
        )?;

        Ok(Self {
            consumer,
            producer,
            topic,
            event_rx,
            handler,
        })
    }

    /// Set up a unique test topic and wait for it to be ready.
    async fn setup_test_topic(partition_count: u16) -> Result<Topic> {
        let topic_name = format!("defer-test-{}", Uuid::new_v4());
        let topic = Topic::from(topic_name.as_str());

        let admin_config = AdminConfiguration {
            bootstrap_servers: vec!["localhost:9094".to_owned()],
        };

        let client = ProsodyAdminClient::new(&admin_config)?;

        let topic_config = TopicConfiguration::builder()
            .name(topic.to_string())
            .partition_count(partition_count)
            .replication_factor(1_u16)
            .build()?;

        client.create_topic(&topic_config).await?;

        Ok(topic)
    }

    /// Send a message to the test topic.
    async fn send_message(&self, key: &str, payload: &Value) -> Result<()> {
        self.producer.send([], self.topic, key, payload).await?;
        Ok(())
    }

    /// Wait for a handler event with timeout.
    async fn expect_event(&mut self, timeout_secs: u64) -> Result<HandlerEvent> {
        timeout(
            Duration::from_secs(timeout_secs.max(30)),
            self.event_rx.recv(),
        )
        .await
        .map_err(|_| {
            eyre!(
                "Timeout waiting for handler event after {} seconds",
                timeout_secs
            )
        })?
        .ok_or_else(|| eyre!("Event channel closed unexpectedly"))
    }

    /// Verify that no event occurs within the given timeout.
    async fn expect_no_event(&mut self, timeout_millis: u64) -> Result<()> {
        let event_result =
            timeout(Duration::from_millis(timeout_millis), self.event_rx.recv()).await;
        ensure!(event_result.is_err(), "Expected no event but received one");
        Ok(())
    }

    /// Configure which values should fail with transient errors.
    fn set_failing_values(&self, values: Vec<i64>) {
        *self.handler.fail_values.lock() = values;
    }

    /// Clear all failing values (all messages will succeed).
    fn clear_failing_values(&self) {
        self.handler.fail_values.lock().clear();
    }

    /// Shut down the consumer.
    async fn shutdown(self) {
        self.consumer.shutdown().await;
    }
}

/// Test: First failure defers message, timer fires, retry succeeds.
#[test]
fn test_first_failure_defers_and_retries() {
    init_test_logging();

    QuickCheck::new()
        .tests(integration_test_count())
        .quickcheck(prop_first_failure_defers_and_retries as fn(()) -> TestResult);
}

fn prop_first_failure_defers_and_retries(_: ()) -> TestResult {
    match TEST_RUNTIME.block_on(run_first_failure_defers_and_retries()) {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(e.to_string()),
    }
}

async fn run_first_failure_defers_and_retries() -> Result<()> {
    let mut env = DeferTestEnvironment::new().await?;

    // Configure handler to fail value=1 initially
    env.set_failing_values(vec![1]);

    // Send message that will fail
    env.send_message("test-key", &json!({"value": 1_i64}))
        .await?;

    // Should receive transient failure event
    let event = env.expect_event(5).await?;
    ensure!(
        matches!(event, HandlerEvent::MessageFailedTransient { ref key, value: 1 } if key == "test-key"),
        "Expected transient failure for value=1"
    );

    // Clear failure condition so retry succeeds
    env.clear_failing_values();

    // Defer middleware handles timer internally and retries via on_message
    // (on_timer is NOT called for DeferRetry timers)
    // Should receive success event after defer delay (~1 second)
    let event = env.expect_event(10).await?;
    ensure!(
        matches!(event, HandlerEvent::MessageSuccess { ref key, value: 1 } if key == "test-key"),
        "Expected retry to succeed after defer delay"
    );

    env.shutdown().await;

    Ok(())
}

/// Test: Multiple messages for same key are queued and processed in order.
#[test]
fn test_multiple_messages_queued_in_order() {
    init_test_logging();

    QuickCheck::new()
        .tests(integration_test_count())
        .quickcheck(prop_multiple_messages_queued_in_order as fn(()) -> TestResult);
}

fn prop_multiple_messages_queued_in_order(_: ()) -> TestResult {
    match TEST_RUNTIME.block_on(run_multiple_messages_queued_in_order()) {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(e.to_string()),
    }
}

async fn run_multiple_messages_queued_in_order() -> Result<()> {
    let mut env = DeferTestEnvironment::new().await?;

    // Configure handler to fail value=1 (will be deferred)
    env.set_failing_values(vec![1]);

    // Send all 3 messages quickly (before timer fires)
    env.send_message("test-key", &json!({"value": 1_i64}))
        .await?;
    env.send_message("test-key", &json!({"value": 2_i64}))
        .await?;
    env.send_message("test-key", &json!({"value": 3_i64}))
        .await?;

    // First message should fail (transient error that will be deferred)
    let event = env.expect_event(5).await?;
    ensure!(
        matches!(event, HandlerEvent::MessageFailedTransient { ref key, value: 1 } if key == "test-key"),
        "Expected first message to fail with transient error"
    );

    // Messages 2 and 3 should be queued (no immediate events)
    env.expect_no_event(500).await?;

    // Clear failure condition so retries succeed
    env.clear_failing_values();

    // Defer middleware handles timer internally and retries via on_message
    // Wait for all 3 messages to be processed in order after defer delay
    for expected_value in 1..=3 {
        let event = env.expect_event(10).await?;
        ensure!(
            matches!(event, HandlerEvent::MessageSuccess { ref key, value }
                if key == "test-key" && value == expected_value),
            "Expected message value={expected_value} to succeed"
        );
    }

    env.shutdown().await;

    Ok(())
}

/// Test: Permanent errors are NOT deferred (they are irrecoverable).
#[test]
fn test_permanent_errors_not_deferred() {
    init_test_logging();

    QuickCheck::new()
        .tests(integration_test_count())
        .quickcheck(prop_permanent_errors_not_deferred as fn(()) -> TestResult);
}

fn prop_permanent_errors_not_deferred(_: ()) -> TestResult {
    match TEST_RUNTIME.block_on(run_permanent_errors_not_deferred()) {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(e.to_string()),
    }
}

async fn run_permanent_errors_not_deferred() -> Result<()> {
    let mut env = DeferTestEnvironment::new_with_permanent_error_handler(999).await?;

    // Send a message that fails with permanent error (LogMiddleware logs it)
    env.send_message("test-key-1", &json!({"value": 999_i64}))
        .await?;

    // Send a successful message with different key to verify consumer continues
    env.send_message("test-key-2", &json!({"value": 1_i64}))
        .await?;

    // Should immediately get success for key-2 (not deferred)
    let event = env.expect_event(5).await?;
    ensure!(
        matches!(event, HandlerEvent::MessageSuccess { ref key, value: 1 } if key == "test-key-2"),
        "Expected key-2 to succeed immediately (permanent errors don't defer)"
    );

    // No timer should fire for key-1 (permanent errors aren't retried)
    env.expect_no_event(2000).await?;

    env.shutdown().await;

    Ok(())
}
