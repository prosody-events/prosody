//! Integration tests for defer middleware functionality.
//!
//! Tests the complete defer middleware flow including:
//! - Message deferral on transient failures
//! - Timer scheduling and retry logic
//! - Multi-message queuing per key
//! - Successful retry and completion
//! - Cache and Cassandra store integration

use color_eyre::eyre::{Result, ensure};
use prosody::cassandra::{CassandraConfiguration, CassandraStore};
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::ConsumerMessage;
use prosody::consumer::middleware::defer::store::cassandra::CassandraDeferStore;
use prosody::consumer::middleware::defer::{DeferConfiguration, DeferMiddleware};
use prosody::consumer::middleware::log::LogMiddleware;
use prosody::consumer::middleware::scheduler::SchedulerConfiguration;
use prosody::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, HandlerMiddleware,
};
use prosody::consumer::{ConsumerConfiguration, DemandType, Keyed, ProsodyConsumer};
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
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::runtime::Builder;
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::{sleep, timeout};
use tracing::{error, info};
use uuid::Uuid;

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

    /// Timer fired for a key.
    TimerFired { key: String },
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
        timer: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let key = timer.key.to_string();
        let _ = self
            .event_tx
            .send(HandlerEvent::TimerFired { key: key.clone() })
            .await;
        info!("Timer fired for key={key}");
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

/// Helper to set up a unique test topic.
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

    // Wait for topic to be ready
    sleep(Duration::from_millis(500)).await;

    Ok(topic)
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
    let runtime = match Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return TestResult::error(format!("failed to initialize runtime: {e}")),
    };

    match runtime.block_on(run_first_failure_defers_and_retries()) {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(e.to_string()),
    }
}

async fn run_first_failure_defers_and_retries() -> Result<()> {
    let topic = setup_test_topic(1).await?;

    // Set up handler that fails value=1, succeeds for others
    let (event_tx, mut event_rx) = channel(100);
    let handler = DeferTestHandler {
        fail_values: Arc::new(parking_lot::Mutex::new(vec![1_i64])),
        event_tx,
    };

    // Configure defer middleware
    let defer_config = DeferConfiguration::builder()
        .base(Duration::from_secs(1))
        .failure_threshold(1.0_f64) // Never disable deferral in tests
        .build()?;

    let consumer_config = ConsumerConfiguration::builder()
        .group_id(format!("defer-test-{}", Uuid::new_v4()))
        .bootstrap_servers(vec!["localhost:9094".to_owned()])
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    let scheduler_config = SchedulerConfiguration::builder().build()?;

    // Create Cassandra store
    let cassandra_config = CassandraConfiguration::builder()
        .nodes(vec!["localhost:9042".to_owned()])
        .keyspace("prosody_integration_test".to_owned())
        .build()?;

    let cassandra_store = CassandraStore::new(&cassandra_config).await?;
    let defer_store =
        CassandraDeferStore::with_store(cassandra_store, "prosody_integration_test").await?;

    let defer_middleware = DeferMiddleware::new(
        defer_config,
        &consumer_config,
        &scheduler_config,
        defer_store,
    )?;

    let handler_provider = defer_middleware
        .layer(LogMiddleware)
        .into_provider(handler.clone());

    // Create consumer directly (it runs background tasks automatically)
    let consumer = ProsodyConsumer::new(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        handler_provider,
        Telemetry::new(),
    )
    .await?;

    // Produce test messages (consumer subscribes on creation)
    let producer = ProsodyProducer::new(
        &ProducerConfiguration::builder()
            .bootstrap_servers(vec!["localhost:9094".to_owned()])
            .source_system("defer-integration-test".to_owned())
            .build()?,
    )?;

    // Send a successful message first to prime the failure tracker
    producer
        .send([], topic, "test-key", &json!({"value": 0_i64}))
        .await?;

    // Wait for the successful message to be processed
    let event = timeout(Duration::from_secs(5), event_rx.recv())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("channel closed"))?;
    ensure!(
        matches!(event, HandlerEvent::MessageSuccess { ref key, value: 0 } if key == "test-key"),
        "Expected priming message to succeed"
    );

    // Now send the failing message
    producer
        .send([], topic, "test-key", &json!({"value": 1_i64}))
        .await?;

    // Expect failure (transient error that will be deferred)
    let event = timeout(Duration::from_secs(5), event_rx.recv())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("channel closed"))?;
    ensure!(
        matches!(event, HandlerEvent::MessageFailedTransient { ref key, value: 1 } if key == "test-key"),
        "Expected message to fail with transient error"
    );

    // Now mark value as success-able
    handler.fail_values.lock().clear();

    // Wait for timer to fire (1 second base delay + jitter)
    let event = timeout(Duration::from_secs(5), event_rx.recv())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("channel closed"))?;
    ensure!(
        matches!(event, HandlerEvent::MessageSuccess { ref key, value: 1 } if key == "test-key"),
        "Expected message to succeed on retry after timer fired"
    );

    // Cleanup
    consumer.shutdown().await;

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
    let runtime = match Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return TestResult::error(format!("failed to initialize runtime: {e}")),
    };

    match runtime.block_on(run_multiple_messages_queued_in_order()) {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(e.to_string()),
    }
}

async fn run_multiple_messages_queued_in_order() -> Result<()> {
    let topic = setup_test_topic(1).await?;

    let (event_tx, mut event_rx) = channel(100);
    // Only fail message 1 - messages 2 and 3 will be queued without trying
    let handler = DeferTestHandler {
        fail_values: Arc::new(parking_lot::Mutex::new(vec![1_i64])),
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
        .build()?;

    let scheduler_config = SchedulerConfiguration::builder().build()?;

    let cassandra_config = CassandraConfiguration::builder()
        .nodes(vec!["localhost:9042".to_owned()])
        .keyspace("prosody_integration_test".to_owned())
        .build()?;

    let cassandra_store = CassandraStore::new(&cassandra_config).await?;
    let defer_store =
        CassandraDeferStore::with_store(cassandra_store, "prosody_integration_test").await?;

    let defer_middleware = DeferMiddleware::new(
        defer_config,
        &consumer_config,
        &scheduler_config,
        defer_store,
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

    // Produce messages (consumer subscribes on creation)
    let producer = ProsodyProducer::new(
        &ProducerConfiguration::builder()
            .bootstrap_servers(vec!["localhost:9094".to_owned()])
            .source_system("defer-integration-test".to_owned())
            .build()?,
    )?;

    // Send a successful message first to prime the failure tracker
    producer
        .send([], topic, "test-key", &json!({"value": 0_i64}))
        .await?;

    // Wait for the successful message to be processed
    let event = timeout(Duration::from_secs(5), event_rx.recv())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("channel closed"))?;
    ensure!(
        matches!(event, HandlerEvent::MessageSuccess { ref key, value: 0 } if key == "test-key"),
        "Expected priming message to succeed"
    );

    // Send all 3 messages quickly (before timer fires)
    producer
        .send([], topic, "test-key", &json!({"value": 1_i64}))
        .await?;
    producer
        .send([], topic, "test-key", &json!({"value": 2_i64}))
        .await?;
    producer
        .send([], topic, "test-key", &json!({"value": 3_i64}))
        .await?;

    // First message should fail (transient error that will be deferred)
    let event = timeout(Duration::from_secs(5), event_rx.recv())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("channel closed"))?;
    ensure!(
        matches!(event, HandlerEvent::MessageFailedTransient { ref key, value: 1 } if key == "test-key"),
        "Expected first message to fail with transient error"
    );

    // Messages 2 and 3 should be queued (no immediate events)
    let no_event = timeout(Duration::from_millis(500), event_rx.recv()).await;
    ensure!(
        no_event.is_err(),
        "Expected no immediate events for queued messages"
    );

    // Clear failure condition so retries succeed
    handler.fail_values.lock().clear();

    // Wait for all 3 messages to be processed in order
    for expected_value in 1..=3 {
        let event = timeout(Duration::from_secs(10), event_rx.recv())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("channel closed"))?;
        ensure!(
            matches!(event, HandlerEvent::MessageSuccess { ref key, value }
                if key == "test-key" && value == expected_value),
            "Expected message value={expected_value} to succeed"
        );
    }

    consumer.shutdown().await;

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
    let runtime = match Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return TestResult::error(format!("failed to initialize runtime: {e}")),
    };

    match runtime.block_on(run_permanent_errors_not_deferred()) {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(e.to_string()),
    }
}

async fn run_permanent_errors_not_deferred() -> Result<()> {
    let topic = setup_test_topic(1).await?;

    let (event_tx, mut event_rx) = channel(100);
    // Handler that returns permanent error for value=999, succeeds for others
    let handler = DeferTestHandler {
        fail_values: Arc::new(parking_lot::Mutex::new(vec![])), /* No failures (value=999 handled
                                                                 * by wrapper) */
        event_tx,
    };

    let permanent_handler = PermanentErrorHandler {
        inner: handler,
        permanent_value: 999,
    };

    let defer_config = DeferConfiguration::builder()
        .base(Duration::from_secs(1))
        .failure_threshold(1.0_f64) // Never disable deferral in tests
        .build()?;

    let consumer_config = ConsumerConfiguration::builder()
        .group_id(format!("defer-test-{}", Uuid::new_v4()))
        .bootstrap_servers(vec!["localhost:9094".to_owned()])
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    let scheduler_config = SchedulerConfiguration::builder().build()?;

    let cassandra_config = CassandraConfiguration::builder()
        .nodes(vec!["localhost:9042".to_owned()])
        .keyspace("prosody_integration_test".to_owned())
        .build()?;

    let cassandra_store = CassandraStore::new(&cassandra_config).await?;
    let defer_store =
        CassandraDeferStore::with_store(cassandra_store, "prosody_integration_test").await?;

    let defer_middleware = DeferMiddleware::new(
        defer_config,
        &consumer_config,
        &scheduler_config,
        defer_store,
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

    // Send a message that fails with permanent error (LogMiddleware logs it)
    producer
        .send([], topic, "test-key-1", &json!({"value": 999_i64}))
        .await?;

    // Send a successful message with different key to verify consumer continues
    producer
        .send([], topic, "test-key-2", &json!({"value": 1_i64}))
        .await?;

    // Should immediately get success for key-2 (not deferred)
    let event = timeout(Duration::from_secs(5), event_rx.recv())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("channel closed"))?;
    ensure!(
        matches!(event, HandlerEvent::MessageSuccess { ref key, value: 1 } if key == "test-key-2"),
        "Expected key-2 message to succeed"
    );

    // Wait to verify no timer fires for the permanent error (key-1)
    // If permanent errors were incorrectly deferred, we'd see a timer event
    let no_timer = timeout(Duration::from_secs(3), event_rx.recv()).await;
    ensure!(
        no_timer.is_err(),
        "Permanent errors should not be deferred or retried"
    );

    consumer.shutdown().await;

    Ok(())
}
