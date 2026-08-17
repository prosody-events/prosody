//! Integration tests for defer middleware functionality.
//!
//! Tests the complete defer middleware flow including:
//! - Message deferral on transient failures
//! - Timer scheduling and retry logic
//! - Multi-message queuing per key
//! - Successful retry and completion
//! - Cassandra store integration
//!
//! The leaf [`FallibleHandler`] impls below (`DeferTestHandler`,
//! `PermanentErrorHandler`) rely on the default no-op `after_commit` /
//! `after_abort` hooks. The framework guarantees exactly one of those hooks
//! fires per `on_message` / `on_timer` invocation that runs and returns; we
//! observe behavior end-to-end via the `event_tx` channel rather than via the
//! apply hooks themselves.
//!
//! # Running Tests
//!
//! These tests use real Kafka and Cassandra instances. Each test creates one
//! environment (topic, consumer group, store handles) per test process and
//! repeats its scenario `INTEGRATION_TESTS` times against it. Iterations
//! isolate through unique keys and payload values: per-key ordering and
//! key-scoped defer queues make each iteration an independent domain, so no
//! per-iteration topics or consumer groups are needed.
//!
//! ```bash
//! cargo test --test defer_middleware
//! ```

#![recursion_limit = "256"]

use color_eyre::eyre::{Result, ensure};
use prosody::JsonCodec;
use prosody::cassandra::{CassandraConfiguration, CassandraStore};
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::ConsumerMessage;
use prosody::consumer::middleware::deduplication::DEFAULT_IDEMPOTENCE_VERSION;
use prosody::consumer::middleware::defer::message::store::CassandraMessageDeferStoreProvider;
use prosody::consumer::middleware::defer::message::store::cassandra::MessageQueries;
use prosody::consumer::middleware::defer::segment::CassandraSegmentStore;
use prosody::consumer::middleware::defer::{
    DeferConfiguration, FailureTracker, MessageDeferMiddleware,
};
use prosody::consumer::middleware::log::LogMiddleware;
use prosody::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use prosody::consumer::{DemandType, Keyed};
use prosody::error::{ClassifyError, ErrorCategory};
use prosody::heartbeat::HeartbeatRegistry;
use prosody::loader::KafkaLoader;
use prosody::telemetry::Telemetry;
use prosody::timers::Trigger;
use prosody::tracing::init_test_logging;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tracing::info;

mod common;
use common::TEST_RUNTIME;
use common::kafka::ConsumerEnv;

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

impl HandlerEvent {
    /// The key the event's message was addressed to.
    fn key(&self) -> &str {
        let (Self::MessageSuccess { key, .. } | Self::MessageFailedTransient { key, .. }) = self;
        key
    }
}

/// Test handler that fails configured values a bounded number of times.
#[derive(Clone)]
struct DeferTestHandler {
    /// Remaining transient failures per value: each failing invocation
    /// consumes one, and at zero the value succeeds. A counted budget —
    /// rather than a flag the test body clears mid-flight — keeps the
    /// handler's event stream deterministic: it can never race the defer
    /// middleware's retry timers, so tests assert on event content and
    /// order, never on timing. Iterations use disjoint values, so budgets
    /// are configured up front per iteration and never cleared.
    fail_budget: Arc<parking_lot::Mutex<HashMap<i64, u64>>>,

    /// Channel to report events.
    event_tx: Sender<HandlerEvent>,
}

impl FallibleHandler for DeferTestHandler {
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
        let key = message.key().to_string();
        let payload = message.payload();

        if let Some(value) = payload.get("value").and_then(Value::as_i64) {
            let should_fail = match self.fail_budget.lock().get_mut(&value) {
                Some(remaining) if *remaining > 0 => {
                    *remaining -= 1;
                    true
                }
                _ => false,
            };

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
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        // DeferRetry timers are consumed by the defer middleware: it loads
        // the deferred message and re-dispatches it through `on_message`,
        // so this leaf `on_timer` does not run for those triggers (and
        // therefore the framework fires neither apply hook on this leaf for
        // that path). This method only runs for non-defer timers, which the
        // tests in this file do not schedule.
        Ok(())
    }

    async fn shutdown(self) {}

    async fn on_excise<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<()>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(())
    }
}

/// Handler that wraps [`DeferTestHandler`] to return permanent errors for a
/// specific value. With `permanent_value: None` no error is injected and it
/// behaves exactly like the inner handler.
#[derive(Clone)]
struct PermanentErrorHandler {
    inner: DeferTestHandler,
    permanent_value: Option<i64>,
}

impl FallibleHandler for PermanentErrorHandler {
    type Error = TestError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Value>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let payload = message.payload();
        if self.permanent_value.is_some()
            && payload.get("value").and_then(Value::as_i64) == self.permanent_value
        {
            return Err(TestError::Permanent);
        }
        self.inner.on_message(context, message, demand_type).await
    }

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.inner.on_timer(context, timer, demand_type).await
    }

    async fn shutdown(self) {
        self.inner.shutdown().await;
    }

    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<()>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.inner.on_excise(context, message, demand_type).await
    }
}

/// Test environment wrapping [`ConsumerEnv`] with the defer middleware stack
/// and the handler's event channel.
///
/// Created once per test process and shared by all iterations; each iteration
/// uses keys and values no other iteration touches.
struct DeferTestEnvironment {
    env: ConsumerEnv,
    event_rx: Receiver<HandlerEvent>,
    handler: DeferTestHandler,
}

impl DeferTestEnvironment {
    /// Create a new test environment with defer middleware.
    async fn new() -> Result<Self> {
        Self::build(None).await
    }

    /// Create test environment that fails `permanent_value` permanently.
    async fn new_with_permanent_error_handler(permanent_value: i64) -> Result<Self> {
        Self::build(Some(permanent_value)).await
    }

    async fn build(permanent_value: Option<i64>) -> Result<Self> {
        let (event_tx, event_rx) = channel(100);
        let handler = DeferTestHandler {
            fail_budget: Arc::default(),
            event_tx,
        };
        let leaf = PermanentErrorHandler {
            inner: handler.clone(),
            permanent_value,
        };

        let env = ConsumerEnv::new("defer-test", async move |consumer_config| {
            let defer_config = DeferConfiguration::builder()
                .base(Duration::from_secs(1))
                .failure_threshold(1.0_f64) // Never disable deferral in tests
                .build()?;

            // Share the migrated keyspace like every other integration test;
            // isolation comes from the per-test topic and consumer group, so
            // no per-test keyspace is created (or left to leak).
            let keyspace = common::TEST_KEYSPACE.to_owned();
            let cassandra_config = CassandraConfiguration::builder()
                .nodes(vec!["localhost:9042".to_owned()])
                .keyspace(keyspace.clone())
                .build()?;

            let cassandra_store = CassandraStore::new(&cassandra_config).await?;
            let segment_store =
                CassandraSegmentStore::new(cassandra_store.clone(), &keyspace).await?;
            let message_queries =
                Arc::new(MessageQueries::new(cassandra_store.session(), &keyspace).await?);
            let message_provider = CassandraMessageDeferStoreProvider::new(
                cassandra_store.clone(),
                message_queries,
                segment_store,
            );

            let telemetry = Telemetry::new();
            let heartbeats =
                HeartbeatRegistry::new("defer-test".to_owned(), Duration::from_mins(1));
            let failure_tracker = FailureTracker::new(
                defer_config.failure_window,
                defer_config.failure_threshold,
                &telemetry,
                &heartbeats,
            );
            let loader =
                KafkaLoader::<JsonCodec>::for_consumer(consumer_config, None, &heartbeats)?;
            let defer_middleware = MessageDeferMiddleware::new(
                defer_config,
                consumer_config,
                message_provider,
                failure_tracker,
                loader,
                DEFAULT_IDEMPOTENCE_VERSION,
                &telemetry,
            )?;

            Ok(defer_middleware
                .layer(LogMiddleware::new())
                .into_provider(leaf))
        })
        .await?;

        Ok(Self {
            env,
            event_rx,
            handler,
        })
    }

    /// Send a message to the test topic.
    async fn send_message(&self, key: &str, payload: Value) -> Result<()> {
        self.env.send_message(key, payload).await
    }

    /// Wait for the next handler event for `key`, skipping events addressed
    /// to other keys (they belong to other iterations' key domains).
    async fn expect_event(&mut self, key: &str, timeout_secs: u64) -> Result<HandlerEvent> {
        loop {
            let event = common::receive::expect_event(
                &mut self.event_rx,
                Duration::from_secs(timeout_secs.max(30)),
            )
            .await?;
            if event.key() == key {
                return Ok(event);
            }
            info!("ignoring event for another iteration's key: {event:?}");
        }
    }

    /// Verify that no event occurs within the given timeout.
    async fn expect_no_event(&mut self, timeout_millis: u64) -> Result<()> {
        common::receive::expect_no_event(&mut self.event_rx, Duration::from_millis(timeout_millis))
            .await
    }

    /// Budget `times` transient failures for `value`; once consumed, further
    /// invocations succeed. Configured up front (never mid-flight), so the
    /// event stream cannot race the defer middleware's retry timers.
    fn fail_value_times(&self, value: i64, times: u64) {
        self.handler.fail_budget.lock().insert(value, times);
    }

    /// Shut down the consumer and delete the test topic.
    async fn shutdown(self) {
        self.env.shutdown().await;
    }
}

/// Runs `scenario` against one shared environment for `INTEGRATION_TESTS`
/// iterations, shutting the consumer down before propagating any failure
/// (dropping a live consumer hangs rdkafka threads).
fn run_iterations<F>(env_result: Result<DeferTestEnvironment>, scenario: &F) -> Result<()>
where
    F: AsyncFn(&mut DeferTestEnvironment, u64) -> Result<()>,
{
    TEST_RUNTIME.block_on(async {
        let mut env = env_result?;
        let mut result = Ok(());
        for iteration in 0..common::integration_test_count() {
            result = scenario(&mut env, iteration).await;
            if result.is_err() {
                break;
            }
        }
        env.shutdown().await;
        result
    })
}

/// Test: First failure defers message, timer fires, retry succeeds.
#[path = "defer_middleware/scenarios.rs"]
mod scenarios;
