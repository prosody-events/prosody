//! Integration tests for telemetry event emission via Kafka.
//!
//! Validates that telemetry events (message lifecycle, producer message sent)
//! are serialized to JSON and produced to a dedicated Kafka telemetry topic.

use color_eyre::eyre::{Result, ensure, eyre};
use prosody::Topic;
use prosody::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use prosody::cassandra::config::CassandraConfigurationBuilder;
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::ConsumerMessage;
use prosody::consumer::middleware::FallibleHandler;
use prosody::consumer::middleware::defer::DeferConfigurationBuilder;
use prosody::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
use prosody::consumer::middleware::retry::RetryConfigurationBuilder;
use prosody::consumer::middleware::scheduler::SchedulerConfigurationBuilder;
use prosody::consumer::middleware::timeout::TimeoutConfigurationBuilder;
use prosody::consumer::middleware::topic::FailureTopicConfigurationBuilder;
use prosody::consumer::{ConsumerConfigurationBuilder, DemandType, Keyed};
use prosody::error::{ClassifyError, ErrorCategory};
use prosody::high_level::mode::Mode;
use prosody::high_level::{ConsumerBuilders, HighLevelClient};
use prosody::producer::ProducerConfigurationBuilder;
use prosody::telemetry::TelemetryEmitterConfiguration;
use prosody::timers::TimerType;
use prosody::timers::Trigger;
use prosody::timers::datetime::CompactDateTime;
use prosody::timers::duration::CompactDuration;
use prosody::tracing::init_test_logging;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde_json::{Value, json};
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::time::Duration;
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::{Instant, timeout};
use uuid::Uuid;

mod common;

const BOOTSTRAP: &str = "localhost:9094";
const CASSANDRA_HOST: &str = "localhost:9042";
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(30);
/// Top-level timeout for any single integration test.
const TEST_TIMEOUT: Duration = Duration::from_secs(45);
/// Timeout for tests that involve timer scheduling (3 s delay + startup).
const TIMER_TEST_TIMEOUT: Duration = Duration::from_secs(60);

// ── Test Handlers ────────────────────────────────────────────────────────────

/// Test error type for handler results.
#[derive(Debug, Clone)]
struct TestError;

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

/// Handler that forwards message keys to a channel.
#[derive(Clone)]
struct ForwardHandler {
    tx: Sender<String>,
}

impl FallibleHandler for ForwardHandler {
    type Error = TestError;

    async fn on_message<C>(
        &self,
        _ctx: C,
        msg: ConsumerMessage,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let _ = self.tx.send(msg.key().to_string()).await;
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        Ok(())
    }

    async fn shutdown(self) {}
}

/// Handler that always fails on messages.
#[derive(Clone)]
struct FailingHandler {
    tx: Sender<String>,
}

impl FallibleHandler for FailingHandler {
    type Error = TestError;

    async fn on_message<C>(
        &self,
        _ctx: C,
        msg: ConsumerMessage,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let _ = self.tx.send(msg.key().to_string()).await;
        Err(TestError)
    }

    async fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        Ok(())
    }

    async fn shutdown(self) {}
}

/// Handler that schedules a timer on message, then succeeds on timer.
#[derive(Clone)]
struct TimerSchedulingHandler {
    msg_tx: Sender<String>,
    timer_tx: Sender<String>,
}

impl FallibleHandler for TimerSchedulingHandler {
    type Error = TestError;

    async fn on_message<C>(
        &self,
        ctx: C,
        msg: ConsumerMessage,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let key = msg.key().to_string();
        let schedule_time = CompactDateTime::now()
            .and_then(|now| now.add_duration(CompactDuration::new(3)))
            .map_err(|_| TestError)?;
        ctx.schedule(schedule_time, TimerType::Application)
            .await
            .map_err(|_| TestError)?;
        let _ = self.msg_tx.send(key).await;
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _ctx: C,
        trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let _ = self.timer_tx.send(trigger.key.to_string()).await;
        Ok(())
    }

    async fn shutdown(self) {}
}

/// Handler that schedules a timer on message, then fails on timer.
#[derive(Clone)]
struct TimerFailingHandler {
    msg_tx: Sender<String>,
    timer_tx: Sender<String>,
}

impl FallibleHandler for TimerFailingHandler {
    type Error = TestError;

    async fn on_message<C>(
        &self,
        ctx: C,
        msg: ConsumerMessage,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let key = msg.key().to_string();
        let schedule_time = CompactDateTime::now()
            .and_then(|now| now.add_duration(CompactDuration::new(3)))
            .map_err(|_| TestError)?;
        ctx.schedule(schedule_time, TimerType::Application)
            .await
            .map_err(|_| TestError)?;
        let _ = self.msg_tx.send(key).await;
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _ctx: C,
        trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let _ = self.timer_tx.send(trigger.key.to_string()).await;
        Err(TestError)
    }

    async fn shutdown(self) {}
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn bootstrap_servers() -> Vec<String> {
    vec![BOOTSTRAP.to_owned()]
}

async fn create_topic(admin: &ProsodyAdminClient, name: &str) -> Result<()> {
    admin
        .create_topic(
            &TopicConfiguration::builder()
                .name(name.to_owned())
                .partition_count(1_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;
    Ok(())
}

fn create_telemetry_consumer(telemetry_topic: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP)
        .set("group.id", Uuid::new_v4().to_string())
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()?;
    consumer.subscribe(&[telemetry_topic])?;
    Ok(consumer)
}

/// Reads telemetry events from Kafka until finding one matching `type_str`.
async fn consume_telemetry_event_by_type(
    consumer: &StreamConsumer,
    type_str: &str,
    deadline: Duration,
) -> Result<Value> {
    let start = Instant::now();
    loop {
        let remaining = deadline
            .checked_sub(start.elapsed())
            .ok_or_else(|| eyre!("timeout waiting for telemetry event type={type_str}"))?;

        let msg = timeout(remaining, consumer.recv()).await??;
        let payload = msg
            .payload()
            .ok_or_else(|| eyre!("telemetry message has no payload"))?;
        let value: Value = serde_json::from_slice(payload)?;
        if value.get("type").and_then(Value::as_str) == Some(type_str) {
            return Ok(value);
        }
    }
}

/// Validates the field contract for a `prosody.message.succeeded` event.
fn assert_succeeded_contract(succeeded: &Value, expected_key: &str) -> Result<()> {
    assert_eq!(
        succeeded.get("type").and_then(Value::as_str),
        Some("prosody.message.succeeded"),
        "succeeded type mismatch"
    );
    let event_time = succeeded
        .get("eventTime")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("succeeded: missing eventTime"))?;
    ensure!(
        chrono::DateTime::parse_from_rfc3339(event_time).is_ok(),
        "succeeded: eventTime not RFC 3339: {event_time}"
    );
    ensure!(
        succeeded.get("offset").and_then(Value::as_i64).is_some(),
        "succeeded: offset should be an integer"
    );
    ensure!(
        succeeded.get("topic").and_then(Value::as_str).is_some(),
        "succeeded: topic should be a string"
    );
    ensure!(
        succeeded.get("partition").and_then(Value::as_i64).is_some(),
        "succeeded: partition should be an integer"
    );
    assert_eq!(
        succeeded.get("key").and_then(Value::as_str),
        Some(expected_key),
        "succeeded: key mismatch"
    );
    ensure!(
        succeeded
            .get("source")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        "succeeded: source should be non-empty"
    );
    ensure!(
        succeeded
            .get("hostname")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        "succeeded: hostname should be non-empty"
    );
    let demand = succeeded
        .get("demandType")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("succeeded: missing demandType"))?;
    ensure!(
        demand == "normal" || demand == "failure",
        "succeeded: demandType invalid: {demand}"
    );
    ensure!(
        succeeded.get("errorCategory").is_none(),
        "succeeded: errorCategory should be absent"
    );
    ensure!(
        succeeded.get("exception").is_none(),
        "succeeded: exception should be absent"
    );
    Ok(())
}

/// Asserts no telemetry events arrive within the given duration.
async fn assert_no_telemetry_events(consumer: &StreamConsumer, wait: Duration) -> Result<()> {
    let result = timeout(wait, consumer.recv()).await;
    ensure!(
        result.is_err(),
        "expected no telemetry events but received one"
    );
    Ok(())
}

/// Build a `HighLevelClient` for best-effort mode with a custom telemetry
/// topic.
fn build_client(
    source_topic: &str,
    telemetry_topic: &str,
    emitter_enabled: bool,
) -> Result<HighLevelClient<ForwardHandler>> {
    let mut producer_builder = ProducerConfigurationBuilder::default();
    producer_builder
        .bootstrap_servers(bootstrap_servers())
        .source_system("test-telemetry");

    let mut consumer_builder = ConsumerConfigurationBuilder::default();
    consumer_builder
        .bootstrap_servers(bootstrap_servers())
        .group_id(Uuid::new_v4().to_string())
        .subscribed_topics(vec![source_topic.to_owned()])
        .probe_port(None);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_builder,
        retry: RetryConfigurationBuilder::default(),
        failure_topic: FailureTopicConfigurationBuilder::default(),
        scheduler: SchedulerConfigurationBuilder::default(),
        monopolization: MonopolizationConfigurationBuilder::default(),
        defer: DeferConfigurationBuilder::default(),
        timeout: TimeoutConfigurationBuilder::default(),
        emitter: TelemetryEmitterConfiguration {
            topic: telemetry_topic.to_owned(),
            enabled: emitter_enabled,
        },
    };

    let mut cassandra_builder = CassandraConfigurationBuilder::default();
    cassandra_builder.nodes(vec![CASSANDRA_HOST.to_owned()]);

    let client = HighLevelClient::new(
        Mode::BestEffort,
        &mut producer_builder,
        &consumer_builders,
        &cassandra_builder,
    )?;
    Ok(client)
}

fn build_typed_client<T: FallibleHandler>(
    source_topic: &str,
    telemetry_topic: &str,
) -> Result<HighLevelClient<T>> {
    let mut producer_builder = ProducerConfigurationBuilder::default();
    producer_builder
        .bootstrap_servers(bootstrap_servers())
        .source_system("test-telemetry");

    let mut consumer_builder = ConsumerConfigurationBuilder::default();
    consumer_builder
        .bootstrap_servers(bootstrap_servers())
        .group_id(Uuid::new_v4().to_string())
        .subscribed_topics(vec![source_topic.to_owned()])
        .probe_port(None);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_builder,
        retry: RetryConfigurationBuilder::default(),
        failure_topic: FailureTopicConfigurationBuilder::default(),
        scheduler: SchedulerConfigurationBuilder::default(),
        monopolization: MonopolizationConfigurationBuilder::default(),
        defer: DeferConfigurationBuilder::default(),
        timeout: TimeoutConfigurationBuilder::default(),
        emitter: TelemetryEmitterConfiguration {
            topic: telemetry_topic.to_owned(),
            enabled: true,
        },
    };

    let mut cassandra_builder = CassandraConfigurationBuilder::default();
    cassandra_builder.nodes(vec![CASSANDRA_HOST.to_owned()]);

    let client = HighLevelClient::new(
        Mode::BestEffort,
        &mut producer_builder,
        &consumer_builders,
        &cassandra_builder,
    )?;
    Ok(client)
}

// ── Integration Tests ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn message_lifecycle_events_on_kafka() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let source_topic = Uuid::new_v4().to_string();
        let source: Topic = source_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &source_topic).await?;

        let client = build_client(&source_topic, &telemetry_topic, true)?;

        let (msg_tx, mut msg_rx) = channel(16);
        client.subscribe(ForwardHandler { tx: msg_tx }).await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "test-key", &json!({"v": 1_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        let dispatched = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.message.dispatched",
            RECEIVE_TIMEOUT,
        )
        .await?;
        assert_eq!(
            dispatched.get("key").and_then(Value::as_str),
            Some("test-key")
        );

        let succeeded = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.message.succeeded",
            RECEIVE_TIMEOUT,
        )
        .await?;
        assert_eq!(
            succeeded.get("type").and_then(Value::as_str),
            Some("prosody.message.succeeded")
        );

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_message_sent_on_kafka() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let dest_topic = Uuid::new_v4().to_string();
        let dest: Topic = dest_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &dest_topic).await?;

        let client = build_client(&dest_topic, &telemetry_topic, true)?;
        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client.send(dest, "sent-key", &json!({"v": 1_i32})).await?;

        let sent = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.message.sent",
            RECEIVE_TIMEOUT,
        )
        .await?;

        assert_eq!(
            sent.get("type").and_then(Value::as_str),
            Some("prosody.message.sent")
        );
        assert_eq!(sent.get("key").and_then(Value::as_str), Some("sent-key"));
        assert!(
            sent.get("offset").and_then(Value::as_i64).is_some(),
            "sent event should have offset"
        );
        assert!(
            sent.get("source").and_then(Value::as_str).is_some(),
            "sent event should have source"
        );

        let sent_event_time = sent
            .get("eventTime")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("sent: missing eventTime"))?;
        ensure!(
            chrono::DateTime::parse_from_rfc3339(sent_event_time).is_ok(),
            "sent: eventTime not RFC 3339: {sent_event_time}"
        );
        ensure!(
            sent.get("topic")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty()),
            "sent: topic should be a non-empty string"
        );
        ensure!(
            sent.get("partition").and_then(Value::as_i64).is_some(),
            "sent: partition should be an integer"
        );
        ensure!(
            sent.get("hostname")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty()),
            "sent: hostname should be non-empty"
        );

        admin.delete_topic(&dest_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn emitter_disabled_no_events() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let source_topic = Uuid::new_v4().to_string();
        let source: Topic = source_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &source_topic).await?;

        let client = build_client(&source_topic, &telemetry_topic, false)?;

        let (msg_tx, mut msg_rx) = channel(16);
        client.subscribe(ForwardHandler { tx: msg_tx }).await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "no-emit-key", &json!({"v": 1_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        assert_no_telemetry_events(&telemetry_consumer, Duration::from_secs(5)).await?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn json_payload_contract_validation() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let source_topic = Uuid::new_v4().to_string();
        let source: Topic = source_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &source_topic).await?;

        let client = build_client(&source_topic, &telemetry_topic, true)?;

        let (msg_tx, mut msg_rx) = channel(16);
        client.subscribe(ForwardHandler { tx: msg_tx }).await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "contract-key", &json!({"v": 1_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        let event = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.message.dispatched",
            RECEIVE_TIMEOUT,
        )
        .await?;

        // type
        assert_eq!(
            event.get("type").and_then(Value::as_str),
            Some("prosody.message.dispatched"),
            "type field mismatch"
        );

        // eventTime (RFC 3339)
        let event_time = event
            .get("eventTime")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("missing eventTime"))?;
        ensure!(
            chrono::DateTime::parse_from_rfc3339(event_time).is_ok(),
            "eventTime is not valid RFC 3339: {event_time}"
        );

        // offset (integer)
        ensure!(
            event.get("offset").and_then(Value::as_i64).is_some(),
            "offset should be an integer"
        );

        // topic (string)
        ensure!(
            event.get("topic").and_then(Value::as_str).is_some(),
            "topic should be a string"
        );

        // partition (integer)
        ensure!(
            event.get("partition").and_then(Value::as_i64).is_some(),
            "partition should be an integer"
        );

        // key
        assert_eq!(
            event.get("key").and_then(Value::as_str),
            Some("contract-key"),
            "key field mismatch"
        );

        // source (non-empty string)
        let source_val = event
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("missing source"))?;
        ensure!(!source_val.is_empty(), "source should be non-empty");

        // hostname (non-empty string)
        let hostname = event
            .get("hostname")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("missing hostname"))?;
        ensure!(!hostname.is_empty(), "hostname should be non-empty");

        // demandType (one of "normal"/"failure")
        let demand_type = event
            .get("demandType")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("missing demandType"))?;
        ensure!(
            demand_type == "normal" || demand_type == "failure",
            "demandType should be 'normal' or 'failure', got: {demand_type}"
        );

        // Error fields should NOT be present on dispatched
        ensure!(
            event.get("errorCategory").is_none(),
            "errorCategory should be absent on dispatched event"
        );
        ensure!(
            event.get("exception").is_none(),
            "exception should be absent on dispatched event"
        );

        // ── succeeded event contract ──
        let succeeded = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.message.succeeded",
            RECEIVE_TIMEOUT,
        )
        .await?;
        assert_succeeded_contract(&succeeded, "contract-key")?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn message_failed_event_on_kafka() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let source_topic = Uuid::new_v4().to_string();
        let source: Topic = source_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &source_topic).await?;

        let client: HighLevelClient<FailingHandler> =
            build_typed_client(&source_topic, &telemetry_topic)?;

        let (fail_tx, mut fail_rx) = channel(16);
        client.subscribe(FailingHandler { tx: fail_tx }).await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "fail-key", &json!({"v": 1_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, fail_rx.recv()).await?;

        let failed = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.message.failed",
            RECEIVE_TIMEOUT,
        )
        .await?;

        assert_eq!(
            failed.get("type").and_then(Value::as_str),
            Some("prosody.message.failed"),
            "type mismatch"
        );
        assert_eq!(
            failed.get("key").and_then(Value::as_str),
            Some("fail-key"),
            "key mismatch"
        );
        assert_eq!(
            failed.get("errorCategory").and_then(Value::as_str),
            Some("permanent"),
            "errorCategory should be 'permanent'"
        );
        ensure!(
            failed
                .get("exception")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty()),
            "exception should be non-empty"
        );

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_lifecycle_events_on_kafka() -> Result<()> {
    timeout(TIMER_TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let source_topic = Uuid::new_v4().to_string();
        let source: Topic = source_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &source_topic).await?;

        let client: HighLevelClient<TimerSchedulingHandler> =
            build_typed_client(&source_topic, &telemetry_topic)?;

        let (msg_tx, mut msg_rx) = channel(16);
        let (timer_tx, mut timer_rx) = channel(16);
        client
            .subscribe(TimerSchedulingHandler { msg_tx, timer_tx })
            .await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "timer-key", &json!({"v": 1_i32}))
            .await?;

        // Wait for message handler to complete (timer scheduled inside)
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Wait for timer handler to fire (~3 s delay)
        let _ = timeout(Duration::from_secs(15), timer_rx.recv()).await?;

        let scheduled = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.timer.scheduled",
            RECEIVE_TIMEOUT,
        )
        .await?;
        assert_eq!(
            scheduled.get("type").and_then(Value::as_str),
            Some("prosody.timer.scheduled")
        );
        assert_eq!(
            scheduled.get("key").and_then(Value::as_str),
            Some("timer-key")
        );
        let sched_time = scheduled
            .get("scheduledTime")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("scheduled: missing scheduledTime"))?;
        ensure!(
            chrono::DateTime::parse_from_rfc3339(sched_time).is_ok(),
            "scheduled: scheduledTime not RFC 3339: {sched_time}"
        );
        ensure!(
            scheduled.get("timerType").and_then(Value::as_str).is_some(),
            "scheduled: timerType should be present"
        );

        let dispatched = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.timer.dispatched",
            RECEIVE_TIMEOUT,
        )
        .await?;
        assert_eq!(
            dispatched.get("type").and_then(Value::as_str),
            Some("prosody.timer.dispatched")
        );
        assert_eq!(
            dispatched.get("key").and_then(Value::as_str),
            Some("timer-key")
        );
        let disp_sched_time = dispatched
            .get("scheduledTime")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("dispatched: missing scheduledTime"))?;
        ensure!(
            chrono::DateTime::parse_from_rfc3339(disp_sched_time).is_ok(),
            "dispatched: scheduledTime not RFC 3339"
        );

        let timer_succeeded = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.timer.succeeded",
            RECEIVE_TIMEOUT,
        )
        .await?;
        assert_eq!(
            timer_succeeded.get("type").and_then(Value::as_str),
            Some("prosody.timer.succeeded")
        );
        assert_eq!(
            timer_succeeded.get("key").and_then(Value::as_str),
            Some("timer-key")
        );
        ensure!(
            timer_succeeded
                .get("timerType")
                .and_then(Value::as_str)
                .is_some(),
            "succeeded: timerType should be present"
        );

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_failed_event_on_kafka() -> Result<()> {
    timeout(TIMER_TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let source_topic = Uuid::new_v4().to_string();
        let source: Topic = source_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &source_topic).await?;

        let client: HighLevelClient<TimerFailingHandler> =
            build_typed_client(&source_topic, &telemetry_topic)?;

        let (msg_tx, mut msg_rx) = channel(16);
        let (timer_tx, mut timer_rx) = channel(16);
        client
            .subscribe(TimerFailingHandler { msg_tx, timer_tx })
            .await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "timer-fail-key", &json!({"v": 1_i32}))
            .await?;

        // Wait for message handler to complete (timer scheduled inside)
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Wait for timer handler to fire (~3 s delay)
        let _ = timeout(Duration::from_secs(15), timer_rx.recv()).await?;

        let timer_failed = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.timer.failed",
            RECEIVE_TIMEOUT,
        )
        .await?;

        assert_eq!(
            timer_failed.get("type").and_then(Value::as_str),
            Some("prosody.timer.failed")
        );
        assert_eq!(
            timer_failed.get("key").and_then(Value::as_str),
            Some("timer-fail-key")
        );
        assert_eq!(
            timer_failed.get("errorCategory").and_then(Value::as_str),
            Some("permanent"),
            "errorCategory should be 'permanent'"
        );
        ensure!(
            timer_failed
                .get("exception")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty()),
            "exception should be non-empty"
        );

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}
