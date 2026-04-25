#![recursion_limit = "256"]
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
use prosody::consumer::middleware::deduplication::DeduplicationConfigurationBuilder;
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
const TIMER_TEST_TIMEOUT: Duration = Duration::from_mins(1);
/// Timeout for defer tests: warm-up + 3 s timer + backoff + telemetry drain.
const DEFER_TEST_TIMEOUT: Duration = Duration::from_mins(2);

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
    type Outcome = ();

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
    type Outcome = ();

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
    type Outcome = ();

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
    type Outcome = ();

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

/// Handler that schedules a timer at t+60 then immediately cancels it.
#[derive(Clone)]
struct TimerCancellingHandler {
    msg_tx: Sender<String>,
}

impl FallibleHandler for TimerCancellingHandler {
    type Error = TestError;
    type Outcome = ();

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
            .and_then(|now| now.add_duration(CompactDuration::new(60)))
            .map_err(|_| TestError)?;
        ctx.schedule(schedule_time, TimerType::Application)
            .await
            .map_err(|_| TestError)?;
        ctx.unschedule(schedule_time, TimerType::Application)
            .await
            .map_err(|_| TestError)?;
        let _ = self.msg_tx.send(key).await;
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

/// Handler that schedules a timer on first message, then on second message
/// calls `clear_and_schedule` with a new time.
#[derive(Clone)]
struct ClearAndScheduleHandler {
    msg_tx: Sender<String>,
}

impl FallibleHandler for ClearAndScheduleHandler {
    type Error = TestError;
    type Outcome = ();

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
        let step = msg
            .payload()
            .get("step")
            .and_then(Value::as_i64)
            .ok_or(TestError)?;

        if step == 1 {
            // First message: schedule at t+60
            let schedule_time = CompactDateTime::now()
                .and_then(|now| now.add_duration(CompactDuration::new(60)))
                .map_err(|_| TestError)?;
            ctx.schedule(schedule_time, TimerType::Application)
                .await
                .map_err(|_| TestError)?;
        } else {
            // Second message: clear_and_schedule at t+120
            let new_time = CompactDateTime::now()
                .and_then(|now| now.add_duration(CompactDuration::new(120)))
                .map_err(|_| TestError)?;
            ctx.clear_and_schedule(new_time, TimerType::Application)
                .await
                .map_err(|_| TestError)?;
        }

        let _ = self.msg_tx.send(key).await;
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

/// Handler that schedules a timer on first message, then calls
/// `clear_and_schedule` on second message and reports both the replacement
/// time and the actual trigger time so the test can verify the inline
/// replacement path.
#[derive(Clone)]
struct InlineReplacementHandler {
    messages: Sender<String>,
    replacement_time: Sender<CompactDateTime>,
    timer_fired: Sender<CompactDateTime>,
}

impl FallibleHandler for InlineReplacementHandler {
    type Error = TestError;
    type Outcome = ();

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
        let step = msg
            .payload()
            .get("step")
            .and_then(Value::as_i64)
            .ok_or(TestError)?;

        if step == 1 {
            let schedule_time = CompactDateTime::now()
                .and_then(|now| now.add_duration(CompactDuration::new(3)))
                .map_err(|_| TestError)?;
            ctx.schedule(schedule_time, TimerType::Application)
                .await
                .map_err(|_| TestError)?;
        } else {
            let new_time = CompactDateTime::now()
                .and_then(|now| now.add_duration(CompactDuration::new(5)))
                .map_err(|_| TestError)?;
            ctx.clear_and_schedule(new_time, TimerType::Application)
                .await
                .map_err(|_| TestError)?;
            let _ = self.replacement_time.send(new_time).await;
        }

        let _ = self.messages.send(key).await;
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
        let _ = self.timer_fired.send(trigger.time).await;
        Ok(())
    }

    async fn shutdown(self) {}
}

/// Test error type that classifies as transient.
#[derive(Debug, Clone)]
struct TransientError;

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

/// Fails transiently on `Normal` demand when the key starts with `"defer-"`,
/// triggering `DeferredMessage` retry. All other keys succeed immediately,
/// allowing warm-up messages to seed the `FailureTracker` with successes.
#[derive(Clone)]
struct TransientMessageHandler {
    done_tx: Sender<String>,
}

impl FallibleHandler for TransientMessageHandler {
    type Error = TransientError;
    type Outcome = ();

    async fn on_message<C>(
        &self,
        _ctx: C,
        msg: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Non-defer keys always succeed (used to seed the FailureTracker).
        if !msg.key().starts_with("defer-") {
            let _ = self.done_tx.send(msg.key().to_string()).await;
            return Ok(());
        }
        // Fail on Normal for defer keys so retry exhausts and defer activates.
        // Only succeed when re-driven by the DeferredMessage timer.
        if demand_type == DemandType::Normal {
            return Err(TransientError);
        }
        let _ = self.done_tx.send(msg.key().to_string()).await;
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

/// Schedules a timer on message, fails transiently on `Normal` timer attempts
/// for keys starting with `"defer-"` (triggering `DeferredTimer`), then
/// succeeds when re-driven by the defer retry. Non-defer keys always succeed
/// immediately, allowing warm-up messages to seed the `FailureTracker`.
#[derive(Clone)]
struct TransientTimerHandler {
    msg_tx: Sender<String>,
    done_tx: Sender<String>,
}

impl FallibleHandler for TransientTimerHandler {
    type Error = TransientError;
    type Outcome = ();

    async fn on_message<C>(
        &self,
        ctx: C,
        msg: ConsumerMessage,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Non-defer keys don't schedule a timer — just succeed immediately.
        if !msg.key().starts_with("defer-") {
            let _ = self.msg_tx.send(msg.key().to_string()).await;
            return Ok(());
        }
        let schedule_time = CompactDateTime::now()
            .and_then(|now| now.add_duration(CompactDuration::new(3)))
            .map_err(|_| TransientError)?;
        ctx.schedule(schedule_time, TimerType::Application)
            .await
            .map_err(|_| TransientError)?;
        let _ = self.msg_tx.send(msg.key().to_string()).await;
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _ctx: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Fail on Normal for defer keys so retry exhausts and defer activates.
        // Only succeed when re-driven by the DeferredTimer.
        if trigger.key.starts_with("defer-") && demand_type == DemandType::Normal {
            return Err(TransientError);
        }
        let _ = self.done_tx.send(trigger.key.to_string()).await;
        Ok(())
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

/// Collects telemetry events whose `key` field matches `key` and whose
/// `type` starts with `"prosody.timer."` until `done` returns `true`
/// for the accumulated set.
///
/// Each individual `recv` is guarded by `per_event_timeout`.
async fn collect_timer_events_for_key(
    consumer: &StreamConsumer,
    key: &str,
    per_event_timeout: Duration,
    done: impl Fn(&[Value]) -> bool,
) -> Result<Vec<Value>> {
    let mut events = Vec::new();
    loop {
        if done(&events) {
            break;
        }
        let msg = timeout(per_event_timeout, consumer.recv())
            .await
            .map_err(|_| {
                eyre!(
                    "timed out waiting for timer events for key {key:?}; collected so far: \
                     {events:?}",
                )
            })?
            .map_err(|e| eyre!("consumer error: {e}"))?;
        let Some(payload) = msg.payload() else {
            continue;
        };
        let Ok(value) = serde_json::from_slice::<Value>(payload) else {
            continue;
        };
        let matches_key = value.get("key").and_then(Value::as_str) == Some(key);
        let is_timer = value
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|t| t.starts_with("prosody.timer."));
        if matches_key && is_timer {
            events.push(value);
        }
    }
    Ok(events)
}

/// Returns the number of events whose `type` field equals `event_type`.
fn count_type(events: &[Value], event_type: &str) -> usize {
    events
        .iter()
        .filter(|e| e.get("type").and_then(Value::as_str) == Some(event_type))
        .count()
}

/// Returns `true` when `events` contains at least `n` events whose
/// `type` field equals `event_type`.
fn has_at_least(events: &[Value], event_type: &str, n: usize) -> bool {
    count_type(events, event_type) >= n
}

/// Returns `true` when `events` contains the two-event message invariant:
/// dispatched + succeeded (or dispatched + failed).
fn has_message_lifecycle(events: &[Value], expect_success: bool) -> bool {
    let has_dispatched = has_at_least(events, "prosody.message.dispatched", 1);
    let has_outcome = if expect_success {
        has_at_least(events, "prosody.message.succeeded", 1)
    } else {
        has_at_least(events, "prosody.message.failed", 1)
    };
    has_dispatched && has_outcome
}

/// Returns `true` when `events` contains the three-event invariant
/// (scheduled, dispatched, succeeded/failed) for `timer_type`.
fn has_timer_lifecycle(events: &[Value], timer_type: &str) -> bool {
    let matching: Vec<&str> = events
        .iter()
        .filter(|e| e.get("timerType").and_then(Value::as_str) == Some(timer_type))
        .filter_map(|e| e.get("type").and_then(Value::as_str))
        .collect();
    matching.contains(&"prosody.timer.scheduled")
        && matching.contains(&"prosody.timer.dispatched")
        && (matching.contains(&"prosody.timer.succeeded")
            || matching.contains(&"prosody.timer.failed"))
}

/// Collects telemetry events whose `key` matches and whose `type` starts
/// with `"prosody.message."` until `done` returns `true` for the accumulated
/// set.
///
/// Each individual `recv` is guarded by `per_event_timeout`.
async fn collect_message_events_for_key(
    consumer: &StreamConsumer,
    key: &str,
    per_event_timeout: Duration,
    done: impl Fn(&[Value]) -> bool,
) -> Result<Vec<Value>> {
    let mut events = Vec::new();
    loop {
        if done(&events) {
            break;
        }
        let msg = timeout(per_event_timeout, consumer.recv())
            .await
            .map_err(|_| {
                eyre!(
                    "timed out waiting for message events for key {key:?}; collected so far: \
                     {events:?}",
                )
            })?
            .map_err(|e| eyre!("consumer error: {e}"))?;
        let Some(payload) = msg.payload() else {
            continue;
        };
        let Ok(value) = serde_json::from_slice::<Value>(payload) else {
            continue;
        };
        let matches_key = value.get("key").and_then(Value::as_str) == Some(key);
        let is_message = value
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|t| t.starts_with("prosody.message."));
        if matches_key && is_message {
            events.push(value);
        }
    }
    Ok(events)
}

/// Asserts the three-event invariant for a timer: scheduled → dispatched →
/// succeeded/failed, all with the expected `timerType`.
///
/// Filters the event slice to only those matching `expected_timer_type` before
/// checking, so mixed-type slices (e.g. both `application` and `deferredTimer`
/// events) can each be validated independently.
fn assert_timer_three_event_invariant(
    events: &[Value],
    expected_timer_type: &str,
    expect_success: bool,
) -> Result<()> {
    let matching: Vec<&Value> = events
        .iter()
        .filter(|e| e.get("timerType").and_then(Value::as_str) == Some(expected_timer_type))
        .collect();

    let types: Vec<&str> = matching
        .iter()
        .filter_map(|e| e.get("type").and_then(Value::as_str))
        .collect();

    ensure!(
        types.contains(&"prosody.timer.scheduled"),
        "missing prosody.timer.scheduled for timerType={expected_timer_type}; got: {types:?}"
    );
    ensure!(
        types.contains(&"prosody.timer.dispatched"),
        "missing prosody.timer.dispatched for timerType={expected_timer_type}; got: {types:?}"
    );
    if expect_success {
        ensure!(
            types.contains(&"prosody.timer.succeeded"),
            "missing prosody.timer.succeeded for timerType={expected_timer_type}; got: {types:?}"
        );
    } else {
        ensure!(
            types.contains(&"prosody.timer.failed"),
            "missing prosody.timer.failed for timerType={expected_timer_type}; got: {types:?}"
        );
    }

    Ok(())
}

/// Validates the JSON field contract for a `prosody.timer.cancelled` event.
fn assert_timer_cancelled_contract(event: &Value, expected_key: &str) -> Result<()> {
    assert_eq!(
        event.get("type").and_then(Value::as_str),
        Some("prosody.timer.cancelled"),
        "cancelled type mismatch"
    );
    let event_time = event
        .get("eventTime")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("cancelled: missing eventTime"))?;
    ensure!(
        chrono::DateTime::parse_from_rfc3339(event_time).is_ok(),
        "cancelled: eventTime not RFC 3339: {event_time}"
    );
    let scheduled_time = event
        .get("scheduledTime")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("cancelled: missing scheduledTime"))?;
    ensure!(
        chrono::DateTime::parse_from_rfc3339(scheduled_time).is_ok(),
        "cancelled: scheduledTime not RFC 3339: {scheduled_time}"
    );
    ensure!(
        event
            .get("timerType")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        "cancelled: timerType should be non-empty"
    );
    assert_eq!(
        event.get("key").and_then(Value::as_str),
        Some(expected_key),
        "cancelled: key mismatch"
    );
    ensure!(
        event
            .get("source")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        "cancelled: source should be non-empty"
    );
    ensure!(
        event
            .get("hostname")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        "cancelled: hostname should be non-empty"
    );
    ensure!(
        event.get("demandType").is_none(),
        "cancelled: demandType should be absent"
    );
    ensure!(
        event.get("errorCategory").is_none(),
        "cancelled: errorCategory should be absent"
    );
    ensure!(
        event.get("exception").is_none(),
        "cancelled: exception should be absent"
    );
    Ok(())
}

/// Asserts the two-event invariant for a message: dispatched →
/// succeeded/failed.
fn assert_message_two_event_invariant(events: &[Value], expect_success: bool) -> Result<()> {
    let types: Vec<&str> = events
        .iter()
        .filter_map(|e| e.get("type").and_then(Value::as_str))
        .collect();

    ensure!(
        types.contains(&"prosody.message.dispatched"),
        "missing prosody.message.dispatched; got: {types:?}"
    );
    if expect_success {
        ensure!(
            types.contains(&"prosody.message.succeeded"),
            "missing prosody.message.succeeded; got: {types:?}"
        );
    } else {
        ensure!(
            types.contains(&"prosody.message.failed"),
            "missing prosody.message.failed; got: {types:?}"
        );
    }

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
        dedup: DeduplicationConfigurationBuilder::default(),
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
    build_typed_client_with_defer(
        source_topic,
        telemetry_topic,
        DeferConfigurationBuilder::default(),
    )
}

fn build_typed_client_with_defer<T: FallibleHandler>(
    source_topic: &str,
    telemetry_topic: &str,
    defer: DeferConfigurationBuilder,
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
        defer,
        dedup: DeduplicationConfigurationBuilder::default(),
        timeout: TimeoutConfigurationBuilder::default(),
        emitter: TelemetryEmitterConfiguration {
            topic: telemetry_topic.to_owned(),
            enabled: true,
        },
    };

    let mut cassandra_builder = CassandraConfigurationBuilder::default();
    cassandra_builder.nodes(vec![CASSANDRA_HOST.to_owned()]);

    let client = HighLevelClient::new(
        Mode::Pipeline,
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

        let events = collect_message_events_for_key(
            &telemetry_consumer,
            "test-key",
            RECEIVE_TIMEOUT,
            |evts| has_message_lifecycle(evts, true),
        )
        .await?;
        assert_message_two_event_invariant(&events, true)?;

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

        let events = collect_message_events_for_key(
            &telemetry_consumer,
            "fail-key",
            RECEIVE_TIMEOUT,
            |evts| has_message_lifecycle(evts, false),
        )
        .await?;
        assert_message_two_event_invariant(&events, false)?;

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

        let events = collect_timer_events_for_key(
            &telemetry_consumer,
            "timer-key",
            RECEIVE_TIMEOUT,
            |evts| {
                has_at_least(evts, "prosody.timer.scheduled", 1)
                    && has_at_least(evts, "prosody.timer.dispatched", 1)
                    && has_at_least(evts, "prosody.timer.succeeded", 1)
            },
        )
        .await?;
        assert_timer_three_event_invariant(&events, "application", true)?;

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

        let events = collect_timer_events_for_key(
            &telemetry_consumer,
            "timer-fail-key",
            RECEIVE_TIMEOUT,
            |evts| {
                has_at_least(evts, "prosody.timer.scheduled", 1)
                    && has_at_least(evts, "prosody.timer.dispatched", 1)
                    && has_at_least(evts, "prosody.timer.failed", 1)
            },
        )
        .await?;
        assert_timer_three_event_invariant(&events, "application", false)?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn deferred_message_timer_three_event_invariant() -> Result<()> {
    timeout(DEFER_TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let source_topic = Uuid::new_v4().to_string();
        let source: Topic = source_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &source_topic).await?;

        let mut defer = DeferConfigurationBuilder::default();
        defer.failure_threshold(1.0_f64);
        let client: HighLevelClient<TransientMessageHandler> =
            build_typed_client_with_defer(&source_topic, &telemetry_topic, defer)?;

        let (done_tx, mut done_rx) = channel(16);
        client
            .subscribe(TransientMessageHandler { done_tx })
            .await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        // Seed the FailureTracker with successes so failure_rate < 1.0 when
        // the transient failure arrives and deferral is enabled.
        for i in 0_i32..3_i32 {
            client
                .send(source, &format!("warmup-{i}"), &json!({"v": 0_i32}))
                .await?;
            let _ = timeout(RECEIVE_TIMEOUT, done_rx.recv()).await?;
        }

        client
            .send(source, "defer-msg-key", &json!({"v": 1_i32}))
            .await?;

        // Wait for the retry to succeed
        let _ = timeout(Duration::from_secs(30), done_rx.recv()).await?;

        // Collect all timer events — there must be a full scheduled → dispatched
        // → succeeded lifecycle for the deferredMessage timer.
        let events = collect_timer_events_for_key(
            &telemetry_consumer,
            "defer-msg-key",
            RECEIVE_TIMEOUT,
            |evts| {
                has_at_least(evts, "prosody.timer.scheduled", 1)
                    && has_at_least(evts, "prosody.timer.dispatched", 1)
                    && has_at_least(evts, "prosody.timer.succeeded", 1)
            },
        )
        .await?;
        assert_timer_three_event_invariant(&events, "deferredMessage", true)?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {DEFER_TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn deferred_timer_timer_three_event_invariant() -> Result<()> {
    timeout(DEFER_TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let source_topic = Uuid::new_v4().to_string();
        let source: Topic = source_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &source_topic).await?;

        let mut defer = DeferConfigurationBuilder::default();
        defer.failure_threshold(1.0_f64);
        let client: HighLevelClient<TransientTimerHandler> =
            build_typed_client_with_defer(&source_topic, &telemetry_topic, defer)?;

        let (msg_tx, mut msg_rx) = channel(16);
        let (done_tx, mut done_rx) = channel(16);
        client
            .subscribe(TransientTimerHandler { msg_tx, done_tx })
            .await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        // Seed the FailureTracker with successes so failure_rate < 1.0 when
        // the transient timer failure arrives and deferral is enabled.
        for i in 0_i32..3_i32 {
            client
                .send(source, &format!("warmup-{i}"), &json!({"v": 0_i32}))
                .await?;
            let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;
        }

        client
            .send(source, "defer-timer-key", &json!({"v": 1_i32}))
            .await?;

        // Wait for the message handler to schedule the application timer
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Wait for the deferred timer retry to succeed (~3 s application timer
        // + defer backoff)
        let _ = timeout(Duration::from_secs(30), done_rx.recv()).await?;

        // Collect timer events until both the application and deferredTimer
        // lifecycles are complete (each has scheduled + dispatched + terminal).
        let all_events = collect_timer_events_for_key(
            &telemetry_consumer,
            "defer-timer-key",
            RECEIVE_TIMEOUT,
            |evts| {
                has_timer_lifecycle(evts, "application")
                    && has_timer_lifecycle(evts, "deferredTimer")
            },
        )
        .await?;
        assert_timer_three_event_invariant(&all_events, "application", true)?;

        // The deferredTimer retry timer must also have its three events.
        assert_timer_three_event_invariant(&all_events, "deferredTimer", true)?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {DEFER_TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_cancelled_event_on_kafka() -> Result<()> {
    timeout(TIMER_TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let source_topic = Uuid::new_v4().to_string();
        let source: Topic = source_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &source_topic).await?;

        let client: HighLevelClient<TimerCancellingHandler> =
            build_typed_client(&source_topic, &telemetry_topic)?;

        let (msg_tx, mut msg_rx) = channel(16);
        client.subscribe(TimerCancellingHandler { msg_tx }).await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "cancel-key", &json!({"v": 1_i32}))
            .await?;

        // Wait for message handler to complete (schedule + cancel inside)
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        let events = collect_timer_events_for_key(
            &telemetry_consumer,
            "cancel-key",
            RECEIVE_TIMEOUT,
            |evts| {
                has_at_least(evts, "prosody.timer.scheduled", 1)
                    && has_at_least(evts, "prosody.timer.cancelled", 1)
            },
        )
        .await?;

        let types: Vec<&str> = events
            .iter()
            .filter_map(|e| e.get("type").and_then(Value::as_str))
            .collect();

        ensure!(
            types.contains(&"prosody.timer.scheduled"),
            "missing prosody.timer.scheduled; got: {types:?}"
        );
        ensure!(
            types.contains(&"prosody.timer.cancelled"),
            "missing prosody.timer.cancelled; got: {types:?}"
        );
        ensure!(
            !types.contains(&"prosody.timer.dispatched"),
            "prosody.timer.dispatched should NOT be present (timer was cancelled); got: {types:?}"
        );

        // Validate field contract on the cancelled event
        let cancelled = events
            .iter()
            .find(|e| e.get("type").and_then(Value::as_str) == Some("prosody.timer.cancelled"))
            .ok_or_else(|| eyre!("cancelled event not found"))?;
        assert_timer_cancelled_contract(cancelled, "cancel-key")?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn clear_and_schedule_emits_cancelled_and_scheduled() -> Result<()> {
    timeout(TIMER_TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let source_topic = Uuid::new_v4().to_string();
        let source: Topic = source_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &source_topic).await?;

        let client: HighLevelClient<ClearAndScheduleHandler> =
            build_typed_client(&source_topic, &telemetry_topic)?;

        let (msg_tx, mut msg_rx) = channel(16);
        client.subscribe(ClearAndScheduleHandler { msg_tx }).await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        // First message: schedule a timer at t+60
        client
            .send(source, "cas-key", &json!({"step": 1_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Second message (same key): clear_and_schedule at t+120
        client
            .send(source, "cas-key", &json!({"step": 2_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        let events =
            collect_timer_events_for_key(&telemetry_consumer, "cas-key", RECEIVE_TIMEOUT, |evts| {
                has_at_least(evts, "prosody.timer.scheduled", 2)
                    && has_at_least(evts, "prosody.timer.cancelled", 1)
            })
            .await?;

        let types: Vec<&str> = events
            .iter()
            .filter_map(|e| e.get("type").and_then(Value::as_str))
            .collect();

        // Should have: scheduled (first), cancelled (from clear), scheduled (new)
        let scheduled_count = types
            .iter()
            .filter(|&&t| t == "prosody.timer.scheduled")
            .count();
        let cancelled_count = types
            .iter()
            .filter(|&&t| t == "prosody.timer.cancelled")
            .count();

        ensure!(
            scheduled_count >= 2,
            "expected at least 2 scheduled events (original + new); got {scheduled_count}; types: \
             {types:?}"
        );
        ensure!(
            cancelled_count >= 1,
            "expected at least 1 cancelled event (old timer cleared); got {cancelled_count}; \
             types: {types:?}"
        );

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}

/// Verifies the Inline→Inline tombstone-free timer replacement path:
/// `schedule` followed by `clear_and_schedule` on the same key results in
/// exactly one timer firing at the replacement time.
#[tokio::test(flavor = "multi_thread")]
async fn inline_replacement_fires_once_at_replacement_time() -> Result<()> {
    timeout(TIMER_TEST_TIMEOUT, async {
        init_test_logging();

        let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
        let telemetry_topic = Uuid::new_v4().to_string();
        let source_topic = Uuid::new_v4().to_string();
        let source: Topic = source_topic.as_str().into();

        create_topic(admin, &telemetry_topic).await?;
        create_topic(admin, &source_topic).await?;

        let client: HighLevelClient<InlineReplacementHandler> =
            build_typed_client(&source_topic, &telemetry_topic)?;

        let (messages, mut msg_rx) = channel(16);
        let (replacement_time, mut replacement_time_rx) = channel(16);
        let (timer_fired, mut timer_rx) = channel(16);
        client
            .subscribe(InlineReplacementHandler {
                messages,
                replacement_time,
                timer_fired,
            })
            .await?;

        // Step 1: schedule at t+3s
        client
            .send(source, "replace-key", &json!({"step": 1_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Step 2: clear_and_schedule at t+5s (replaces the original)
        client
            .send(source, "replace-key", &json!({"step": 2_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Capture the replacement time the handler recorded
        let replacement_time = timeout(RECEIVE_TIMEOUT, replacement_time_rx.recv())
            .await
            .map_err(|_| eyre!("timeout waiting for replacement time"))?
            .ok_or_else(|| eyre!("replacement_time channel closed"))?;

        // Wait for the timer to fire (should be ~5s from step 2)
        let trigger_time = timeout(Duration::from_secs(15), timer_rx.recv())
            .await
            .map_err(|_| eyre!("timeout waiting for on_timer — timer never fired"))?
            .ok_or_else(|| eyre!("timer_fired channel closed"))?;

        // The timer must fire at the replacement time, not the original
        ensure!(
            trigger_time == replacement_time,
            "timer fired at {trigger_time:?} but expected replacement time {replacement_time:?}"
        );

        // Verify no second timer fires (the original t+3s was replaced)
        let second = timeout(Duration::from_secs(5), timer_rx.recv()).await;
        ensure!(second.is_err(), "expected no second timer but received one");

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}
