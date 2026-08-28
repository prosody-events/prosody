#![recursion_limit = "512"]
//! Integration tests for telemetry event emission via Kafka.
//!
//! Validates that telemetry events (message lifecycle, producer message sent)
//! are serialized to JSON and produced to a dedicated Kafka telemetry topic.

use color_eyre::eyre::{Result, ensure, eyre};
use prosody::JsonCodec;
use prosody::Topic;
use prosody::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use prosody::cassandra::config::CassandraConfigurationBuilder;
use prosody::codec::UnitCodec;
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::ConsumerMessage;
use prosody::consumer::middleware::FallibleHandler;
use prosody::consumer::middleware::defer::DeferConfigurationBuilder;
use prosody::consumer::{ConsumerConfigurationBuilder, DemandType, Keyed};
use prosody::high_level::mode::Mode;
use prosody::high_level::{CassandraHighLevelClient, ClientHandler, Codecs, ConsumerBuilders};
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
use std::future::{Future, ready};
use std::time::Duration;
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::{Instant, timeout};
use uuid::Uuid;

mod common;
use common::handler::{FallibleTestHandler, TestError, TransientError};

const BOOTSTRAP: &str = "localhost:9094";
const CASSANDRA_HOST: &str = "localhost:9042";
/// Hang-guard for an individual wait on an event that *must* arrive (a message
/// handler completing, a timer firing, a telemetry record landing). These tests
/// assert on event *content* — scheduled times, lifecycle completeness — which
/// is deterministic; the wait itself only guards against a genuine hang, so it
/// is sized generously. A slow or degraded cluster (e.g. late in a long suite)
/// must never trip it. Kept comfortably below the per-test deadlines so a hung
/// step surfaces its own granular error before the outer deadline fires.
const RECEIVE_TIMEOUT: Duration = Duration::from_mins(1);
/// Top-level deadline for a single non-timer integration test.
const TEST_TIMEOUT: Duration = Duration::from_mins(2);
/// Deadline for tests that involve timer scheduling (a few-second timer delay
/// plus consumer startup and telemetry drain).
const TIMER_TEST_TIMEOUT: Duration = Duration::from_mins(3);
/// Deadline for defer tests: warm-up + timer + retry backoff + telemetry drain.
const DEFER_TEST_TIMEOUT: Duration = Duration::from_mins(5);

macro_rules! ignore_excise {
    () => {
        fn on_excise<C>(
            &self,
            _: C,
            _: ConsumerMessage<()>,
            _: DemandType,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send
        where
            C: EventContext<Payload = Value>,
        {
            ready(Ok(()))
        }
    };
}

// ── Test Handlers ────────────────────────────────────────────────────────────
//
// The plain forward-to-channel handler lives in `common`
// ([`FallibleTestHandler`]); only specialized handlers (error injection,
// timer scheduling) are defined here.

/// Handler that always fails on messages.
#[derive(Clone)]
struct FailingHandler {
    tx: Sender<String>,
}

impl FallibleHandler for FailingHandler {
    type Error = TestError;
    type Output = ();
    type Payload = Value;

    ignore_excise!();

    async fn on_message<C>(
        &self,
        _ctx: C,
        msg: ConsumerMessage<Value>,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let _ = self.tx.send(msg.key().to_string()).await;
        Err(TestError)
    }

    fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
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
    type Output = ();
    type Payload = Value;

    ignore_excise!();

    async fn on_message<C>(
        &self,
        ctx: C,
        msg: ConsumerMessage<Value>,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
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
        C: EventContext<Payload = Self::Payload>,
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
    type Output = ();
    type Payload = Value;

    ignore_excise!();

    async fn on_message<C>(
        &self,
        ctx: C,
        msg: ConsumerMessage<Value>,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
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
        C: EventContext<Payload = Self::Payload>,
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
    type Output = ();
    type Payload = Value;

    ignore_excise!();

    async fn on_message<C>(
        &self,
        ctx: C,
        msg: ConsumerMessage<Value>,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
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

    fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
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
    type Output = ();
    type Payload = Value;

    ignore_excise!();

    async fn on_message<C>(
        &self,
        ctx: C,
        msg: ConsumerMessage<Value>,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
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

    fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
    }

    async fn shutdown(self) {}
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
    type Output = ();
    type Payload = Value;

    ignore_excise!();

    async fn on_message<C>(
        &self,
        _ctx: C,
        msg: ConsumerMessage<Value>,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
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

    fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
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
    type Output = ();
    type Payload = Value;

    ignore_excise!();

    async fn on_message<C>(
        &self,
        ctx: C,
        msg: ConsumerMessage<Value>,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
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
        C: EventContext<Payload = Self::Payload>,
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

macro_rules! impl_client_handlers {
    ($codec:ty => $($handler:ty),+ $(,)?) => {
        $(
            impl ClientHandler for $handler {
                type Codecs = Codecs<JsonCodec, UnitCodec>;
            }
        )+
    };
}

impl_client_handlers!(
    TestError =>
        FailingHandler,
        TimerSchedulingHandler,
        TimerFailingHandler,
        TimerCancellingHandler,
        ClearAndScheduleHandler,
);
impl_client_handlers!(TransientError => TransientMessageHandler, TransientTimerHandler);

// ── Helpers ──────────────────────────────────────────────────────────────────

#[path = "telemetry/support.rs"]
mod support;
use support::*;
#[path = "telemetry/message_events.rs"]
mod message_events;
