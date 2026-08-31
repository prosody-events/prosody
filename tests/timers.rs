//! Integration tests for timer functionality in the Prosody system.
//!
//! This module tests the timer scheduling, triggering, and cancellation
//! capabilities of Prosody consumers. It verifies that timers can be set
//! from message handlers, triggered at the correct times, and properly
//! canceled when needed.

#![recursion_limit = "512"]

use ahash::HashSet;
use color_eyre::eyre::{Result, ensure, eyre};
use prosody::cassandra::config::CassandraConfigurationBuilder;
use prosody::codec::UnitCodec;
use prosody::consumer::event_context::EventContext;
use prosody::high_level::mode::Mode;
use prosody::high_level::{CassandraHighLevelClient, ClientHandler, Codecs, ConsumerBuilders};
use prosody::producer::ProducerConfigurationBuilder;
use prosody::telemetry::TelemetryEmitterConfiguration;
use prosody::tracing::init_test_logging;
use prosody::{
    JsonCodec,
    consumer::ConsumerConfigurationBuilder,
    consumer::message::{ConsumerMessage, UncommittedMessage},
    consumer::middleware::{CloneProvider, FallibleHandler},
    consumer::{DemandType, EventHandler, Keyed, Uncommitted},
    timers::TimerType,
    timers::Trigger,
    timers::UncommittedTimer,
    timers::datetime::CompactDateTime,
    timers::duration::CompactDuration,
};
use serde_json::{Value, json};
use std::future::{Future, ready};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::timeout;
use tracing::info;
use uuid::Uuid;

mod common;
use common::handler::TestError;
use common::kafka::ConsumerEnv;

/// Hang-guard for an individual wait on an event that *must* arrive (a message
/// reaching the handler, a timer firing). These tests assert on event
/// *content* — keys, payloads, scheduled times — which is deterministic; the
/// wait itself only guards against a genuine hang, so it is sized generously.
/// A slow or degraded cluster must never trip it.
const RECEIVE_TIMEOUT: Duration = Duration::from_mins(1);

/// Generous outer hang-guard: bounds total test runtime while staying well
/// above the per-event waits inside the harness, so a genuinely hung step
/// surfaces its own granular error before this fires. Sized so a slow or
/// degraded cluster never trips it on mere slowness.
const TIMER_TEST_TIMEOUT: Duration = Duration::from_mins(3);

/// Test handler that schedules timers based on incoming messages and tracks
/// timer events.
#[derive(Clone)]
struct TimerTestHandler {
    /// Channel for sending timer events to the test
    timer_tx: Sender<TimerEvent>,
    /// Channel for sending message events to the test
    message_tx: Sender<MessageEvent>,
}

/// Represents a timer event for test verification
#[derive(Debug, Clone, PartialEq)]
struct TimerEvent {
    key: String,
    time: CompactDateTime,
}

/// Represents a message event for test verification
#[derive(Debug, Clone, PartialEq)]
struct MessageEvent {
    key: String,
    payload: Value,
}

impl EventHandler for TimerTestHandler {
    type Payload = Value;

    async fn on_message<C>(
        &self,
        context: C,
        message: UncommittedMessage<Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        let (msg, uncommitted) = message.into_inner();
        let key = msg.key().to_string();
        let payload = msg.payload().clone();

        // Send message event for verification
        if let Err(e) = self
            .message_tx
            .send(MessageEvent {
                key: key.clone(),
                payload: payload.clone(),
            })
            .await
        {
            tracing::error!("Failed to send message event: {e}");
            uncommitted.commit().await;
            return;
        }

        // Handle different message types
        if let Some(action) = payload.get("action").and_then(|v| v.as_str()) {
            match action {
                "schedule_timer" => {
                    // Support both absolute time and delay-based scheduling
                    if let Some(target_time_secs) =
                        payload.get("target_time_secs").and_then(Value::as_u64)
                    {
                        // Absolute time scheduling
                        let schedule_time = CompactDateTime::from(target_time_secs as u32);
                        if let Err(e) = context
                            .schedule(schedule_time, TimerType::Application)
                            .await
                        {
                            tracing::error!("Failed to schedule timer for key {key}: {e}");
                        } else {
                            info!("Scheduled timer for key {key} at time {schedule_time}");
                        }
                    } else if let Some(delay_secs) =
                        payload.get("delay_secs").and_then(Value::as_u64)
                    {
                        // Delay-based scheduling
                        let delay = CompactDuration::new(delay_secs as u32);
                        match CompactDateTime::now().and_then(|now| now.add_duration(delay)) {
                            Ok(schedule_time) => {
                                if let Err(e) = context
                                    .schedule(schedule_time, TimerType::Application)
                                    .await
                                {
                                    tracing::error!("Failed to schedule timer for key {key}: {e}");
                                } else {
                                    info!("Scheduled timer for key {key} at time {schedule_time}");
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to calculate schedule time: {e}");
                            }
                        }
                    }
                }
                "cancel_timer" => {
                    // Clear all scheduled timers for this key
                    if let Err(e) = context.clear_scheduled(TimerType::Application).await {
                        tracing::error!("Failed to cancel timers for key {key}: {e}");
                    } else {
                        info!("Canceled all timers for key {key}");
                    }
                }
                _ => {
                    info!("Received message with unknown action: {action}");
                }
            }
        }

        uncommitted.commit().await;
    }

    async fn on_excise<C>(
        &self,
        _context: C,
        message: UncommittedMessage<()>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        message.commit().await;
    }

    async fn on_timer<C, U>(&self, _context: C, timer: U, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        U: UncommittedTimer,
    {
        let key = timer.key().to_string();
        let time = timer.time();

        info!("Timer triggered for key {key} at time {time}");

        // Send timer event for verification
        let timer_event = TimerEvent {
            key: key.clone(),
            time,
        };
        if let Err(e) = self.timer_tx.send(timer_event).await {
            tracing::error!("Failed to send timer event: {e}");
        }

        timer.commit().await;
    }

    async fn shutdown(self) {
        info!("TimerTestHandler shutdown");
    }
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
    type Output = ();
    type Payload = Value;

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
        C: EventContext<Payload = Self::Payload>,
    {
        let _ = self.timer_fired.send(trigger.time).await;
        Ok(())
    }

    async fn shutdown(self) {}

    fn on_excise<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<()>,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
    }
}

impl ClientHandler for InlineReplacementHandler {
    type Codecs = Codecs<JsonCodec, UnitCodec>;
}

/// Test environment wrapping [`ConsumerEnv`] with the timer handler's event
/// channels and timer-specific assertion helpers.
struct TestEnvironment {
    env: ConsumerEnv,
    timer_rx: Receiver<TimerEvent>,
    message_rx: Receiver<MessageEvent>,
}

impl TestEnvironment {
    /// Create a new test environment with all necessary components
    async fn new(test_name: &str) -> Result<Self> {
        // Set up channels for test events
        let (timer_tx, timer_rx) = channel(50);
        let (message_tx, message_rx) = channel(50);

        let handler = TimerTestHandler {
            timer_tx,
            message_tx,
        };
        let env =
            ConsumerEnv::new(test_name, async move |_| Ok(CloneProvider::new(handler))).await?;

        Ok(Self {
            env,
            timer_rx,
            message_rx,
        })
    }

    /// Send a message with the given key and payload
    async fn send_message(&self, key: &str, payload: Value) -> Result<()> {
        self.env.send_message(key, payload).await
    }

    /// Send a timer scheduling message
    async fn schedule_timer(&self, key: &str, delay_secs: u32) -> Result<()> {
        let message = json!({
            "action": "schedule_timer",
            "delay_secs": delay_secs
        });
        self.send_message(key, message).await
    }

    /// Send a timer scheduling message with absolute time
    async fn schedule_timer_at(&self, key: &str, target_time_secs: u64) -> Result<()> {
        let message = json!({
            "action": "schedule_timer",
            "target_time_secs": target_time_secs
        });
        self.send_message(key, message).await
    }

    /// Send a timer cancellation message
    async fn cancel_timer(&self, key: &str) -> Result<()> {
        let message = json!({
            "action": "cancel_timer"
        });
        self.send_message(key, message).await
    }

    /// Wait for a message event under the receive hang-guard
    async fn expect_message(&mut self) -> Result<MessageEvent> {
        common::receive::expect_event(&mut self.message_rx, RECEIVE_TIMEOUT).await
    }

    /// Wait for a timer event under the receive hang-guard
    async fn expect_timer(&mut self) -> Result<TimerEvent> {
        common::receive::expect_event(&mut self.timer_rx, RECEIVE_TIMEOUT).await
    }

    /// Wait for exactly `count` timer events, then verify no extras arrive
    async fn expect_timers(&mut self, count: usize) -> Result<Vec<TimerEvent>> {
        let mut received_timers = Vec::with_capacity(count);

        for i in 0..count {
            let timer_event = common::receive::expect_event(&mut self.timer_rx, RECEIVE_TIMEOUT)
                .await
                .map_err(|e| eyre!("waiting for timer {} of {}: {e}", i + 1, count))?;
            received_timers.push(timer_event);
        }

        // Verify no extra timers are received
        if let Ok(Some(extra_timer)) =
            timeout(Duration::from_millis(100), self.timer_rx.recv()).await
        {
            return Err(eyre!(
                "Received unexpected extra timer for key '{}'",
                extra_timer.key
            ));
        }

        Ok(received_timers)
    }

    /// Verify that no timer event occurs within the given window
    async fn expect_no_timer(&mut self, window_secs: u32) -> Result<()> {
        common::receive::expect_no_event(
            &mut self.timer_rx,
            Duration::from_secs(u64::from(window_secs)),
        )
        .await
    }

    /// Verify a message event matches expected key and payload
    fn verify_message_event(
        event: &MessageEvent,
        expected_key: &str,
        expected_payload: &Value,
    ) -> Result<()> {
        ensure!(
            event.key == expected_key,
            "Message key mismatch: expected '{}', got '{}'",
            expected_key,
            event.key
        );
        ensure!(
            event.payload == *expected_payload,
            "Message payload mismatch: expected {:?}, got {:?}",
            expected_payload,
            event.payload
        );
        Ok(())
    }

    /// Verify a timer event matches expected key
    fn verify_timer_event(event: &TimerEvent, expected_key: &str) -> Result<()> {
        ensure!(
            event.key == expected_key,
            "Timer key mismatch: expected '{}', got '{}'",
            expected_key,
            event.key
        );
        info!(
            "Timer triggered for key '{}' at time {}",
            event.key, event.time
        );
        Ok(())
    }

    /// Verify timers are in chronological order
    fn verify_timer_order(timers: &[TimerEvent]) -> Result<()> {
        for i in 1..timers.len() {
            ensure!(
                timers[i - 1].time.epoch_seconds() <= timers[i].time.epoch_seconds(),
                "Timers not in chronological order: timer {} at {} came after timer {} at {}",
                timers[i - 1].key,
                timers[i - 1].time,
                timers[i].key,
                timers[i].time
            );
        }
        Ok(())
    }

    /// Verify timers contain exactly the expected keys
    fn verify_timer_keys(timers: &[TimerEvent], expected_keys: &HashSet<String>) -> Result<()> {
        let actual_keys: HashSet<String> = timers.iter().map(|t| t.key.clone()).collect();
        ensure!(
            actual_keys == *expected_keys,
            "Timer keys mismatch: expected {:?}, got {:?}",
            expected_keys,
            actual_keys
        );
        Ok(())
    }

    /// Clean up resources (consumer shutdown, topic deletion)
    async fn cleanup(self) {
        self.env.shutdown().await;
    }
}

/// Run a test with timeout and proper error handling
#[path = "timers/timer_cases.rs"]
mod timer_cases;
