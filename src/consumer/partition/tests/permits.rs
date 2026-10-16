//! Tests for the handler permits that bound the handlers that run at once.

use super::runtime::create_test_message;
use super::{default_config, start};
use crate::Key;
use crate::consumer::message::{ConsumerRecord, UncommittedMessage};
use crate::consumer::partition::ShutdownPhase;
use crate::consumer::{DemandType, EventContext, EventHandler, Keyed, Uncommitted};
use crate::timers::UncommittedTimer;
use color_eyre::eyre::{Result, bail, ensure};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};
use tokio::time::timeout;

/// The hang-guard for each handler start.
const DEADLINE: Duration = Duration::from_secs(5);

/// A handler that waits for the test to open its gate, and records the keys it
/// starts and the most handlers that ran at once.
#[derive(Clone)]
struct GateHandler {
    gate: Arc<Semaphore>,
    started: Arc<Mutex<Vec<Key>>>,
    start: Arc<Notify>,
    running: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl GateHandler {
    /// Creates a handler whose gate starts closed.
    fn new() -> Self {
        Self {
            gate: Arc::new(Semaphore::new(0)),
            started: Arc::default(),
            start: Arc::default(),
            running: Arc::default(),
            peak: Arc::default(),
        }
    }

    /// Waits until `count` handlers have started.
    async fn wait_started(&self, count: usize) -> Result<()> {
        while self.started.lock().len() < count {
            if timeout(DEADLINE, self.start.notified()).await.is_err() {
                bail!("{} of {count} handlers started", self.started.lock().len());
            }
        }
        Ok(())
    }
}

impl EventHandler for GateHandler {
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        let running = self.running.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak.fetch_max(running, Ordering::AcqRel);
        self.started.lock().push(message.key().clone());
        self.start.notify_one();

        if let Ok(permit) = self.gate.acquire().await {
            permit.forget();
        }
        self.running.fetch_sub(1, Ordering::AcqRel);
        message.commit().await;
    }

    async fn on_excise<C>(&self, _: C, _: UncommittedMessage<()>, _: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
    {
    }

    async fn on_timer<C, U>(&self, _: C, _: U, _: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        U: UncommittedTimer,
    {
    }

    async fn shutdown(self) {}
}

/// Handlers that run at once never exceed the handler permits, and an event
/// that waits behind a busy key holds no permit. With two permits, key `b`
/// starts while key `a` holds back two events.
///
/// Falsify: remove the permit in `run_partition`, and three handlers run at
/// once. Or take the permit before the key queue, and `b` never starts.
#[tokio::test]
async fn handler_permits_skip_events_behind_busy_keys() -> Result<()> {
    let handler = GateHandler::new();
    let mut config = default_config();
    config.handler_permits = Arc::new(Semaphore::new(2));
    let (partition_manager, records) = start(config, handler.clone());

    for (offset, key) in [(0, "a"), (1, "a"), (2, "a"), (3, "b"), (4, "c")] {
        let record = ConsumerRecord::Message(create_test_message(offset, key)?);
        ensure!(
            records.try_send(record).is_ok(),
            "the record channel is full"
        );
    }

    // Key `c` waits for a permit while `a` and `b` run.
    handler.wait_started(2).await?;
    let first = handler.started.lock().clone();
    ensure!(
        first == [Key::from("a"), Key::from("b")],
        "first starts: {first:?}"
    );

    handler.gate.add_permits(5);
    handler.wait_started(5).await?;
    partition_manager.shutdown().await;

    let peak = handler.peak.load(Ordering::Acquire);
    ensure!(peak <= 2, "{peak} handlers ran at once");
    Ok(())
}

/// An event that still waits for a handler permit when its partition drains
/// never starts.
///
/// Falsify: remove the drain arm of the permit wait in `run_partition`. Key
/// `b` then starts when `a` frees its permit.
#[tokio::test]
async fn drain_stops_events_that_wait_for_a_permit() -> Result<()> {
    let handler = GateHandler::new();
    let mut config = default_config();
    config.handler_permits = Arc::new(Semaphore::new(1));
    let (partition_manager, records) = start(config, handler.clone());

    for (offset, key) in [(0, "a"), (1, "b")] {
        let record = ConsumerRecord::Message(create_test_message(offset, key)?);
        ensure!(
            records.try_send(record).is_ok(),
            "the record channel is full"
        );
    }
    handler.wait_started(1).await?;

    ensure!(
        partition_manager
            .shutdown_tx
            .send(ShutdownPhase::Draining)
            .is_ok()
    );
    handler.gate.add_permits(2);
    partition_manager.shutdown().await;

    let started = handler.started.lock().clone();
    ensure!(started == [Key::from("a")], "started: {started:?}");
    Ok(())
}
