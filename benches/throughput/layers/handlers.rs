//! The two benchmark handlers: one for the direct consumer, one for the
//! high-level client. Both record each message and do no other work, except
//! an optional sleep that simulates handler I/O.

use crate::progress::Progress;
use prosody::JsonCodec;
use prosody::codec::UnitCodec;
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::{ConsumerMessage, UncommittedMessage};
use prosody::consumer::middleware::FallibleHandler;
use prosody::consumer::{DemandType, EventHandler, Uncommitted};
use prosody::high_level::{ClientHandler, Codecs};
use prosody::timers::{Trigger, UncommittedTimer};
use serde_json::Value;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::sleep;

/// An `EventHandler` for `ProsodyConsumer::new`. It commits each message.
///
/// The direct consumer has no scheduler middleware. A semaphore of the same
/// size bounds its handler concurrency, so the two layers compare fairly.
#[derive(Clone)]
pub(super) struct DirectHandler {
    progress: Arc<Progress>,
    delay: Duration,
    concurrency: Arc<Semaphore>,
}

/// A `FallibleHandler` for the high-level client. It never fails.
#[derive(Clone)]
pub(super) struct BenchHandler {
    progress: Arc<Progress>,
    delay: Duration,
}

impl DirectHandler {
    pub(super) fn new(progress: Arc<Progress>, delay: Duration, max_concurrency: usize) -> Self {
        Self {
            progress,
            delay,
            concurrency: Arc::new(Semaphore::new(max_concurrency)),
        }
    }
}

impl EventHandler for DirectHandler {
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage<Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        // The semaphore is never closed, so the acquire cannot fail.
        if let Ok(_permit) = self.concurrency.acquire().await {
            simulate_io(self.delay).await;
        }
        self.progress.record();
        message.commit().await;
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
        timer.commit().await;
    }

    async fn shutdown(self) {}
}

impl BenchHandler {
    pub(super) fn new(progress: Arc<Progress>, delay: Duration) -> Self {
        Self { progress, delay }
    }
}

impl FallibleHandler for BenchHandler {
    type Error = Infallible;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Value>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        simulate_io(self.delay).await;
        self.progress.record();
        Ok(())
    }

    fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
    }

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

    async fn shutdown(self) {}
}

impl ClientHandler for BenchHandler {
    type Codecs = Codecs<JsonCodec, UnitCodec>;
}

/// Sleeps for `delay` to simulate handler I/O. This is backpressure
/// simulation, the one sanctioned use of `sleep`.
async fn simulate_io(delay: Duration) {
    if !delay.is_zero() {
        sleep(delay).await;
    }
}
