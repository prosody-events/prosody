//! Event handlers the integration suite shares, and the two error types their
//! fallible variants classify with.
//!
//! Specialized handlers — error injection, timer scheduling, context capture —
//! stay local to the test file that needs them.

use prosody::JsonCodec;
use prosody::codec::UnitCodec;
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::{ConsumerMessage, UncommittedMessage};
use prosody::consumer::middleware::FallibleHandler;
use prosody::consumer::{DemandType, EventHandler, Keyed, Uncommitted};
use prosody::error::{ClassifyError, ErrorCategory};
use prosody::high_level::{ClientHandler, Codecs};
use prosody::timers::{Trigger, UncommittedTimer};
use serde_json::Value;
use std::time::Duration as StdDuration;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tracing::{error, info};

/// The generic forward-to-channel [`EventHandler`]: sends every received
/// `(key, payload)` pair to a channel and commits.
///
/// An optional per-message `delay` simulates backpressure — the suite's only
/// sanctioned use of `sleep`.
#[derive(Clone, Debug)]
pub(crate) struct ChannelHandler {
    /// A channel for transmitting received messages.
    messages_tx: Sender<(String, Value)>,

    /// Per-message processing delay (backpressure simulation); zero for none.
    delay: StdDuration,
}

impl ChannelHandler {
    /// A handler that forwards immediately.
    #[must_use]
    pub(crate) fn new(messages_tx: Sender<(String, Value)>) -> Self {
        Self::with_delay(messages_tx, StdDuration::ZERO)
    }

    /// A handler that sleeps `delay` before forwarding, simulating a slow
    /// consumer for backpressure tests.
    #[must_use]
    pub(crate) fn with_delay(messages_tx: Sender<(String, Value)>, delay: StdDuration) -> Self {
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
            sleep(self.delay).await;
        }

        if let Err(error) = self
            .messages_tx
            .send((msg.key().to_string(), msg.payload().clone()))
            .await
        {
            error!("failed to send message: {error:#}");
        }

        uncommitted.commit().await;
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

/// A handler error that classifies [`ErrorCategory::Permanent`].
#[derive(Clone, Debug, Default, Error)]
#[error("test error")]
pub(crate) struct TestError;

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

/// A handler error that classifies [`ErrorCategory::Transient`].
#[derive(Clone, Debug, Default, Error)]
#[error("transient test error")]
pub(crate) struct TransientError;

impl ClassifyError for TransientError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Transient
    }
}

/// A [`FallibleHandler`] that forwards every message to a channel.
///
/// A leaf handler: it relies on the default no-op `after_commit` /
/// `after_abort` apply hooks. The framework still guarantees exactly one of
/// those fires per `on_message` / `on_timer` call, but with `Output = ()` and
/// no staged state, this handler has nothing to do in either.
#[derive(Clone, Debug)]
pub(crate) struct FallibleTestHandler {
    /// Channel for transmitting received messages.
    pub(crate) messages_tx: Sender<(String, Value)>,
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
        // Send errors are irrelevant here: the receiver may already be gone.
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

    async fn shutdown(self) {}
}

impl ClientHandler for FallibleTestHandler {
    type Codecs = Codecs<JsonCodec, UnitCodec>;
}
