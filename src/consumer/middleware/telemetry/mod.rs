//! Handler lifecycle telemetry middleware.
//!
//! Records handler invocation events for observability and monitoring. Captures
//! metrics like execution time, success/failure rates, and error
//! classifications without affecting the processing flow.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. **Record handler invocation** - Log start of processing
//! 2. Pass control to inner middleware layers
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. **Record handler completion** - Log success/failure and timing of the
//!    handler invocation as observed at this layer
//! 3. Pass original result through unchanged
//!
//! Note: the `Succeeded`/`Failed` events recorded here reflect the outcome of
//! the wrapped `on_message` / `on_timer` invocation as it returns to this
//! layer. They are work-level signals only; they do NOT indicate whether the
//! framework will treat the dispatch as final (commit) or non-final (retry of
//! the same logical event). The framework's `FallibleHandler` invariant
//! guarantees that for every `on_message`/`on_timer` call that runs and
//! returns, exactly one of `after_commit` or `after_abort` will subsequently
//! fire on the same handler instance — `after_commit` marking the dispatch as
//! final, `after_abort` marking it as non-final with a retry to come.
//! `TelemetryHandler` forwards both apply hooks verbatim to the inner handler
//! and does not currently emit dedicated events distinguishing the two. The
//! inner is invoked at most once per call; per-invocation invariant trivially
//! upheld.
//!
//! # Telemetry Events
//!
//! - **Handler Invoked**: When processing begins
//! - **Handler Succeeded**: When the handler invocation returns `Ok` at this
//!   layer (work-level outcome; not a commit/abort signal)
//! - **Handler Failed**: When the handler invocation returns `Err` at this
//!   layer, with error category (work-level outcome; not a commit/abort signal)
//! - **Execution Time**: Duration of processing
//! - **Partition Context**: Which topic-partition was processed
//!
//! # Usage
//!
//! Position for visibility across the whole pipeline — typically just inside
//! the scheduler, wrapping cancellation and retry. See the
//! [module docs](crate::consumer::middleware) for a worked composition example.

use tracing::debug;

use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    FallibleHandler, FallibleHandlerProvider, HandlerMiddleware, Settlement, SettlementHandler,
};
use crate::error::ClassifyError;
use crate::telemetry::event::TimerEventType;
use crate::telemetry::{Telemetry, partition::TelemetryPartitionSender};
use crate::timers::Trigger;
use crate::{Partition, Topic};
use std::sync::Arc;

/// Middleware that records telemetry events during message processing.
#[derive(Clone, Debug)]
pub struct TelemetryMiddleware {
    telemetry: Telemetry,
    source: Arc<str>,
}

/// A provider that records telemetry events during message processing.
#[derive(Clone, Debug)]
pub struct TelemetryProvider<T> {
    provider: T,
    telemetry: Telemetry,
    source: Arc<str>,
}

/// A handler that records telemetry events during message processing.
///
/// Wraps another handler and adds telemetry recording capabilities while
/// preserving the original processing behavior and error handling.
#[derive(Clone, Debug)]
pub struct TelemetryHandler<T> {
    handler: T,
    sender: TelemetryPartitionSender,
    source: Arc<str>,
}

impl TelemetryMiddleware {
    /// Creates a new `TelemetryMiddleware` from the telemetry system and the
    /// consumer `group_id` used as the source identifier in emitted events.
    #[must_use]
    pub fn new(telemetry: Telemetry, source: Arc<str>) -> Self {
        Self { telemetry, source }
    }
}

impl<P: Send + Sync + 'static> HandlerMiddleware<P> for TelemetryMiddleware {
    type Provider<T>
        = TelemetryProvider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>,
    {
        TelemetryProvider {
            provider,
            telemetry: self.telemetry.clone(),
            source: self.source.clone(),
        }
    }
}

impl<T> FallibleHandlerProvider for TelemetryProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = TelemetryHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        let partition_sender = self.telemetry.partition_sender(topic, partition);
        TelemetryHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            sender: partition_sender,
            source: self.source.clone(),
        }
    }
}

impl<T> FallibleHandler for TelemetryHandler<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;
    type Output = T::Output;
    type Payload = T::Payload;

    /// Processes a message and records telemetry events for handler lifecycle,
    /// passing through the wrapped handler's result (and error) unchanged.
    ///
    /// Records the following events:
    /// - `HandlerInvoked` when the handler is called
    /// - `HandlerSucceeded` when the handler completes successfully
    /// - `HandlerFailed` when the handler returns an error
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let key = message.key().clone();
        let offset = message.offset();

        // Record handler invocation
        self.sender.handler_invoked(key.clone(), demand_type);

        // Emit message dispatched
        self.sender
            .message_dispatched(key.clone(), offset, demand_type, self.source.clone());

        // Process the message with the wrapped handler
        let result = self.handler.on_message(context, message, demand_type).await;

        // Record success or failure
        match &result {
            Ok(_) => {
                self.sender.handler_succeeded(key.clone(), demand_type);
                self.sender
                    .message_succeeded(key, offset, demand_type, self.source.clone());
            }
            Err(e) => {
                self.sender.handler_failed(key.clone(), demand_type);
                self.sender.message_failed(
                    key,
                    offset,
                    demand_type,
                    self.source.clone(),
                    e.classify_error(),
                    format!("{e:?}").into_boxed_str(),
                );
            }
        }

        result
    }

    /// Processes a timer and records telemetry events for handler lifecycle,
    /// passing through the wrapped handler's result (and error) unchanged.
    ///
    /// Records the following events:
    /// - `HandlerInvoked` when the handler is called
    /// - `HandlerSucceeded` when the handler completes successfully
    /// - `HandlerFailed` when the handler returns an error
    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let key = trigger.key.clone();
        let scheduled_time = trigger.time;
        let timer_type = trigger.timer_type;

        // Record handler invocation
        self.sender.handler_invoked(key.clone(), demand_type);

        // Emit timer dispatched
        self.sender.timer_dispatched(
            key.clone(),
            scheduled_time,
            timer_type,
            demand_type,
            self.source.clone(),
        );

        // Process the timer with the wrapped handler
        let result = self.handler.on_timer(context, trigger, demand_type).await;

        // Record success or failure
        match &result {
            Ok(_) => {
                self.sender.handler_succeeded(key.clone(), demand_type);
                self.sender.timer_succeeded(
                    key,
                    scheduled_time,
                    timer_type,
                    demand_type,
                    self.source.clone(),
                );
            }
            Err(e) => {
                self.sender.handler_failed(key.clone(), demand_type);
                self.sender.emit_timer(
                    TimerEventType::Failed {
                        demand_type,
                        error_category: e.classify_error(),
                        exception: format!("{e:?}").into_boxed_str(),
                    },
                    key,
                    scheduled_time,
                    timer_type,
                    self.source.clone(),
                );
            }
        }

        result
    }

    /// Pass-through forwarder for the framework's terminal apply hook.
    ///
    /// `TelemetryHandler` is a pure pass-through middleware
    /// (`Output = T::Output`, `Error = T::Error`) and therefore cannot change
    /// the dispatch's final outcome. The framework calls this hook to mark
    /// the dispatch as FINAL: no retry of the same logical event will follow
    /// through this consumer. Per the `FallibleHandler` invariant, exactly
    /// one of `after_commit` or `after_abort` fires for each `on_message` /
    /// `on_timer` call that runs and returns; this layer simply forwards the
    /// hook verbatim so the inner handler observes the framework-level
    /// commit/abort decision unchanged.
    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handler.after_commit(context, result).await;
    }

    /// Pass-through forwarder for the framework's non-terminal apply hook.
    ///
    /// `TelemetryHandler` is a pure pass-through middleware
    /// (`Output = T::Output`, `Error = T::Error`) and therefore cannot change
    /// the dispatch's final outcome. The framework calls this hook to mark
    /// the dispatch as NOT final: a retry of the same logical event is coming
    /// through this consumer. Per the `FallibleHandler` invariant, exactly
    /// one of `after_commit` or `after_abort` fires for each `on_message` /
    /// `on_timer` call that runs and returns; this layer simply forwards the
    /// hook verbatim so the inner handler observes the framework-level
    /// commit/abort decision unchanged.
    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handler.after_abort(context, result).await;
    }

    async fn shutdown(self) {
        debug!("shutting down telemetry handler");

        // No telemetry-specific state to clean up (sender is shared)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
    }
}

impl<T> SettlementHandler for TelemetryHandler<T>
where
    T: SettlementHandler,
{
    /// Pass-through: telemetry adds no Output or error variants of its own.
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement {
        T::settlement(result)
    }
}

#[cfg(test)]
mod tests;
