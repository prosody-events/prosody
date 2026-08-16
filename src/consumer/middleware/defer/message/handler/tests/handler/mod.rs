//! Mock handler for trace-based property testing.
//!
//! Provides [`OutcomeHandler`] that implements [`FallibleHandler`] and returns
//! outcomes specified by the test trace. This allows property tests to control
//! exactly how the inner handler behaves for each event.
//!
//! The apply-hook invariant (exactly one `after_commit`/`after_abort` per
//! dispatch) is proven at the real `settle` boundary by
//! `consumer::middleware::tests`, not from this mock.

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::tests::test_support::{MockEventContext, OutcomeSlot};
use crate::timers::Trigger;
use crate::{Key, Offset};
use parking_lot::Mutex;
use std::fmt::{self, Debug};
use std::sync::Arc;
use tracing::span::Id;

pub use crate::consumer::middleware::tests::test_support::{HandlerOutcome, OutcomeError};

/// A processed message record: (key, offset).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessedMessage {
    /// The key of the processed message.
    pub key: Key,
    /// The offset of the processed message.
    pub offset: Offset,
}

// ============================================================================
// OutcomeHandler - Mock handler returning trace-specified outcomes
// ============================================================================

/// `(ambient span id, event span id)` recorded inside one handler call.
type AmbientPair = (Option<Id>, Option<Id>);

/// Handler that returns predetermined outcomes from the trace.
///
/// The test harness sets the next outcome before each event using
/// [`OutcomeHandler::set_outcome`]. When the defer middleware calls
/// `on_message()` or `on_timer()`, this handler returns the preset outcome.
#[derive(Clone)]
pub struct OutcomeHandler {
    /// Next outcome to return (set by harness before each event).
    outcome: OutcomeSlot,
    /// Record of all messages processed by this handler (in order).
    processed: Arc<scc::Queue<ProcessedMessage>>,
    /// When set, triggers partition shutdown before returning the outcome.
    ///
    /// Used to simulate shutdown occurring mid-handler-execution, exercising
    /// post-call cancellation promotion paths.
    shutdown_trigger: Arc<Mutex<Option<MockEventContext>>>,
    /// Pairs observed inside each `on_message` call — pins that dispatch
    /// entered the message's span.
    ambient_pairs: Arc<Mutex<Vec<AmbientPair>>>,
}

impl OutcomeHandler {
    /// Creates a new handler with no preset outcome.
    #[must_use]
    pub fn new() -> Self {
        Self {
            outcome: OutcomeSlot::default(),
            processed: Arc::new(scc::Queue::default()),
            shutdown_trigger: Arc::new(Mutex::new(None)),
            ambient_pairs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Sets the outcome for the next handler call.
    ///
    /// Must be called before each `on_message()` or `on_timer()` invocation.
    pub fn set_outcome(&self, outcome: HandlerOutcome) {
        self.outcome.set(outcome);
    }

    /// Configures shutdown to be signaled when this handler is next called.
    ///
    /// The provided context shares its shutdown channel with any clones, so
    /// contexts passed to outer middleware layers will also observe shutdown
    /// after the handler fires.
    pub fn set_shutdown_trigger(&self, ctx: MockEventContext) {
        *self.shutdown_trigger.lock() = Some(ctx);
    }

    /// Fires the shutdown trigger if one is configured.
    fn maybe_trigger_shutdown(&self) {
        if let Some(ctx) = self.shutdown_trigger.lock().as_ref() {
            ctx.request_shutdown();
        }
    }

    /// Returns all processed messages in order (drains the queue).
    #[must_use]
    pub fn processed(&self) -> Vec<ProcessedMessage> {
        let mut result = Vec::with_capacity(self.processed.len());
        while let Some(entry) = self.processed.pop() {
            result.push((**entry).clone());
        }
        result
    }

    /// Records a processed message.
    fn record_processed(&self, key: Key, offset: Offset) {
        self.processed.push(ProcessedMessage { key, offset });
    }

    /// Returns the `(ambient, message-span)` id pairs recorded per call.
    #[must_use]
    pub fn ambient_pairs(&self) -> Vec<AmbientPair> {
        self.ambient_pairs.lock().clone()
    }

    /// Takes the next outcome, returning Success if none was set.
    ///
    /// This is called internally by `on_message()` and `on_timer()`.
    fn take_outcome(&self) -> HandlerOutcome {
        self.outcome.take()
    }
}

impl Default for OutcomeHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for OutcomeHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutcomeHandler")
            .field("outcome", &self.outcome)
            .field("processed_count", &self.processed.len())
            .field("shutdown_trigger", &self.shutdown_trigger.lock().is_some())
            .finish_non_exhaustive()
    }
}

impl FallibleHandler for OutcomeHandler {
    type Error = OutcomeError;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        FallibleHandler::on_message(self, context, message, demand_type).await
    }

    async fn on_message<C>(
        &self,
        _context: C,
        message: ConsumerMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        use crate::consumer::Keyed;
        let key = message.key().clone();
        let offset = message.offset();
        let outcome = self.take_outcome();
        tracing::info!(
            "OutcomeHandler.on_message: key={:?}, offset={}, outcome={:?}",
            key,
            offset,
            outcome
        );

        // Record this message as processed (for order verification)
        self.record_processed(key, offset);
        self.ambient_pairs
            .lock()
            .push((tracing::Span::current().id(), message.span().id()));

        self.maybe_trigger_shutdown();
        outcome.into_result()
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let outcome = self.take_outcome();
        tracing::debug!(
            "OutcomeHandler.on_timer: key={:?}, outcome={:?}",
            trigger.key,
            outcome
        );
        self.maybe_trigger_shutdown();
        outcome.into_result()
    }

    async fn shutdown(self) {
        // No-op for test handler
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Key;
    use crate::consumer::middleware::defer::message::handler::tests::{
        MockEventContext, TEST_RUNTIME,
    };
    use crate::error::{ClassifyError, ErrorCategory};
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use crate::tracing::init_test_logging;

    fn make_test_trigger(key_name: &str) -> Trigger {
        let key: Key = Arc::from(key_name);
        let time = CompactDateTime::from(1000_u32);
        Trigger::for_testing(key, time, TimerType::DeferredMessage)
    }

    #[test]
    fn outcome_handler_returns_configured_outcome() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            for (outcome, expected_category) in [
                (HandlerOutcome::Success, None),
                (HandlerOutcome::Permanent, Some(ErrorCategory::Permanent)),
                (HandlerOutcome::Transient, Some(ErrorCategory::Transient)),
            ] {
                let handler = OutcomeHandler::new();
                let context = MockEventContext::new();
                let trigger = make_test_trigger("test-key");

                handler.set_outcome(outcome);
                let result = handler.on_timer(context, trigger, DemandType::Normal).await;
                assert_eq!(
                    result.as_ref().err().map(OutcomeError::classify_error),
                    expected_category
                );
            }
        });
    }

    #[test]
    fn outcome_handler_is_clone_safe() {
        init_test_logging();

        let handler1 = OutcomeHandler::new();
        let handler2 = handler1.clone();

        // Both should share state
        handler1.set_outcome(HandlerOutcome::Permanent);

        // handler2 should see the outcome set by handler1
        // (they share Arc<Mutex>)
        let outcome = handler2.take_outcome();
        assert!(matches!(outcome, HandlerOutcome::Permanent));
    }

    #[test]
    fn outcome_handler_defaults_to_success_when_not_set() {
        init_test_logging();

        let handler = OutcomeHandler::new();

        // When no outcome is set, take_outcome should return Success
        let outcome = handler.take_outcome();
        assert!(matches!(outcome, HandlerOutcome::Success));
    }
}

/// The settlement pins for the message-defer seams, each driven end to end
/// through the real settle boundary (the blanket `EventHandler` impl, or
/// retry's for the last-wins pin):
///
/// - a defer swallow (`Ok(Deferred)`) records **no** marker and stages
///   **nothing**, so the deferred reload re-runs unfiltered;
/// - a deferred reload records the **reloaded message's** marker while staging
///   under the timer's `EventRef`, and a redelivery of that message filters;
/// - a reload that fails permanently records the reloaded id;
/// - a retry re-dispatch that loads a **different** queue head records under
///   the new head's id (the last-wins override).
#[cfg(test)]
mod settlement_pins;
