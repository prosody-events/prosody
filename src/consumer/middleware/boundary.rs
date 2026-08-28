use std::convert::Infallible;
use std::io::Error as IoError;

use super::{ClassifyError, ErrorCategory, FallibleHandler, SettlementHandler, settle};
use crate::consumer::event_context::EventContext;
use crate::consumer::message::UncommittedMessage;
use crate::consumer::{DemandType, EventHandler};
use crate::timers::UncommittedTimer;

/// Marks a [`FallibleHandler`] as the **durability boundary**, getting the
/// blanket [`EventHandler`] impl below.
///
/// # The durability sequence has one owner: `settle`
///
/// The blanket impl invokes the inner `FallibleHandler` method **exactly
/// once**, then hands the single result to `settle` — the one place that
/// runs the keyed-state durability sequence in straight-line code:
///
/// ```text
/// Bypassed final                → commit → after_commit (no stage, no marker)
/// Final Ok  → stage provisional cells / write resolved (retry transient failures in place)
///           → rerun posture only: arm the StateRecovery backstop
///             (arm-if-sooner; per-key singleton)
///           → record the message marker (read from the session's event
///             identity; STRICTLY after the stage)
///           → sweep posture: receipt → promote → retire the source
///           → rerun posture: commit the source → promote
///           → after_commit(Ok)
/// Final Err Transient/Permanent → record marker iff Permanent → commit → after_commit(Err)
/// Err Terminal                  → abort → after_abort
/// ```
///
/// Because the marker record is textually *after* the stage in one function,
/// the marker-before-durable-state bug class is **unwritable**, not merely
/// avoided. The crash-window argument for the full step order — including
/// why each posture uses its order — lives on
/// `settle_committed` in `settle.rs`. The timer marker (trigger tag) is
/// written outside the stack by the marker commit; the message marker here
/// restores message/timer symmetry.
///
/// [`RetryHandler`](crate::consumer::middleware::retry::RetryHandler) is a
/// second durability boundary (it owns its own `EventHandler` impl so it can
/// map shutdown to abort rather than commit); it routes its final outcome
/// through the **same** `settle` / `abandon` functions, so the sequence still
/// has a single owner. No other middleware should implement `EventHandler`
/// directly.
///
/// **Stack contract:** whether a dispatch settles the event is a pure
/// function of the *final* result the stack returns — the crate-internal
/// `settlement()` classification. A middleware that swallows or rescues (a
/// defer swallow into `Ok(Deferred)`, a DLQ route into `Ok(Routed)`, a dedup
/// skip into `Ok(None)`) classifies its own variants `Bypassed`, so nothing
/// stages and no marker records for the swallowed attempt; there is no reset
/// protocol to remember. The blanket impl below therefore requires both this
/// trait and `SettlementHandler`.
///
/// Per-invocation apply-hook correctness is preserved: one inner invocation
/// pairs with exactly one `after_commit` / `after_abort` firing.
pub trait FallibleEventHandler: FallibleHandler {
    /// Called when message processing fails.
    fn on_message_error(&self, _error: &Self::Error) {}

    /// Called when timer processing fails.
    fn on_timer_error(&self, _error: &Self::Error) {}
}

impl<T> EventHandler for T
where
    T: FallibleEventHandler + SettlementHandler,
{
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: UncommittedMessage<Self::Payload>,
        demand_type: DemandType,
    ) where
        C: EventContext<Payload = T::Payload>,
    {
        // Invoke the inner FallibleHandler EXACTLY ONCE, then hand its single
        // result to the shared durability sequence. `settle` fires EXACTLY
        // ONE apply hook, so the per-invocation invariant holds.
        let (inner_message, uncommitted_offset) = message.into_inner();
        let result =
            FallibleHandler::on_message(self, context.clone(), inner_message, demand_type).await;
        if let Err(error) = &result {
            self.on_message_error(error);
        }
        settle(self, context, uncommitted_offset, result).await;
    }

    async fn on_excise<C>(
        &self,
        context: C,
        message: UncommittedMessage<()>,
        demand_type: DemandType,
    ) where
        C: EventContext<Payload = T::Payload>,
    {
        let (message, uncommitted_offset) = message.into_inner();
        let result = FallibleHandler::on_excise(self, context.clone(), message, demand_type).await;
        if let Err(error) = &result {
            self.on_message_error(error);
        }
        settle(self, context, uncommitted_offset, result).await;
    }

    async fn on_timer<C, U>(&self, context: C, timer: U, demand_type: DemandType)
    where
        C: EventContext<Payload = T::Payload>,
        U: UncommittedTimer,
    {
        let (trigger, uncommitted_timer) = timer.into_inner();
        let result = FallibleHandler::on_timer(self, context.clone(), trigger, demand_type).await;
        if let Err(error) = &result {
            self.on_timer_error(error);
        }
        settle(self, context, uncommitted_timer, result).await;
    }

    async fn shutdown(self) {
        FallibleHandler::shutdown(self).await;
    }
}

impl ClassifyError for Infallible {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Terminal
    }
}

impl ClassifyError for IoError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Transient
    }
}
