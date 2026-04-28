//! Mock handler for trace-based property testing.
//!
//! Provides [`OutcomeHandler`] that implements [`FallibleHandler`] and returns
//! outcomes specified by the test trace. This allows property tests to control
//! exactly how the inner handler behaves for each event.
//!
//! In addition to recording `on_message` / `on_timer` invocations, the mock
//! also records every `after_commit` / `after_abort` invocation it observes
//! (via [`OutcomeHandler::applied`]). Tests use those records to assert
//! apply-hook routing: for every dispatch that ran, at most one hook fires,
//! and `MessageDeferOutput::Deferred(e)` must produce `after_abort(Err(e))`.
// TODO(apply-hooks): the harness/properties tests in
// `consumer::middleware::defer::message::handler::tests::{harness,
// properties, integration}` do not yet inspect [`OutcomeHandler::applied`]
// to verify the apply-hook invariant end-to-end. A follow-up should:
//   * Drain `inner_handler.applied()` after each scenario.
//   * For each recorded `on_message`/`on_timer` dispatch, assert exactly one
//     matching apply-hook record is present.
//   * For Transient outcomes that produced `MessageDeferOutput::Deferred`,
//     assert the matching record is `AppliedHook::Abort` carrying
//     `Err(ErrorCategory::Transient)`.
//   * For Success / Permanent outcomes, assert the matching record is
//     `AppliedHook::Commit` carrying `Ok(())` / `Err(Permanent)`.

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::test_support::MockEventContext;
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::Trigger;
use crate::{Key, Offset};
use parking_lot::Mutex;
use std::error::Error;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;

/// A processed message record: (key, offset).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessedMessage {
    /// The key of the processed message.
    pub key: Key,
    /// The offset of the processed message.
    pub offset: Offset,
}

/// Which apply hook fired for a recorded dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppliedHook {
    /// `after_commit` fired — this dispatch is final.
    Commit,
    /// `after_abort` fired — this dispatch is not final; a retry is coming.
    Abort,
}

/// Record of an apply-hook invocation observed by the mock inner handler.
#[derive(Clone, Debug)]
pub struct AppliedRecord {
    /// Which hook fired.
    pub hook: AppliedHook,
    /// Whether the recorded `Result` was `Ok` (true) or `Err` (false).
    ///
    /// We project to a bool because `Self::Output` is `()` and the error
    /// is already classified by category if richer assertions are needed
    /// (see [`AppliedRecord::error_category`]).
    pub is_ok: bool,
    /// The category of the recorded error, if any. `None` for `Ok`.
    pub error_category: Option<ErrorCategory>,
}

// ============================================================================
// HandlerOutcome - What the trace specifies
// ============================================================================

/// Outcome that the handler should return, as specified by the trace.
#[derive(Clone, Debug)]
pub enum HandlerOutcome {
    /// Handler succeeds.
    Success,
    /// Handler fails with a permanent error.
    Permanent,
    /// Handler fails with a transient error.
    Transient,
}

// ============================================================================
// OutcomeError - The error type returned by OutcomeHandler
// ============================================================================

/// Error returned by [`OutcomeHandler`] based on trace specification.
///
/// Implements [`ClassifyError`] to return the appropriate [`ErrorCategory`]
/// for the defer middleware to handle correctly.
#[derive(Clone)]
pub struct OutcomeError {
    /// The category of this error (Permanent or Transient).
    category: ErrorCategory,
}

impl OutcomeError {
    /// Creates a permanent error.
    #[must_use]
    pub fn permanent() -> Self {
        Self {
            category: ErrorCategory::Permanent,
        }
    }

    /// Creates a transient error.
    #[must_use]
    pub fn transient() -> Self {
        Self {
            category: ErrorCategory::Transient,
        }
    }
}

impl Debug for OutcomeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutcomeError")
            .field("category", &self.category)
            .finish()
    }
}

impl Display for OutcomeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.category {
            ErrorCategory::Permanent => write!(f, "permanent test error"),
            ErrorCategory::Transient => write!(f, "transient test error"),
            ErrorCategory::Terminal => write!(f, "terminal test error"),
        }
    }
}

impl Error for OutcomeError {}

impl ClassifyError for OutcomeError {
    fn classify_error(&self) -> ErrorCategory {
        self.category
    }
}

// ============================================================================
// OutcomeHandler - Mock handler returning trace-specified outcomes
// ============================================================================

/// Handler that returns predetermined outcomes from the trace.
///
/// The test harness sets the next outcome before each event using
/// [`OutcomeHandler::set_outcome`]. When the defer middleware calls
/// `on_message()` or `on_timer()`, this handler returns the preset outcome.
///
/// # Example
///
/// ```ignore
/// let handler = OutcomeHandler::new();
///
/// // Before processing a message that should succeed
/// handler.set_outcome(HandlerOutcome::Success);
/// defer_handler.on_message(context, message, demand_type).await?;
///
/// // Before processing a message that should fail transiently
/// handler.set_outcome(HandlerOutcome::Transient);
/// defer_handler.on_message(context, message, demand_type).await?;
/// ```
#[derive(Clone)]
pub struct OutcomeHandler {
    /// Next outcome to return (set by harness before each event).
    next_outcome: Arc<Mutex<Option<HandlerOutcome>>>,
    /// Record of all messages processed by this handler (in order).
    processed: Arc<scc::Queue<ProcessedMessage>>,
    /// Apply-hook records in call order. Drained by
    /// [`OutcomeHandler::applied`].
    applied: Arc<scc::Queue<AppliedRecord>>,
    /// When set, triggers partition shutdown before returning the outcome.
    ///
    /// Used to simulate shutdown occurring mid-handler-execution, exercising
    /// post-call cancellation promotion paths.
    shutdown_trigger: Arc<Mutex<Option<MockEventContext>>>,
}

impl OutcomeHandler {
    /// Creates a new handler with no preset outcome.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_outcome: Arc::new(Mutex::new(None)),
            processed: Arc::new(scc::Queue::default()),
            applied: Arc::new(scc::Queue::default()),
            shutdown_trigger: Arc::new(Mutex::new(None)),
        }
    }

    /// Sets the outcome for the next handler call.
    ///
    /// Must be called before each `on_message()` or `on_timer()` invocation.
    pub fn set_outcome(&self, outcome: HandlerOutcome) {
        *self.next_outcome.lock() = Some(outcome);
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

    /// Returns all apply-hook records in order (drains the queue).
    #[must_use]
    pub fn applied(&self) -> Vec<AppliedRecord> {
        let mut result = Vec::with_capacity(self.applied.len());
        while let Some(entry) = self.applied.pop() {
            result.push((**entry).clone());
        }
        result
    }

    /// Records a processed message.
    fn record_processed(&self, key: Key, offset: Offset) {
        self.processed.push(ProcessedMessage { key, offset });
    }

    /// Records an apply-hook invocation.
    fn record_applied(&self, hook: AppliedHook, result: &Result<(), OutcomeError>) {
        let (is_ok, error_category) = match result {
            Ok(()) => (true, None),
            Err(error) => (false, Some(error.classify_error())),
        };
        self.applied.push(AppliedRecord {
            hook,
            is_ok,
            error_category,
        });
    }

    /// Takes the next outcome, returning Success if none was set.
    ///
    /// This is called internally by `on_message()` and `on_timer()`.
    fn take_outcome(&self) -> HandlerOutcome {
        self.next_outcome
            .lock()
            .take()
            // Return Success as a safe default if no outcome was set
            .unwrap_or(HandlerOutcome::Success)
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
            .field("next_outcome", &self.next_outcome.lock())
            .field("processed_count", &self.processed.len())
            .field("applied_count", &self.applied.len())
            .field("shutdown_trigger", &self.shutdown_trigger.lock().is_some())
            .finish()
    }
}

impl FallibleHandler for OutcomeHandler {
    type Error = OutcomeError;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: ConsumerMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
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

        self.maybe_trigger_shutdown();
        match outcome {
            HandlerOutcome::Success => Ok(()),
            HandlerOutcome::Permanent => Err(OutcomeError::permanent()),
            HandlerOutcome::Transient => Err(OutcomeError::transient()),
        }
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        let outcome = self.take_outcome();
        tracing::debug!(
            "OutcomeHandler.on_timer: key={:?}, outcome={:?}",
            trigger.key,
            outcome
        );
        self.maybe_trigger_shutdown();
        match outcome {
            HandlerOutcome::Success => Ok(()),
            HandlerOutcome::Permanent => Err(OutcomeError::permanent()),
            HandlerOutcome::Transient => Err(OutcomeError::transient()),
        }
    }

    async fn after_commit<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        self.record_applied(AppliedHook::Commit, &result);
    }

    async fn after_abort<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        self.record_applied(AppliedHook::Abort, &result);
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
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use crate::tracing::init_test_logging;

    fn make_test_trigger(key_name: &str) -> Trigger {
        let key: Key = Arc::from(key_name);
        let time = CompactDateTime::from(1000_u32);
        Trigger::for_testing(key, time, TimerType::DeferredMessage)
    }

    #[test]
    fn outcome_error_permanent_classifies_correctly() {
        init_test_logging();

        let error = OutcomeError::permanent();
        assert!(matches!(error.classify_error(), ErrorCategory::Permanent));
    }

    #[test]
    fn outcome_error_transient_classifies_correctly() {
        init_test_logging();

        let error = OutcomeError::transient();
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn outcome_handler_returns_configured_success() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            let handler = OutcomeHandler::new();
            let context = MockEventContext::new();
            let trigger = make_test_trigger("test-key");

            handler.set_outcome(HandlerOutcome::Success);
            let result = handler.on_timer(context, trigger, DemandType::Normal).await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn outcome_handler_returns_configured_permanent() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            let handler = OutcomeHandler::new();
            let context = MockEventContext::new();
            let trigger = make_test_trigger("test-key");

            handler.set_outcome(HandlerOutcome::Permanent);
            let result = handler.on_timer(context, trigger, DemandType::Normal).await;
            assert!(matches!(
                result.as_ref().err().map(OutcomeError::classify_error),
                Some(ErrorCategory::Permanent)
            ));
        });
    }

    #[test]
    fn outcome_handler_returns_configured_transient() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            let handler = OutcomeHandler::new();
            let context = MockEventContext::new();
            let trigger = make_test_trigger("test-key");

            handler.set_outcome(HandlerOutcome::Transient);
            let result = handler.on_timer(context, trigger, DemandType::Normal).await;
            assert!(matches!(
                result.as_ref().err().map(OutcomeError::classify_error),
                Some(ErrorCategory::Transient)
            ));
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

    #[test]
    fn after_commit_records_ok_invocation() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            let handler = OutcomeHandler::new();
            let context = MockEventContext::new();

            handler.after_commit(context, Ok(())).await;

            let applied = handler.applied();
            assert_eq!(applied.len(), 1);
            assert_eq!(applied[0].hook, AppliedHook::Commit);
            assert!(applied[0].is_ok);
            assert!(applied[0].error_category.is_none());
        });
    }

    #[test]
    fn after_abort_records_transient_err_invocation() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            let handler = OutcomeHandler::new();
            let context = MockEventContext::new();

            // Mirrors the `MessageDeferOutput::Deferred(e)` contract: when
            // the defer middleware captures a Transient inner error and
            // enqueues a retry, the inner must observe
            // `after_abort(Err(transient))`.
            handler
                .after_abort(context, Err(OutcomeError::transient()))
                .await;

            let applied = handler.applied();
            assert_eq!(applied.len(), 1);
            assert_eq!(applied[0].hook, AppliedHook::Abort);
            assert!(!applied[0].is_ok);
            assert_eq!(applied[0].error_category, Some(ErrorCategory::Transient));
        });
    }

    #[test]
    fn applied_drains_in_order() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            let handler = OutcomeHandler::new();

            handler.after_commit(MockEventContext::new(), Ok(())).await;
            handler
                .after_abort(MockEventContext::new(), Err(OutcomeError::permanent()))
                .await;
            handler.after_commit(MockEventContext::new(), Ok(())).await;

            let applied = handler.applied();
            assert_eq!(applied.len(), 3);
            assert_eq!(applied[0].hook, AppliedHook::Commit);
            assert_eq!(applied[1].hook, AppliedHook::Abort);
            assert_eq!(applied[1].error_category, Some(ErrorCategory::Permanent));
            assert_eq!(applied[2].hook, AppliedHook::Commit);

            // Drained — second call should be empty.
            assert!(handler.applied().is_empty());
        });
    }
}
