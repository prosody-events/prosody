//! Error logging middleware.
//!
//! Logs handler failures based on [`ErrorCategory`] classification while
//! preserving the original error flow verbatim. This middleware is a pure
//! pass-through: it observes outcomes and emits log records, but it never
//! changes the dispatch result or the apply-hook outcome that downstream
//! layers see. Typically positioned as an outer layer for comprehensive
//! error visibility.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. Pass control to inner middleware layers (no-op)
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. **Log error if present** - Categorizes and logs with appropriate severity
//! 3. Pass original result through unchanged
//!
//! **Apply Hooks:**
//!
//! `after_commit` and `after_abort` are forwarded verbatim to the inner
//! handler. Logging is observability only and does not influence whether a
//! dispatch is final (`after_commit`) or will be retried (`after_abort`),
//! so this middleware participates in the apply-hook contract by simply
//! relaying the framework's choice without alteration. The inner is invoked
//! at most once per call; per-invocation invariant trivially upheld.
//!
//! # Error Classification (observability)
//!
//! These categories drive the *log severity and wording* only. They do not
//! make any commitment about whether the work will be retried or discarded;
//! that decision is owned by downstream durability/retry layers.
//!
//! - **Transient errors**: Logged as errors with context for retry analysis
//! - **Permanent errors**: Logged as errors indicating business logic issues
//! - **Terminal errors**: Logged as errors indicating system failures
//!
//! # Usage
//!
//! Position as outer middleware for complete error visibility:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::log::*;
//! # use prosody::consumer::middleware::scheduler::*;
//! # use prosody::consumer::middleware::cancellation::CancellationMiddleware;
//! # use prosody::consumer::DemandType;
//! # use prosody::consumer::event_context::EventContext;
//! # use prosody::consumer::message::ConsumerMessage;
//! # use prosody::telemetry::Telemetry;
//! # use prosody::timers::Trigger;
//! # use std::convert::Infallible;
//! # #[derive(Clone)]
//! # struct MyHandler;
//! # impl FallibleHandler for MyHandler {
//! #     type Error = Infallible;
//! #     type Output = ();
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_timer<C>(&self, _: C, _: Trigger, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn shutdown(self) {}
//! # }
//! # let config = SchedulerConfigurationBuilder::default().build().unwrap();
//! # let telemetry = Telemetry::default();
//! # let handler = MyHandler;
//!
//! let provider = SchedulerMiddleware::new(&config, &telemetry).unwrap()
//!     .layer(CancellationMiddleware)
//!     .layer(LogMiddleware) // Logs all errors from inner layers
//!     .into_provider(handler);
//! ```
//!
//! [`ErrorCategory`]: crate::consumer::middleware::ErrorCategory

use tracing::{debug, error};

use crate::consumer::DemandType;
use crate::consumer::HandlerProvider;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleEventHandler, FallibleHandler, FallibleHandlerProvider,
    HandlerMiddleware,
};
use crate::timers::Trigger;
use crate::{Partition, Topic};

/// Middleware that logs failures during message processing.
#[derive(Copy, Clone, Debug)]
pub struct LogMiddleware;

/// A provider that logs failures during message processing.
#[derive(Clone, Debug)]
pub struct LogProvider<T> {
    provider: T,
}

/// A handler that logs failures during message processing.
///
/// Wraps another handler and adds logging capabilities while preserving the
/// original error handling behavior.
#[derive(Clone, Debug)]
pub struct LogHandler<T> {
    handler: T,
}

impl HandlerMiddleware for LogMiddleware {
    type Provider<T: FallibleHandlerProvider> = LogProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        LogProvider { provider }
    }
}

impl<T> FallibleHandlerProvider for LogProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = LogHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        LogHandler {
            handler: self.provider.handler_for_partition(topic, partition),
        }
    }
}

impl<T> HandlerProvider for LogProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = LogHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        LogHandler {
            handler: self.provider.handler_for_partition(topic, partition),
        }
    }
}

impl<T> FallibleHandler for LogHandler<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;
    type Output = T::Output;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        // Attempt to process the message with the wrapped handler
        let error = match self.handler.on_message(context, message, demand_type).await {
            Ok(output) => return Ok(output),
            Err(error) => error,
        };

        // Log the error based on its category
        match error.classify_error() {
            ErrorCategory::Transient => {
                error!("transient error occurred during message processing: {error:#}");
            }
            ErrorCategory::Permanent => {
                error!("permanent error occurred during message processing: {error:#}");
            }
            ErrorCategory::Terminal => {
                error!("terminal error occurred during message processing: {error:#}");
            }
        }

        Err(error)
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        // Attempt to process the timer with the wrapped handler
        let error = match self.handler.on_timer(context, trigger, demand_type).await {
            Ok(output) => return Ok(output),
            Err(error) => error,
        };

        // Log the error based on its category
        match error.classify_error() {
            ErrorCategory::Transient => {
                error!("transient error occurred during timer processing: {error:#}");
            }
            ErrorCategory::Permanent => {
                error!("permanent error occurred during timer processing: {error:#}");
            }
            ErrorCategory::Terminal => {
                error!("terminal error occurred during timer processing: {error:#}");
            }
        }

        Err(error)
    }

    /// Pass-through forwarder for the final-dispatch apply hook.
    ///
    /// Logging cannot change whether a dispatch is final, so this method
    /// forwards the framework's `after_commit` call to the inner handler
    /// verbatim. The inner handler observes exactly the `(context, result)`
    /// the framework chose to commit on.
    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        self.handler.after_commit(context, result).await;
    }

    /// Pass-through forwarder for the non-final apply hook.
    ///
    /// Logging cannot change whether a retry is coming, so this method
    /// forwards the framework's `after_abort` call to the inner handler
    /// verbatim. The inner handler observes exactly the `(context, result)`
    /// the framework chose to abort on, with the same retry semantics it
    /// would see without this middleware in the stack.
    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        self.handler.after_abort(context, result).await;
    }

    async fn shutdown(self) {
        debug!("shutting down log handler");

        // No log-specific state to clean up (logging is stateless)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
    }
}

impl<T> FallibleEventHandler for LogHandler<T>
where
    T: FallibleHandler,
{
    // NOTE on log-message wording:
    //
    // The trailing phrases ("discarding message", "discarding timer",
    // "aborting processing") describe what *this dispatch attempt* did with
    // the error and are intentionally preserved as stable observability
    // strings (operators grep for them). Under the apply-hook contract they
    // are NOT a claim that the logical event is permanently dropped: a
    // Transient failure observed here typically results in `after_abort` and
    // a subsequent retry through this consumer. Only the durability boundary
    // decides whether a message is truly discarded; the messages below are
    // accurate as per-attempt observability and should be read accordingly.
    fn on_message_error(&self, error: &Self::Error) {
        match error.classify_error() {
            ErrorCategory::Transient => {
                error!("transient error occurred during processing: {error:#}; discarding message");
            }
            ErrorCategory::Permanent => {
                error!("permanent error occurred during processing: {error:#}; discarding message");
            }
            ErrorCategory::Terminal => {
                error!("terminal error occurred during processing: {error:#}; aborting processing");
            }
        }
    }

    fn on_timer_error(&self, error: &Self::Error) {
        match error.classify_error() {
            ErrorCategory::Transient => {
                error!("transient error occurred during processing: {error:#}; discarding timer");
            }
            ErrorCategory::Permanent => {
                error!("permanent error occurred during processing: {error:#}; discarding timer");
            }
            ErrorCategory::Terminal => {
                error!("terminal error occurred during processing: {error:#}; aborting processing");
            }
        }
    }
}
