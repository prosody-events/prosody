//! Error logging middleware.
//!
//! Logs handler failures based on [`ErrorCategory`] classification while
//! preserving the original error flow. Typically positioned as an outer layer
//! for comprehensive error visibility.
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
//! # Error Classification
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
//! #     type Outcome = ();
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
    type Outcome = T::Outcome;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<Self::Outcome, Self::Error>
    where
        C: EventContext,
    {
        // Attempt to process the message with the wrapped handler
        let outcome = match self.handler.on_message(context, message, demand_type).await {
            Ok(outcome) => return Ok(outcome),
            Err(error) => error,
        };

        // Log the error based on its category
        match outcome.classify_error() {
            ErrorCategory::Transient => {
                error!("transient error occurred during message processing: {outcome:#}");
            }
            ErrorCategory::Permanent => {
                error!("permanent error occurred during message processing: {outcome:#}");
            }
            ErrorCategory::Terminal => {
                error!("terminal error occurred during message processing: {outcome:#}");
            }
        }

        Err(outcome)
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Outcome, Self::Error>
    where
        C: EventContext,
    {
        // Attempt to process the timer with the wrapped handler
        let outcome = match self.handler.on_timer(context, trigger, demand_type).await {
            Ok(outcome) => return Ok(outcome),
            Err(error) => error,
        };

        // Log the error based on its category
        match outcome.classify_error() {
            ErrorCategory::Transient => {
                error!("transient error occurred during timer processing: {outcome:#}");
            }
            ErrorCategory::Permanent => {
                error!("permanent error occurred during timer processing: {outcome:#}");
            }
            ErrorCategory::Terminal => {
                error!("terminal error occurred during timer processing: {outcome:#}");
            }
        }

        Err(outcome)
    }

    async fn after_commit<C>(
        &self,
        context: C,
        result: Result<Self::Outcome, Self::Error>,
    ) where
        C: EventContext,
    {
        self.handler.after_commit(context, result).await;
    }

    async fn after_abort<C>(
        &self,
        context: C,
        result: Result<Self::Outcome, Self::Error>,
    ) where
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
