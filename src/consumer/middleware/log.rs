//! Logging middleware for message processing.
//!
//! This module provides logging capabilities that wrap handlers and log errors
//! according to their severity while maintaining the original error flow.

use crate::consumer::HandlerProvider;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleEventHandler, FallibleHandler, HandlerMiddleware,
};
use crate::timers::Trigger;
use crate::{Partition, Topic};
use tracing::error;

/// Middleware that logs failures during message processing.
#[derive(Copy, Clone, Debug)]
pub struct LogMiddleware;

/// A provider that logs failures during message processing.
#[derive(Clone, Debug)]
struct LogProvider<T> {
    provider: T,
}

/// A handler that logs failures during message processing.
///
/// Wraps another handler and adds logging capabilities while preserving the
/// original error handling behavior.
#[derive(Clone, Debug)]
struct LogHandler<T> {
    handler: T,
}

impl HandlerMiddleware for LogMiddleware {
    fn with_provider<T>(&self, provider: T) -> impl HandlerProvider<Handler: FallibleHandler>
    where
        T: HandlerProvider,
        T::Handler: FallibleHandler,
    {
        LogProvider { provider }
    }
}

impl<T> HandlerProvider for LogProvider<T>
where
    T: HandlerProvider<Handler: FallibleHandler>,
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

    async fn on_message<C>(&self, context: C, message: ConsumerMessage) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Attempt to process the message with the wrapped handler
        let Err(error) = self.handler.on_message(context, message).await else {
            return Ok(());
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

    async fn on_timer<C>(&self, context: C, trigger: Trigger) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Attempt to process the timer with the wrapped handler
        let Err(error) = self.handler.on_timer(context, trigger).await else {
            return Ok(());
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
}

impl<T> FallibleEventHandler for LogHandler<T> where T: FallibleHandler {}
