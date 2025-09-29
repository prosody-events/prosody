//! Logging middleware for message processing.
//!
//! This module provides logging capabilities that wrap handlers and log errors
//! according to their severity while maintaining the original error flow.

use tracing::error;

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
