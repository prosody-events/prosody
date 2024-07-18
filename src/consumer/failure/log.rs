//! Logging strategy for failure handling in message processing.
//!
//! This module provides a `LogStrategy` that wraps handlers and logs errors
//! when message processing fails.

use tracing::error;

use crate::consumer::failure::{FailureStrategy, FallibleHandler};
use crate::consumer::message::{ConsumerMessage, MessageContext, UncommittedMessage};
use crate::consumer::{HandlerProvider, Keyed, MessageHandler};

/// A strategy that logs errors when message processing fails.
#[derive(Copy, Clone, Debug)]
pub struct LogStrategy;

/// A handler wrapped with logging functionality.
#[derive(Clone, Debug)]
struct LogHandler<T>(T);

impl FailureStrategy for LogStrategy {
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler,
    {
        LogHandler(handler)
    }
}

impl<T> FallibleHandler for LogHandler<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;

    async fn handle(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        let topic = message.topic;
        let partition = message.partition;
        let key = message.key.clone();
        let offset = message.offset;

        // Attempt to handle the message and log any errors
        self.0.handle(context, message).await.inspect_err(|error| {
            error!(%topic, %partition, %key, %offset, "failed to handle message: {error:#}");
        })
    }
}

impl<T> MessageHandler for LogHandler<T>
where
    T: FallibleHandler,
{
    async fn handle(&self, context: MessageContext, message: UncommittedMessage) {
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key().to_owned();
        let offset = message.offset();
        let (message, uncommitted_offset) = message.into_inner();

        // Attempt to handle the message and log any errors
        if let Err(error) = self.0.handle(context, message).await {
            error!(%topic, %partition, %key, %offset, "failed to handle message: {error:#}");
        }
        uncommitted_offset.commit();
    }

    async fn shutdown(self) {
        // No shutdown behavior needed for logging
    }
}
