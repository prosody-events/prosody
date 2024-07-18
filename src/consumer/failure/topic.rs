//! Failure topic strategy for error handling in message processing.
//!
//! This module provides a `FailureTopicStrategy` that sends failed messages
//! to a designated failure topic for later analysis or reprocessing.

use chrono::SecondsFormat;
use derive_builder::Builder;
use tracing::error;
use validator::{Validate, ValidationErrors};

use crate::consumer::failure::{FailureStrategy, FallibleHandler};
use crate::consumer::message::{ConsumerMessage, MessageContext, UncommittedMessage};
use crate::consumer::{HandlerProvider, MessageHandler};
use crate::producer::{ProducerError, ProsodyProducer};
use crate::util::from_env;
use crate::Topic;

/// Configuration for the failure topic strategy.
#[derive(Builder, Clone, Debug, Validate)]
pub struct FailureTopicConfiguration {
    /// Failure topic name.
    ///
    /// Environment variable: `PROSODY_FAILURE_TOPIC`
    /// Default: None (must be specified)
    ///
    /// The topic to which messages that have failed processing will be sent.
    #[builder(default = "from_env(\"PROSODY_FAILURE_TOPIC\")?", setter(into))]
    #[validate(length(min = 1_u64))]
    failure_topic: String,
}

/// A strategy that sends failed messages to a designated failure topic.
#[derive(Clone, Debug)]
pub struct FailureTopicStrategy {
    config: FailureTopicConfiguration,
    producer: ProsodyProducer,
    group_id: String,
}

impl FailureTopicStrategy {
    /// Creates a new `FailureTopicStrategy` with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for the failure topic strategy.
    /// * `group_id` - The consumer group ID.
    /// * `producer` - The producer used to send messages to the failure topic.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `FailureTopicStrategy` if the
    /// configuration is valid, or `ValidationErrors` if the configuration
    /// is invalid.
    ///
    /// # Errors
    ///
    /// This method will return `ValidationErrors` if:
    /// - The `failure_topic` in the configuration is an empty string.
    /// - Any other validation defined in the `FailureTopicConfiguration` struct
    ///   fails.
    pub fn new(
        config: FailureTopicConfiguration,
        group_id: String,
        producer: ProsodyProducer,
    ) -> Result<Self, ValidationErrors> {
        config.validate()?;
        Ok(Self {
            config,
            producer,
            group_id,
        })
    }
}

/// A handler wrapped with failure topic functionality.
#[derive(Clone, Debug)]
struct FailureTopicHandler<T> {
    topic: Topic,
    producer: ProsodyProducer,
    group_id: String,
    handler: T,
}

impl FailureStrategy for FailureTopicStrategy {
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler,
    {
        FailureTopicHandler {
            topic: self.config.failure_topic.as_str().into(),
            producer: self.producer.clone(),
            group_id: self.group_id.clone(),
            handler,
        }
    }
}

impl<T> FallibleHandler for FailureTopicHandler<T>
where
    T: FallibleHandler,
{
    type Error = ProducerError;

    /// Handles a message, attempting to process it with the wrapped handler.
    /// If processing fails, sends the message to the failure topic.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The message to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` that is `Ok(())` if the message was processed successfully or
    /// sent to the failure topic, or an `Err` containing a `ProducerError`
    /// if sending to the failure topic failed.
    async fn handle(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        let topic = message.topic;
        let partition = message.partition;
        let key = message.key.clone();
        let offset = message.offset;

        let timestamp = message
            .timestamp
            .to_rfc3339_opts(SecondsFormat::AutoSi, true);

        // Attempt to process the message with the wrapped handler
        let Err(error) = self.handler.handle(context, message.clone()).await else {
            return Ok(());
        };

        // Log the error and prepare to send to failure topic
        error!(
            %topic, %partition, %key, %offset,
            "failed to process message: {error:#}; sending to {}",
            self.topic
        );

        // Prepare headers for the failure message
        let headers = [
            ("source-topic", topic.as_ref()),
            ("source-partition", &partition.to_string()),
            ("source-offset", &offset.to_string()),
            ("source-timestamp", &timestamp),
            ("source-group-id", &self.group_id),
        ];

        // Send the failed message to the failure topic
        self.producer
            .send(headers, self.topic, &key, message.payload)
            .await
    }
}

impl<T> MessageHandler for FailureTopicHandler<T>
where
    T: FallibleHandler,
{
    /// Handles an uncommitted message, committing the offset after processing.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The uncommitted message to be processed.
    async fn handle(&self, context: MessageContext, message: UncommittedMessage) {
        let (message, uncommitted_offset) = message.into_inner();

        // Attempt to handle the message and send to failure topic if it fails
        let Err(error) = FallibleHandler::handle(self, context, message).await else {
            uncommitted_offset.commit();
            return;
        };

        // Log the error if sending to the failure topic failed
        error!("failed to send message to failure topic: {error:#}; discarding message");
        uncommitted_offset.commit();
    }

    /// Shuts down the handler.
    ///
    /// This method is currently a no-op for `FailureTopicHandler`.
    async fn shutdown(self) {}
}
