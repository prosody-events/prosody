//! Failure topic middleware for error handling in message processing.
//!
//! This module provides middleware that sends failed messages
//! to a designated failure topic for later analysis or reprocessing.

use chrono::{DateTime, SecondsFormat, Utc};
use derive_builder::Builder;
use thiserror::Error;
use tracing::{error, info};
use validator::{Validate, ValidationErrors};

use crate::Topic;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleEventHandler, FallibleHandler, HandlerMiddleware,
};
use crate::consumer::{HandlerProvider, Keyed};
use crate::producer::{ProducerError, ProsodyProducer};
use crate::timers::Trigger;
use crate::util::from_env;
use serde_json::json;

/// Configuration for failure topic middleware.
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
    pub failure_topic: String,
}

impl FailureTopicConfiguration {
    /// Creates a new [`FailureTopicConfigurationBuilder`].
    ///
    /// # Returns
    ///
    /// A [`FailureTopicConfigurationBuilder`] instance.
    #[must_use]
    pub fn builder() -> FailureTopicConfigurationBuilder {
        FailureTopicConfigurationBuilder::default()
    }
}

/// Middleware that sends failed messages to a designated failure topic.
#[derive(Clone, Debug)]
pub struct FailureTopicMiddleware {
    config: FailureTopicConfiguration,
    producer: ProsodyProducer,
    group_id: String,
}

impl FailureTopicMiddleware {
    /// Creates a new [`FailureTopicMiddleware`] with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - A [`FailureTopicConfiguration`] specifying the failure
    ///   topic.
    /// * `group_id` - The consumer group ID.
    /// * `producer` - The [`ProsodyProducer`] used to send failure events.
    ///
    /// # Returns
    ///
    /// A [`Result<Self, ValidationErrors>`] where:
    /// - `Ok` contains the new [`FailureTopicMiddleware`] when the
    ///   configuration is valid.
    /// - `Err` contains [`ValidationErrors`] if the configuration is invalid.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErrors`] if:
    /// - `failure_topic` is empty.
    /// - Any other validation error occurs.
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

impl HandlerMiddleware for FailureTopicMiddleware {
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
    type Error = FailureTopicError<T::Error>;

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
    /// sent to the failure topic, or an `Err` containing a `FailureTopicError`
    /// if processing or sending to the failure topic failed.
    ///
    /// # Errors
    ///
    /// Returns a `FailureTopicError::Handler` if the wrapped handler fails with
    /// a terminal error. Returns a `FailureTopicError::Producer` if sending
    /// to the failure topic fails.
    async fn on_message<C>(&self, context: C, message: ConsumerMessage) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let topic = message.topic().as_ref();
        let partition = message.partition();
        let key = message.key();
        let offset = message.offset();

        let timestamp = message
            .timestamp()
            .to_rfc3339_opts(SecondsFormat::AutoSi, true);

        // Attempt to process the message with the wrapped handler
        let Err(error) = self.handler.on_message(context, message.clone()).await else {
            return Ok(());
        };

        // Handle terminal errors by aborting
        if matches!(error.classify_error(), ErrorCategory::Terminal) {
            info!(
                topic,
                partition,
                key = key.as_str(),
                offset,
                "terminal condition encountered while handling message: {error:#}; aborting"
            );
            return Err(FailureTopicError::Handler(error));
        }

        // Log the error and prepare to send to failure topic
        error!(
            topic,
            partition,
            key = key.as_str(),
            offset,
            "failed to process message: {error:#}; sending to {}",
            self.topic
        );

        // Prepare headers for the failure message
        let headers = [
            ("source-kind", "message"),
            ("source-topic", topic),
            ("source-partition", &partition.to_string()),
            ("source-offset", &offset.to_string()),
            ("source-timestamp", &timestamp),
            ("source-group-id", &self.group_id),
            ("source-error", &error.to_string()),
        ];

        // Send the failed message to the failure topic
        self.producer
            .send(headers, self.topic, key, message.payload())
            .await?;

        Ok(())
    }

    async fn on_timer<C>(&self, context: C, timer: Trigger) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Attempt to process the timer with the wrapped handler
        let Err(error) = self.handler.on_timer(context, timer.clone()).await else {
            return Ok(());
        };

        // Terminal errors abort and propagate
        if matches!(error.classify_error(), ErrorCategory::Terminal) {
            info!(
                key = %timer.key,
                "terminal condition encountered while handling timer: {error:#}; aborting"
            );
            return Err(FailureTopicError::Handler(error));
        }

        // Extract the timer key as &str to avoid moving the Flexstr
        let key_str = timer.key.as_ref();
        // Log the error and prepare to send to failure topic
        error!(
            key = key_str,
            "failed to process timer: {error:#}; sending to {}", self.topic
        );

        // Prepare headers for the failure timer message
        let timestamp: DateTime<Utc> = timer.time.into();
        let timestamp = timestamp.to_rfc3339_opts(SecondsFormat::AutoSi, true);

        let headers = [
            ("source-kind", "timer"),
            ("source-timestamp", timestamp.as_str()),
            ("source-group-id", &self.group_id),
            ("source-error", &error.to_string()),
        ];

        // Build payload for replaying the timer
        let payload = json!({ "key": key_str, "time": timestamp });

        // Send the failed timer event to the failure topic
        self.producer
            .send(headers, self.topic, key_str, &payload)
            .await?;

        Ok(())
    }
}

impl<T> FallibleEventHandler for FailureTopicHandler<T> where T: FallibleHandler {}

/// Errors that can occur during failure topic handling.
#[derive(Debug, Error)]
pub enum FailureTopicError<E> {
    /// Error from the wrapped handler.
    #[error(transparent)]
    Handler(E),

    /// Error from the producer when sending to the failure topic.
    #[error(transparent)]
    Producer(#[from] ProducerError),
}

impl<E> ClassifyError for FailureTopicError<E>
where
    E: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            FailureTopicError::Handler(error) => error.classify_error(),
            FailureTopicError::Producer(_) => ErrorCategory::Transient,
        }
    }
}
