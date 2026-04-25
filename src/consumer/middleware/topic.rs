//! Dead letter queue (failure topic) middleware.
//!
//! Routes failed messages to a designated failure topic for later analysis or
//! reprocessing. All non-terminal errors (both [`ErrorCategory::Permanent`] and
//! [`ErrorCategory::Transient`]) are sent to the failure topic. Only
//! [`ErrorCategory::Terminal`] errors bypass the DLQ and propagate immediately,
//! as they indicate partition shutdown rather than recoverable failures.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. Pass control to inner middleware layers
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. **If error is terminal**: Pass through unchanged (triggers partition
//!    shutdown)
//! 3. **If error is permanent or transient**: Send message to failure topic
//!    with metadata
//! 4. **If failure topic write fails**: Return that error for outer retry
//!    middleware
//!
//! # Failure Topic Message Format
//!
//! Messages sent to the failure topic include:
//! - **Original message**: Complete original payload and headers
//! - **Error metadata**: Error message, timestamp, source topic/partition
//! - **Consumer metadata**: Group ID and processing context
//! - **Correlation ID**: For tracking and debugging
//!
//! # Usage
//!
//! Typically positioned between retry layers for sophisticated error handling:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::retry::*;
//! # use prosody::consumer::middleware::scheduler::*;
//! # use prosody::consumer::middleware::cancellation::CancellationMiddleware;
//! # use prosody::consumer::middleware::topic::*;
//! # use prosody::consumer::DemandType;
//! # use prosody::consumer::event_context::EventContext;
//! # use prosody::consumer::message::ConsumerMessage;
//! # use prosody::producer::{ProducerConfiguration, ProsodyProducer};
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
//! # let retry_config = RetryConfiguration::builder().build().unwrap();
//! # let topic_config = FailureTopicConfiguration::builder().build().unwrap();
//! # let producer_config = ProducerConfiguration::builder().bootstrap_servers(vec!["kafka:9092".to_string()]).build().unwrap();
//! # let producer = ProsodyProducer::new(&producer_config, Telemetry::new().sender()).unwrap();
//! # let telemetry = Telemetry::default();
//! # let handler = MyHandler;
//!
//! let provider = SchedulerMiddleware::new(&config, &telemetry).unwrap()
//!     .layer(CancellationMiddleware)
//!     .layer(RetryMiddleware::new(retry_config.clone()).unwrap()) // Retry handler failures
//!     .layer(FailureTopicMiddleware::new(topic_config, "consumer-group".to_string(), producer).unwrap()) // Route to DLQ
//!     .layer(RetryMiddleware::new(retry_config).unwrap()) // Retry DLQ writes
//!     .into_provider(handler);
//! ```
//!
//! [`ErrorCategory::Permanent`]: crate::consumer::middleware::ErrorCategory::Permanent
//! [`ErrorCategory::Transient`]: crate::consumer::middleware::ErrorCategory::Transient
//! [`ErrorCategory::Terminal`]: crate::consumer::middleware::ErrorCategory::Terminal

use chrono::{DateTime, SecondsFormat, Utc};
use derive_builder::Builder;
use serde_json::json;
use thiserror::Error;
use tracing::{debug, error, info};
use validator::{Validate, ValidationErrors};

use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::producer::{ProducerError, ProsodyProducer};
use crate::timers::Trigger;
use crate::util::from_env;
use crate::{Partition, Topic, Topic as TopicType};

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

/// A provider that wraps handlers with failure topic functionality.
#[derive(Clone, Debug)]
pub struct FailureTopicProvider<T> {
    provider: T,
    config: FailureTopicConfiguration,
    producer: ProsodyProducer,
    group_id: String,
}

/// A handler wrapped with failure topic functionality.
#[derive(Clone, Debug)]
pub struct FailureTopicHandler<T> {
    topic: Topic,
    producer: ProsodyProducer,
    group_id: String,
    handler: T,
}

impl HandlerMiddleware for FailureTopicMiddleware {
    type Provider<T: FallibleHandlerProvider> = FailureTopicProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        FailureTopicProvider {
            provider,
            config: self.config.clone(),
            producer: self.producer.clone(),
            group_id: self.group_id.clone(),
        }
    }
}

impl<T> FallibleHandlerProvider for FailureTopicProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = FailureTopicHandler<T::Handler>;

    fn handler_for_partition(&self, topic: TopicType, partition: Partition) -> Self::Handler {
        FailureTopicHandler {
            topic: self.config.failure_topic.as_str().into(),
            producer: self.producer.clone(),
            group_id: self.group_id.clone(),
            handler: self.provider.handler_for_partition(topic, partition),
        }
    }
}

impl<T> FallibleHandler for FailureTopicHandler<T>
where
    T: FallibleHandler,
{
    type Error = FailureTopicError<T::Error>;
    /// `Some(inner_outcome)` when the wrapped handler succeeded; `None` when
    /// the handler failed with a non-Terminal error and the message/timer was
    /// routed to the failure topic instead. This mirrors the dedup pattern:
    /// `None` means the inner's apply work must not run.
    type Outcome = Option<T::Outcome>;

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
    /// A `Result` that is `Ok(Some(outcome))` if the message was processed
    /// successfully, `Ok(None)` if it was sent to the failure topic, or an
    /// `Err` containing a `FailureTopicError` if processing terminated or
    /// sending to the failure topic failed.
    ///
    /// # Errors
    ///
    /// Returns a `FailureTopicError::Handler` if the wrapped handler fails with
    /// a terminal error. Returns a `FailureTopicError::Producer` if sending
    /// to the failure topic fails.
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<Self::Outcome, Self::Error>
    where
        C: EventContext,
    {
        let topic = message.topic().as_ref();
        let partition = message.partition();
        let key = message.key();
        let offset = message.offset();

        let timestamp = message
            .timestamp()
            .to_rfc3339_opts(SecondsFormat::Millis, true);

        // Attempt to process the message with the wrapped handler
        let error = match self
            .handler
            .on_message(context, message.clone(), demand_type)
            .await
        {
            Ok(outcome) => return Ok(Some(outcome)),
            Err(error) => error,
        };

        // Handle terminal errors by aborting
        if matches!(error.classify_error(), ErrorCategory::Terminal) {
            info!(
                topic,
                partition,
                key = key.as_ref(),
                offset,
                "terminal condition encountered while handling message: {error:#}; aborting"
            );
            return Err(FailureTopicError::Handler(error));
        }

        // Log the error and prepare to send to failure topic
        error!(
            topic,
            partition,
            key = key.as_ref(),
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

        Ok(None)
    }

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Outcome, Self::Error>
    where
        C: EventContext,
    {
        // Attempt to process the timer with the wrapped handler
        let error = match self
            .handler
            .on_timer(context, timer.clone(), demand_type)
            .await
        {
            Ok(outcome) => return Ok(Some(outcome)),
            Err(error) => error,
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
        let timestamp = timestamp.to_rfc3339_opts(SecondsFormat::Secs, true);

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

        Ok(None)
    }

    async fn after_commit<C>(&self, context: C, result: Result<Self::Outcome, Self::Error>)
    where
        C: EventContext,
    {
        // `Ok(None)` — inner failed with a non-Terminal error and the
        // message/timer was routed to the failure topic; the inner's failed
        // result is consumed by the DLQ hand-off, so its apply hook does not
        // fire.
        // `Err(Producer(_))` — inner failed with a non-Terminal error and the
        // failure-topic send itself failed; the producer error subsumes the
        // inner's failure, so the inner's apply hook is intentionally dropped
        // rather than forwarded.
        match result {
            Ok(Some(outcome)) => self.handler.after_commit(context, Ok(outcome)).await,
            Err(FailureTopicError::Handler(inner)) => {
                self.handler.after_commit(context, Err(inner)).await;
            }
            Ok(None) | Err(FailureTopicError::Producer(_)) => {}
        }
    }

    async fn after_abort<C>(&self, context: C, result: Result<Self::Outcome, Self::Error>)
    where
        C: EventContext,
    {
        // See `after_commit` for the rationale on the dropped variants.
        match result {
            Ok(Some(outcome)) => self.handler.after_abort(context, Ok(outcome)).await,
            Err(FailureTopicError::Handler(inner)) => {
                self.handler.after_abort(context, Err(inner)).await;
            }
            Ok(None) | Err(FailureTopicError::Producer(_)) => {}
        }
    }

    async fn shutdown(self) {
        debug!("shutting down failure topic handler");

        // No failure topic-specific state to clean up (producer is shared)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
    }
}

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
            FailureTopicError::Producer(error) => error.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCategory;
    use std::error::Error;
    use std::fmt::{Display, Formatter, Result as FmtResult};

    /// Test error type with configurable classification.
    #[derive(Debug, Clone)]
    struct TestError(ErrorCategory);

    impl Display for TestError {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            write!(f, "test error ({:?})", self.0)
        }
    }

    impl Error for TestError {}

    impl ClassifyError for TestError {
        fn classify_error(&self) -> ErrorCategory {
            self.0
        }
    }

    // === Error Classification Tests ===

    #[test]
    fn handler_error_delegates_classification_transient() {
        let error: FailureTopicError<TestError> =
            FailureTopicError::Handler(TestError(ErrorCategory::Transient));
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn handler_error_delegates_classification_permanent() {
        let error: FailureTopicError<TestError> =
            FailureTopicError::Handler(TestError(ErrorCategory::Permanent));
        assert!(matches!(error.classify_error(), ErrorCategory::Permanent));
    }

    #[test]
    fn handler_error_delegates_classification_terminal() {
        let error: FailureTopicError<TestError> =
            FailureTopicError::Handler(TestError(ErrorCategory::Terminal));
        assert!(matches!(error.classify_error(), ErrorCategory::Terminal));
    }

    #[test]
    fn producer_error_delegates_classification() {
        // Kafka errors with transient error codes should be classified as transient
        use rdkafka::error::{KafkaError, RDKafkaErrorCode};
        let kafka_error = KafkaError::MessageProduction(RDKafkaErrorCode::BrokerNotAvailable);
        let error: FailureTopicError<TestError> =
            FailureTopicError::Producer(ProducerError::Kafka(kafka_error));
        assert!(
            matches!(error.classify_error(), ErrorCategory::Transient),
            "BrokerNotAvailable Kafka errors should be transient"
        );
    }

    // === Configuration Tests ===

    #[test]
    fn configuration_requires_non_empty_failure_topic() {
        let config = FailureTopicConfiguration {
            failure_topic: String::new(),
        };
        assert!(
            config.validate().is_err(),
            "Empty failure topic should fail validation"
        );
    }

    #[test]
    fn configuration_accepts_valid_failure_topic() {
        let config = FailureTopicConfiguration {
            failure_topic: String::from("dlq-topic"),
        };
        assert!(
            config.validate().is_ok(),
            "Non-empty failure topic should pass validation"
        );
    }
}
