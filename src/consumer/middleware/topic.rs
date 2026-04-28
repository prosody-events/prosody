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
//! 2. **If error is terminal**: Pass through unchanged as
//!    [`FailureTopicError::Handler`] (triggers partition shutdown)
//! 3. **If error is permanent or transient**: Send message to failure topic
//!    with metadata. On success, surface the inner's error in
//!    [`FailureTopicOutput::Routed`] so the apply hook can forward it.
//! 4. **If failure topic write fails**: Surface
//!    [`FailureTopicError::DlqSendFailed`] (which carries both the inner error
//!    and the producer error) for outer retry middleware
//!
//! # Failure Topic Message Format
//!
//! Messages sent to the failure topic include:
//! - **Original message**: Complete original payload and headers
//! - **Error metadata**: Error message, timestamp, source topic/partition
//! - **Consumer metadata**: Group ID and processing context
//! - **Correlation ID**: For tracking and debugging
//!
//! # Apply hooks (work-centric invariant)
//!
//! This middleware is a **rescue** layer: when the inner handler fails with a
//! non-Terminal error, the failure is routed to the DLQ instead of bubbling
//! up. The inner handler is invoked **at most once per call** to
//! [`FallibleHandler::on_message`] / [`FallibleHandler::on_timer`] on this
//! middleware; the subsequent `producer.send` to the failure topic is a
//! Kafka producer call, **not** another inner-handler invocation. The
//! per-invocation invariant — exactly one of `inner.after_commit` /
//! `inner.after_abort` fires per inner invocation that ran and returned —
//! is upheld: every arm of `after_commit` / `after_abort` forwards exactly
//! one call to the inner.
//!
//! The inner's [`FallibleHandler::after_commit`] /
//! [`FallibleHandler::after_abort`] hook still has to fire — and the choice
//! of hook is driven by whether the inner will see this same logical
//! message/timer again:
//!
//! - **Inner Ok**: forward `after_commit(Ok(inner_output))` /
//!   `after_abort(Ok(inner_output))` per the outer's marker outcome.
//! - **Inner Terminal Err**: forward the typed error through whichever apply
//!   hook the outer resolves (normally `after_abort` since Terminal aborts).
//! - **Inner non-Terminal Err, DLQ ack**: the marker commits, the inner will
//!   not be re-dispatched. The inner's typed error is preserved in the
//!   `Output::Routed(inner_err)` variant and forwarded as
//!   `after_commit(Err(inner_err))`.
//! - **Inner non-Terminal Err, DLQ send fails**: the produced error is surfaced
//!   as [`FailureTopicError::DlqSendFailed`], which carries both the producer
//!   error (used by the outer retry layer for classification) and the original
//!   inner error. When the outer retry re-drives the stack, this dispatch is
//!   *not* final from the inner's POV, so the inner sees
//!   `after_abort(Err(inner_err))`.
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
//! #     type Payload = serde_json::Value;
//! #     type Error = Infallible;
//! #     type Output = ();
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage<serde_json::Value>, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
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

use std::marker::PhantomData;

use chrono::{DateTime, SecondsFormat, Utc};
use derive_builder::Builder;
use thiserror::Error;
use tracing::{debug, error, info};
use validator::{Validate, ValidationErrors};

use crate::Codec;
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
use crate::{EventIdentity, Partition, TimerReplayPayload, Topic, Topic as TopicType};

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
pub struct FailureTopicMiddleware<Enc: Codec = crate::JsonCodec> {
    config: FailureTopicConfiguration,
    producer: ProsodyProducer<Enc>,
    group_id: String,
    _codec: PhantomData<fn() -> Enc>,
}

impl<Enc: Codec> FailureTopicMiddleware<Enc> {
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
        producer: ProsodyProducer<Enc>,
    ) -> Result<Self, ValidationErrors> {
        config.validate()?;
        Ok(Self {
            config,
            producer,
            group_id,
            _codec: PhantomData,
        })
    }
}

/// A provider that wraps handlers with failure topic functionality.
#[derive(Clone, Debug)]
pub struct FailureTopicProvider<T, Enc: Codec> {
    provider: T,
    config: FailureTopicConfiguration,
    producer: ProsodyProducer<Enc>,
    group_id: String,
    _codec: PhantomData<fn() -> Enc>,
}

/// A handler wrapped with failure topic functionality.
#[derive(Clone, Debug)]
pub struct FailureTopicHandler<T, Enc: Codec> {
    topic: Topic,
    producer: ProsodyProducer<Enc>,
    group_id: String,
    handler: T,
    _codec: PhantomData<fn() -> Enc>,
}

/// Outcome of a [`FailureTopicHandler`] dispatch.
///
/// The inner handler always ran by the time this value is constructed; the
/// variant records whether the inner succeeded or whether it failed with a
/// non-Terminal error that was rescued by routing to the failure topic.
///
/// The inner's typed error is preserved on the `Routed` path so that the
/// apply hook can forward `Err(inner_err)` to the inner per the
/// work-centric invariant on [`FallibleHandler`].
#[derive(Clone, Debug)]
pub enum FailureTopicOutput<O, E> {
    /// Inner handler ran and returned `Ok(output)`.
    Inner(O),
    /// Inner handler returned a non-Terminal `Err(_)`; the failure was
    /// rescued by routing the message/timer to the failure topic, which the
    /// producer accepted. The inner's typed error is preserved here so the
    /// apply hook can fire `inner.after_commit(Err(_))`.
    Routed(E),
}

impl<Enc> HandlerMiddleware for FailureTopicMiddleware<Enc>
where
    Enc: Codec,
    Enc::Payload: TimerReplayPayload + EventIdentity,
{
    type Payload = Enc::Payload;

    type Provider<T> = FailureTopicProvider<T, Enc>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = Enc::Payload>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = Enc::Payload>,
    {
        FailureTopicProvider {
            provider,
            config: self.config.clone(),
            producer: self.producer.clone(),
            group_id: self.group_id.clone(),
            _codec: PhantomData,
        }
    }
}

impl<T, Enc> FallibleHandlerProvider for FailureTopicProvider<T, Enc>
where
    T: FallibleHandlerProvider,
    Enc: Codec<Payload = <T::Handler as FallibleHandler>::Payload>,
    Enc::Payload: TimerReplayPayload + EventIdentity,
{
    type Handler = FailureTopicHandler<T::Handler, Enc>;

    fn handler_for_partition(&self, topic: TopicType, partition: Partition) -> Self::Handler {
        FailureTopicHandler {
            topic: self.config.failure_topic.as_str().into(),
            producer: self.producer.clone(),
            group_id: self.group_id.clone(),
            handler: self.provider.handler_for_partition(topic, partition),
            _codec: PhantomData,
        }
    }
}

impl<T, Enc> FallibleHandler for FailureTopicHandler<T, Enc>
where
    T: FallibleHandler,
    Enc: Codec<Payload = T::Payload>,
    Enc::Payload: TimerReplayPayload + EventIdentity,
{
    type Error = FailureTopicError<T::Error>;
    /// Output for the DLQ middleware. The inner handler always ran when this
    /// type is produced (unlike middlewares that may short-circuit), so the
    /// variants encode whether the inner succeeded or whether its
    /// non-Terminal error was rescued by routing to the failure topic.
    ///
    /// - [`FailureTopicOutput::Inner`] — the inner returned `Ok(_)`; carries
    ///   the inner's [`FallibleHandler::Output`] for downstream 2PC.
    /// - [`FailureTopicOutput::Routed`] — the inner returned a non-Terminal
    ///   `Err(_)` and the DLQ producer accepted the routed message. The inner's
    ///   typed error is **preserved** here so the apply hook can forward it to
    ///   the inner as `Err(_)` per the [`FallibleHandler`] work-centric
    ///   invariant. We must not collapse this to `()` — see the trait-level
    ///   docs.
    type Output = FailureTopicOutput<T::Output, T::Error>;
    type Payload = T::Payload;

    /// Handles a message, attempting to process it with the wrapped handler.
    /// If processing fails with a non-Terminal error, sends the message to
    /// the failure topic.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The message to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` whose variants are:
    /// - `Ok(FailureTopicOutput::Inner(output))` — inner ran and succeeded.
    /// - `Ok(FailureTopicOutput::Routed(inner_err))` — inner ran and returned a
    ///   non-Terminal error; DLQ producer accepted the routed message. The
    ///   inner error is preserved for the apply hook.
    /// - `Err(FailureTopicError::Handler(inner_err))` — inner returned a
    ///   Terminal error.
    /// - `Err(FailureTopicError::DlqSendFailed { inner, producer })` — inner
    ///   returned a non-Terminal error, but routing to the failure topic
    ///   failed. Both the inner error and the producer error are preserved so
    ///   the outer retry layer can classify on `producer` and the inner's apply
    ///   hook can fire as `after_abort(Err(inner))` on re-dispatch.
    ///
    /// # Errors
    ///
    /// See `Returns` above for the error variants.
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
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
            Ok(output) => return Ok(FailureTopicOutput::Inner(output)),
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

        // Send the failed message to the failure topic. On failure, surface
        // BOTH the inner handler error and the producer error so the inner's
        // apply hook can fire on outer-retry re-dispatch.
        match self
            .producer
            .send(headers, self.topic, key, message.payload())
            .await
        {
            Ok(()) => Ok(FailureTopicOutput::Routed(error)),
            Err(producer) => Err(FailureTopicError::DlqSendFailed {
                inner: error,
                producer,
            }),
        }
    }

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        // Attempt to process the timer with the wrapped handler
        let error = match self
            .handler
            .on_timer(context, timer.clone(), demand_type)
            .await
        {
            Ok(output) => return Ok(FailureTopicOutput::Inner(output)),
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
        let payload = Enc::Payload::timer_replay(key_str, &timestamp);

        // Send the failed timer event to the failure topic. On failure,
        // surface BOTH the inner handler error and the producer error so the
        // inner's apply hook can fire on outer-retry re-dispatch.
        match self
            .producer
            .send(headers, self.topic, key_str, &payload)
            .await
        {
            Ok(()) => Ok(FailureTopicOutput::Routed(error)),
            Err(producer) => Err(FailureTopicError::DlqSendFailed {
                inner: error,
                producer,
            }),
        }
    }

    /// Resolves the inner's apply hook on a **committed** marker.
    ///
    /// Routing per the work-centric invariant:
    /// - `Ok(Inner(o))` → `inner.after_commit(Ok(o))`. Inner ran, succeeded;
    ///   dispatch is final.
    /// - `Ok(Routed(e))` → `inner.after_commit(Err(e))`. DLQ accepted, the
    ///   marker committed, the inner will not see this logical message/timer
    ///   again — fire its apply hook with its original error.
    /// - `Err(Handler(e))` → `inner.after_commit(Err(e))`. Terminal error that
    ///   the framework chose to commit (rather than abort); forward it to the
    ///   inner.
    /// - `Err(DlqSendFailed { inner, .. })` → `inner.after_commit(Err(inner))`.
    ///   This branch only fires if the outer treats the producer error as final
    ///   (no retry); the inner's typed error is still forwarded so 2PC handlers
    ///   further down can finalise correctly.
    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        match result {
            Ok(FailureTopicOutput::Inner(output)) => {
                self.handler.after_commit(context, Ok(output)).await;
            }
            Ok(FailureTopicOutput::Routed(inner))
            | Err(
                FailureTopicError::Handler(inner) | FailureTopicError::DlqSendFailed { inner, .. },
            ) => {
                self.handler.after_commit(context, Err(inner)).await;
            }
        }
    }

    /// Resolves the inner's apply hook on an **aborted** marker.
    ///
    /// Routing per the work-centric invariant:
    /// - `Ok(Inner(o))` → `inner.after_abort(Ok(o))`. Inner succeeded but the
    ///   outer aborted (e.g. shutdown intervened); forward Ok.
    /// - `Ok(Routed(e))` → `inner.after_abort(Err(e))`. Rare path: the outer
    ///   aborted despite the DLQ accepting the routed message; re-dispatch is
    ///   coming, so the inner sees abort with its original error.
    /// - `Err(Handler(e))` → `inner.after_abort(Err(e))`. Terminal error;
    ///   marker aborted.
    /// - `Err(DlqSendFailed { inner, .. })` → `inner.after_abort(Err(inner))`.
    ///   The outer retry layer will re-drive the whole stack including the
    ///   inner; the inner's apply hook fires as `after_abort` with its original
    ///   error.
    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        match result {
            Ok(FailureTopicOutput::Inner(output)) => {
                self.handler.after_abort(context, Ok(output)).await;
            }
            Ok(FailureTopicOutput::Routed(inner))
            | Err(
                FailureTopicError::Handler(inner) | FailureTopicError::DlqSendFailed { inner, .. },
            ) => {
                self.handler.after_abort(context, Err(inner)).await;
            }
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
    /// Error from the wrapped handler that the middleware did not rescue
    /// (e.g. a Terminal error). Carries the inner's typed error so the
    /// apply hook can forward it.
    #[error(transparent)]
    Handler(E),

    /// The wrapped handler returned a non-Terminal error and the producer
    /// failed to accept the routed message.
    ///
    /// Both errors are preserved so the framework can:
    /// - classify on `producer` (the immediate failure that the outer retry
    ///   layer should react to), and
    /// - fire the inner's apply hook with `Err(inner)` when re-dispatch happens
    ///   (`after_abort(Err(inner))`) or, in the unlikely case the outer commits
    ///   despite this error, `after_commit(Err(inner))`.
    #[error("failure-topic send failed: {producer}")]
    DlqSendFailed {
        /// Inner handler's original (non-Terminal) error.
        inner: E,
        /// Producer error from the failure-topic send.
        #[source]
        producer: ProducerError,
    },
}

impl<E> ClassifyError for FailureTopicError<E>
where
    E: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            FailureTopicError::Handler(error) => error.classify_error(),
            // Outer retry layers should react to the producer-level failure
            // (e.g. transient broker errors) rather than the inner's
            // classification; the inner error is only carried through for
            // apply-hook forwarding.
            FailureTopicError::DlqSendFailed { producer, .. } => producer.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::middleware::test_support::MockEventContext;
    use crate::error::ErrorCategory;
    use crate::producer::ProducerConfiguration;
    use crate::telemetry::Telemetry;
    use parking_lot::Mutex;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use std::error::Error;
    use std::fmt::{Display, Formatter, Result as FmtResult};
    use std::sync::Arc;

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
    fn dlq_send_failed_classifies_by_producer_error() {
        // Kafka errors with transient error codes should be classified as transient.
        // `DlqSendFailed` must classify on the producer error, not the inner
        // handler error, so the outer retry layer reacts to the producer-level
        // failure.
        let kafka_error = KafkaError::MessageProduction(RDKafkaErrorCode::BrokerNotAvailable);
        let error: FailureTopicError<TestError> = FailureTopicError::DlqSendFailed {
            // Inner is Permanent, but we expect the classification to follow
            // the producer error (Transient), not the inner.
            inner: TestError(ErrorCategory::Permanent),
            producer: ProducerError::Kafka(kafka_error),
        };
        assert!(
            matches!(error.classify_error(), ErrorCategory::Transient),
            "DlqSendFailed should classify by the producer error, not the inner",
        );
    }

    // === Apply-hook wiring tests ===
    //
    // These tests construct a `FailureTopicHandler` directly and drive its
    // apply hooks with synthetic `Result<Output, Error>` values. We are not
    // exercising the full dispatch path here — we only verify that each
    // arm of the new routing matrix forwards the correct
    // `Result<inner::Output, inner::Error>` to the inner handler.

    /// Records the inner-result a probe handler observes in each apply hook.
    #[derive(Debug, Clone)]
    enum InnerHookEvent {
        Commit(Result<u64, TestError>),
        Abort(Result<u64, TestError>),
    }

    /// Probe inner handler that records every apply-hook call.
    #[derive(Clone)]
    struct ProbeInner {
        log: Arc<Mutex<Vec<InnerHookEvent>>>,
    }

    impl ProbeInner {
        fn new() -> Self {
            Self {
                log: Arc::default(),
            }
        }
    }

    impl FallibleHandler for ProbeInner {
        type Error = TestError;
        type Output = u64;
        type Payload = serde_json::Value;

        async fn on_message<C>(
            &self,
            _context: C,
            _message: ConsumerMessage<Self::Payload>,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext,
        {
            // Apply-hook tests never invoke this path.
            Ok(0)
        }

        async fn on_timer<C>(
            &self,
            _context: C,
            _trigger: Trigger,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext,
        {
            Ok(0)
        }

        async fn after_commit<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
        where
            C: EventContext,
        {
            self.log.lock().push(InnerHookEvent::Commit(result));
        }

        async fn after_abort<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
        where
            C: EventContext,
        {
            self.log.lock().push(InnerHookEvent::Abort(result));
        }

        async fn shutdown(self) {}
    }

    /// Constructs a `FailureTopicHandler` over a probe inner using a mock
    /// producer (no real Kafka connection required).
    fn make_handler(
        inner: ProbeInner,
    ) -> color_eyre::Result<FailureTopicHandler<ProbeInner, crate::JsonCodec>> {
        // The mock-flag short-circuits the bootstrap-server lookup, but the
        // builder still validates the field, so we supply a sentinel value
        // along with a non-empty source system.
        let config = ProducerConfiguration::builder()
            .bootstrap_servers(vec!["mock:9092".to_owned()])
            .source_system("test")
            .mock(true)
            .build()?;
        let telemetry = Telemetry::default();
        let producer = ProsodyProducer::new(&config, telemetry.sender())?;
        Ok(FailureTopicHandler {
            topic: "dlq".into(),
            producer,
            group_id: "group".to_owned(),
            handler: inner,
            _codec: PhantomData,
        })
    }

    fn dlq_send_failed_err(category: ErrorCategory) -> FailureTopicError<TestError> {
        FailureTopicError::DlqSendFailed {
            inner: TestError(category),
            producer: ProducerError::Kafka(KafkaError::MessageProduction(
                RDKafkaErrorCode::BrokerNotAvailable,
            )),
        }
    }

    #[tokio::test]
    async fn after_commit_routed_forwards_inner_err_to_inner() -> color_eyre::Result<()> {
        // DLQ accepted: marker commits, the inner will not be re-dispatched,
        // so the inner's apply hook MUST fire as `after_commit(Err(inner))`.
        let inner = ProbeInner::new();
        let log = inner.log.clone();
        let handler = make_handler(inner)?;

        let result: Result<FailureTopicOutput<u64, TestError>, FailureTopicError<TestError>> = Ok(
            FailureTopicOutput::Routed(TestError(ErrorCategory::Permanent)),
        );
        handler.after_commit(MockEventContext::new(), result).await;

        let events = log.lock().clone();
        assert_eq!(events.len(), 1, "exactly one inner hook should fire");
        assert!(
            matches!(
                &events[0],
                InnerHookEvent::Commit(Err(TestError(ErrorCategory::Permanent))),
            ),
            "expected Commit(Err(Permanent)), got {:?}",
            events[0],
        );
        Ok(())
    }

    #[tokio::test]
    async fn after_commit_inner_ok_forwards_inner_output() -> color_eyre::Result<()> {
        let inner = ProbeInner::new();
        let log = inner.log.clone();
        let handler = make_handler(inner)?;

        let result: Result<FailureTopicOutput<u64, TestError>, FailureTopicError<TestError>> =
            Ok(FailureTopicOutput::Inner(42));
        handler.after_commit(MockEventContext::new(), result).await;

        let events = log.lock().clone();
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], InnerHookEvent::Commit(Ok(42))),
            "inner Ok output should be forwarded to inner.after_commit",
        );
        Ok(())
    }

    #[tokio::test]
    async fn after_commit_dlq_send_failed_forwards_inner_err() -> color_eyre::Result<()> {
        // Even though `DlqSendFailed` would normally route through the outer
        // retry layer (and thus surface to the inner via `after_abort`),
        // when the framework decides the marker commits anyway, we still
        // owe the inner a typed `Err(inner)` here.
        let inner = ProbeInner::new();
        let log = inner.log.clone();
        let handler = make_handler(inner)?;

        let result: Result<FailureTopicOutput<u64, TestError>, FailureTopicError<TestError>> =
            Err(dlq_send_failed_err(ErrorCategory::Transient));
        handler.after_commit(MockEventContext::new(), result).await;

        let events = log.lock().clone();
        assert_eq!(events.len(), 1);
        assert!(
            matches!(
                &events[0],
                InnerHookEvent::Commit(Err(TestError(ErrorCategory::Transient))),
            ),
            "expected Commit(Err(Transient)), got {:?}",
            events[0],
        );
        Ok(())
    }

    #[tokio::test]
    async fn after_abort_dlq_send_failed_forwards_inner_err() -> color_eyre::Result<()> {
        // Outer retry path: the producer error fired the outer retry, the
        // whole stack will be re-dispatched, so the inner sees
        // `after_abort(Err(inner))`.
        let inner = ProbeInner::new();
        let log = inner.log.clone();
        let handler = make_handler(inner)?;

        let result: Result<FailureTopicOutput<u64, TestError>, FailureTopicError<TestError>> =
            Err(dlq_send_failed_err(ErrorCategory::Permanent));
        handler.after_abort(MockEventContext::new(), result).await;

        let events = log.lock().clone();
        assert_eq!(events.len(), 1);
        assert!(
            matches!(
                &events[0],
                InnerHookEvent::Abort(Err(TestError(ErrorCategory::Permanent))),
            ),
            "expected Abort(Err(Permanent)), got {:?}",
            events[0],
        );
        Ok(())
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
