//! Dead letter queue (failure topic) middleware.
//!
//! Routes failed **messages** to a designated failure topic for later analysis
//! or reprocessing. All non-terminal errors (both [`ErrorCategory::Permanent`]
//! and [`ErrorCategory::Transient`]) are sent to the failure topic. Only
//! [`ErrorCategory::Terminal`] errors bypass the DLQ and propagate immediately,
//! as they indicate partition shutdown rather than recoverable failures.
//!
//! Timer failures are **not** routed to the failure topic. Every timer error
//! propagates as [`FailureTopicError::Handler`] with its original
//! classification — the failure-topic middleware is a no-op on the timer
//! path. Outer retry / telemetry observe the inner's classification directly.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. Pass control to inner middleware layers
//!
//! **Response Path (messages):**
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
//! For messages, this middleware is a **rescue** layer: when the inner handler
//! fails with a non-Terminal error, the failure is routed to the DLQ instead
//! of bubbling up. The inner handler is invoked **at most once per call** to
//! [`FallibleHandler::on_message`] on this middleware; the subsequent
//! `producer.send` to the failure topic is a Kafka producer call, **not**
//! another inner-handler invocation. The per-invocation invariant — exactly
//! one of `inner.after_commit` / `inner.after_abort` fires per inner
//! invocation that ran and returned — is upheld: every arm of `after_commit`
//! / `after_abort` forwards exactly one call to the inner.
//!
//! For timers, [`FallibleHandler::on_timer`] propagates every error through
//! [`FailureTopicError::Handler`] without any producer call — symmetric with
//! how the message path treats Terminal errors. Outer retry's standard policy
//! (max-retries on `Transient`, immediate pass-through on `Permanent`) handles
//! whatever classification the inner produced.
//!
//! The inner's apply hook still has to fire, chosen by whether the inner
//! will see this same logical event again; the inner's typed error is
//! preserved ([`FailureTopicOutput::Routed`] /
//! [`FailureTopicError::DlqSendFailed`]) so the hook can forward it.
//! [`FallibleHandler`] owns the general per-invocation invariant; the
//! per-arm routing matrices live on this middleware's `after_commit` /
//! `after_abort` impls on [`FailureTopicHandler`].
//!
//! # Usage
//!
//! Typically positioned between two retry layers: an inner retry re-drives the
//! handler, and an outer retry re-drives the dead-letter write. See the
//! [module docs](crate::consumer::middleware) for that worked example.
//!
//! [`ErrorCategory::Permanent`]: crate::consumer::middleware::ErrorCategory::Permanent
//! [`ErrorCategory::Transient`]: crate::consumer::middleware::ErrorCategory::Transient
//! [`ErrorCategory::Terminal`]: crate::consumer::middleware::ErrorCategory::Terminal

use chrono::SecondsFormat;
use derive_builder::Builder;
use thiserror::Error;
use tracing::{debug, error, info};
use validator::{Validate, ValidationErrors};

use crate::Codec;
use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, Record};
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
    Settlement, SettlementHandler,
};
use crate::producer::{ProducerError, ProsodyProducer};
use crate::timers::Trigger;
use crate::util::from_env;
use crate::{EventIdentity, Partition, Topic, Topic as TopicType};

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
}

impl<Enc: Codec> FailureTopicMiddleware<Enc> {
    /// Creates a new [`FailureTopicMiddleware`] with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErrors`] if `failure_topic` is empty or otherwise
    /// fails validation.
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
}

/// A handler wrapped with failure topic functionality.
#[derive(Clone, Debug)]
pub struct FailureTopicHandler<T, Enc: Codec> {
    topic: Topic,
    producer: ProsodyProducer<Enc>,
    group_id: String,
    handler: T,
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
    /// Inner handler returned a non-Terminal `Err(_)`; the DLQ producer
    /// accepted the routed message, so the marker can commit. The inner's
    /// typed error is preserved here so the apply hook can fire
    /// `inner.after_commit(Err(_))`. Only used on the message path; timer
    /// errors surface through [`FailureTopicError::Handler`] instead.
    Routed(E),
}

impl<Enc> HandlerMiddleware<Enc::Payload> for FailureTopicMiddleware<Enc>
where
    Enc: Codec,
    Enc::Payload: Clone + EventIdentity,
{
    type Provider<T>
        = FailureTopicProvider<T, Enc>
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
        }
    }
}

impl<T, Enc> FallibleHandlerProvider for FailureTopicProvider<T, Enc>
where
    T: FallibleHandlerProvider,
    Enc: Codec<Payload = <T::Handler as FallibleHandler>::Payload>,
    Enc::Payload: Clone + EventIdentity,
{
    type Handler = FailureTopicHandler<T::Handler, Enc>;

    fn handler_for_partition(&self, topic: TopicType, partition: Partition) -> Self::Handler {
        FailureTopicHandler {
            topic: self.config.failure_topic.as_str().into(),
            producer: self.producer.clone(),
            group_id: self.group_id.clone(),
            handler: self.provider.handler_for_partition(topic, partition),
        }
    }
}

impl<T, Enc> FallibleHandler for FailureTopicHandler<T, Enc>
where
    T: FallibleHandler,
    Enc: Codec<Payload = T::Payload>,
    T::Payload: Clone + EventIdentity,
{
    type Error = FailureTopicError<T::Error, Enc::Error>;
    /// Output for the DLQ middleware. The inner handler always ran when this
    /// type is produced (unlike middlewares that may short-circuit):
    /// [`FailureTopicOutput::Inner`] carries the inner's success,
    /// [`FailureTopicOutput::Routed`] preserves the rescued inner error so
    /// the apply hook can forward it — see `after_commit` / `after_abort`
    /// below. We must not collapse this to `()` — see the
    /// [`FallibleHandler`] trait-level docs.
    type Output = FailureTopicOutput<T::Output, T::Error>;
    type Payload = T::Payload;

    /// Handles a message, attempting to process it with the wrapped handler.
    /// If processing fails with a non-Terminal error, sends the message to
    /// the failure topic.
    ///
    /// Returns `Ok` wrapping a [`FailureTopicOutput`] (inner success, or a
    /// rescued non-Terminal error the DLQ accepted); fails with
    /// [`FailureTopicError::Handler`] for a Terminal inner error or
    /// [`FailureTopicError::DlqSendFailed`] when the failure-topic send
    /// fails — see the variant docs for what each preserves.
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
    {
        let topic = message.topic().as_ref();
        let partition = message.partition();
        let key = message.key();
        let offset = message.offset();

        let timestamp = message
            .timestamp()
            .to_rfc3339_opts(SecondsFormat::Millis, true);

        // Attempt to process the message with the wrapped handler.
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
        let sent = match message.record() {
            Record::Message(payload) => {
                self.producer
                    .send(headers, self.topic, key, payload.clone())
                    .await
            }
            Record::Excise => self.producer.excise(headers, self.topic, key).await,
        };
        match sent {
            // The inner attempt failed but the dispatch resolves `Ok`: the
            // `Routed` variant classifies `Bypassed` at the settle boundary,
            // so the failed attempt's dirty ops never stage and no marker
            // records — the swallow's safety is the result value itself.
            Ok(()) => Ok(FailureTopicOutput::Routed(error)),
            Err(producer) => Err(FailureTopicError::DlqSendFailed {
                inner: error,
                producer,
            }),
        }
    }

    /// Timer failures are not routed to the failure topic. `Ok(o)` becomes
    /// [`FailureTopicOutput::Inner`]; every error — Terminal, Permanent, or
    /// Transient — propagates as [`FailureTopicError::Handler`] with its
    /// original classification. Outer retry / telemetry decide what to do.
    fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        FallibleHandler::on_message(self, context, message, demand_type)
    }

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
    {
        match self.handler.on_timer(context, timer, demand_type).await {
            Ok(output) => Ok(FailureTopicOutput::Inner(output)),
            Err(error) => Err(FailureTopicError::Handler(error)),
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
        C: EventContext<Payload = T::Payload>,
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
        C: EventContext<Payload = T::Payload>,
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

impl<T, Enc> SettlementHandler for FailureTopicHandler<T, Enc>
where
    T: SettlementHandler,
    Enc: Codec<Payload = T::Payload>,
    T::Payload: Clone + EventIdentity,
{
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement {
        match result {
            // Inner ran and succeeded: its result is the dispatch's outcome.
            Ok(FailureTopicOutput::Inner(output)) => T::settlement(Ok(output)),
            // Routed to the DLQ: the outcome lives there — nothing here may
            // stage or record.
            Ok(FailureTopicOutput::Routed(_)) => Settlement::Bypassed,
            // Inner ran and its error surfaced un-rescued.
            Err(FailureTopicError::Handler(error)) => T::settlement(Err(error)),
            // Marker eligibility follows the INNER error, guarded by its
            // category, even though the retry-facing classification is the
            // producer's:
            // - a Permanent inner would have certified on its own (it is final regardless of the
            //   DLQ), so delegate its settlement;
            // - a Transient inner never certifies — the message is neither handled nor in the DLQ,
            //   so a marker here would silently filter its redelivery under a Permanent producer
            //   error. An unconditional delegate would bottom out at the leaf's `Final` and do
            //   exactly that.
            Err(FailureTopicError::DlqSendFailed { inner, .. }) => match inner.classify_error() {
                ErrorCategory::Permanent => T::settlement(Err(inner)),
                _ => Settlement::Bypassed,
            },
        }
    }
}

/// Errors that can occur during failure topic handling.
#[derive(Debug, Error)]
pub enum FailureTopicError<E, P> {
    /// Error from the wrapped handler that the middleware did not rescue.
    /// Used for any Terminal message error, every timer error, and any
    /// non-Terminal message error that does not reach the DLQ branch.
    /// Carries the inner's typed error so the apply hook can forward it.
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
        producer: ProducerError<P>,
    },
}

impl<E, P> ClassifyError for FailureTopicError<E, P>
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
mod tests;
