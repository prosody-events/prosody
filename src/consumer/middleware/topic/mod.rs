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
use tracing::{debug, error, info};
use validator::{Validate, ValidationErrors};

use crate::Codec;

mod error;

use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
    Settlement, SettlementHandler,
};
use crate::producer::{ProducerError, ProsodyProducer};
use crate::timers::Trigger;
use crate::util::from_env;
use crate::{EventIdentity, Partition, Topic, Topic as TopicType};
pub use error::FailureTopicError;

const MESSAGE_SOURCE_KIND: &str = "message";
const EXCISE_SOURCE_KIND: &str = "excise";

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

mod handler;

#[cfg(test)]
mod tests;
