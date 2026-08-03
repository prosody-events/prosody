//! Answering a record that asked for a response.
//!
//! A Kafka record can name a request, a node and the subsystems it awaits. When
//! this consumer answers for one of them, the parsed tag rides the message.
//! This layer reads that tag, carries it on both result arms, and — from
//! [`after_commit`](FallibleHandler::after_commit) and nowhere else — moves the
//! typed result into a destination slot.
//!
//! **Which apply hook fired decides whether a response happens.** The layer
//! never reads an error category to make that decision, so a transient failure
//! that exhausts its retries answers its requester while the attempts before it
//! stay silent. The category rides the frame as a label only.
//!
//! [`into_responding_provider`] is the only way to build the layer, and it
//! mints the chain's leaf adapter itself. So the layer sits directly around the
//! application handler by construction, where it sees the handler's own result
//! and never runs for a dispatch the handler never reached.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the consumer wiring that hands a consumer its peer runtime is this module's \
                  production caller; every item here is exercised by this module's tests"
    )
)]

use super::providers::{FallibleCloneProvider, LeafHandler};
use super::{FallibleHandler, HandlerMiddleware, Settlement, SettlementHandler};
use crate::codec::Codec;
use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::error::{ClassifyError, ErrorCategory};
use crate::response::ResponseStatus;
use crate::response::frame::FrameCap;
use crate::response::headers::RequestTag;
use crate::response::sender::{SendCounters, TypedSender};
use crate::router::Router;
use crate::router::fleet::config::FleetConfigurationError;
use crate::subsystem::SubsystemName;
use crate::timers::Trigger;
use std::sync::Arc;
use thiserror::Error;

#[cfg(test)]
mod tests;

/// Queues one typed response for a subsystem.
///
/// Construction requires the process router. Thus, a responder cannot detach
/// from the directory and fleet that route its work.
///
/// `subsystem` must match the name that admitted the request tag. Otherwise,
/// the frame would claim a subsystem that the requester did not await.
pub(crate) struct Responder<C: Codec> {
    sender: TypedSender<C>,
    subsystem: SubsystemName,
}

/// A successful result and its optional response metadata.
///
/// The metadata rides the output because an event context has no request tag.
/// One middleware field cannot hold it because different keys dispatch at once.
pub(crate) struct Responded<T> {
    inner: T,
    meta: Option<RequestTag>,
}

/// A failed result and its optional response metadata.
///
/// A transparent error cannot hold a second field. This form preserves the
/// inner display text and exposes the inner error as its source.
#[derive(Debug, Error)]
#[error("{inner}")]
pub(crate) struct RespondError<E> {
    #[source]
    inner: E,
    meta: Option<RequestTag>,
}

/// Wraps one handler and moves its final message result to a responder.
///
/// The bounds this layer's [`FallibleHandler`] impl carries narrow the handlers
/// it accepts: the codec's payload must be `Sync + 'static`, and the error must
/// be `'static` to ride as an error source. A bare `FallibleHandler` needs
/// neither.
pub(crate) struct RespondHandler<H, C: Codec> {
    handler: H,
    responder: Arc<Responder<C>>,
}

impl<C: Codec> Responder<C> {
    /// Builds a responder from the process router and the response frame cap.
    pub(crate) fn new<R: Router>(
        router: &R,
        cap: FrameCap,
        subsystem: SubsystemName,
    ) -> Result<Self, FleetConfigurationError> {
        Ok(Self {
            sender: TypedSender::new(router, cap)?,
            subsystem,
        })
    }

    /// Waits until all queued responses finish.
    ///
    /// The process runtime owns this call. A handler must not call it during
    /// shutdown because response delivery outlives a partition.
    pub(crate) async fn drain(self) {
        self.sender.drain().await;
    }

    /// Returns the response outcome counters.
    pub(crate) fn counters(&self) -> Arc<SendCounters> {
        self.sender.counters()
    }
}

impl<H, C: Codec> Clone for RespondHandler<H, C>
where
    H: Clone,
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            responder: Arc::clone(&self.responder),
        }
    }
}

impl<H, C: Codec> RespondHandler<H, C> {
    fn new(handler: H, responder: Arc<Responder<C>>) -> Self {
        Self { handler, responder }
    }
}

impl<E: ClassifyError> ClassifyError for RespondError<E> {
    /// This delegation is what keeps every retry, defer, settlement and marker
    /// decision exactly what it was without the layer.
    fn classify_error(&self) -> ErrorCategory {
        self.inner.classify_error()
    }
}

impl<H, C> FallibleHandler for RespondHandler<H, C>
where
    H: FallibleHandler,
    H::Output: Sync + 'static,
    H::Error: Sync + 'static,
    C: Codec<Payload = Result<H::Output, H::Error>>,
{
    type Error = RespondError<H::Error>;
    type Output = Responded<H::Output>;
    type Payload = H::Payload;

    async fn on_message<C2>(
        &self,
        context: C2,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C2: EventContext<Payload = Self::Payload>,
    {
        let meta = message.request();
        self.handler
            .on_message(context, message, demand_type)
            .await
            .map(|inner| Responded { inner, meta })
            .map_err(|inner| RespondError { inner, meta })
    }

    /// Forwards a timer without response metadata.
    ///
    /// A trigger has no headers, so this path cannot construct response
    /// metadata. A timer dispatch therefore cannot respond.
    ///
    /// A deferred message reload enters the defer layer through a timer. That
    /// layer dispatches the reloaded value as a message, so the reload
    /// responds.
    async fn on_timer<C2>(
        &self,
        context: C2,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C2: EventContext<Payload = Self::Payload>,
    {
        self.handler
            .on_timer(context, trigger, demand_type)
            .await
            .map(|inner| Responded { inner, meta: None })
            .map_err(|inner| RespondError { inner, meta: None })
    }

    /// Queues a requested response after a final invocation.
    ///
    /// When the sender takes the result, the inner hook does not run. The
    /// response becomes the disposition of that value.
    ///
    /// A worker can later drop a queued response. In that case, neither the
    /// wire nor the inner hook receives it. Apply hooks are best-effort, so
    /// nothing above depends on either one running.
    async fn after_commit<C2>(&self, context: C2, result: Result<Self::Output, Self::Error>)
    where
        C2: EventContext<Payload = Self::Payload>,
    {
        let (result, meta) = split(result);
        let Some(meta) = meta else {
            return self.handler.after_commit(context, result).await;
        };
        let header = meta.header(self.responder.subsystem.clone(), status(&result));
        // Nothing is encoded here. The hook moves the typed result into the
        // slot. The worker encodes it against its own scratch.
        if let Err(rejected) = self.responder.sender.send(header, result) {
            // Nothing was sent or encoded. The handler still owns the result.
            self.handler.after_commit(context, rejected.payload).await;
        }
    }

    /// Forwards a non-final invocation's result to the inner hook.
    ///
    /// Another invocation is coming for this event, so nothing is answered
    /// here. The tag is dropped with the carrier, and this arm reaches no
    /// sender at all.
    async fn after_abort<C2>(&self, context: C2, result: Result<Self::Output, Self::Error>)
    where
        C2: EventContext<Payload = Self::Payload>,
    {
        let (result, _tag) = split(result);
        self.handler.after_abort(context, result).await;
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}

impl<H, C> SettlementHandler for RespondHandler<H, C>
where
    H: SettlementHandler,
    H::Output: Sync + 'static,
    H::Error: Sync + 'static,
    C: Codec<Payload = Result<H::Output, H::Error>>,
{
    /// Delegates the inner classification.
    ///
    /// An unconditional final answer would discard an inner wrapper's
    /// classification.
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement {
        H::settlement(
            result
                .map(|value| &value.inner)
                .map_err(|error| &error.inner),
        )
    }
}

/// Terminates a middleware stack with a responding application handler.
///
/// This is the mirror of [`HandlerMiddleware::into_provider`], and the only way
/// to build the layer. It mints the chain's leaf adapter itself, so the layer
/// always sits directly outside the application handler.
///
/// The layer wraps that adapter rather than the raw handler, because only the
/// adapter classifies a result for the settlement boundary. The other nesting
/// would leave the layer with nothing to delegate to.
///
/// The common middleware block deliberately excludes this layer: a consumer
/// answers peer requests or it does not, while every member of that block is
/// mandatory.
pub(crate) fn responding_provider<M, H, C>(
    middleware: &M,
    handler: H,
    responder: Arc<Responder<C>>,
) -> M::Provider<FallibleCloneProvider<RespondHandler<LeafHandler<H>, C>>>
where
    M: HandlerMiddleware<H::Payload>,
    H: FallibleHandler + Clone + Send + Sync + 'static,
    H::Output: Sync + 'static,
    H::Error: Sync + 'static,
    C: Codec<Payload = Result<H::Output, H::Error>>,
{
    middleware.with_provider(FallibleCloneProvider::new(RespondHandler::new(
        LeafHandler::new(handler),
        responder,
    )))
}

/// Takes one result apart into the inner handler's own result and the tag the
/// carriers held.
fn split<O, E>(
    result: Result<Responded<O>, RespondError<E>>,
) -> (Result<O, E>, Option<RequestTag>) {
    match result {
        Ok(Responded { inner, meta }) => (Ok(inner), meta),
        Err(RespondError { inner, meta }) => (Err(inner), meta),
    }
}

/// The label one result puts on its frame.
///
/// The category labels a frame and never gates a send. Which apply hook fired
/// decides whether a response happens at all. A category test here would turn
/// retry exhaustion into a silent timeout for the requester.
fn status<O, E: ClassifyError>(result: &Result<O, E>) -> ResponseStatus {
    match result {
        Ok(_) => ResponseStatus::Success,
        Err(error) => ResponseStatus::Error(error.classify_error()),
    }
}
