//! Answering a record that asked for a response.
//!
//! A Kafka record can name a request, a node and the subsystems it awaits. When
//! this consumer answers for one of them, the parsed tag rides the message.
//! This layer reads that tag and the record's own trace into an [`Answering`],
//! carries it on both result arms, and — from
//! [`after_commit`](FallibleHandler::after_commit) and nowhere else — moves
//! the typed result into a destination slot.
//!
//! **A dispatch that never reaches the final apply hook is never answered.**
//! The layer never reads an error category to make that decision, so a
//! transient failure that exhausts its retries answers its requester while the
//! attempts before it stay silent. The category rides the frame as a label
//! only.
//!
//! Three live paths commit a tagged record and answer nothing. The dedup layer
//! fires no inner hook for a duplicate, so a redelivery of an answered record
//! stays silent and its requester waits out its own deadline. A send this layer
//! cannot queue — the fleet refuses a slot, or the destination's queue is full
//! — returns the result to the inner hook instead. A crash between the durable
//! commit and this hook loses the answer, and the marker suppresses the
//! redelivery. A requester therefore always needs its own deadline.
//!
//! One live path answers a record whose keyed-state writes did not last. A
//! permanently rejected stage still commits and still fires this hook, so the
//! answer leaves with the label the handler's own result gives. The label
//! reports the handler result, never the durability of the writes behind it.
//!
//! [`responding_provider`] is the only way to build the layer from outside this
//! module, and it states what that placement guarantees. Its production caller
//! is
//! [`PreparedResponder::terminate`](crate::consumer::wiring::peer::PreparedResponder::terminate),
//! which a responding consumer reaches through its own entry point.

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
#[cfg(test)]
use crate::response::sender::SendCounters;
use crate::response::sender::{ResponseWorkers, TypedSender};
#[cfg(test)]
use crate::router::Router;
use crate::router::directory::NodeDirectory;
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfigurationError;
use crate::router::{LocalTarget, ResponseSender, RouterHandle};
use crate::subsystem::SubsystemName;
use crate::timers::Trigger;
use opentelemetry::Context;
use std::sync::Arc;
use thiserror::Error;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[cfg(test)]
mod tests;

/// Queues one typed response for a subsystem.
///
/// Construction requires the process router. Thus, a responder cannot detach
/// from the directory and fleet that route its work.
///
/// `subsystem` is the name this consumer answers peer requests for. The decode
/// path admits a request tag for one name, and it reads that name back from
/// this responder through
/// [`PeerAttachment::responder`](crate::consumer::wiring::peer::PeerAttachment::responder).
/// So the name a record is admitted by and the name its answer claims are one
/// value.
pub(crate) struct Responder<C: Codec> {
    sender: TypedSender<C>,
    subsystem: SubsystemName,
}

/// The request one message asked this consumer to answer, and the trace that
/// request belongs to.
///
/// Both are read from the message rather than from the invocation that answers
/// it. A deferred reload answers the request its own record names, in that
/// record's trace, whatever span the settle boundary later runs under — and by
/// default a timer dispatch starts a trace of its own, so an ambient context
/// would answer outside the requester's trace.
///
/// A [`Context`] rather than a [`Span`](tracing::Span): the response is framed
/// in one task and sent from another, and a span finishes when any clone of it
/// finishes.
///
/// The capture happens while the message's processing state is still live, so
/// `message.span()` is never `Span::none()` and the response leg can never open
/// as a root of a trace of its own.
///
/// One per in-flight event, and both exits are here:
/// [`after_commit`](FallibleHandler::after_commit) moves it into the sender,
/// and [`after_abort`](FallibleHandler::after_abort) drops it.
#[derive(Debug)]
pub(crate) struct Answering {
    tag: RequestTag,
    trace: Context,
}

/// A successful result and the [`Answering`] carrier, when the message asked
/// for a response.
///
/// The carrier rides the result because an event context has no request tag,
/// and one middleware field cannot hold it because different keys dispatch at
/// once.
pub(crate) struct Responded<T> {
    inner: T,
    meta: Option<Answering>,
}

/// A failed result and the same [`Answering`] carrier.
///
/// A transparent error cannot hold a second field. This form preserves the
/// inner display text and exposes the inner error as its source.
///
/// This sits beside [`Responded`] rather than at the end of the file: the two
/// are one pair, the arms of one result.
#[derive(Debug, Error)]
#[error("{inner}")]
pub(crate) struct RespondError<E> {
    #[source]
    inner: E,
    meta: Option<Answering>,
}

/// Wraps one handler and moves its final message result to a responder.
///
/// The bounds this layer's [`FallibleHandler`] impl carries narrow the handlers
/// it accepts. The codec's payload is the handler's own `Result`, and a
/// [`Codec::Payload`] must be `Send + Sync + 'static`. So both halves of that
/// result need `Sync + 'static`, which a bare `FallibleHandler` does not.
pub(crate) struct RespondHandler<H, C: Codec> {
    handler: H,
    responder: Arc<Responder<C>>,
}

impl<C: Codec> Responder<C> {
    /// Builds a responder from the process router and the response frame cap.
    ///
    /// Returns the responder and its [`ResponseWorkers`], which owns the order
    /// the caller must join them in.
    #[cfg(test)]
    pub(crate) fn new_without_local<R: Router>(
        router: &R,
        cap: FrameCap,
        subsystem: SubsystemName,
    ) -> Result<(Self, ResponseWorkers), FleetConfigurationError> {
        let (sender, workers) = TypedSender::new_without_local(router, cap)?;
        Ok((Self { sender, subsystem }, workers))
    }

    /// Builds a responder that deposits same-node responses without gRPC.
    pub(crate) fn new<S: ResponseSender, D: NodeDirectory>(
        router: &RouterHandle<S, D>,
        cap: FrameCap,
        subsystem: SubsystemName,
    ) -> Result<(Self, ResponseWorkers), FleetConfigurationError> {
        let (sender, workers) = TypedSender::new(router, cap)?;
        Ok((Self { sender, subsystem }, workers))
    }

    /// Builds a responder that can only deposit same-node responses.
    pub(crate) fn new_local(
        local: &LocalTarget,
        fleet: &Arc<DestinationFleet>,
        cap: FrameCap,
        subsystem: SubsystemName,
    ) -> Result<(Self, ResponseWorkers), FleetConfigurationError> {
        let (sender, workers) = TypedSender::new_local(local, fleet, cap)?;
        Ok((Self { sender, subsystem }, workers))
    }

    /// The name this responder answers for, and the one the decode path admits.
    pub(crate) fn subsystem(&self) -> &SubsystemName {
        &self.subsystem
    }

    /// Returns the response outcome counters.
    #[cfg(test)]
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
        // The message's own span, not the ambient one: a middleware span
        // between the dispatch and this layer must not become the response's
        // parent.
        let meta = message.request().map(|tag| Answering {
            tag,
            trace: message.span().context(),
        });
        // Matched rather than mapped: a `map` / `map_err` pair would move the
        // carrier into two closures.
        match self.handler.on_message(context, message, demand_type).await {
            Ok(inner) => Ok(Responded { inner, meta }),
            Err(inner) => Err(RespondError { inner, meta }),
        }
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
        let (result, meta) = match result {
            Ok(Responded { inner, meta }) => (Ok(inner), meta),
            Err(RespondError { inner, meta }) => (Err(inner), meta),
        };
        let Some(Answering { tag, trace }) = meta else {
            return self.handler.after_commit(context, result).await;
        };
        let header = tag.header(self.responder.subsystem().clone(), status(&result));
        // Nothing is encoded here. The hook moves the typed result into the
        // slot. The worker encodes it against its own scratch.
        if let Err(payload) = self.responder.sender.send(header, trace, result) {
            // Nothing was sent or encoded. The handler still owns the result.
            self.handler.after_commit(context, payload).await;
        }
    }

    /// Forwards a non-final invocation's result to the inner hook.
    ///
    /// Another invocation is coming for this event, so nothing is answered
    /// here. This arm drops the tag with the carrier and never binds it, so it
    /// holds nothing a frame header could be built from.
    async fn after_abort<C2>(&self, context: C2, result: Result<Self::Output, Self::Error>)
    where
        C2: EventContext<Payload = Self::Payload>,
    {
        let result = match result {
            Ok(Responded { inner, .. }) => Ok(inner),
            Err(RespondError { inner, .. }) => Err(inner),
        };
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
/// to build the layer from outside this module. It mints the chain's leaf
/// adapter itself, so the layer always sits directly outside the application
/// handler.
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

/// The label one result puts on its frame.
///
/// The category labels a frame and never gates a send. A category test here
/// would turn retry exhaustion into a silent timeout for the requester.
fn status<O, E: ClassifyError>(result: &Result<O, E>) -> ResponseStatus {
    match result {
        Ok(_) => ResponseStatus::Success,
        Err(error) => ResponseStatus::Error(error.classify_error()),
    }
}
