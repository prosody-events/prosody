//! Delivering a handler result that a Kafka record requested.
//!
//! A Kafka record can name a request, a peer and the subsystems it awaits. When
//! this consumer answers for one of them, the parsed request rides the message.
//! This layer reads that request and the record's own trace into an
//! [`ResultDelivery`], carries it on both result arms, and — from
//! [`after_commit`](FallibleHandler::after_commit) and nowhere else — moves
//! the typed result to the response route.
//!
//! **A dispatch that never reaches the final apply hook is never answered.**
//! The layer never reads an error category to make that decision, so a
//! transient failure that exhausts its retries answers its requester while the
//! attempts before it stay silent. A failed result carries its category and
//! display text in Prosody's protocol.
//!
//! Two live paths commit a requesting record and answer nothing. Dedup fires no
//! inner hook for a duplicate, so its requester waits out its deadline. A crash
//! between the durable commit and this hook loses the answer. The marker
//! suppresses redelivery. A requester therefore always needs a deadline.
//!
//! One live path answers a record whose keyed-state writes did not last. A
//! permanently rejected stage still commits and still fires this hook, so the
//! answer carries the handler's own result. It never reports the durability of
//! the writes behind it.
use super::{FallibleHandler, Settlement, SettlementHandler};
use crate::codec::Codec;
use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::error::{ClassifyError, ErrorCategory};
use crate::response::headers::ResultRequest;
use crate::response::sender::{ResponseRoute, deliver_response, stage};
use crate::subsystem::SubsystemName;
use crate::timers::Trigger;
use opentelemetry::Context;
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[cfg(test)]
mod tests;

/// Sends one typed response for a subsystem.
///
/// Construction takes a route from the process router. The route always tries
/// local delivery before network delivery.
///
/// `subsystem` is the name this consumer answers peer requests for. The decode
/// path and this responder use the same subsystem value.
pub(crate) struct Responder<C: Codec, R: ResponseRoute> {
    route: R,
    subsystem: SubsystemName,
    codec: PhantomData<fn() -> C>,
}

/// The metadata needed to deliver one requested handler result.
///
/// Both are read from the message rather than from the invocation that answers
/// it. A deferred reload answers the request its own record names, in that
/// record's trace, whatever span the settle boundary later runs under — and by
/// default a timer dispatch starts a trace of its own, so an ambient context
/// would answer outside the requester's trace.
///
/// A [`Context`] preserves the request trace across the final apply hook.
///
/// The capture happens while the message's processing state is still live, so
/// `message.span()` is never `Span::none()` and the response leg can never open
/// as a root of a trace of its own.
///
/// One exists per requested in-flight event. Both exits are here:
/// [`after_commit`](FallibleHandler::after_commit) moves it into the responder,
/// and [`after_abort`](FallibleHandler::after_abort) drops it.
#[derive(Debug)]
struct ResultDelivery {
    request: ResultRequest,
    trace: Context,
}

/// One result arm and its [`ResultDelivery`] carrier.
///
/// The enclosing [`Result`] distinguishes success from failure. This carrier
/// keeps the same metadata shape on both arms. Its error implementation keeps
/// a failed arm's display text and source.
#[derive(Debug, Error)]
#[error("{inner}")]
pub(crate) struct Responded<T> {
    #[source]
    inner: T,
    meta: Option<ResultDelivery>,
}

/// Wraps one handler and moves its final message result to a responder.
///
/// Response encoding borrows the final result before the apply hook consumes
/// it.
pub(crate) struct RespondHandler<H, C: Codec, R: ResponseRoute> {
    handler: H,
    responder: Arc<Responder<C, R>>,
}

impl<C: Codec, R: ResponseRoute> Responder<C, R> {
    /// Builds a responder from one statically composed response route.
    pub(crate) fn new(route: R, subsystem: SubsystemName) -> Self {
        Self {
            route,
            subsystem,
            codec: PhantomData,
        }
    }
}

impl<H, C: Codec, R: ResponseRoute> Clone for RespondHandler<H, C, R>
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

impl<H, C: Codec, R: ResponseRoute> RespondHandler<H, C, R> {
    pub(crate) fn new(handler: H, responder: Arc<Responder<C, R>>) -> Self {
        Self { handler, responder }
    }
}

impl<E: ClassifyError> ClassifyError for Responded<E> {
    /// This delegation is what keeps every retry, defer, settlement and marker
    /// decision exactly what it was without the layer.
    fn classify_error(&self) -> ErrorCategory {
        self.inner.classify_error()
    }
}

impl<H, C, R> FallibleHandler for RespondHandler<H, C, R>
where
    H: FallibleHandler,
    H::Output: Sync + 'static,
    H::Error: Sync + 'static,
    C: Codec<Payload = H::Output>,
    R: ResponseRoute,
{
    type Error = Responded<H::Error>;
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
        let meta = message.request().map(|request| ResultDelivery {
            request,
            trace: message.span().context(),
        });
        // Matched rather than mapped: a `map` / `map_err` pair would move the
        // carrier into two closures.
        match self.handler.on_message(context, message, demand_type).await {
            Ok(inner) => Ok(Responded { inner, meta }),
            Err(inner) => Err(Responded { inner, meta }),
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
            .map_err(|inner| Responded { inner, meta: None })
    }

    /// Sends a requested response after a final invocation.
    ///
    /// The handler's hook runs before response delivery. Thus, the response
    /// cannot report completion before the handler applies its final result.
    async fn after_commit<C2>(&self, context: C2, result: Result<Self::Output, Self::Error>)
    where
        C2: EventContext<Payload = Self::Payload>,
    {
        let (result, meta) = match result {
            Ok(Responded { inner, meta }) => (Ok(inner), meta),
            Err(Responded { inner, meta }) => (Err(inner), meta),
        };
        let Some(ResultDelivery { request, trace }) = meta else {
            return self.handler.after_commit(context, result).await;
        };
        let deadline = request.deadline();
        let header = request.delivery_header(self.responder.subsystem.clone());
        let response = stage::<C, _>(header, result.as_ref());
        self.handler.after_commit(context, result).await;
        deliver_response(&self.responder.route, response, trace, deadline).await;
    }

    /// Forwards a non-final invocation's result to the inner hook.
    ///
    /// Another invocation is coming for this event, so nothing is answered
    /// here. This arm drops the request without binding it. Thus, it holds
    /// nothing that can build a frame header.
    async fn after_abort<C2>(&self, context: C2, result: Result<Self::Output, Self::Error>)
    where
        C2: EventContext<Payload = Self::Payload>,
    {
        let result = match result {
            Ok(Responded { inner, .. }) => Ok(inner),
            Err(Responded { inner, .. }) => Err(inner),
        };
        self.handler.after_abort(context, result).await;
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}

impl<H, C, R> SettlementHandler for RespondHandler<H, C, R>
where
    H: SettlementHandler,
    H::Output: Sync + 'static,
    H::Error: Sync + 'static,
    C: Codec<Payload = H::Output>,
    R: ResponseRoute,
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
