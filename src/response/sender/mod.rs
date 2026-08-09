//! Typed response delivery.

mod metrics;
mod route;

pub(crate) use self::metrics::DropReason;
use self::route::{PreparedResponse, deliver_response, stage};
use crate::codec::Codec;
use crate::response::frame::FrameHeader;
use crate::response::headers::RequestDeadline;
use opentelemetry::Context;
use std::marker::PhantomData;

#[cfg(test)]
mod tests;

/// Encodes a typed response and sends it through one composed route.
pub(crate) struct TypedSender<C: Codec, R: ResponseRoute> {
    route: R,
    _codec: PhantomData<fn() -> C>,
}

#[cfg(test)]
pub(crate) use route::Delivery as RouteDelivery;
pub(crate) use route::{ResponseRoute, RouteOutcome, Then};

impl<C: Codec, R: ResponseRoute> TypedSender<C, R> {
    /// Builds a sender from one statically composed response route.
    pub(crate) const fn new_route(route: R) -> Self {
        Self {
            route,
            _codec: PhantomData,
        }
    }

    /// Sends one prepared response in the trace that `trace` names.
    ///
    /// `trace` is the requester's trace, captured by `Answering` in the respond
    /// layer, which states why it is a context rather than an ambient span.
    ///
    /// Applies transport backpressure until delivery finishes.
    pub(crate) async fn send(
        &self,
        prepared: PreparedResponse,
        trace: Context,
        deadline: RequestDeadline,
    ) -> bool {
        deliver_response(&self.route, prepared, trace, deadline).await
    }
}

/// Encodes one response through the standard codec cache and buffer.
pub(crate) fn prepare<C: Codec>(header: FrameHeader, payload: &C::Payload) -> PreparedResponse {
    stage::<C>(header, payload)
}
