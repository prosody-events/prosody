//! Typed response delivery.

mod metrics;
mod route;

pub(crate) use self::metrics::DropReason;
use self::route::{PreparedResponse, deliver_response, stage};
use crate::codec::Codec;
use crate::response::frame::FrameHeader;
use crate::response::headers::RequestDeadline;
use crate::router::fleet::DestinationFleet;
use opentelemetry::Context;
use std::marker::PhantomData;
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// Encodes a typed response and sends it through one composed route.
pub(crate) struct TypedSender<C: Codec, R: ResponseRoute> {
    /// The route preferences shared by every response from this process.
    fleet: Arc<DestinationFleet>,
    route: R,
    _codec: PhantomData<fn() -> C>,
}

#[cfg(test)]
pub(crate) use route::Delivery as RouteDelivery;
pub(crate) use route::{ResponseRoute, RouteOutcome, Then};

impl<C: Codec, R: ResponseRoute> TypedSender<C, R> {
    /// Builds a sender from one statically composed response route.
    pub(crate) fn new_route(route: R, fleet: &Arc<DestinationFleet>) -> Self {
        Self::build(fleet, route)
    }

    fn build(fleet: &Arc<DestinationFleet>, route: R) -> Self {
        let fleet = Arc::clone(fleet);
        Self {
            fleet,
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
        let destination = self.fleet.destination(prepared.header().target);
        deliver_response(&self.route, prepared, trace, &destination, deadline).await
    }
}

/// Encodes one response through the standard codec cache and buffer.
pub(crate) fn prepare<C: Codec>(header: FrameHeader, payload: &C::Payload) -> PreparedResponse {
    stage::<C>(header, payload)
}
