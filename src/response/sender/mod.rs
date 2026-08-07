//! Typed response delivery.

mod metrics;
mod route;

pub(crate) use self::metrics::DropReason;
use self::metrics::Stage;
use self::route::deliver_response;
use crate::codec::Codec;
use crate::response::frame::encode::FrameEncoder;
use crate::response::frame::{FrameCap, FrameHeader};
use crate::router::fleet::DestinationFleet;
use opentelemetry::Context;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

#[cfg(test)]
mod tests;

/// Encodes a typed response and sends it through one composed route.
pub(crate) struct TypedSender<C: Codec, R: ResponseRoute> {
    /// The route preferences shared by every response from this process.
    fleet: Arc<DestinationFleet>,
    route: R,
    cap: FrameCap,
    delivery_timeout: Duration,
    _codec: PhantomData<fn() -> C>,
}

#[cfg(test)]
pub(crate) use route::Delivery as RouteDelivery;
pub(crate) use route::{ResponseRoute, RouteOutcome, Then};

impl<C: Codec, R: ResponseRoute> TypedSender<C, R> {
    /// Builds a sender from one statically composed response route.
    pub(crate) fn new_route(route: R, fleet: &Arc<DestinationFleet>, cap: FrameCap) -> Self {
        Self::build(fleet, route, cap)
    }

    fn build(fleet: &Arc<DestinationFleet>, route: R, cap: FrameCap) -> Self {
        let fleet = Arc::clone(fleet);
        let config = fleet.config();
        Self {
            fleet,
            route,
            cap,
            delivery_timeout: config.response_timeout,
            _codec: PhantomData,
        }
    }

    /// Sends one response in the trace that `trace` names.
    ///
    /// `trace` is the requester's trace, captured by `Answering` in the respond
    /// layer, which states why it is a context rather than an ambient span.
    ///
    /// Applies transport backpressure until delivery finishes.
    pub(crate) async fn send(
        &self,
        header: FrameHeader,
        trace: Context,
        payload: C::Payload,
    ) -> bool {
        Stage::Attempted.record();
        let destination = self.fleet.destination(header.target);
        let expires_at = Instant::now() + self.delivery_timeout;
        deliver_response(
            &self.route,
            FrameEncoder::new(C::default(), self.cap),
            header,
            payload,
            trace,
            &destination,
            expires_at,
        )
        .await
    }
}
