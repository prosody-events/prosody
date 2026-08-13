//! Statically composed response routes.

use super::metrics::{DropReason, Stage};
use crate::codec::Codec;
use crate::error::ClassifyError;
use crate::otel::context_with_parent;
use crate::peer::response::ResponseDisposition;
use crate::peer::response::frame::FrameHeader;
use crate::peer::response::frame::encode::{Staged, stage_error, stage_success};
use crate::peer::response::headers::RequestDeadline;
use crate::peer::router::{EndpointKind, NetworkRouter, ResponseSender};
use opentelemetry::Context;
use opentelemetry_semantic_conventions::attribute::ERROR_TYPE;
use std::fmt::Display;
use std::future::Future;
use tokio::time::{Instant, timeout_at};
use tracing::field::Empty;
use tracing::{Instrument, Span, debug_span, error, warn};

use crate::peer::router::LocalTarget;

/// One response route in a statically composed route chain.
pub trait ResponseRoute: Clone + Send + Sync + 'static {
    /// Tries this route. `Declined` lets the next route try the same frame.
    /// `context` is the request context after any enabled transport span.
    fn deliver(
        &self,
        frame: Staged,
        deadline: RequestDeadline,
        context: &Context,
    ) -> impl Future<Output = Result<RouteOutcome, DropReason>> + Send;
}

/// Two routes evaluated in order.
#[derive(Clone)]
pub(crate) struct Then<A, B>(pub(crate) A, pub(crate) B);

/// Frames and delivers one response.
///
/// Every response ends as exactly one outcome. It moves one stage or one
/// drop reason, one of this sender's two counters, and the `peer.disposition`
/// attribute on its own span. A delivered job also records
/// `peer.endpoint.kind`. Every count of a response's outcome sits in this one
/// match. Thus, no counters can disagree.
/// The `peer.response.send` span is opened here and covers the delivery alone.
/// It is a child of the trace the job carries, so the listener's
/// `peer.response.receive` — parented on the context this span's own injection
/// writes — lands under the call that asked for the response.
pub(crate) async fn deliver_response<R: ResponseRoute>(
    router: &R,
    prepared: PreparedResponse,
    trace: Context,
    deadline: RequestDeadline,
) {
    let header = prepared.header();
    let span = debug_span!(
        "peer.response.send",
        otel.kind = "client",
        rpc.system.name = Empty,
        rpc.method = Empty,
        rpc.response.status_code = Empty,
        server.address = Empty,
        server.port = Empty,
        error.type = Empty,
        peer.target = %header.target,
        peer.request = %header.request,
        peer.subsystem = %header.subsystem,
        peer.disposition = Empty,
        peer.endpoint.kind = Empty,
    );
    let context = context_with_parent(&span, trace);
    let outcome = match prepared {
        PreparedResponse::Ready(frame) => {
            deliver_route(router, frame, deadline, &context)
                .instrument(span.clone())
                .await
        }
        PreparedResponse::Rejected(_, reason) => Err(reason),
    };
    // Recorded through the owned handle rather than the current span: a
    // level-disabled span never becomes current.
    match outcome {
        Ok(delivery) => {
            span.record("peer.disposition", "delivered");
            let endpoint_kind = match delivery {
                Delivery::Local => "local",
                Delivery::Remote(kind) => kind.label(),
            };
            span.record("peer.endpoint.kind", endpoint_kind);
            Stage::Delivered.record();
        }
        Err(reason) => {
            span.record("peer.disposition", reason.label());
            if reason != DropReason::SendFailed {
                span.record(ERROR_TYPE, reason.label());
            }
            span.in_scope(|| error!(error = %reason.label()));
            reason.record();
        }
    }
}

/// Resolves one response's route, frames it and delivers it.
///
/// `Ok` carries the route that accepted the frame. Every other outcome names
/// why the response was dropped.
async fn deliver_route<R: ResponseRoute>(
    router: &R,
    frame: Staged,
    deadline: RequestDeadline,
    context: &Context,
) -> Result<Delivery, DropReason> {
    match router.deliver(frame, deadline, context).await? {
        RouteOutcome::Delivered(delivery) => Ok(delivery),
        RouteOutcome::Declined(_) => Err(DropReason::UnresolvablePeer),
    }
}

impl ResponseRoute for LocalTarget {
    async fn deliver(
        &self,
        frame: Staged,
        _deadline: RequestDeadline,
        _context: &Context,
    ) -> Result<RouteOutcome, DropReason> {
        if !self.owns(frame.target()) {
            return Ok(RouteOutcome::Declined(frame));
        }
        let disposition = self.accept(frame.into_local_frame());
        disposition.record();
        if disposition == ResponseDisposition::Accepted {
            Ok(RouteOutcome::Delivered(Delivery::Local))
        } else {
            Span::current().record(ERROR_TYPE, disposition.label());
            Err(DropReason::SendFailed)
        }
    }
}

impl<R: NetworkRouter> ResponseRoute for R {
    async fn deliver(
        &self,
        frame: Staged,
        deadline: RequestDeadline,
        context: &Context,
    ) -> Result<RouteOutcome, DropReason> {
        if Instant::now() >= deadline.expires_at() {
            return Err(DropReason::DeadlineExceeded);
        }
        // Cancellation drops the active directory, cache, or transport
        // future. No awaited future retains progress another task requires.
        let delivery = async {
            let target = frame.target();
            // No address originates anywhere but a registration. A missing or
            // unreachable peer is not dialed.
            let route = match self.route(target).await {
                Ok(Some(route)) => route,
                Ok(None) => return Ok(RouteOutcome::Declined(frame)),
                Err(error) => {
                    warn!(%error, peer = %target, "peer route lookup failed");
                    return Err(DropReason::LookupFailed);
                }
            };
            let (kind, address) = route.endpoint();
            if let Err(failure) = self
                .sender()
                .deliver(address, &frame, deadline.expires_at(), context)
                .await
            {
                warn!(%failure, peer = %target, endpoint_kind = kind.label(), "response delivery failed");
                return Err(DropReason::SendFailed);
            }
            Ok(RouteOutcome::Delivered(Delivery::Remote(kind)))
        };
        timeout_at(deadline.expires_at(), delivery)
            .await
            .unwrap_or(Err(DropReason::DeadlineExceeded))
    }
}

impl<A: ResponseRoute, B: ResponseRoute> ResponseRoute for Then<A, B> {
    async fn deliver(
        &self,
        frame: Staged,
        deadline: RequestDeadline,
        context: &Context,
    ) -> Result<RouteOutcome, DropReason> {
        match self.0.deliver(frame, deadline, context).await? {
            RouteOutcome::Declined(frame) => self.1.deliver(frame, deadline, context).await,
            delivered @ RouteOutcome::Delivered(_) => Ok(delivered),
        }
    }
}

/// How a response reached its requester.
pub enum Delivery {
    /// The local registry accepted it without transport work.
    Local,
    /// A remote endpoint accepted it.
    Remote(EndpointKind),
}

/// Whether one route accepted a frame or left it for the next route.
pub enum RouteOutcome {
    Declined(Staged),
    Delivered(Delivery),
}

/// One encoded response, or the bounded reason encoding refused it.
pub(crate) enum PreparedResponse {
    Ready(Staged),
    Rejected(FrameHeader, DropReason),
}

impl PreparedResponse {
    pub(crate) fn header(&self) -> &FrameHeader {
        match self {
            Self::Ready(frame) => frame.header(),
            Self::Rejected(header, _) => header,
        }
    }
}

/// Encodes one payload and records the common frame stage.
pub(crate) fn stage<C, E>(header: FrameHeader, result: Result<&C::Payload, &E>) -> PreparedResponse
where
    C: Codec,
    E: ClassifyError + Display,
{
    Stage::Attempted.record();
    let encoded = match result {
        Ok(payload) => stage_success::<C>(&header, payload),
        Err(error) => {
            Stage::Framed.record();
            return PreparedResponse::Ready(stage_error(
                &header,
                error.classify_error(),
                error.to_string(),
            ));
        }
    };
    match encoded {
        Ok(staged) => {
            Stage::Framed.record();
            PreparedResponse::Ready(staged)
        }
        Err(error) => {
            warn!(%error, peer = %header.target, "response could not be framed");
            PreparedResponse::Rejected(header, DropReason::EncodeFailed)
        }
    }
}
