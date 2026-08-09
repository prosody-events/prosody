//! Statically composed response routes.

use super::metrics::{DropReason, Stage, record_fallback};
use crate::codec::Codec;
use crate::otel::carry_parent;
use crate::response::ResponseDisposition;
use crate::response::frame::FrameHeader;
use crate::response::frame::encode::{Staged, stage as encode};
use crate::response::headers::RequestDeadline;
use crate::router::{NetworkRouter, Preference, ResponseSender};
use opentelemetry::Context;
use std::future::Future;
use tracing::field::Empty;
use tracing::{Instrument, debug_span, warn};

use crate::router::LocalTarget;

/// One response route in a statically composed route chain.
pub trait ResponseRoute: Clone + Send + Sync + 'static {
    /// Tries this route. `Declined` lets the next route try the same frame.
    fn deliver(
        &self,
        frame: Staged,
        deadline: RequestDeadline,
    ) -> impl Future<Output = Result<RouteOutcome, DropReason>> + Send;
}

/// Two routes evaluated in order.
#[derive(Clone)]
pub(crate) struct Then<A, B>(pub(crate) A, pub(crate) B);

/// Frames and delivers one response.
///
/// Every response ends as exactly one outcome. It moves one stage or one
/// drop reason, one of this sender's two counters, and the `peer.disposition`
/// attribute on its own span. A delivered job also records `peer.preference`
/// and counts one fallback transition when its walk made one. Every count of a
/// response's outcome sits in this one match. Thus, no counters can disagree.
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
        peer.target = %header.target,
        peer.request = %header.request,
        peer.subsystem = %header.subsystem,
        peer.disposition = Empty,
        peer.preference = Empty,
    );
    carry_parent(&span, trace);
    let outcome = match prepared {
        PreparedResponse::Ready(frame) => {
            deliver_route(router, frame, deadline)
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
            match delivery {
                Delivery::Local => {
                    span.record("peer.preference", "local");
                }
                Delivery::Remote { preference, from } => {
                    span.record("peer.preference", preference.label());
                    if let Some(from) = from {
                        record_fallback(from, preference);
                    }
                }
            }
            Stage::Delivered.record();
        }
        Err(reason) => {
            span.record("peer.disposition", reason.label());
            reason.record();
        }
    }
}

/// Resolves one response's route, frames it and delivers it.
///
/// `Ok` carries the candidate that accepted the frame, and the candidate tried
/// before it when the walk fell back; every other outcome names why the
/// response was dropped. The transition travels out rather than being counted
/// here, so the caller counts the whole outcome of one response in one
/// place.
async fn deliver_route<R: ResponseRoute>(
    router: &R,
    frame: Staged,
    deadline: RequestDeadline,
) -> Result<Delivery, DropReason> {
    match router.deliver(frame, deadline).await? {
        RouteOutcome::Delivered(delivery) => Ok(delivery),
        RouteOutcome::Declined(_) => Err(DropReason::UnresolvableNode),
    }
}

impl ResponseRoute for LocalTarget {
    async fn deliver(
        &self,
        frame: Staged,
        _deadline: RequestDeadline,
    ) -> Result<RouteOutcome, DropReason> {
        if !self.owns(frame.target()) {
            return Ok(RouteOutcome::Declined(frame));
        }
        let disposition = self.accept(frame.into_local_frame());
        disposition.record();
        if disposition == ResponseDisposition::Accepted {
            Ok(RouteOutcome::Delivered(Delivery::Local))
        } else {
            Err(DropReason::SendFailed)
        }
    }
}

impl<R: NetworkRouter> ResponseRoute for R {
    async fn deliver(
        &self,
        frame: Staged,
        deadline: RequestDeadline,
    ) -> Result<RouteOutcome, DropReason> {
        let target = frame.target();
        // No address originates anywhere but a registration: a node the directory
        // does not hold is not dialed at all, and a node the rules refuse to reach
        // from here is not dialed either.
        let route = match self.route(target).await {
            Ok(Some(route)) => route,
            Ok(None) => return Ok(RouteOutcome::Declined(frame)),
            Err(error) => {
                warn!(%error, node = %target, "peer route lookup failed");
                return Err(DropReason::LookupFailed);
            }
        };
        let destination = self.destination(target);
        let mut remembered = None;
        let mut last_failure = None;
        // The candidate that failed the turn before this one. Inside the loop it
        // proves this is no longer the first candidate, and it is the `from` of a
        // fallback.
        let mut previous = None;
        let preferred = destination.preferred();
        let candidates = route.candidates(preferred);
        let has_fallback = candidates[1].is_some();
        for (preference, address) in candidates.into_iter().flatten() {
            match self
                .sender()
                .deliver(address, &frame, deadline.expires_at())
                .await
            {
                Ok(()) => {
                    destination.prefer(Some(preference));
                    return Ok(RouteOutcome::Delivered(Delivery::Remote {
                        preference,
                        from: previous,
                    }));
                }
                Err(failure) => {
                    last_failure = Some((preference, failure));
                    if !failure.is_wrong_endpoint() {
                        // A failure that is not a wrong endpoint is a status the
                        // path answered, so this endpoint is the one that reaches
                        // the node — refusal and all. Every other failure proves
                        // nothing about which endpoint serves the node, so it
                        // leaves nothing remembered.
                        remembered = Some(preference);
                        break;
                    }
                    previous = Some(preference);
                }
            }
        }
        destination.prefer(remembered);
        if let Some((preference, failure)) = last_failure {
            // What the walk did, not what the route offered. The last turn sets
            // `previous` as well, so a route of one candidate needs both terms.
            warn!(
                %failure,
                node = %target,
                preference = preference.label(),
                fell_back = has_fallback && previous.is_some(),
                "response delivery failed"
            );
        }
        Err(DropReason::SendFailed)
    }
}

impl<A: ResponseRoute, B: ResponseRoute> ResponseRoute for Then<A, B> {
    async fn deliver(
        &self,
        frame: Staged,
        deadline: RequestDeadline,
    ) -> Result<RouteOutcome, DropReason> {
        match self.0.deliver(frame, deadline).await? {
            RouteOutcome::Declined(frame) => self.1.deliver(frame, deadline).await,
            delivered @ RouteOutcome::Delivered(_) => Ok(delivered),
        }
    }
}

/// How a response reached its requester.
pub enum Delivery {
    /// The local registry accepted it without transport work.
    Local,
    /// A remote endpoint accepted it, after an optional fallback.
    Remote {
        preference: Preference,
        from: Option<Preference>,
    },
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
pub(crate) fn stage<C: Codec>(header: FrameHeader, payload: &C::Payload) -> PreparedResponse {
    Stage::Attempted.record();
    match encode::<C>(&header, payload) {
        Ok(staged) => {
            Stage::Framed.record();
            PreparedResponse::Ready(staged)
        }
        Err(error) => {
            warn!(%error, node = %header.target, "response could not be framed");
            PreparedResponse::Rejected(header, DropReason::EncodeFailed)
        }
    }
}
