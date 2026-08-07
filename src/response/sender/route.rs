//! Statically composed response routes.

use super::metrics::{DropReason, Stage, record_fallback};
use crate::codec::Codec;
use crate::otel::carry_parent;
use crate::response::ResponseDisposition;
use crate::response::frame::FrameHeader;
use crate::response::frame::encode::{FrameEncoder, Staged};
use crate::router::directory::Endpoint;
use crate::router::fleet::Destination;
use crate::router::{Framed, Preference, ResponseSender, Router, SendFailure};
use opentelemetry::Context;
use std::future::Future;
use tokio::select;
use tokio::time::{Instant, sleep_until, timeout_at};
use tracing::field::Empty;
use tracing::{Instrument, debug_span, warn};

use crate::router::LocalTarget;

/// One in this many of the deadline that is left is what an endpoint keeps for
/// the fallback behind it — and all an endpoint that has never answered may
/// spend. See [`Share`].
const FALLBACK_DIVISOR: u32 = 4;

/// One response route in a statically composed route chain.
pub trait ResponseRoute: Clone + Send + Sync + 'static {
    /// Tries this route. `Declined` lets the next route try the same frame.
    fn deliver<C: Codec>(
        &self,
        encoder: &mut FrameEncoder<C>,
        header: &FrameHeader,
        payload: C::Payload,
        destination: &Destination,
        expires_at: Instant,
    ) -> impl Future<Output = Result<RouteOutcome<C::Payload>, DropReason>> + Send;
}

/// Two routes evaluated in order.
#[derive(Clone)]
pub(crate) struct Then<A, B>(pub(crate) A, pub(crate) B);

/// How much of what is left of a response's deadline one endpoint may spend.
///
/// While a route still has a candidate untried, no endpoint gets the whole
/// budget. An address which drops packets instead of refusing them would
/// otherwise spend the deadline unanswered and leave the endpoint that works
/// untried — and that is exactly what a misapplied label reaches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Share {
    /// An endpoint with a fallback behind it that this destination has never
    /// answered on. One [`FALLBACK_DIVISOR`]th of what is left.
    Probe,
    /// An endpoint with a fallback behind it that this destination answered on
    /// before. Everything but the [`FALLBACK_DIVISOR`]th it keeps for that
    /// fallback, because an endpoint that already answered is worth waiting
    /// for.
    Most,
    /// The last endpoint of a route. Everything that is left.
    Rest,
}

impl Share {
    /// The instant this endpoint must give up at, measured from now.
    ///
    /// Read after the pacing wait rather than before it, so a share is a slice
    /// of the time that is left to reach the network with.
    fn until(self, expires_at: Instant) -> Instant {
        let now = Instant::now();
        let left = expires_at.saturating_duration_since(now);
        let reserved = left / FALLBACK_DIVISOR;
        match self {
            Self::Probe => now + reserved,
            Self::Most => now + left.saturating_sub(reserved),
            Self::Rest => expires_at,
        }
    }
}

/// Frames and delivers one response.
///
/// Every response ends as exactly one outcome. It moves one stage or one
/// drop reason, one of this sender's two counters, and the `peer.disposition`
/// attribute on its own span. A delivered job also records `peer.preference`
/// and counts one fallback transition when its walk made one. Every count of a
/// response's outcome sits in this one match. Thus, no counters can disagree.
/// The deadline is the biased arm of the select. Thus, an expired response is
/// dropped before the route is
/// polled at all — nothing is paced, encoded or sent for it.
/// Work already inside one poll still finishes: this is a deadline the pipeline
/// is measured against between polls, never an absolute wall-clock cut.
///
/// The `peer.response.send` span is opened here and covers the delivery alone.
/// It is a child of the trace the job carries, so the listener's
/// `peer.response.receive` — parented on the context this span's own injection
/// writes — lands under the call that asked for the response.
pub(super) async fn deliver_response<C: Codec, R: ResponseRoute>(
    router: &R,
    mut encoder: FrameEncoder<C>,
    header: FrameHeader,
    payload: C::Payload,
    trace: Context,
    destination: &Destination,
    expires_at: Instant,
) -> bool {
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
    // One deadline over the whole pipeline — the pacing wait, the address
    // read, the encode and every attempt — so a transport that never
    // answers still ends the delivery.
    let outcome = select! {
        biased;
        () = sleep_until(expires_at) => Err(DropReason::Deadline),
        outcome = deliver_route(
            &mut encoder,
            destination,
            router,
            header,
            payload,
            expires_at,
        ).instrument(span.clone()) => {
            outcome
        }
    };
    // Recorded through the owned handle rather than the current span: a
    // level-disabled span never becomes current, and the deadline arm has
    // already left the instrumented future in any case.
    let delivered = outcome.is_ok();
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
    encoder.release();
    delivered
}

/// Resolves one response's route, frames it and delivers it.
///
/// `Ok` carries the candidate that accepted the frame, and the candidate tried
/// before it when the walk fell back; every other outcome names why the
/// response was dropped. The transition travels out rather than being counted
/// here, so the caller counts the whole outcome of one response in one
/// place.
///
/// The *time* budget is split the same way, and for the same reason. [`Share`]
/// owns how much of it one endpoint may spend, and the endpoint this
/// destination remembers is the one that gets the larger part.
async fn deliver_route<C: Codec, R: ResponseRoute>(
    encoder: &mut FrameEncoder<C>,
    destination: &Destination,
    router: &R,
    header: FrameHeader,
    payload: C::Payload,
    expires_at: Instant,
) -> Result<Delivery, DropReason> {
    match router
        .deliver(encoder, &header, payload, destination, expires_at)
        .await?
    {
        RouteOutcome::Delivered(delivery) => Ok(delivery),
        RouteOutcome::Declined(_) => Err(DropReason::UnresolvableNode),
    }
}

impl ResponseRoute for LocalTarget {
    async fn deliver<C: Codec>(
        &self,
        encoder: &mut FrameEncoder<C>,
        header: &FrameHeader,
        payload: C::Payload,
        _destination: &Destination,
        _expires_at: Instant,
    ) -> Result<RouteOutcome<C::Payload>, DropReason> {
        if !self.owns(header.target) {
            return Ok(RouteOutcome::Declined(payload));
        }
        let staged = stage(encoder, header, payload)?;
        let disposition = self.accept(staged.local_frame());
        disposition.record();
        if disposition == ResponseDisposition::Accepted {
            Ok(RouteOutcome::Delivered(Delivery::Local))
        } else {
            Err(DropReason::SendFailed)
        }
    }
}

impl<R: Router> ResponseRoute for R {
    async fn deliver<C: Codec>(
        &self,
        encoder: &mut FrameEncoder<C>,
        header: &FrameHeader,
        payload: C::Payload,
        destination: &Destination,
        expires_at: Instant,
    ) -> Result<RouteOutcome<C::Payload>, DropReason> {
        let target = header.target;
        // No address originates anywhere but a registration: a node the directory
        // does not hold is not dialed at all, and a node the rules refuse to reach
        // from here is not dialed either.
        let route = match self.route(target).await {
            Ok(Some(route)) => route,
            Ok(None) => return Ok(RouteOutcome::Declined(payload)),
            Err(error) => {
                warn!(%error, node = %target, "peer route lookup failed");
                return Err(DropReason::LookupFailed);
            }
        };
        let staged = stage(encoder, header, payload)?;
        let mut remembered = None;
        let mut last_failure = None;
        // The candidate that failed the turn before this one. Inside the loop it
        // proves this is no longer the first candidate, and it is the `from` of a
        // fallback.
        let mut previous = None;
        let preferred = destination.preferred();
        let candidates = route.candidates(preferred);
        let has_fallback = candidates[1].is_some();
        let proven = candidates[0].is_some_and(|(preference, _)| Some(preference) == preferred);
        for (preference, address) in candidates.into_iter().flatten() {
            let share = if previous.is_some() || !has_fallback {
                Share::Rest
            } else if proven {
                Share::Most
            } else {
                Share::Probe
            };
            match deliver(self.sender(), address, &staged, expires_at, share).await {
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
    async fn deliver<C: Codec>(
        &self,
        encoder: &mut FrameEncoder<C>,
        header: &FrameHeader,
        payload: C::Payload,
        destination: &Destination,
        expires_at: Instant,
    ) -> Result<RouteOutcome<C::Payload>, DropReason> {
        match self
            .0
            .deliver(encoder, header, payload, destination, expires_at)
            .await?
        {
            RouteOutcome::Declined(payload) => {
                self.1
                    .deliver(encoder, header, payload, destination, expires_at)
                    .await
            }
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
pub enum RouteOutcome<P> {
    Declined(P),
    Delivered(Delivery),
}

/// Encodes one payload and records the common frame stage.
fn stage<'a, C: Codec>(
    encoder: &'a mut FrameEncoder<C>,
    header: &'a FrameHeader,
    payload: C::Payload,
) -> Result<Staged<'a>, DropReason> {
    match encoder.stage(header, payload) {
        Ok(staged) => {
            Stage::Framed.record();
            Ok(staged)
        }
        Err(error) => {
            warn!(%error, node = %header.target, "response could not be framed");
            Err(DropReason::EncodeFailed)
        }
    }
}

/// Delivers one frame to one endpoint within its route share.
async fn deliver<S: ResponseSender, F: Framed + Sync>(
    sender: &S,
    address: &Endpoint,
    frame: &F,
    expires_at: Instant,
    share: Share,
) -> Result<(), SendFailure> {
    let until = share.until(expires_at);
    attempt(sender, address, frame, expires_at, until).await
}

/// One attempt, bounded twice over: by `expires_at` on the wire and by `until`
/// here.
///
/// The two deadlines are different on purpose. `expires_at` is what the peer is
/// told to answer inside, so a `DEADLINE_EXCEEDED` still means what it says —
/// the whole response ran out of time — rather than "this process moved on".
/// `until` is what this process spends on this one endpoint, and it covers the
/// channel lookup and the readiness wait as well, neither of which the
/// `grpc-timeout` header reaches. Giving up on it therefore reads as
/// [`SendFailure::Unreachable`]: nothing answered here, and the next candidate
/// keeps what the response has left.
async fn attempt<S: ResponseSender, F: Framed + Sync>(
    sender: &S,
    address: &Endpoint,
    frame: &F,
    expires_at: Instant,
    until: Instant,
) -> Result<(), SendFailure> {
    match timeout_at(until, sender.deliver(address, frame, expires_at)).await {
        Ok(outcome) => outcome,
        Err(_) => Err(SendFailure::Unreachable),
    }
}
