//! The peer method: what one process does with a response frame another
//! process sent it.

use super::TRANSPORT;
use super::deadline::inbound_deadline;
use super::generated::peer_server::Peer;
use super::inject::MetadataExtractor;
use crate::otel::carry_parent;
use crate::propagator::new_propagator;
use crate::requester::registry::PendingRegistry;
use crate::response::ResponseDisposition;
use crate::response::frame::FrameCap;
use crate::response::frame::ResponseFrame;
use crate::response::frame::encode::Forwarded;
use crate::router::relay::{Relay, RelayFailure, Routing, routing};
use crate::router::{NodeId, RelayHop};
use async_trait::async_trait;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tonic::{Request, Response, Status};
use tracing::field::{Empty, display};
use tracing::{Instrument, Span, debug_span};

/// Serves [`DeliverResponse`](Peer::deliver_response) for one node.
///
/// A frame is accepted only by the process it names, sent on once when it names
/// another, and refused when it already passed through a relay. The id every
/// frame is compared against is the one this service was built with, and no
/// frame can supply that id.
pub(crate) struct PeerService<R> {
    node: NodeId,
    registry: Arc<PendingRegistry>,
    relay: Relay<R>,
    cap: FrameCap,
    /// This process's own ceiling on one forward. [`inbound_deadline`] owns
    /// what it does to the budget a caller stated.
    budget: Duration,
    propagator: TextMapCompositePropagator,
}

impl<R> PeerService<R> {
    /// Serves `registry` on behalf of `node`, and sends every other frame on
    /// through `relay`.
    pub(crate) fn new(
        node: NodeId,
        registry: Arc<PendingRegistry>,
        relay: Relay<R>,
        cap: FrameCap,
        budget: Duration,
    ) -> Self {
        Self {
            node,
            registry,
            relay,
            cap,
            budget,
            propagator: new_propagator(),
        }
    }
}

#[async_trait]
impl<R: RelayHop> Peer for PeerService<R> {
    /// Hands one frame to the waiter it names, sends it on to the process it
    /// names, or refuses it — and answers with the status the whole path came
    /// to.
    ///
    /// The invocation is counted before anything can return, so "the service
    /// never ran" is observable: a frame the transport refused leaves that
    /// counter alone, which is what separates a transport rejection from a
    /// registry outcome.
    async fn deliver_response(
        &self,
        request: Request<ResponseFrame>,
    ) -> Result<Response<()>, Status> {
        TRANSPORT.record_served();
        // Hand-built rather than `#[instrument]`: the span relates to a context
        // this call carried, which the attribute cannot express. Every record
        // goes through this owned handle, because a level-disabled span never
        // becomes current.
        let span = debug_span!(
            "peer.response.receive",
            otel.kind = "server",
            peer.request = Empty,
            peer.subsystem = Empty,
            peer.disposition = Empty,
            peer.deadline_ms = Empty,
        );
        // A caller that sent no propagation headers, or broken ones, still gets
        // its response delivered: the span is simply unparented.
        carry_parent(
            &span,
            self.propagator
                .extract(&MetadataExtractor::new(request.metadata())),
        );
        // The caller's budget becomes an instant on arrival, and everything
        // this call does is spent against it. A duration passed on unchanged
        // would hand a second hop a fresh full budget.
        let deadline = inbound_deadline(request.metadata(), self.budget);
        let granted = deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        span.record(
            "peer.deadline_ms",
            i64::try_from(granted).unwrap_or(i64::MAX),
        );
        let frame = request.into_inner();
        async {
            span.record("peer.request", display(frame.header.request));
            span.record("peer.subsystem", display(&frame.header.subsystem));
            let target = frame.header.target;
            let routing = routing(self.node, target, frame.header.relay);
            // Counted here rather than inside the arms, so a routing outcome
            // added later cannot reach the wire without being counted.
            if routing != Routing::Accept {
                TRANSPORT.record_misrouted();
            }
            match routing {
                Routing::Accept => answer(&span, self.registry.accept(frame)),
                Routing::AlreadyRelayed => answer(&span, ResponseDisposition::AlreadyRelayed),
                Routing::Forward => {
                    TRANSPORT.record_forwarded();
                    // The forwarded form carries this process's own id, so a
                    // relay id the caller supplied cannot survive the hop.
                    let Some(forwarded) = Forwarded::new(frame, self.node, self.cap) else {
                        return answer(&span, ResponseDisposition::ResponseTooLarge);
                    };
                    let forward = debug_span!(
                        "peer.response.forward",
                        otel.kind = "client",
                        peer.target = %target,
                    );
                    // Awaited rather than spawned, so this answer covers the
                    // whole path: a responder is never told it succeeded while
                    // the requester still waits.
                    match self
                        .relay
                        .forward(target, deadline, &forwarded)
                        .instrument(forward)
                        .await
                    {
                        // The target decided this one and counted it there.
                        Ok(()) => {
                            span.record(
                                "peer.disposition",
                                ResponseDisposition::Accepted.message(),
                            );
                            Ok(Response::new(()))
                        }
                        Err(RelayFailure::NoCapacity) => {
                            answer(&span, ResponseDisposition::NoRelayCapacity)
                        }
                        Err(RelayFailure::DeadlineExceeded) => {
                            answer(&span, ResponseDisposition::RelayDeadlineExceeded)
                        }
                        Err(RelayFailure::Unreachable) => {
                            answer(&span, ResponseDisposition::Unreachable)
                        }
                        // The target read the frame and answered. Its status is
                        // passed through as it gave it, because rewriting a code
                        // here would silently change the responder's own retry
                        // decision.
                        Err(RelayFailure::Target(code)) => {
                            span.record("peer.disposition", code.description());
                            Err(Status::new(code, code.description()))
                        }
                    }
                }
            }
        }
        .instrument(span.clone())
        .await
    }
}

/// Reports one outcome as the status it names.
///
/// Every refusal is named rather than caught, so a disposition added later does
/// not compile until somebody decides here whether it means the response was
/// stored. That decision is what `OK` reports.
fn answer(span: &Span, disposition: ResponseDisposition) -> Result<Response<()>, Status> {
    span.record("peer.disposition", disposition.message());
    disposition.record();
    match disposition {
        ResponseDisposition::Accepted => Ok(Response::new(())),
        ResponseDisposition::UnknownRequest
        | ResponseDisposition::ClosedRequest
        | ResponseDisposition::DuplicateSubsystem
        | ResponseDisposition::UnexpectedSubsystem
        | ResponseDisposition::FormatMismatch
        | ResponseDisposition::ResponseTooLarge
        | ResponseDisposition::MalformedTarget
        | ResponseDisposition::AlreadyRelayed
        | ResponseDisposition::NoRelayCapacity
        | ResponseDisposition::RelayDeadlineExceeded
        | ResponseDisposition::Unreachable => {
            Err(Status::new(disposition.status(), disposition.message()))
        }
    }
}
