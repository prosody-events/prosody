//! The peer method: what one process does with a response frame another
//! process sent it.

use super::codec::refusal;
use super::deadline::inbound_deadline;
use super::generated::DeliverResultRequest;
use super::generated::peer_service_server::PeerService as PeerServiceApi;
use super::inject::MetadataExtractor;
use super::telemetry::{METHOD, record_status};
use crate::otel::context_with_parent;
use crate::peer::metrics::PeerMetrics;
use crate::peer::response::ResponseDisposition;
use crate::peer::response::frame::ResponseFrame;
use crate::peer::response::frame::encode::Forwarded;
use crate::peer::router::relay::{Relay, RelayFailure, Routing, routing};
use crate::peer::router::{LocalTarget, RelayHop};
use crate::propagator::new_propagator;
use async_trait::async_trait;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use tokio::time::Instant;
use tonic::{Code, Request, Response, Status};
use tracing::field::{Empty, display};
use tracing::{Instrument, Span, debug_span, error};

/// Serves [`DeliverResult`](PeerServiceApi::deliver_result) for one peer.
///
/// A frame is accepted only by the process it names, sent on once when it names
/// another, and refused when it already passed through a relay. The id every
/// frame is compared against is the one this service was built with, and no
/// frame can supply that id.
pub(crate) struct PeerService<R> {
    local: LocalTarget,
    relay: Relay<R>,
    propagator: TextMapCompositePropagator,
}

impl<R> PeerService<R> {
    /// Serves `local` and sends every other frame through `relay`.
    pub(crate) fn new(local: LocalTarget, relay: Relay<R>) -> Self {
        Self {
            local,
            relay,
            propagator: new_propagator(),
        }
    }

    fn answer(
        &self,
        span: &Span,
        disposition: ResponseDisposition,
    ) -> Result<Response<()>, Status> {
        answer(span, self.local.pending().metrics(), disposition)
    }
}

#[async_trait]
impl<R: RelayHop> PeerServiceApi for PeerService<R> {
    /// Hands one frame to the waiter it names, sends it on to the process it
    /// names, or refuses it — and answers with the status the whole path came
    /// to.
    async fn deliver_result(
        &self,
        request: Request<DeliverResultRequest>,
    ) -> Result<Response<()>, Status> {
        // Hand-built rather than `#[instrument]`: the span relates to a context
        // this call carried, which the attribute cannot express. Every record
        // goes through this owned handle, because a level-disabled span never
        // becomes current.
        let span = debug_span!(
            "request.response.receive",
            otel.kind = "server",
            rpc.system.name = "grpc",
            rpc.method = METHOD,
            rpc.response.status_code = Empty,
            error.type = Empty,
            request.id = Empty,
            subsystem = Empty,
            request.target = Empty,
            request.relay = Empty,
            request.disposition = Empty,
            request.deadline_ms = Empty,
        );
        // A caller that sent no propagation headers, or broken ones, still gets
        // its response delivered: the span is simply unparented.
        let context = context_with_parent(
            &span,
            self.propagator
                .extract(&MetadataExtractor::new(request.metadata())),
        );
        // Convert the caller's timeout to an instant on arrival. Forward the
        // remaining duration so a second hop does not restart the timeout.
        let deadline = inbound_deadline(request.metadata()).ok_or_else(|| {
            record_status(&span, Code::InvalidArgument);
            span.in_scope(|| error!(error = "grpc-timeout is missing or invalid"));
            Status::invalid_argument("grpc-timeout is missing or invalid")
        })?;
        let remaining_ms = deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        span.record(
            "request.deadline_ms",
            i64::try_from(remaining_ms).unwrap_or(i64::MAX),
        );
        let frame: ResponseFrame = request.into_inner().try_into().map_err(|error| {
            record_status(&span, Code::InvalidArgument);
            span.in_scope(|| error!(%error, "peer frame is invalid"));
            refusal(&error)
        })?;
        async {
            span.record("request.id", display(frame.header.request));
            span.record("subsystem", display(&frame.header.subsystem));
            let target = frame.header.target;
            span.record("request.target", display(target));
            if let Some(relay) = frame.header.relay {
                span.record("request.relay", display(relay));
            }
            let routing = routing(self.local.peer, target, frame.header.relay);
            match routing {
                Routing::Accept => self.answer(&span, self.local.accept(frame)),
                Routing::AlreadyRelayed => self.answer(&span, ResponseDisposition::AlreadyRelayed),
                Routing::Forward => {
                    // The forwarded form carries this process's own id, so a
                    // relay id the caller supplied cannot survive the hop.
                    let forwarded = Forwarded::new(frame, self.local.peer);
                    let forward = debug_span!(
                        "request.response.forward",
                        otel.kind = "client",
                        rpc.system.name = "grpc",
                        rpc.method = METHOD,
                        rpc.response.status_code = Empty,
                        error.type = Empty,
                        request.target = %target,
                    );
                    let forward_context = context_with_parent(&forward, context.clone());
                    // Awaited rather than spawned, so this answer covers the
                    // whole path: a responder is never told it succeeded while
                    // the requester still waits.
                    let outcome = self
                        .relay
                        .forward(target, deadline, &forwarded, &forward_context)
                        .instrument(forward.clone())
                        .await;
                    match outcome {
                        Ok(()) => {
                            record_status(&forward, Code::Ok);
                            record_status(&span, Code::Ok);
                            span.record(
                                "request.disposition",
                                ResponseDisposition::Accepted.label(),
                            );
                            Ok(Response::new(()))
                        }
                        Err(RelayFailure::DeadlineExceeded) => {
                            record_status(&forward, Code::DeadlineExceeded);
                            forward.in_scope(|| error!(error = "the relay deadline elapsed"));
                            self.answer(&span, ResponseDisposition::RelayDeadlineExceeded)
                        }
                        Err(RelayFailure::Unreachable) => {
                            record_status(&forward, Code::Unavailable);
                            forward.in_scope(|| error!(error = "the relay target was unavailable"));
                            self.answer(&span, ResponseDisposition::Unreachable)
                        }
                        // The hop came to a status. It is passed through
                        // unchanged: rewriting a code here can silently change
                        // the responder's own retry decision.
                        Err(RelayFailure::Target(code)) => {
                            record_status(&forward, code);
                            forward.in_scope(|| error!(error = %code.description()));
                            record_status(&span, code);
                            span.record("request.disposition", "target_error");
                            span.in_scope(|| error!(error = %code.description()));
                            Err(service_status(code, code.description()))
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
fn answer(
    span: &Span,
    metrics: &PeerMetrics,
    disposition: ResponseDisposition,
) -> Result<Response<()>, Status> {
    span.record("request.disposition", disposition.label());
    disposition.record(metrics);
    record_status(span, disposition.status());
    match disposition {
        ResponseDisposition::Accepted => Ok(Response::new(())),
        ResponseDisposition::UnknownRequest
        | ResponseDisposition::ClosedRequest
        | ResponseDisposition::AlreadyRelayed
        | ResponseDisposition::RelayDeadlineExceeded
        | ResponseDisposition::Unreachable => {
            span.in_scope(|| error!(error = %disposition.label()));
            Err(service_status(disposition.status(), disposition.message()))
        }
    }
}

/// Marks one status as a service result, not a transport refusal.
fn service_status(code: Code, message: &'static str) -> Status {
    Status::new(code, message)
}
