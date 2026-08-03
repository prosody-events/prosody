//! The peer method: what one node does with a response frame another node sent
//! it.

use super::TRANSPORT;
use super::generated::peer_server::Peer;
use super::inject::MetadataExtractor;
use crate::propagator::new_propagator;
use crate::requester::registry::PendingRegistry;
use crate::response::ResponseDisposition;
use crate::response::frame::ResponseFrame;
use crate::router::NodeId;
use async_trait::async_trait;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use std::sync::Arc;
use tonic::{Code, Request, Response, Status};
use tracing::field::{Empty, debug as debug_field, display};
use tracing::{Span, debug, debug_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Serves [`DeliverResponse`](Peer::deliver_response) for one node.
///
/// The node id it carries is what makes "a node that is not the target never
/// accepts a frame" structural: the check reads this process's own id, which no
/// frame can supply.
pub(crate) struct PeerService {
    node: NodeId,
    registry: Arc<PendingRegistry>,
    propagator: TextMapCompositePropagator,
}

impl PeerService {
    /// Serves `registry` on behalf of `node`.
    pub(crate) fn new(node: NodeId, registry: Arc<PendingRegistry>) -> Self {
        Self {
            node,
            registry,
            propagator: new_propagator(),
        }
    }

    /// Checks the frame is for this node, hands it to the registry, and turns
    /// the disposition into a status.
    fn accept(&self, span: &Span, frame: ResponseFrame) -> Result<Response<()>, Status> {
        span.record("peer.request", display(frame.header.request));
        span.record("peer.subsystem", display(frame.header.subsystem.as_str()));
        // This node does not relay, so a frame for another node is one whose
        // target it cannot reach. Forwarding replaces this arm; until then the
        // registry never sees a frame addressed elsewhere.
        let disposition = if frame.header.target == self.node {
            self.registry.accept(frame)
        } else {
            TRANSPORT.record_misrouted();
            ResponseDisposition::Unreachable
        };
        span.record("peer.disposition", debug_field(disposition));
        // Matched on the status rather than on the disposition, so a
        // disposition added later cannot fall into the accepting arm. The
        // message allocates, on the refused path only: an accepted response
        // carries no message at all.
        match disposition.status() {
            Code::Ok => Ok(Response::new(())),
            code => Err(Status::new(code, format!("{disposition:?}"))),
        }
    }
}

#[async_trait]
impl Peer for PeerService {
    /// Hands one frame to the waiter it names, and answers with the status that
    /// waiter's disposition names.
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
        );
        // A caller that sent no propagation headers, or broken ones, still gets
        // its response delivered: the span is simply unparented.
        if let Err(error) = span.set_parent(
            self.propagator
                .extract(&MetadataExtractor::new(request.metadata())),
        ) {
            debug!(%error, "the peer call carried no usable trace context");
        }
        span.in_scope(|| self.accept(&span, request.into_inner()))
    }
}
