//! Router health on the peer listener.
//!
//! The peer service reports `SERVING` when its listener answers. It does not
//! depend on consumer readiness or liveness. Producer-only clients therefore
//! remain reachable for responses.

use super::generated::peer_service_server::SERVICE_NAME;
use crate::heartbeat::HeartbeatRegistry;
use async_trait::async_trait;
use futures::stream::Empty;
use tonic::{Request, Response, Status};
use tonic_health::ServingStatus;
use tonic_health::pb::health_check_response::ServingStatus as WireStatus;
use tonic_health::pb::health_server::Health;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

/// Where the router runtime's readiness and liveness come from.
///
/// This source contains router state only. It never reads consumer state.
pub(crate) trait ProcessHealth: Send + Sync + 'static {
    /// Whether this process is ready to take work.
    fn ready(&self) -> bool;

    /// Whether this process is making progress.
    fn live(&self) -> bool;
}

/// Health of the peer runtime that serves this check.
pub(crate) struct RuntimeHealth {
    heartbeats: HeartbeatRegistry,
}

/// Serves `grpc.health.v1.Health` from one router health source.
pub(crate) struct PeerHealth<H> {
    health: H,
}

impl RuntimeHealth {
    /// Reads liveness from the peer runtime's own heartbeat registry.
    pub(crate) const fn new(heartbeats: HeartbeatRegistry) -> Self {
        Self { heartbeats }
    }
}

impl ProcessHealth for RuntimeHealth {
    fn ready(&self) -> bool {
        !self.heartbeats.any_stalled()
    }

    fn live(&self) -> bool {
        true
    }
}

impl<H> PeerHealth<H> {
    /// Answers for `health`, and for the peer service this listener serves.
    pub(crate) const fn new(health: H) -> Self {
        Self { health }
    }
}

#[async_trait]
impl<H: ProcessHealth> Health for PeerHealth<H> {
    /// A `Watch` needs a change signal. The router computes health on demand
    /// and publishes no such signal.
    type WatchStream = Empty<Result<HealthCheckResponse, Status>>;

    /// Answers for the router runtime under the empty name. Answers for the
    /// peer service under its own name.
    ///
    /// The empty name requires the router runtime to be ready and live.
    ///
    /// The peer service serves whenever this call is answered at all, because
    /// answering it *is* the evidence that the listener is up. Any other name
    /// is `NOT_FOUND`, which the protocol requires.
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let name = request.into_inner().service;
        let status = if name.is_empty() {
            if self.health.ready() && self.health.live() {
                ServingStatus::Serving
            } else {
                ServingStatus::NotServing
            }
        } else if name == SERVICE_NAME {
            ServingStatus::Serving
        } else {
            // The name is a caller's own bytes, on an unauthenticated port, so
            // it is never echoed back.
            return Err(Status::not_found("this service is not served here"));
        };
        Ok(Response::new(HealthCheckResponse {
            status: i32::from(WireStatus::from(status)),
        }))
    }

    async fn watch(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented(
            "peer health is answered by Check; this process publishes no change signal",
        ))
    }
}
