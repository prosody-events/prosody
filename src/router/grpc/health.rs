//! Router health on the peer listener.
//!
//! The peer service reports `SERVING` when its listener answers. It does not
//! depend on consumer readiness or liveness. Producer-only clients therefore
//! remain reachable for responses.

use super::generated::peer_service_server::SERVICE_NAME;
use async_trait::async_trait;
use futures::stream::Empty;
use tonic::{Request, Response, Status};
use tonic_health::ServingStatus;
use tonic_health::pb::health_check_response::ServingStatus as WireStatus;
use tonic_health::pb::health_server::Health;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

/// Serves `grpc.health.v1.Health` for one active router.
pub(crate) struct PeerHealth;

impl PeerHealth {
    /// Creates health for one active router.
    pub(crate) const fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Health for PeerHealth {
    /// A `Watch` needs a change signal. The router computes health on demand
    /// and publishes no such signal.
    type WatchStream = Empty<Result<HealthCheckResponse, Status>>;

    /// Answers for the router runtime under the empty name. Answers for the
    /// peer service under its own name.
    ///
    /// The empty name and peer service report `SERVING` while this router
    /// answers. Any other name is `NOT_FOUND`.
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let name = request.into_inner().service;
        let status = if name.is_empty() || name == SERVICE_NAME {
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
