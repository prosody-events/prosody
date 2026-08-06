//! `grpc.health.v1` beside the peer method.
//!
//! This service computes no predicate of its own: it calls [`is_ready`] and
//! [`is_live`], the pair `/readyz` and `/livez` already call. So a verdict here
//! and a verdict over HTTP cannot come from different state, and traffic never
//! reaches a process its own probe calls unready.
//!
//! Every `Check` recomputes. A cached verdict someone must refresh is exactly
//! the second opinion this service exists to avoid.

use super::generated::peer_server::SERVICE_NAME;
use crate::heartbeat::HeartbeatRegistry;
use async_trait::async_trait;
use futures::stream::Empty;
use tonic::{Request, Response, Status};
use tonic_health::ServingStatus;
use tonic_health::pb::health_check_response::ServingStatus as WireStatus;
use tonic_health::pb::health_server::Health;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

/// Where a process's readiness and liveness come from.
///
/// A source selector, never a flag: a process that runs no consumer has no
/// partition managers and must still answer health, so the source is a type
/// rather than an option on one.
pub(crate) trait ProcessHealth: Send + Sync + 'static {
    /// Whether this process is ready to take work.
    fn ready(&self) -> bool;

    /// Whether this process is making progress.
    fn live(&self) -> bool;
}

/// Health of a peer runtime that is serving this check.
pub(crate) struct RuntimeHealth {
    heartbeats: HeartbeatRegistry,
}

/// Serves `grpc.health.v1.Health` from one [`ProcessHealth`] source.
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
    /// A `Watch` needs a signal that a verdict changed, and this process has
    /// none: both predicates are computed on demand. Inventing one would be the
    /// cached second opinion this service avoids, and orchestrators probe with
    /// `Check`.
    type WatchStream = Empty<Result<HealthCheckResponse, Status>>;

    /// Answers for the whole process under the empty name, and for the peer
    /// service under its own.
    ///
    /// The empty name answers for the process, which means ready **and** live.
    /// A probe that wants liveness alone must ask `/livez`, because one serving
    /// status cannot carry two verdicts.
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
