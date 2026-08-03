//! `grpc.health.v1` beside the peer method.
//!
//! An orchestrator that probes over gRPC must get the same answer the HTTP
//! probes give. So this service computes no predicate of its own: it calls
//! [`is_ready`] and [`is_live`], the pair `/readyz` and `/livez` already call.
//! Two health surfaces that can disagree would route traffic to a process its
//! own probe calls unready.
//!
//! Every `Check` recomputes. A cached verdict someone must refresh is exactly
//! the second opinion this service exists to avoid.

use crate::consumer::Managers;
use crate::consumer::probes::{is_live, is_ready};
use crate::heartbeat::HeartbeatRegistry;
use async_trait::async_trait;
use futures::stream::Empty;
use std::sync::Arc;
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

/// A consumer's health, read from the same state the HTTP probes read.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the consumer wiring is this type's production caller; it is exercised by this \
                  module's tests"
    )
)]
pub(crate) struct ConsumerHealth<P> {
    managers: Arc<Managers<P>>,
    heartbeats: HeartbeatRegistry,
}

/// Serves `grpc.health.v1.Health` from one [`ProcessHealth`] source.
pub(crate) struct PeerHealth<H> {
    health: H,
    /// The peer service this listener answers for by name.
    service: &'static str,
}

impl<P> ConsumerHealth<P> {
    /// Reads the consumer that owns `managers` and `heartbeats`.
    pub(crate) const fn new(managers: Arc<Managers<P>>, heartbeats: HeartbeatRegistry) -> Self {
        Self {
            managers,
            heartbeats,
        }
    }
}

impl<P: Send + Sync + 'static> ProcessHealth for ConsumerHealth<P> {
    fn ready(&self) -> bool {
        is_ready(&self.managers)
    }

    fn live(&self) -> bool {
        is_live(&self.managers, &self.heartbeats)
    }
}

impl<H> PeerHealth<H> {
    /// Answers for `health`, and for the peer service named `service`.
    pub(crate) const fn new(health: H, service: &'static str) -> Self {
        Self { health, service }
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
        } else if name == self.service {
            ServingStatus::Serving
        } else {
            return Err(Status::not_found(format!(
                "service {name} is not served here"
            )));
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
