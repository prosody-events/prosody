//! Shared request and response delivery for producers and consumers.
//!
//! A peer runtime belongs to neither side. A producer uses it to wait for
//! responses. A consumer uses the same runtime to send responses. A combined
//! client constructs one runtime and shares it with both.

mod backend;
mod router;
pub(crate) mod runtime;

pub(crate) use backend::PeerBackend;
pub(crate) use runtime::{ConsumerResources, NoPeer};

pub use crate::requester::{Outcome, ProsodyRequester, RequestError, ResponseFailure};
pub use crate::router::config::{
    PeerConfiguration, PeerConfigurationBuilder, PeerConfigurationBuilderError,
};
pub use router::{GrpcRouter, LocalRouter, Router};

use crate::codec::Codec;
use crate::consumer::middleware::respond::Responder;
use crate::heartbeat::HeartbeatRegistry;
use crate::response::frame::FrameCap;
use crate::response::sender::{ResponseRoute, ResponseWorkers};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfigurationError;
use crate::subsystem::SubsystemName;
use std::sync::Arc;
use std::time::Duration;

/// How long the peer event loops may make no progress.
const STALL_THRESHOLD: Duration = Duration::from_secs(30);

/// Creates the heartbeat registry owned by one peer runtime.
pub(crate) fn heartbeat_registry() -> HeartbeatRegistry {
    HeartbeatRegistry::new("peer".to_owned(), STALL_THRESHOLD)
}

/// Builds typed responders over one shared peer route.
#[derive(Clone)]
pub(crate) struct PeerResponder<R> {
    route: R,
    fleet: Arc<DestinationFleet>,
    cap: FrameCap,
}

impl<R: ResponseRoute> PeerResponder<R> {
    /// Captures one route, fleet, and frame ceiling.
    pub(crate) const fn new(route: R, fleet: Arc<DestinationFleet>, cap: FrameCap) -> Self {
        Self { route, fleet, cap }
    }

    /// Binds one response codec and subsystem to this peer route.
    pub(crate) fn responder<C: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<(Responder<C>, ResponseWorkers), FleetConfigurationError> {
        Responder::new_route(&self.route, &self.fleet, self.cap, subsystem)
    }
}
