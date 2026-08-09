//! Shared request and response delivery for producers and consumers.
//!
//! A peer runtime belongs to neither side. A producer uses it to wait for
//! responses. A consumer uses the same runtime to send responses. A combined
//! client constructs one runtime and shares it with both.
//!
//! Standalone code starts a [`LocalRouter`] or [`GrpcRouter`], then calls
//! [`Router::split`]. Retain the owner until shutdown. Give the other two
//! capabilities to the producer and consumer.

mod backend;
mod router;
pub(crate) mod runtime;

pub(crate) use backend::PeerBackend;
pub(crate) use router::responding_provider;
pub(crate) use runtime::{ConsumerResources, NoPeer};

pub use crate::requester::{Outcome, ProsodyRequester, RequestError, ResponseFailure};
pub use crate::router::config::{
    PeerConfiguration, PeerConfigurationBuilder, PeerConfigurationBuilderError,
};
pub use router::{ConsumerRouter, GrpcConsumer, GrpcRouter, LocalConsumer, LocalRouter, Router};
pub use runtime::{ProducerHandle, RouterOwner};

use crate::codec::Codec;
use crate::consumer::middleware::respond::Responder;
use crate::heartbeat::HeartbeatRegistry;
use crate::response::sender::ResponseRoute;
use crate::router::fleet::DestinationFleet;
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
}

impl<R: ResponseRoute> PeerResponder<R> {
    /// Captures one route and fleet.
    pub(crate) const fn new(route: R, fleet: Arc<DestinationFleet>) -> Self {
        Self { route, fleet }
    }

    /// Binds one response codec and subsystem to this peer route.
    pub(crate) fn responder<C: Codec>(&self, subsystem: SubsystemName) -> Responder<C, R> {
        Responder::new_route(self.route.clone(), &self.fleet, subsystem)
    }
}

impl<R> PeerResponder<R> {
    fn map_route<T>(self, map: impl FnOnce(R) -> T) -> PeerResponder<T> {
        PeerResponder {
            route: map(self.route),
            fleet: self.fleet,
        }
    }
}
