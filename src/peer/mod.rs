//! Shared request and response delivery for producers and consumers.
//!
//! A peer runtime belongs to neither side. A producer uses it to wait for
//! responses. A consumer uses the same runtime to send responses. A combined
//! client constructs one runtime and shares it with both.
//!
//! Standalone code retains a [`LocalRouter`] or [`GrpcRouter`] while it uses
//! its capabilities. Dropping the router starts teardown. Call
//! [`Router::shutdown`] to wait for teardown.

mod backend;
pub(crate) mod metrics;
pub(crate) mod requester;
pub(crate) mod response;
pub(crate) mod router;
pub(crate) mod runtime;

#[cfg(test)]
pub(crate) use backend::PeerBackend;

pub use requester::{ProsodyRequester, RequestError, ResponseError};
pub use router::api::{GrpcRouter, LocalRouter, Router};
pub use router::config::{
    PeerConfiguration, PeerConfigurationBuilder, PeerConfigurationBuilderError,
};
pub use runtime::ProducerHandle;

use crate::heartbeat::HeartbeatRegistry;
use std::time::Duration;

/// How long the peer event loops may make no progress.
const STALL_THRESHOLD: Duration = Duration::from_secs(30);

/// Creates the heartbeat registry owned by one peer runtime.
pub(crate) fn heartbeat_registry() -> HeartbeatRegistry {
    HeartbeatRegistry::new("peer".to_owned(), STALL_THRESHOLD)
}
