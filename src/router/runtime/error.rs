//! Peer runtime startup errors.

use super::discovery::DiscoveryError;
use crate::router::fleet::config::FleetConfigurationError;
use crate::router::grpc::TransportError;
use thiserror::Error;
use validator::ValidationErrors;

/// What can stop a process from serving its peer machinery.
///
/// It names no directory error. Preparation writes nothing. The first write
/// belongs to
/// [`PreparedPeerRuntime::activate`](super::PreparedPeerRuntime::activate).
/// That function returns the directory error beside the unspent value.
#[derive(Debug, Error)]
pub(crate) enum PeerRuntimeError {
    /// The configuration this process was started with is invalid.
    #[error("router configuration is invalid: {0:#}")]
    Configuration(#[from] ValidationErrors),

    /// This process could not learn what only its machine knows.
    #[error("this process could not read what only its machine knows: {0:#}")]
    Discovery(#[from] DiscoveryError),

    /// The destination limits were invalid.
    #[error("the destination fleet could not be built: {0:#}")]
    Fleet(#[from] FleetConfigurationError),

    /// The bound peer listener could not start its service.
    #[error("the peer listener could not be served: {0:#}")]
    Listener(#[from] TransportError),
}
