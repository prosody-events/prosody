//! Peer runtime startup errors.

use super::discovery::DiscoveryError;
use crate::router::cache_config::PeerCacheConfigurationError;
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

    /// The peer cache bound was invalid.
    #[error("the peer cache configuration is invalid: {0:#}")]
    Cache(#[from] PeerCacheConfigurationError),

    /// The bound peer listener could not start its service.
    #[error("the peer listener could not be served: {0:#}")]
    Listener(#[from] TransportError),
}
