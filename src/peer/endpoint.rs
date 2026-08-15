//! The public connect endpoint for peer traffic.

use std::str::FromStr;
use thiserror::Error;
use tonic::transport::{Endpoint, Error as TransportError};

/// A validated endpoint that another peer can connect to.
///
/// This type keeps the transport implementation outside the public API.
#[derive(Clone, Debug)]
pub struct PeerEndpoint(Endpoint);

impl PeerEndpoint {
    pub(crate) fn into_inner(self) -> Endpoint {
        self.0
    }
}

impl FromStr for PeerEndpoint {
    type Err = PeerEndpointError;

    fn from_str(connect: &str) -> Result<Self, Self::Err> {
        Endpoint::from_shared(connect.to_owned())
            .map(Self)
            .map_err(PeerEndpointError)
    }
}

impl TryFrom<String> for PeerEndpoint {
    type Error = PeerEndpointError;

    fn try_from(connect: String) -> Result<Self, Self::Error> {
        Endpoint::from_shared(connect)
            .map(Self)
            .map_err(PeerEndpointError)
    }
}

/// Why a peer connect endpoint is invalid.
#[derive(Debug, Error)]
#[error("invalid peer connect endpoint: {0}")]
pub struct PeerEndpointError(TransportError);
