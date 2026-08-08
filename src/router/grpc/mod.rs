//! Both ends of the peer wire: the listener a node serves and the client it
//! dials another node with.
//!
//! This directory is the only place in the crate that names tonic's transport,
//! service, codec, metadata, health and reflection surface. One type is shared
//! outside it — [`tonic::Code`] — because a gRPC status is the wire's own
//! vocabulary for an outcome. That is the rule rather than a list of sites: any
//! item outside this directory that must name what a destination answered names
//! it as a [`tonic::Code`] and carries the code itself. A status enum of this
//! crate's own would need a translation at both ends and would say nothing
//! more.
//!
//! The router carries no response vocabulary except here, at the wire seam it
//! owns: the peer method's message *is* the response frame, so the frame, the
//! subsystem it names and the registry that waits for it appear under this
//! module and nowhere else in the router.

pub(crate) mod client;
pub(crate) mod codec;
mod conn;
mod deadline;
pub(crate) mod health;
mod inject;
pub(crate) mod service;

/// The peer service, written from `proto/peer.proto` at build time.
pub(crate) mod generated {
    include!(concat!(env!("OUT_DIR"), "/prosody.peer.v1.rs"));
}

#[cfg(test)]
mod tests;

use self::conn::incoming;
use self::generated::peer_server::PeerServer;
use self::health::{PeerHealth, ProcessHealth};
use self::service::PeerService;
use crate::response::frame::FrameCap;
use crate::router::RelayHop;
use derive_builder::Builder;
use std::io::Error as IoError;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic_health::pb::health_server::HealthServer;
use tonic_reflection::server::{Builder as ReflectionBuilder, Error as ReflectionError};
use tracing::error;
use validator::{Validate, ValidationErrors};

/// The peer schema, embedded so reflection can publish it without a file.
const DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/peer_descriptor.bin"));

/// How often the listener pings a connection that carries nothing.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// How long a pinged peer has to answer before its connection is closed.
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Most one health or reflection request may carry. Both messages are a service
/// name at most, so this ceiling is small and fixed rather than configurable.
const CONTROL_MESSAGE_BYTES: usize = 4 * 1024;

/// Internal peer listener settings.
#[derive(Builder, Clone, Copy, Debug, Validate)]
#[builder(setter(into), default)]
pub(crate) struct TransportConfiguration {
    /// The address the listener binds. Port zero asks the operating system for
    /// one, and [`BoundListener`] is then the only place that port can be read.
    pub(crate) bind: SocketAddr,

    /// The internal ceiling on one encoded frame, in both directions.
    pub(crate) frame_cap: FrameCap,
}

/// A listener that is already bound, the address it bound, and the
/// configuration it was bound under.
///
/// The address is the one the operating system assigned, and it is the only
/// address this type will give up: a caller cannot reach the configured one, so
/// registration can only publish a port something bound. [`serve`] reads its
/// caps from the configuration carried here, so serving a listener under caps
/// nothing validated is unwritable in the same way.
pub(crate) struct BoundListener {
    listener: TcpListener,
    address: SocketAddr,
    config: TransportConfiguration,
}

impl Default for TransportConfiguration {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
            frame_cap: FrameCap::DEFAULT,
        }
    }
}

impl BoundListener {
    /// Binds the address `config` names.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Configuration`] when a cap is out of range,
    /// and [`TransportError::Bind`] when the address cannot be bound.
    pub(crate) async fn bind(config: &TransportConfiguration) -> Result<Self, TransportError> {
        config.validate()?;
        let listener = TcpListener::bind(config.bind).await?;
        let address = listener.local_addr()?;
        Ok(Self {
            listener,
            address,
            config: *config,
        })
    }

    /// The address the operating system assigned. Its port is what registration
    /// publishes.
    pub(crate) const fn address(&self) -> SocketAddr {
        self.address
    }

    /// The frame ceiling that the listener enforces.
    pub(crate) const fn frame_cap(&self) -> FrameCap {
        self.config.frame_cap
    }
}

/// Serves the peer method, health, and reflection on `bound` until `shutdown`
/// completes.
///
/// This is the one place the peer server is built. Transport security and peer
/// authorization are designed separately, and they attach here.
///
/// Tonic and HTTP/2 provide flow control. Prosody adds no concurrency limit.
///
/// # Errors
///
/// Returns [`TransportError::Reflection`] when the embedded schema cannot be
/// published.
pub(in crate::router) fn serve<R, H, F>(
    bound: BoundListener,
    service: PeerService<R>,
    health: H,
    shutdown: F,
) -> Result<JoinHandle<()>, TransportError>
where
    R: RelayHop,
    H: ProcessHealth,
    F: Future<Output = ()> + Send + 'static,
{
    let config = bound.config;
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(DESCRIPTOR_SET)
        .build_v1()?
        .max_decoding_message_size(CONTROL_MESSAGE_BYTES);
    let incoming = incoming(bound.listener);
    let router = Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .add_service(PeerServer::new(service).max_decoding_message_size(config.frame_cap.bytes()))
        .add_service(
            HealthServer::new(PeerHealth::new(health))
                .max_decoding_message_size(CONTROL_MESSAGE_BYTES),
        )
        .add_service(reflection);
    Ok(tokio::spawn(async move {
        if let Err(error) = router
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
        {
            error!(%error, "the peer listener stopped with an error");
        }
    }))
}

/// Why a peer listener could not be built or bound.
#[derive(Debug, Error)]
pub(crate) enum TransportError {
    /// The configuration this listener was asked for is invalid.
    #[error("peer transport configuration is invalid: {0:#}")]
    Configuration(#[from] ValidationErrors),

    /// The listener could not bind the address it was given.
    #[error("the peer listener could not bind: {0:#}")]
    Bind(#[from] IoError),

    /// The embedded peer schema could not be published.
    #[error("the peer schema could not be published: {0:#}")]
    Reflection(#[from] ReflectionError),
}
