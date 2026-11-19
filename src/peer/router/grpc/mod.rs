//! Both ends of the peer wire: the listener a peer serves and the client it
//! dials another peer with.
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
mod deadline;
mod inject;
pub(crate) mod service;
mod telemetry;

/// The peer service, written from the peer Protobuf schema at build time.
#[expect(
    clippy::absolute_paths,
    clippy::doc_markdown,
    clippy::impl_trait_in_params,
    clippy::trivially_copy_pass_by_ref,
    reason = "written by tonic-prost-build, not by hand"
)]
pub(crate) mod generated {
    include!(concat!(env!("OUT_DIR"), "/prosody.peer.v1.rs"));
}

#[cfg(test)]
mod tests;

use self::generated::peer_service_server::PeerServiceServer;
use self::service::PeerService;
use crate::peer::router::RelayHop;
use crate::peer::router::interface::{InterfaceError, default_address};
use std::io::{Error as IoError, ErrorKind};
use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic_health::server::health_reporter;
use tonic_reflection::server::{Builder as ReflectionBuilder, Error as ReflectionError};
use tracing::{error, info};

/// The peer schema, embedded so reflection can publish it without a file.
const DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/peer_descriptor.bin"));

/// How often the listener pings a connection that carries nothing.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// How long a pinged peer has to answer before its connection is closed.
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// A listener that is already bound and the address that peers connect to.
///
/// The port is the one the operating system assigned, so registration can only
/// publish a port something bound. The IP address is never unspecified.
pub(crate) struct BoundListener {
    listener: TcpListener,
    address: SocketAddr,
}

impl BoundListener {
    /// Binds `address`. Port zero asks the operating system for one.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError`] when the address cannot be bound or has no
    /// address to publish.
    pub(crate) async fn bind(address: SocketAddr) -> Result<Self, TransportError> {
        Self::listen(TcpListener::bind(address).await?)
    }

    /// Binds `address`. If another process holds the port, binds a port that
    /// the operating system selects.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError`] when neither port can be bound or the
    /// listener has no address to publish.
    pub(crate) async fn bind_any_port(address: SocketAddr) -> Result<Self, TransportError> {
        match TcpListener::bind(address).await {
            Err(held) if held.kind() == ErrorKind::AddrInUse => {
                let mut any_port = address;
                any_port.set_port(0);
                let listener = TcpListener::bind(any_port)
                    .await
                    .map_err(|retry| TransportError::Held { held, retry })?;
                let bound = Self::listen(listener)?;
                info!(
                    requested = %address,
                    bound = %bound.address,
                    "Another process holds the requested peer port. Prosody uses a port that \
                     the operating system selected. If a firewall limits peer traffic, allow \
                     this port."
                );
                Ok(bound)
            }
            bound => Self::listen(bound?),
        }
    }

    /// Wraps a bound listener. A listener on an unspecified IP address
    /// publishes the default interface address of the same family.
    fn listen(listener: TcpListener) -> Result<Self, TransportError> {
        let bound = listener.local_addr()?;
        let address = if bound.ip().is_unspecified() {
            default_address(Some(bound.ip()), bound.port())?
        } else {
            bound
        };
        Ok(Self { listener, address })
    }

    /// The address that registration publishes.
    pub(crate) const fn address(&self) -> SocketAddr {
        self.address
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
pub(in crate::peer::router) async fn serve<R, F>(
    bound: BoundListener,
    service: PeerService<R>,
    shutdown: F,
) -> Result<JoinHandle<()>, TransportError>
where
    R: RelayHop,
    F: Future<Output = ()> + Send + 'static,
{
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(DESCRIPTOR_SET)
        .build_v1()?;
    let incoming = TcpIncoming::from(bound.listener).with_nodelay(Some(true));
    let (health_reporter, health) = health_reporter();
    health_reporter
        .set_serving::<PeerServiceServer<PeerService<R>>>()
        .await;
    let router = Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .add_service(PeerServiceServer::new(service))
        .add_service(health)
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
    /// The listener could not bind the address it was given.
    #[error("the peer listener could not bind: {0:#}")]
    Bind(#[from] IoError),

    /// Another process holds the port, and no other port could be bound.
    #[error(
        "another process holds the peer port ({held:#}), and no other port could be bound: \
         {retry:#}"
    )]
    Held { held: IoError, retry: IoError },

    /// A wildcard listener has no interface address to publish.
    #[error("the peer listener has no address to publish: {0:#}")]
    Interface(#[from] InterfaceError),

    /// The embedded peer schema could not be published.
    #[error("the peer schema could not be published: {0:#}")]
    Reflection(#[from] ReflectionError),
}
