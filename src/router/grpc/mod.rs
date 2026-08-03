//! Both ends of the peer wire: the listener a node serves and the client it
//! dials another node with.
//!
//! This directory is the only place in the crate that names tonic's transport,
//! service, codec, metadata, health and reflection surface. One type is shared
//! outside it — [`tonic::Code`] — because a gRPC status is the wire's own
//! vocabulary for an outcome, and
//! [`ResponseDisposition`](crate::response::ResponseDisposition) owns the
//! mapping onto it.
//!
//! The router carries no response vocabulary except here, at the wire seam it
//! owns: the peer method's message *is* the response frame, so the frame, the
//! subsystem it names and the registry that waits for it appear under this
//! module and nowhere else in the router.

// The `not(test)` gate is what makes this an *expectation* rather than a
// blanket permission. It holds while any item in this module is
// production-dead, and it is deleted the day the process runtime builds the
// listener.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the process runtime is this module's production caller; every item here is \
                  exercised by this module's tests"
    )
)]

pub(crate) mod client;
pub(crate) mod codec;
mod conn;
pub(crate) mod health;
mod inject;
pub(crate) mod service;

/// The peer service, written from `proto/peer.proto` at build time.
pub(crate) mod generated {
    include!(concat!(env!("OUT_DIR"), "/prosody.peer.v1.rs"));
}

#[cfg(test)]
mod tests;

use self::conn::admitted;
use self::generated::peer_server::PeerServer;
use self::health::{PeerHealth, ProcessHealth};
use self::service::PeerService;
use crate::response::frame::FrameCap;
use derive_builder::Builder;
use std::io::Error as IoError;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
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
///
/// Together with [`KEEPALIVE_INTERVAL`] this is what bounds a permit whose peer
/// stopped answering. A peer that dies without a FIN would otherwise hold its
/// admission permit for the life of the process.
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Most one health or reflection request may carry. Both messages are a service
/// name at most, so this ceiling is small and fixed rather than configurable.
const CONTROL_MESSAGE_BYTES: usize = 4 * 1024;

/// Most concurrent connections one listener may be configured to hold. Each one
/// costs a file descriptor, so this ceiling is the file-descriptor budget one
/// process is given.
const MAX_CONNECTIONS: usize = 4_096;

/// Most concurrent streams one connection may be configured to open. Each open
/// stream holds h2 buffers of its own, so this ceiling is what one connection
/// may make this process hold.
const MAX_STREAMS: u32 = 1_024;

/// Connections one listener holds when an operator sets no number.
const DEFAULT_MAX_CONNECTIONS: usize = 256;

/// Streams one connection opens by default.
const DEFAULT_MAX_STREAMS: u32 = 64;

/// What this process's peer listener refused, and how often its service ran.
///
/// One process serves one peer listener, so these are the process's counters.
/// They are reached through this static rather than held as fields because the
/// generated service builds the frame codec through [`Default`] and can pass it
/// nothing — and the codec is the only place that can see a frame the transport
/// refuses.
pub(crate) static TRANSPORT: TransportCounters = TransportCounters::new();

/// What an operator sets for the peer listener.
///
/// Every field has a working default, so a process that serves peers needs no
/// configuration at all. The per-message ceiling bounds one frame; the two
/// concurrency caps bound how many of them can be in flight before any registry
/// admission is consulted.
#[derive(Builder, Clone, Copy, Debug, Validate)]
#[builder(setter(into), default)]
pub(crate) struct TransportConfiguration {
    /// The address the listener binds. Port zero asks the operating system for
    /// one, and [`BoundListener`] is then the only place that port can be read.
    pub(crate) bind: SocketAddr,

    /// The ceiling on one encoded frame, in both directions. The listener
    /// refuses a larger message before it is decoded, and the client refuses
    /// one before it is sent.
    pub(crate) frame_cap: FrameCap,

    /// How many connections the listener holds at once. One over the cap is
    /// refused and counted, never queued.
    #[validate(range(min = 1_usize, max = MAX_CONNECTIONS))]
    pub(crate) max_connections: usize,

    /// How many streams one connection may run at once.
    #[validate(range(min = 1_u32, max = MAX_STREAMS))]
    pub(crate) max_concurrent_streams: u32,

    /// Whether the listener publishes the peer schema through server
    /// reflection. It is the one thing here that hands an unauthenticated
    /// caller information rather than a rejection, so it is configurable.
    pub(crate) reflection: bool,
}

/// A listener that is already bound, the address it bound, and the
/// configuration it was bound under.
///
/// The address is the one the operating system assigned. There is no configured
/// address to read from this type, so publishing a port nothing listens on is
/// unwritable rather than merely discouraged. [`serve`] reads its caps from the
/// configuration carried here, so serving a listener under caps nothing
/// validated is unwritable in the same way.
pub(crate) struct BoundListener {
    listener: TcpListener,
    address: SocketAddr,
    config: TransportConfiguration,
}

/// Counts kept by the peer listener.
///
/// Every count is monotonic and read as a difference: what the transport
/// refused, and how often the peer method actually ran. A frame the transport
/// refuses leaves [`served`](Self::served) alone, which is what separates a
/// transport rejection from a registry outcome.
pub(crate) struct TransportCounters {
    served: AtomicU64,
    refused_connections: AtomicU64,
    rejected_frames: AtomicU64,
    misrouted: AtomicU64,
}

impl Default for TransportConfiguration {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
            frame_cap: FrameCap::DEFAULT,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_concurrent_streams: DEFAULT_MAX_STREAMS,
            reflection: true,
        }
    }
}

impl TransportConfiguration {
    /// Creates a transport configuration builder.
    #[must_use]
    pub(crate) fn builder() -> TransportConfigurationBuilder {
        TransportConfigurationBuilder::default()
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
}

impl TransportCounters {
    const fn new() -> Self {
        Self {
            served: AtomicU64::new(0),
            refused_connections: AtomicU64::new(0),
            rejected_frames: AtomicU64::new(0),
            misrouted: AtomicU64::new(0),
        }
    }

    /// How often the peer method ran, whatever it then answered.
    pub(crate) fn served(&self) -> u64 {
        self.served.load(Relaxed)
    }

    /// How many connections were refused over the concurrency cap.
    pub(crate) fn refused_connections(&self) -> u64 {
        self.refused_connections.load(Relaxed)
    }

    /// How many frames the reader refused before the service could run.
    ///
    /// A frame over the listener's configured ceiling is not one of them: the
    /// transport refuses it before the reader sees it, and the `OUT_OF_RANGE`
    /// the peer is answered is the signal for that refusal.
    pub(crate) fn rejected_frames(&self) -> u64 {
        self.rejected_frames.load(Relaxed)
    }

    /// How many frames named a node other than this one.
    pub(crate) fn misrouted(&self) -> u64 {
        self.misrouted.load(Relaxed)
    }

    fn record_served(&self) {
        self.served.fetch_add(1, Relaxed);
    }

    fn record_refused_connection(&self) {
        self.refused_connections.fetch_add(1, Relaxed);
    }

    fn record_rejected_frame(&self) {
        self.rejected_frames.fetch_add(1, Relaxed);
    }

    fn record_misrouted(&self) {
        self.misrouted.fetch_add(1, Relaxed);
    }
}

/// Serves the peer method, health and — when configured — reflection on
/// `bound`, until `shutdown` completes.
///
/// This is the one place the peer server is built. Transport security and peer
/// authorization are designed separately, and they attach here.
///
/// Only the stream cap is set per connection: for a unary method a concurrency
/// limit beside it would bound service execution inside a connection h2 already
/// limits to that many streams.
///
/// # Errors
///
/// Returns [`TransportError::Reflection`] when the embedded schema cannot be
/// published.
pub(crate) fn serve<H, F>(
    bound: BoundListener,
    service: PeerService,
    health: H,
    shutdown: F,
) -> Result<JoinHandle<()>, TransportError>
where
    H: ProcessHealth,
    F: Future<Output = ()> + Send + 'static,
{
    let config = bound.config;
    let reflection = config
        .reflection
        .then(|| {
            ReflectionBuilder::configure()
                .register_encoded_file_descriptor_set(DESCRIPTOR_SET)
                .build_v1()
        })
        .transpose()?
        .map(|service| service.max_decoding_message_size(CONTROL_MESSAGE_BYTES));
    let incoming = admitted(bound.listener, config.max_connections);
    let router = Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .max_concurrent_streams(config.max_concurrent_streams)
        .add_service(PeerServer::new(service).max_decoding_message_size(config.frame_cap.bytes()))
        .add_service(
            HealthServer::new(PeerHealth::new(health))
                .max_decoding_message_size(CONTROL_MESSAGE_BYTES),
        )
        .add_optional_service(reflection);
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
