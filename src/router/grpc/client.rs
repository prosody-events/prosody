//! The peer client: one framed response, over a real socket, to one address.

use super::codec::ClientFrameCodec;
use super::inject::MetadataInjector;
use crate::propagator::new_propagator;
use crate::router::directory::Endpoint;
use crate::router::fleet::DestinationFleet;
use crate::router::{Framed, ResponseSender, SendFailure};
use ahash::RandomState;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use quick_cache::UnitWeighter;
use quick_cache::sync::{Cache, DefaultLifecycle};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::time::Instant;
use tonic::client::Grpc;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint as Dialled};
use tonic::{Code, Request};
use tracing::{Span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// First timeout that Tonic cannot write as an eight-digit gRPC value.
pub(super) const GRPC_TIMEOUT_LIMIT: Duration = Duration::from_hours(100_000_000);

/// The one method a response travels over.
///
/// A literal rather than a value built per call, so the send path allocates
/// nothing for it. `the_method_path_names_the_generated_service` pins it
/// against the generated service name, so a renamed proto cannot leave it
/// misrouting quietly.
pub(super) static DELIVER_RESPONSE: LazyLock<PathAndQuery> =
    LazyLock::new(|| PathAndQuery::from_static("/prosody.peer.v1.Peer/DeliverResponse"));

/// One channel per live destination, keyed by the address a node published.
type Channels = Cache<Endpoint, Channel, UnitWeighter, RandomState>;

/// The production [`ResponseSender`]: it dials the address a node published and
/// delivers one frame per call.
///
/// # What bounds the memory
///
/// The cache holds as many channels as the fleet it was built from holds
/// destinations. `quick_cache` evicts to stay inside that count, and eviction
/// is the removal path: nothing else holds a channel, so an evicted one closes
/// its connections when its last clone drops. The key is the published address
/// rather than the node, so a node reached on both of its endpoints — while a
/// response probes one and falls back to the other — occupies two entries, as
/// does a node that restarts on another port. Neither grows the cache: the
/// count is the bound, and the entry that stops being dialled goes cold and is
/// evicted first.
pub(crate) struct GrpcSender {
    channels: Arc<Channels>,
    propagator: TextMapCompositePropagator,
}

impl GrpcSender {
    /// A sender for `fleet`.
    pub(crate) fn new(fleet: &DestinationFleet) -> Self {
        let capacity = fleet.config().peer_capacity;
        Self {
            channels: Arc::new(Cache::with(
                capacity,
                capacity as u64,
                UnitWeighter,
                RandomState::default(),
                DefaultLifecycle::default(),
            )),
            propagator: new_propagator(),
        }
    }

    /// The channel for `address`, dialling one on a miss.
    ///
    /// The connect is lazy, so a dead peer surfaces as the call's own status.
    /// The address is parsed here, though, so an address no URI can hold fails
    /// here — and fails the same way every time, which is why it is not
    /// [`SendFailure::Unreachable`]. Only a miss builds the URI, so a hit
    /// allocates nothing.
    async fn channel(&self, address: &Endpoint) -> Result<Channel, SendFailure> {
        match self.channels.get_value_or_guard_async(address).await {
            Ok(channel) => Ok(channel),
            Err(guard) => {
                let Ok(dialled) = Dialled::from_shared(peer_uri(address)) else {
                    warn!(host = %address.host, port = address.port, "a published address is not dialable");
                    return Err(SendFailure::Undialable);
                };
                let channel = dialled.connect_lazy();
                drop(guard.insert(channel.clone()));
                Ok(channel)
            }
        }
    }
}

impl ResponseSender for GrpcSender {
    /// Clones the frame's immutable handles and writes it into Tonic's final
    /// per-call buffer.
    async fn deliver<F: Framed + Sync>(
        &self,
        address: &Endpoint,
        frame: &F,
        deadline: Instant,
    ) -> Result<(), SendFailure> {
        let channel = self.channel(address).await?;
        let bytes = frame.bytes();
        let mut request = Request::new(frame.clone());
        self.propagator.inject_context(
            &Span::current().context(),
            &mut MetadataInjector::new(request.metadata_mut()),
        );
        let mut client = Grpc::new(channel);
        if let Err(error) = client.ready().await {
            warn!(%error, host = %address.host, port = address.port, "a peer channel never became ready");
            return Err(SendFailure::Unreachable);
        }
        // The outbound timeout is written here rather than earlier, because
        // everything above it — the channel lookup and the readiness wait —
        // spends against the same deadline. Nothing has left this process yet,
        // so no time left is this process's own expiry rather than an answer.
        let remaining = outbound_timeout(deadline)?;
        request.set_timeout(remaining);
        // The status is passed through as it arrived.
        match client
            .unary(
                request,
                DELIVER_RESPONSE.clone(),
                ClientFrameCodec::new(bytes),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(status) => Err(SendFailure::Status(status.code())),
        }
    }
}

/// Returns the remaining deadline when Tonic can represent it.
pub(super) fn outbound_timeout(deadline: Instant) -> Result<Duration, SendFailure> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(SendFailure::Expired)
    } else if remaining >= GRPC_TIMEOUT_LIMIT {
        Err(SendFailure::Status(Code::InvalidArgument))
    } else {
        Ok(remaining)
    }
}

/// The URI one endpoint is dialled with.
///
/// An IPv6 literal must be bracketed. Without the brackets the authority
/// carries more than one colon, no URI parser accepts it, and every response to
/// that node is reported unreachable.
pub(super) fn peer_uri(address: &Endpoint) -> String {
    let host = address.host.as_str();
    if host.contains(':') {
        format!("http://[{host}]:{}", address.port)
    } else {
        format!("http://{host}:{}", address.port)
    }
}
