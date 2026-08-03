//! The peer client: one framed response, over a real socket, to one address.

use super::codec::{ClientFrameCodec, FrameBytes};
use super::inject::MetadataInjector;
use crate::propagator::new_propagator;
use crate::response::frame::FrameCap;
use crate::router::directory::Endpoint;
use crate::router::{Framed, ResponseSender, SendFailure};
use ahash::RandomState;
use bytes::BytesMut;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use quick_cache::UnitWeighter;
use quick_cache::sync::{Cache, DefaultLifecycle};
use std::sync::{Arc, LazyLock};
use tonic::Request;
use tonic::client::Grpc;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint as Dialled};
use tracing::{Span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
/// The channel cache holds at most as many channels as the fleet holds
/// destinations, and `quick_cache` evicts to stay inside that. Eviction is the
/// removal path: nothing else holds a channel, so an evicted one closes its
/// connections when its last clone drops.
pub(crate) struct GrpcSender {
    channels: Arc<Channels>,
    cap: FrameCap,
    propagator: TextMapCompositePropagator,
}

impl GrpcSender {
    /// A sender that holds up to `destinations` channels and refuses to encode
    /// a frame over `cap`.
    pub(crate) fn new(cap: FrameCap, destinations: usize) -> Self {
        Self {
            channels: Arc::new(Cache::with(
                destinations,
                destinations as u64,
                UnitWeighter,
                RandomState::default(),
                DefaultLifecycle::default(),
            )),
            cap,
            propagator: new_propagator(),
        }
    }

    /// The channel for `address`, dialling one on a miss.
    ///
    /// The dial is lazy, so it cannot fail here and a dead peer surfaces as the
    /// call's own status. Only a miss builds the URI, so a hit allocates
    /// nothing.
    async fn channel(&self, address: &Endpoint) -> Result<Channel, SendFailure> {
        match self.channels.get_value_or_guard_async(address).await {
            Ok(channel) => Ok(channel),
            Err(guard) => {
                let uri = format!("http://{}:{}", address.host.as_str(), address.port);
                let Ok(dialled) = Dialled::from_shared(uri) else {
                    warn!(host = %address.host, port = address.port, "a published address is not dialable");
                    return Err(SendFailure::Unreachable);
                };
                let channel = dialled.connect_lazy();
                drop(guard.insert(channel.clone()));
                Ok(channel)
            }
        }
    }
}

impl ResponseSender for GrpcSender {
    /// Copies the staged frame into one right-sized buffer and delivers it.
    ///
    /// That copy is the trade [`ResponseSender`] documents: tonic's encoder
    /// must own what it writes, so the borrowed frame is materialized once,
    /// immediately before a network round trip, and never per byte.
    async fn deliver<F: Framed + Sync>(
        &self,
        address: &Endpoint,
        frame: &F,
    ) -> Result<(), SendFailure> {
        let channel = self.channel(address).await?;
        let bytes = frame.bytes();
        let mut buffer = BytesMut::with_capacity(bytes);
        frame.write(&mut buffer);
        let mut request = Request::new(FrameBytes::new(buffer.freeze()));
        self.propagator.inject_context(
            &Span::current().context(),
            &mut MetadataInjector::new(request.metadata_mut()),
        );
        // The encoding ceiling is set explicitly because tonic defaults it to
        // the whole address space. The decoding ceiling is zero because the
        // answer is the status alone and carries no body.
        let mut client = Grpc::new(channel)
            .max_encoding_message_size(self.cap.bytes())
            .max_decoding_message_size(0);
        if let Err(error) = client.ready().await {
            warn!(%error, host = %address.host, port = address.port, "a peer channel never became ready");
            return Err(SendFailure::Unreachable);
        }
        // The status is passed through as the destination gave it. Rewriting a
        // code here would silently change a retry decision, because
        // `SendFailure::is_ambiguous` reads exactly this code.
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
