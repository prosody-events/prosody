//! The peer transport's suites, driven against a real loopback listener.
//!
//! One listener serves the whole test process. Isolation is by request id,
//! which the registry already mints fresh per registration, so no suite needs a
//! listener of its own unless it is testing the listener's own configuration.
//!
//! The transport counters are the process's, so every assertion on them reads a
//! difference across the call under test rather than an absolute.

mod client;
mod dispositions;
mod health;
mod inject;
mod listener;
mod transport;

use super::client::GrpcSender;
use super::service::PeerService;
use super::{BoundListener, TransportConfiguration, serve};
use crate::codec::Codec;
use crate::requester::config::{MAX_IN_FLIGHT, RequesterConfiguration};
use crate::requester::registry::PendingRegistry;
use crate::response::frame::encode::FrameEncoder;
use crate::response::frame::tests::CountingCodec;
use crate::response::frame::{FrameCap, FrameHeader, ResponseFrame};
use crate::response::{FormatToken, RequestId, ResponseStatus};
use crate::router::directory::Endpoint;
use crate::router::fleet::DestinationFleet;
use crate::router::loopback::{TestHealth, config};
use crate::router::{Framed, Host, NodeId, ResponseSender, SendFailure};
use crate::subsystem::SubsystemName;
use bytes::{BufMut, BytesMut};
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::sync::oneshot::{Sender, channel};
use tokio::task::JoinHandle;
use tonic::Code;

/// The listener's frame ceiling. Small enough that an over-cap frame costs one
/// short allocation to build.
const FRAME_CAP: usize = 8 * 1024;

/// The ceiling a sender that must out-reach the listener encodes under.
const WIDE_FRAME_CAP: usize = 2 * FRAME_CAP;

/// Most bytes one accepted payload may carry. Below the frame ceiling, so a
/// payload the registry refuses still fits a frame the transport accepts.
const MAX_RESPONSE_BYTES: usize = 1024;

/// A payload the registry refuses and the transport carries.
const OVER_RESPONSE_BYTES: usize = 2 * MAX_RESPONSE_BYTES;

/// A frame the listener refuses and a wide sender encodes.
const OVER_FRAME_BYTES: usize = FRAME_CAP + 1024;

/// The subsystem a request awaits.
const ALPHA: &str = "alpha";

/// A second subsystem, for the cases that need two positions.
const BETA: &str = "beta";

/// How long a request in these suites stays open. Long enough that no
/// assertion races the deadline sweep.
const TIMEOUT: Duration = Duration::from_secs(30);

/// Destinations a suite's fleet holds, which is what sizes the channel cache of
/// the senders built from it. Every suite reaches one listener, so one channel
/// is enough and the second is slack.
const SUITE_DESTINATIONS: usize = 2;

/// Responses one destination in a suite's fleet may hold at once.
const SUITE_SLOTS: usize = 2;

/// The one listener every suite that needs a wire shares.
static SHARED: OnceCell<Harness> = OnceCell::const_new();

/// A live listener, the registry it serves, and the senders that reach it.
pub(super) struct Harness {
    /// The node the listener answers for.
    pub(super) node: NodeId,
    /// The registry the listener hands frames to.
    pub(super) registry: Arc<PendingRegistry>,
    /// An identically configured registry, driven in process. It is the oracle
    /// the wire's status is compared against.
    pub(super) oracle: Arc<PendingRegistry>,
    /// A sender whose ceiling matches the listener's.
    pub(super) sender: GrpcSender,
    /// A sender whose ceiling is above the listener's, so a frame it refuses
    /// can only have been refused by the listener.
    pub(super) wide: GrpcSender,
    /// Where the listener is.
    pub(super) address: Endpoint,
    /// The ceiling the listener and its matching sender share.
    pub(super) cap: FrameCap,
    stop: Option<Sender<()>>,
    served: Option<JoinHandle<()>>,
}

/// Bytes already framed, so a suite can put a frame on the wire that no encoder
/// would produce.
struct RawFramed(BytesMut);

impl Harness {
    /// The listener every suite shares.
    pub(super) async fn shared() -> Result<&'static Self> {
        SHARED
            .get_or_try_init(|| Self::with(transport(FRAME_CAP)))
            .await
    }

    /// A listener of this suite's own, for the cases that vary its
    /// configuration. Call [`stop`](Self::stop) before the test returns.
    pub(super) async fn with(config: Result<TransportConfiguration>) -> Result<Self> {
        let config = config?;
        let served_registry = registry()?;
        let node = NodeId::new();
        let bound = BoundListener::bind(&config).await?;
        let address = Endpoint {
            host: Host::make("127.0.0.1"),
            port: bound.address().port(),
        };
        let (stop, stopped) = channel();
        let served = serve(
            bound,
            PeerService::new(node, Arc::clone(&served_registry)),
            TestHealth::new(true, true),
            // A signal stops the listener; so does dropping the sender, which
            // is what a harness that is simply dropped does.
            async move { stopped.await.unwrap_or(()) },
        )?;
        Ok(Self {
            node,
            registry: served_registry,
            oracle: registry()?,
            sender: GrpcSender::new(config.frame_cap, &fleet(SUITE_DESTINATIONS)?),
            wide: GrpcSender::new(FrameCap::new(WIDE_FRAME_CAP)?, &fleet(SUITE_DESTINATIONS)?),
            address,
            cap: config.frame_cap,
            stop: Some(stop),
            served: Some(served),
        })
    }

    /// Frames one response and delivers it, reporting the status the listener
    /// answered.
    pub(super) async fn deliver(&self, header: &FrameHeader, payload: Vec<u8>) -> Result<Code> {
        self.deliver_under(&self.sender, self.cap, header, payload)
            .await
    }

    /// [`deliver`](Self::deliver) with the sender and the encode ceiling named,
    /// so a suite can put a frame on the wire that one of the two would refuse.
    pub(super) async fn deliver_under(
        &self,
        sender: &GrpcSender,
        cap: FrameCap,
        header: &FrameHeader,
        payload: Vec<u8>,
    ) -> Result<Code> {
        let mut encoder = FrameEncoder::new(CountingCodec::default(), cap);
        let staged = encoder.stage(header, payload)?;
        status(sender.deliver(&self.address, &staged).await)
    }

    /// Delivers bytes exactly as given.
    pub(super) async fn deliver_raw(&self, sender: &GrpcSender, bytes: BytesMut) -> Result<Code> {
        status(sender.deliver(&self.address, &RawFramed(bytes)).await)
    }

    /// Stops the listener and waits for it to finish.
    ///
    /// The listener task logs its own serve error, so a join failure here is a
    /// panic inside that task and is reported rather than dropped.
    pub(super) async fn stop(mut self) -> Result<()> {
        drop(self.stop.take());
        if let Some(served) = self.served.take() {
            served.await?;
        }
        Ok(())
    }
}

impl Framed for RawFramed {
    fn bytes(&self) -> usize {
        self.0.len()
    }

    fn write<B: BufMut>(&self, dst: &mut B) {
        dst.put_slice(&self.0);
    }
}

/// The listener configuration these suites bind, with `cap` as its ceiling.
pub(super) fn transport(cap: usize) -> Result<TransportConfiguration> {
    Ok(TransportConfiguration {
        bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        frame_cap: FrameCap::new(cap)?,
        ..TransportConfiguration::default()
    })
}

/// A fleet of `destinations` destinations, which is what sizes a sender's
/// channel cache.
pub(super) fn fleet(destinations: usize) -> Result<DestinationFleet> {
    Ok(DestinationFleet::new(config(destinations, SUITE_SLOTS))?)
}

/// A registry with a response ceiling below the frame ceiling.
///
/// Admission is the registry's own ceiling rather than its default. These
/// suites register without a waiter guard, so nothing removes an entry inside a
/// run, and the property below registers one per iteration under a count the
/// environment raises. At the ceiling no run over a real socket can exhaust it,
/// so the property fails on its subject rather than on admission.
pub(super) fn registry() -> Result<Arc<PendingRegistry>> {
    Ok(PendingRegistry::new(&RequesterConfiguration {
        max_in_flight: MAX_IN_FLIGHT,
        max_response_bytes: MAX_RESPONSE_BYTES,
        ..RequesterConfiguration::default()
    })?)
}

/// Registers one request that awaits `subsystems` and reads `expects`.
pub(super) fn register(
    registry: &Arc<PendingRegistry>,
    subsystems: &[&str],
    expects: &'static str,
) -> Result<RequestId> {
    let mut awaited = Vec::with_capacity(subsystems.len());
    for name in subsystems {
        awaited.push(SubsystemName::try_new(name)?);
    }
    Ok(registry.register_unguarded(&awaited, expects, TIMEOUT)?)
}

/// A header for one response to `request`, addressed to `target`.
pub(super) fn header(target: NodeId, request: RequestId, subsystem: &str) -> Result<FrameHeader> {
    Ok(FrameHeader {
        target,
        request,
        subsystem: SubsystemName::try_new(subsystem)?,
        status: ResponseStatus::Success,
        relay: None,
    })
}

/// The frame the registry sees when a delivery is driven in process rather than
/// over the wire.
pub(super) fn frame(header: FrameHeader, payload: &[u8]) -> ResponseFrame {
    ResponseFrame {
        header,
        format: FormatToken::make(CountingCodec::FORMAT_ID),
        payload: BytesMut::from(payload),
    }
}

/// A payload of `bytes` deterministic bytes.
pub(super) fn payload(bytes: usize) -> Vec<u8> {
    vec![b'p'; bytes]
}

/// The status one delivery attempt came to.
fn status(outcome: Result<(), SendFailure>) -> Result<Code> {
    match outcome {
        Ok(()) => Ok(Code::Ok),
        Err(SendFailure::Status(code)) => Ok(code),
        Err(failure) => bail!("the peer listener answered nothing at all: {failure}"),
    }
}
