//! The peer transport's suites, driven against a real loopback listener.
//!
//! One listener serves the whole test process. Isolation is by request id,
//! which the registry already mints fresh per registration, so no suite needs a
//! listener of its own unless it is testing the listener's own configuration.
//!
//! The transport counters are the process's, so every assertion on them reads a
//! difference across the call under test rather than an absolute.

mod client;
mod deadline;
mod dispositions;
mod health;
mod inject;
mod listener;
mod metrics;
mod trace;
mod transport;

use super::client::GrpcSender;
use super::service::PeerService;
use super::{BoundListener, TransportConfiguration};
use crate::requester::registry::PendingRegistry;
use crate::requester::registry::tests::TestRegistration;
use crate::response::frame::encode::FrameEncoder;
use crate::response::frame::tests::CountingCodec;
use crate::response::frame::{FrameCap, FrameHeader};
use crate::response::{RequestId, ResponseStatus};
use crate::router::directory::Endpoint;
use crate::router::fleet::DestinationFleet;
use crate::router::loopback::listener::{FixedRouter, Served, endpoint, transport};
use crate::router::loopback::{TestRouter, config as fleet_config, registration};
use crate::router::relay::Relay;
use crate::router::{Framed, LocalTarget, NodeId, ResponseSender, SendFailure};
use crate::subsystem::SubsystemName;
use bytes::{BufMut, BytesMut};
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::time::Instant;
use tonic::Code;

/// The listener's frame ceiling. Small enough that an over-cap frame costs one
/// short allocation to build.
const FRAME_CAP: usize = 8 * 1024;

/// The ceiling a sender that must out-reach the listener encodes under.
const WIDE_FRAME_CAP: usize = 2 * FRAME_CAP;

/// A frame the listener refuses and a wide sender encodes.
const OVER_FRAME_BYTES: usize = FRAME_CAP + 1024;

/// The subsystem a request awaits.
const ALPHA: &str = "alpha";

/// How long a request in these suites stays open.
const TIMEOUT: Duration = Duration::from_secs(30);

/// The budget a suite gives one delivery and one forward.
const BUDGET: Duration = Duration::from_secs(30);

/// The one listener every suite that needs a wire shares.
static SHARED: OnceCell<Harness> = OnceCell::const_new();

/// A live listener, the registry it serves, and the senders that reach it.
///
/// The listener sends a frame for another node on, exactly as a live process
/// does. Its relay resolves nothing for any id these suites use, because every
/// "another node" here is a freshly minted id and the test router publishes a
/// fixed set. So such a frame reaches no target and answers `UNAVAILABLE`.
pub(super) struct Harness {
    /// The node the listener answers for.
    pub(super) node: NodeId,
    /// The registry the listener hands frames to.
    pub(super) registry: Arc<PendingRegistry>,
    /// A sender whose ceiling matches the listener's.
    pub(super) sender: GrpcSender,
    /// A sender whose ceiling is above the listener's, so a frame it refuses
    /// can only have been refused by the listener.
    pub(super) wide: GrpcSender,
    /// Where the listener is.
    pub(super) address: Endpoint,
    /// The ceiling the listener and its matching sender share.
    pub(super) cap: FrameCap,
    served: Served,
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
        let served_registry = registry();
        let node = NodeId::new();
        let bound = BoundListener::bind(&config).await?;
        let address = endpoint(&bound);
        let (relay_router, _relay_deliveries) = TestRouter::new(fleet_config())?;
        let served = Served::start(
            bound,
            PeerService::new(
                LocalTarget::new(node, Arc::clone(&served_registry)),
                Relay::new(relay_router),
                config.frame_cap,
                BUDGET,
            ),
        )?;
        Ok(Self {
            node,
            registry: served_registry,
            sender: GrpcSender::new(config.frame_cap, &fleet()?),
            wide: GrpcSender::new(FrameCap::new(WIDE_FRAME_CAP)?, &fleet()?),
            address,
            cap: config.frame_cap,
            served,
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
        status(
            sender
                .deliver(&self.address, &staged, Instant::now() + BUDGET)
                .await,
        )
    }

    /// Delivers bytes exactly as given.
    pub(super) async fn deliver_raw(&self, sender: &GrpcSender, bytes: BytesMut) -> Result<Code> {
        status(
            sender
                .deliver(&self.address, &RawFramed(bytes), Instant::now() + BUDGET)
                .await,
        )
    }

    /// Stops the listener and waits for it to finish.
    pub(super) async fn stop(self) -> Result<()> {
        Ok(self.served.stop().await?)
    }
}

/// A router that reaches `address` and nothing else.
pub(super) fn reaching(cap: FrameCap, address: &Endpoint) -> Result<FixedRouter> {
    FixedRouter::new(
        cap,
        fleet_config(),
        Some(registration(address.clone())),
        None,
    )
}

impl Framed for RawFramed {
    fn bytes(&self) -> usize {
        self.0.len()
    }

    fn write<B: BufMut>(&self, dst: &mut B) {
        dst.put_slice(&self.0);
    }
}

/// Builds a destination fleet.
pub(super) fn fleet() -> Result<DestinationFleet> {
    Ok(DestinationFleet::new(fleet_config())?)
}

pub(super) fn registry() -> Arc<PendingRegistry> {
    PendingRegistry::new()
}

/// Registers one request that awaits `subsystems`.
pub(super) fn register(
    registry: &Arc<PendingRegistry>,
    subsystems: &[&str],
) -> Result<TestRegistration> {
    let mut awaited = Vec::with_capacity(subsystems.len());
    for name in subsystems {
        awaited.push(SubsystemName::try_new(name)?);
    }
    TestRegistration::new(registry, &awaited, TIMEOUT)
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
