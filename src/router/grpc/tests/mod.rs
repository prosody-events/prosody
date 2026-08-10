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

use super::BoundListener;
use super::client::GrpcSender;
use super::service::PeerService;
use crate::requester::registry::PendingRegistry;
use crate::requester::registry::tests::TestRegistration;
use crate::response::frame::FrameHeader;
use crate::response::frame::encode::stage;
use crate::response::frame::tests::CountingCodec;
use crate::response::{RequestId, ResponseStatus};
use crate::router::directory::Endpoint;
use crate::router::fleet::DestinationFleet;
use crate::router::loopback::listener::{FixedRouter, Served, bind_address, endpoint};
use crate::router::loopback::{TestRouter, config as fleet_config, registration};
use crate::router::relay::Relay;
use crate::router::{Framed, LocalTarget, NodeId, ResponseSender, SendFailure};
use crate::subsystem::SubsystemName;
use bytes::{BufMut, BytesMut};
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::time::Instant;
use tonic::Code;

/// The subsystem a request awaits.
const ALPHA: &str = "alpha";

/// How long a request in these suites stays open.
const TIMEOUT: Duration = Duration::from_secs(30);

/// The deadline one transport call receives.
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
    pub(super) sender: GrpcSender,
    /// Where the listener is.
    pub(super) address: Endpoint,
    served: Served,
}

/// Bytes already framed, so a suite can put a frame on the wire that no encoder
/// would produce.
#[derive(Clone)]
struct RawFramed(BytesMut);

impl Harness {
    /// The listener every suite shares.
    pub(super) async fn shared() -> Result<&'static Self> {
        SHARED.get_or_try_init(|| Self::with(bind_address())).await
    }

    /// A listener of this suite's own, for the cases that vary its
    /// configuration. Call [`stop`](Self::stop) before the test returns.
    pub(super) async fn with(address: SocketAddr) -> Result<Self> {
        let served_registry = registry();
        let node = NodeId::new();
        let bound = BoundListener::bind(address).await?;
        let address = endpoint(&bound);
        let (relay_router, _relay_deliveries) = TestRouter::new(fleet_config())?;
        let served = Served::start(
            bound,
            PeerService::new(
                LocalTarget::new(node, Arc::clone(&served_registry)),
                Relay::new(relay_router),
            ),
        )?;
        Ok(Self {
            node,
            registry: served_registry,
            sender: GrpcSender::new(&fleet()?),
            address,
            served,
        })
    }

    /// Frames one response and delivers it, reporting the status the listener
    /// answered.
    pub(super) async fn deliver(&self, header: &FrameHeader, payload: Vec<u8>) -> Result<Code> {
        let staged = stage::<CountingCodec>(header, &payload)?;
        status(
            self.sender
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
pub(super) fn reaching(address: &Endpoint) -> Result<FixedRouter> {
    FixedRouter::new(fleet_config(), Some(registration(address.clone())), None)
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
