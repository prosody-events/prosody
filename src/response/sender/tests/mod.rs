//! What the typed sender's own suites share: a router over an in-process
//! transport, and a harness that records every attempt it makes.

use super::{Refused, TypedSender};
use crate::error::ErrorCategory;
use crate::response::RequestId;
use crate::response::frame::tests::CountingCodec;
use crate::response::frame::{FrameCap, FrameHeader};
use crate::router::directory::Endpoint;
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::{Delivery, LoopbackSender, Script};
use crate::router::{Host, NodeId, Router};
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::cell::Cell;
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::timeout;

mod bounds;
mod delivery;
mod isolation;

/// Port of the first test node. Each node binds one of its own, which is also
/// the transport's script key.
const PORT_BASE: u16 = 9000;

/// How many nodes the router publishes an address for.
pub(super) const PUBLISHED_NODES: u8 = 8;

/// A node the router publishes nothing for.
pub(super) const UNPUBLISHED_NODE: u8 = 200;

/// The frame ceiling these suites encode against.
pub(super) const CAP_BYTES: usize = 4096;

/// The response body every queued result in these suites carries.
pub(super) const PAYLOAD: &[u8] = b"response";

/// A deadline on every wait, so a hang fails the test instead of hanging the
/// binary. It is never the assertion.
const HANG_GUARD: Duration = Duration::from_secs(30);

/// A router over an in-process transport, addressing a fixed set of nodes.
#[derive(Clone)]
pub(super) struct TestRouter {
    fleet: Arc<DestinationFleet>,
    transport: Arc<LoopbackSender>,
    addresses: Arc<HashMap<NodeId, Endpoint>>,
}

/// One fleet, one transport, and one typed sender over them.
pub(super) struct Harness {
    router: TestRouter,
    sender: TypedSender<CountingCodec>,
    deliveries: UnboundedReceiver<Delivery>,
    header: FrameHeader,
    /// Responses this harness queued, counted so [`Harness::drain`] can hold
    /// the sender's counters to their conservation rule.
    queued: Cell<u64>,
}

/// What one harness came to once every worker had finished.
pub(super) struct Drained {
    pub(super) deliveries: Vec<Delivery>,
    pub(super) sent: u64,
    pub(super) dropped: u64,
}

impl Router for TestRouter {
    type Error = Infallible;
    type Sender = LoopbackSender;

    fn address(
        &self,
        node: NodeId,
    ) -> impl Future<Output = Result<Option<Endpoint>, Infallible>> + Send {
        let address = self.addresses.get(&node).cloned();
        async move { Ok(address) }
    }

    fn sender(&self) -> &LoopbackSender {
        &self.transport
    }

    fn fleet(&self) -> &Arc<DestinationFleet> {
        &self.fleet
    }
}

impl Harness {
    /// A harness over a fleet built from `config`.
    pub(super) fn new(config: FleetConfiguration) -> Result<Self> {
        let (transport, deliveries) = LoopbackSender::new();
        let addresses = (0..PUBLISHED_NODES)
            .map(|index| {
                (
                    node(index),
                    Endpoint {
                        host: Host::make("10.0.0.1"),
                        port: port(index),
                    },
                )
            })
            .collect();
        let router = TestRouter {
            fleet: Arc::new(DestinationFleet::new(config)?),
            transport: Arc::new(transport),
            addresses: Arc::new(addresses),
        };
        Ok(Self {
            sender: TypedSender::new(&router, FrameCap::new(CAP_BYTES)?)?,
            router,
            deliveries,
            header: header()?,
            queued: Cell::new(0),
        })
    }

    /// Sets what the destination for `index` answers.
    pub(super) fn script(&self, index: u8, script: Script) {
        self.router.transport.script(port(index), script);
    }

    /// The fleet, for the assertions that outlive the harness.
    pub(super) fn fleet(&self) -> Arc<DestinationFleet> {
        Arc::clone(&self.router.fleet)
    }

    /// Queues one response for `index`, and counts it when it was queued.
    pub(super) fn send(&self, index: u8) -> Result<(), Refused> {
        let queued = self.sender.send(
            FrameHeader {
                target: node(index),
                ..self.header.clone()
            },
            PAYLOAD.to_vec(),
        );
        if queued.is_ok() {
            self.queued.set(self.queued.get() + 1);
        }
        queued
    }

    /// The next attempt the transport recorded.
    pub(super) async fn next_delivery(&mut self) -> Result<Delivery> {
        match timeout(HANG_GUARD, self.deliveries.recv()).await {
            Ok(Some(delivery)) => Ok(delivery),
            Ok(None) => bail!("the transport stopped recording before a delivery arrived"),
            Err(_) => bail!("no delivery arrived"),
        }
    }

    /// Drains the sender, collects every attempt its workers made, and holds
    /// the counters to their conservation rule.
    ///
    /// The drain returns once every worker has exited, so the record of one run
    /// is complete without waiting on a clock.
    ///
    /// Every queued response then ends as exactly one of sent or dropped. The
    /// one drop these suites cannot produce is [`Refused::Queue`], which needs
    /// a worker to end between the reservation and the queue.
    pub(super) async fn drain(self) -> Result<Drained> {
        let Self {
            router,
            sender,
            mut deliveries,
            queued,
            ..
        } = self;
        let counters = sender.counters();
        if timeout(HANG_GUARD, sender.drain()).await.is_err() {
            bail!("the destination workers did not finish");
        }
        drop(router);

        // Closing first, so the collection ends at what the workers recorded
        // rather than waiting for a stream that nothing else will write to.
        deliveries.close();
        let mut recorded = Vec::new();
        while let Some(delivery) = deliveries.recv().await {
            recorded.push(delivery);
        }
        let drained = Drained {
            deliveries: recorded,
            sent: counters.sent(),
            dropped: counters.dropped(),
        };
        let queued = queued.get();
        if drained.sent + drained.dropped != queued {
            bail!(
                "{queued} queued responses came to {} sent and {} dropped",
                drained.sent,
                drained.dropped
            );
        }
        Ok(drained)
    }
}

/// A node id from one repeated byte, so a test index reads directly.
pub(super) fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index; 16])
}

/// The port the node for `index` binds.
pub(super) fn port(index: u8) -> u16 {
    PORT_BASE + u16::from(index)
}

/// A current-thread runtime with paused time, so pacing and deadlines are
/// observed exactly and no test waits on a real clock.
pub(super) fn paused() -> Result<Runtime> {
    Ok(Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()?)
}

/// A fleet configuration with `max_destinations` cells and `slots_each` slots.
pub(super) fn config(max_destinations: usize, slots_each: usize) -> FleetConfiguration {
    FleetConfiguration {
        max_destinations,
        slots_each,
        ..FleetConfiguration::default()
    }
}

/// How many of `deliveries` went to the node for `index`.
pub(super) fn attempts(deliveries: &[Delivery], index: u8) -> usize {
    deliveries
        .iter()
        .filter(|delivery| delivery.port == port(index))
        .count()
}

/// The header every queued response in these suites carries, except its target.
fn header() -> Result<FrameHeader> {
    Ok(FrameHeader {
        target: node(0),
        request: RequestId::from_bytes([7; 16]),
        subsystem: SubsystemName::try_new("billing")?,
        category: ErrorCategory::Permanent,
        relay: None,
    })
}
