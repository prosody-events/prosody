//! What the typed sender's own suites share: a router over an in-process
//! transport, and a harness that records every attempt it makes.

use super::TypedSender;
use crate::error::ErrorCategory;
use crate::response::frame::tests::CountingCodec;
use crate::response::frame::{FrameCap, FrameHeader};
use crate::response::{RequestId, ResponseStatus};
use crate::router::RelayHop;
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::{
    Delivery, HANG_GUARD, PUBLISHED_NODES, Script, TestRouter, UNPUBLISHED_NODE, config, node,
    paused, port,
};
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::cell::Cell;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::timeout;

mod bounds;
mod delivery;
mod fallback;
mod isolation;

/// The frame ceiling these suites encode against.
pub(super) const CAP_BYTES: usize = 4096;

/// The response body every queued result in these suites carries.
pub(super) const PAYLOAD: &[u8] = b"response";

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

impl Harness {
    /// A harness over a fleet built from `config`, where every node publishes a
    /// direct endpoint alone.
    pub(super) fn new(config: FleetConfiguration) -> Result<Self> {
        let (router, deliveries) = TestRouter::new(config)?;
        Self::over(router, deliveries)
    }

    /// A harness whose nodes publish both endpoints under a label the dialer
    /// shares, so every route offers a fallback.
    pub(super) fn dual_homed(config: FleetConfiguration) -> Result<Self> {
        let (router, deliveries) = TestRouter::dual_homed(config)?;
        Self::over(router, deliveries)
    }

    fn over(router: TestRouter, deliveries: UnboundedReceiver<Delivery>) -> Result<Self> {
        Ok(Self {
            sender: TypedSender::new(&router, FrameCap::new(CAP_BYTES)?)?,
            router,
            deliveries,
            header: header()?,
            queued: Cell::new(0),
        })
    }

    /// Sets what the destination for `index` answers on its direct endpoint.
    pub(super) fn script(&self, index: u8, script: Script) {
        self.router.script(index, script);
    }

    /// Sets what the destination for `index` answers on its advertised
    /// endpoint.
    pub(super) fn script_advertised(&self, index: u8, script: Script) {
        self.router.script_advertised(index, script);
    }

    /// The fleet, for the assertions that outlive the harness.
    pub(super) fn fleet(&self) -> Arc<DestinationFleet> {
        Arc::clone(self.router.fleet())
    }

    /// Queues one response for `index`, and counts it when it was queued.
    ///
    /// A refusal hands the payload back. These suites never need it again, so a
    /// refusal reads as an error here and the fleet's own counter names which
    /// class it was.
    pub(super) fn send(&self, index: u8) -> Result<()> {
        let queued = self.sender.send(
            FrameHeader {
                target: node(index),
                ..self.header.clone()
            },
            PAYLOAD.to_vec(),
        );
        if queued.is_err() {
            bail!("the sender refused the response");
        }
        self.queued.set(self.queued.get() + 1);
        Ok(())
    }

    /// The next attempt the transport recorded.
    pub(super) async fn next_delivery(&mut self) -> Result<Delivery> {
        next_delivery(&mut self.deliveries).await
    }

    /// Drops the sender without draining it, and keeps the record of what the
    /// transport does afterwards.
    ///
    /// Every worker is a spawned task, so what goes away here is the queue
    /// handles and nothing else. A test that wants the workers joined calls
    /// [`Harness::drain`] instead.
    pub(super) fn release(self) -> UnboundedReceiver<Delivery> {
        let Self {
            sender, deliveries, ..
        } = self;
        drop(sender);
        deliveries
    }

    /// Drains the sender, collects every attempt its workers made, and holds
    /// the counters to their conservation rule.
    ///
    /// The drain returns once every worker has exited, so the record of one run
    /// is complete without waiting on a clock.
    ///
    /// Every queued response then ends as exactly one of sent or dropped. The
    /// one drop these suites cannot produce is a queue refusal, which needs a
    /// worker to end between the reservation and the queue.
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

/// The next attempt `deliveries` records.
pub(super) async fn next_delivery(
    deliveries: &mut UnboundedReceiver<Delivery>,
) -> Result<Delivery> {
    match timeout(HANG_GUARD, deliveries.recv()).await {
        Ok(Some(delivery)) => Ok(delivery),
        Ok(None) => bail!("the transport stopped recording before a delivery arrived"),
        Err(_) => bail!("no delivery arrived"),
    }
}

/// How many of `deliveries` went to the direct endpoint of the node for
/// `index`.
pub(super) fn attempts(deliveries: &[Delivery], index: u8) -> usize {
    attempts_on(deliveries, port(index))
}

/// How many of `deliveries` reached one exact endpoint port. A node's two
/// endpoints have distinct ports, so this is what tells them apart.
pub(super) fn attempts_on(deliveries: &[Delivery], port: u16) -> usize {
    deliveries
        .iter()
        .filter(|delivery| delivery.port == port)
        .count()
}

/// The header every queued response in these suites carries, except its target.
fn header() -> Result<FrameHeader> {
    Ok(FrameHeader {
        target: node(0),
        request: RequestId::from_bytes([7; 16]),
        subsystem: SubsystemName::try_new("billing")?,
        status: ResponseStatus::Error(ErrorCategory::Permanent),
        relay: None,
    })
}
