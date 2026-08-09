//! What the typed sender's own suites share: a router over an in-process
//! transport, and a harness that records every attempt it makes.

use super::{TypedSender, prepare};
use crate::error::ErrorCategory;
use crate::response::frame::FrameHeader;
use crate::response::frame::tests::CountingCodec;
use crate::response::headers::RequestDeadline;
use crate::response::{RequestId, ResponseStatus};
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::{
    Delivery, Drained, Script, TestRouter, collect_deliveries, config, node, paused, port,
};
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use opentelemetry::Context;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

mod delivery;
mod fallback;
mod isolation;
mod metrics;

/// The response body every result in these suites carries.
pub(super) const PAYLOAD: &[u8] = b"response";

/// A future Kafka deadline for transport tests.
const DEADLINE: RequestDeadline = RequestDeadline::from_unix_micros(4_102_444_800_000_000);

/// One fleet, one transport, and one typed sender over them.
pub(super) struct Harness {
    router: TestRouter,
    sender: Arc<TypedSender<CountingCodec, TestRouter>>,
    deliveries: UnboundedReceiver<Delivery>,
    header: FrameHeader,
    outcomes: Arc<Outcomes>,
}

#[derive(Default)]
struct Outcomes {
    sent: AtomicU64,
    dropped: AtomicU64,
}

impl Outcomes {
    fn record(&self, delivered: bool) {
        if delivered {
            self.sent.fetch_add(1, Relaxed);
        } else {
            self.dropped.fetch_add(1, Relaxed);
        }
    }
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
        let sender = TypedSender::new_route(router.clone());
        Ok(Self {
            sender: Arc::new(sender),
            router,
            deliveries,
            header: header()?,
            outcomes: Arc::default(),
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

    /// Sends one response for `index`.
    pub(super) async fn send(&self, index: u8) -> Result<()> {
        self.send_payload(index, PAYLOAD.to_vec()).await
    }

    /// Sends `payload` for `index`.
    pub(super) async fn send_payload(&self, index: u8, payload: Vec<u8>) -> Result<()> {
        let prepared = prepare::<CountingCodec>(
            FrameHeader {
                target: node(index),
                ..self.header.clone()
            },
            &payload,
        );
        let delivered = self
            .sender
            .send(prepared, Context::current(), DEADLINE)
            .await;
        self.outcomes.record(delivered);
        Ok(())
    }

    /// Starts one send that a test synchronizes through transport events.
    pub(super) fn start_send(&self, index: u8) -> JoinHandle<Result<()>> {
        let sender = Arc::clone(&self.sender);
        let outcomes = Arc::clone(&self.outcomes);
        let header = FrameHeader {
            target: node(index),
            ..self.header.clone()
        };
        let prepared = prepare::<CountingCodec>(header, &PAYLOAD.to_vec());
        tokio::spawn(async move {
            let delivered = sender.send(prepared, Context::current(), DEADLINE).await;
            outcomes.record(delivered);
            Ok(())
        })
    }

    /// The next attempt the transport recorded.
    pub(super) async fn next_delivery(&mut self) -> Result<Delivery> {
        next_delivery(&mut self.deliveries).await
    }

    /// Collects every completed attempt and checks response conservation.
    pub(super) async fn drain(self) -> Result<Drained> {
        let Self {
            router,
            sender,
            mut deliveries,
            outcomes,
            ..
        } = self;
        drop(sender);
        drop(router);

        let drained = Drained {
            deliveries: collect_deliveries(&mut deliveries).await,
            sent: outcomes.sent.load(Relaxed),
            dropped: outcomes.dropped.load(Relaxed),
        };
        Ok(drained)
    }
}

/// The next attempt `deliveries` records.
pub(super) async fn next_delivery(
    deliveries: &mut UnboundedReceiver<Delivery>,
) -> Result<Delivery> {
    deliveries
        .recv()
        .await
        .ok_or_else(|| eyre!("the transport stopped recording before a delivery arrived"))
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

/// The header every response in these suites carries, except its target.
fn header() -> Result<FrameHeader> {
    Ok(FrameHeader {
        target: node(0),
        request: RequestId::from_bytes([7; 16]),
        subsystem: SubsystemName::try_new("billing")?,
        status: ResponseStatus::Error(ErrorCategory::Permanent),
        relay: None,
    })
}
