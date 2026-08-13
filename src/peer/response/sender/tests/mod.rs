//! What response delivery suites share: an in-process route and a harness that
//! records every attempt.

use super::{ResponseRoute, RouteOutcome, deliver_response, stage};
use crate::peer::response::RequestId;
use crate::peer::response::frame::FrameHeader;
use crate::peer::response::frame::encode::Staged;
use crate::peer::response::frame::tests::CountingCodec;
use crate::peer::response::headers::RequestDeadline;
use crate::peer::response::sender::DropReason;
use crate::peer::router::loopback::{
    Delivery, Drained, Script, TestRouter, collect_deliveries, direct_uri, paused, peer,
};
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use opentelemetry::Context;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;
use tonic::codegen::http::Uri;

mod delivery;
mod isolation;
mod metrics;

/// The response body every result in these suites carries.
pub(super) const PAYLOAD: &[u8] = b"response";

/// A future Kafka deadline for transport tests.
fn deadline() -> RequestDeadline {
    RequestDeadline::from_unix_micros(4_102_444_800_000_000)
}

/// One fleet and one observed route over an in-process transport.
pub(super) struct Harness {
    router: TestRouter,
    route: Arc<ObservedRoute>,
    deliveries: UnboundedReceiver<Delivery>,
    header: FrameHeader,
    outcomes: Arc<Outcomes>,
}

#[derive(Default)]
struct Outcomes {
    sent: AtomicU64,
    dropped: AtomicU64,
}

#[derive(Clone)]
struct ObservedRoute {
    route: TestRouter,
    outcomes: Arc<Outcomes>,
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

impl ResponseRoute for ObservedRoute {
    fn deliver(
        &self,
        frame: Staged,
        deadline: RequestDeadline,
        context: &Context,
    ) -> impl Future<Output = Result<RouteOutcome, DropReason>> + Send {
        let route = self.route.clone();
        let outcomes = Arc::clone(&self.outcomes);
        async move {
            let outcome = route.deliver(frame, deadline, context).await;
            outcomes.record(matches!(&outcome, Ok(RouteOutcome::Delivered(_))));
            outcome
        }
    }
}

impl Harness {
    /// A harness where every peer publishes a direct endpoint.
    pub(super) fn new() -> Result<Self> {
        let (router, deliveries) = TestRouter::new()?;
        Self::over(router, deliveries)
    }

    fn over(router: TestRouter, deliveries: UnboundedReceiver<Delivery>) -> Result<Self> {
        let outcomes = Arc::new(Outcomes::default());
        let route = ObservedRoute {
            route: router.clone(),
            outcomes: Arc::clone(&outcomes),
        };
        Ok(Self {
            route: Arc::new(route),
            router,
            deliveries,
            header: header()?,
            outcomes,
        })
    }

    /// Sets what the destination for `index` answers on its direct endpoint.
    pub(super) fn script(&self, index: u8, script: Script) -> Result<()> {
        Ok(self.router.script(index, script)?)
    }

    /// Sends one response for `index`.
    pub(super) async fn send(&self, index: u8) -> Result<()> {
        self.send_payload(index, PAYLOAD.to_vec()).await
    }

    /// Sends `payload` for `index`.
    pub(super) async fn send_payload(&self, index: u8, payload: Vec<u8>) -> Result<()> {
        let prepared = stage::<CountingCodec, Infallible>(
            FrameHeader {
                target: peer(index),
                ..self.header.clone()
            },
            Ok(&payload),
        );
        deliver_response(&*self.route, prepared, Context::current(), deadline()).await;
        Ok(())
    }

    /// Starts one send that a test synchronizes through transport events.
    pub(super) fn start_send(&self, index: u8) -> JoinHandle<Result<()>> {
        let route = Arc::clone(&self.route);
        let header = FrameHeader {
            target: peer(index),
            ..self.header.clone()
        };
        let payload = PAYLOAD.to_vec();
        let prepared = stage::<CountingCodec, Infallible>(header, Ok(&payload));
        tokio::spawn(async move {
            deliver_response(&*route, prepared, Context::current(), deadline()).await;
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
            route,
            mut deliveries,
            outcomes,
            ..
        } = self;
        drop(route);
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

/// How many of `deliveries` went to the direct endpoint of the peer for
/// `index`.
pub(super) fn attempts(deliveries: &[Delivery], index: u8) -> Result<usize> {
    Ok(attempts_on(deliveries, &direct_uri(index)?))
}

/// How many of `deliveries` reached one exact endpoint.
pub(super) fn attempts_on(deliveries: &[Delivery], uri: &Uri) -> usize {
    deliveries
        .iter()
        .filter(|delivery| delivery.uri == *uri)
        .count()
}

/// The header every response in these suites carries, except its target.
fn header() -> Result<FrameHeader> {
    Ok(FrameHeader {
        target: peer(0),
        request: RequestId::from_bytes([7; 16]),
        subsystem: SubsystemName::try_new("billing")?,
        relay: None,
    })
}
