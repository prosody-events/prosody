//! What the respond layer's suites share: a codec over handler output, a
//! fixture that owns the fleet and the loopback transport, and the composed
//! stack every cascade test drives.
//!
//! The transport records the whole frame of every attempt, so an assertion
//! reads a decoded frame rather than bytes. Every transport assertion runs
//! after the direct router call returns.

use super::{RespondHandler, Responder};
use crate::codec::Codec;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::consumer::middleware::log::LogMiddleware;
use crate::consumer::middleware::providers::{FallibleCloneProvider, LeafHandler};
use crate::consumer::middleware::retry::{RetryConfiguration, RetryMiddleware};
use crate::consumer::middleware::tests::test_support::TestError;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::{EventHandler, Partition, Topic};
use crate::peer::response::RequestId;
use crate::peer::response::headers::{RequestDeadline, ResultRequest};
use crate::peer::router::loopback::{Delivery, TestRouter, collect_deliveries, peer};
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::bail;
use crossbeam_utils::CachePadded;
use serde_json::Value;
use std::cell::Cell;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::Span;

mod carriage;
mod cascade;
mod defer;
mod delivery;
mod ordering;

/// The subsystem every fixture answers for.
const SUBSYSTEM: &str = "billing";

/// The topic and partition every dispatch in these suites carries.
const TOPIC: &str = "test-topic";
const PARTITION: Partition = 0;

/// The shortest retry backoff the configuration accepts without truncating its
/// jitter to an empty range.
const RETRY_BASE: Duration = Duration::from_millis(1);

thread_local! {
    /// Payloads serialized on this thread, by every probe codec on it.
    ///
    /// A response send builds its own codec through `Default`, so a suite holds
    /// no handle on the instance that encodes. These suites use one
    /// current-thread runtime. Thus, this total includes that serialization. Read it as a
    /// difference around one dispatch, never as an absolute.
    static SERIALIZES: Cell<usize> = const { Cell::new(0) };
}

/// A codec over one successful handler result.
///
/// It cannot reuse the response layer's `CountingCodec`: that codec's payload
/// is `Vec<u8>` and its visibility stops inside the response module.
#[derive(Default)]
struct ResultProbeCodec;

/// One fleet, one loopback transport, and one responder over them.
struct Fixture<C: Codec<Payload = ()>> {
    router: TestRouter,
    responder: Arc<Responder<C, TestRouter>>,
    deliveries: UnboundedReceiver<Delivery>,
}

impl Codec for ResultProbeCodec {
    type Error = Infallible;
    type Payload = ();

    const FORMAT_ID: &'static str = "test-result";

    fn deserialize(&mut self, _buf: &mut [u8]) -> Result<Self::Payload, Infallible> {
        Ok(())
    }

    fn serialize_ref(
        &mut self,
        _payload: &Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), Infallible> {
        SERIALIZES.set(SERIALIZES.get() + 1);
        buf.push(0);
        Ok(())
    }
}

impl<C: Codec<Payload = ()>> Fixture<C> {
    fn new() -> Result<Self> {
        let (router, deliveries) = TestRouter::new()?;
        let responder = Responder::new(router.clone(), SubsystemName::try_new(SUBSYSTEM)?);
        Ok(Self {
            router,
            responder: Arc::new(responder),
            deliveries,
        })
    }

    /// The composed stack a cascade suite drives: log outside retry, retry
    /// outside respond, respond directly around `leaf`.
    ///
    /// Log sits outermost so retry runs mid-stack with a retry ceiling. That is
    /// the only composition in which a transient error can exhaust its retries,
    /// which is the row these suites exist to pin.
    fn stack<H>(
        &self,
        leaf: H,
        max_retries: u32,
    ) -> Result<impl EventHandler<Payload = Value> + use<H, C>>
    where
        H: FallibleHandler<Payload = Value, Output = (), Error = TestError>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        let retry = RetryMiddleware::new(
            RetryConfiguration::builder()
                .base(RETRY_BASE)
                .max_retries(max_retries)
                .build()?,
        )?;
        let provider = retry
            .layer(LogMiddleware::new())
            .with_provider(FallibleCloneProvider::new(RespondHandler::new(
                LeafHandler::new(leaf),
                Arc::clone(&self.responder),
            )));
        Ok(provider.handler_for_partition(TOPIC.into(), PARTITION))
    }

    /// Collects what the transport recorded.
    async fn drain(self) -> Result<Vec<Delivery>> {
        let Self {
            router,
            responder,
            mut deliveries,
        } = self;
        let Some(responder) = Arc::into_inner(responder) else {
            bail!("a responder handle outlived the stack");
        };
        drop(responder);
        drop(router);

        Ok(collect_deliveries(&mut deliveries).await)
    }
}

/// A message asking peer `index` for a response to request `request_byte`.
fn requesting(index: u8, request_byte: u8, key: &str) -> Result<ConsumerMessage<Value>> {
    requesting_under(index, request_byte, key, Span::current())
}

fn requesting_at_under(
    index: u8,
    request_byte: u8,
    key: &str,
    deadline_micros: u64,
    span: Span,
) -> Result<ConsumerMessage<Value>> {
    create_message(
        key,
        Some(ResultRequest::new(
            RequestId::from_bytes([request_byte; 16]),
            peer(index),
            RequestDeadline::from_unix_micros(deadline_micros),
        )),
        span,
    )
}

/// [`requesting`] with the record's own span named, for a suite whose claim is
/// which trace the answer lands in.
fn requesting_under(
    index: u8,
    request_byte: u8,
    key: &str,
    span: Span,
) -> Result<ConsumerMessage<Value>> {
    requesting_at_under(index, request_byte, key, 4_102_444_800_000_000, span)
}

/// A message that asks for nothing — ordinary traffic.
fn plain(key: &str) -> Result<ConsumerMessage<Value>> {
    create_message(key, None, Span::current())
}

fn create_message(
    key: &str,
    request: Option<ResultRequest>,
    span: Span,
) -> Result<ConsumerMessage<Value>> {
    let semaphore = Arc::new(Semaphore::new(1));
    let permit = semaphore.try_acquire_owned()?;
    Ok(ConsumerMessage::new(
        ConsumerMessageValue {
            key: key.into(),
            topic: Topic::from(TOPIC),
            partition: PARTITION,
            request,
            ..Default::default()
        },
        span,
        permit,
    ))
}

/// An offset tracker one dispatch can take its uncommitted offset from.
fn offset_tracker() -> OffsetTracker {
    let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    OffsetTracker::new(TOPIC.into(), PARTITION, 10, Duration::from_secs(5), version)
}

/// How many payloads the probe codecs have serialized on this thread.
fn serialize_count() -> usize {
    SERIALIZES.get()
}
