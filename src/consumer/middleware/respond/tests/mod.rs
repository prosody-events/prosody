//! What the respond layer's suites share: a codec over a handler result, a
//! fixture that owns the fleet and the loopback transport, and the composed
//! stack every cascade test drives.
//!
//! The transport records the whole frame of every attempt, so an assertion
//! reads a decoded frame rather than bytes. Every transport assertion runs
//! after an explicit drain, which joins the delivery workers: nothing can still
//! be in flight when a count is read.

use super::{Responder, responding_provider};
use crate::codec::Codec;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::consumer::middleware::log::LogMiddleware;
use crate::consumer::middleware::retry::{RetryConfiguration, RetryMiddleware};
use crate::consumer::middleware::tests::test_support::TestError;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::{EventHandler, Partition, Topic};
use crate::error::{ErrorCategory, UnknownErrorCategory};
use crate::response::RequestId;
use crate::response::frame::FrameCap;
use crate::response::headers::RequestTag;
use crate::router::RelayHop;
use crate::router::loopback::{Delivery, Drained, TestRouter, collect_deliveries, config, node};
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::bail;
use crossbeam_utils::CachePadded;
use serde_json::Value;
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::Span;

mod carriage;
mod cascade;
mod defer;
mod delivery;

/// The subsystem every fixture answers for.
const SUBSYSTEM: &str = "billing";

/// The frame ceiling the ordinary fixtures encode against.
const CAP_BYTES: usize = 4096;

/// The topic and partition every dispatch in these suites carries.
const TOPIC: &str = "test-topic";
const PARTITION: Partition = 0;

/// The shortest retry backoff the configuration accepts without truncating its
/// jitter to an empty range.
const RETRY_BASE: Duration = Duration::from_millis(1);

thread_local! {
    /// Payloads serialized on this thread, by every probe codec on it.
    ///
    /// A delivery worker builds its own codec through `Default`, so a suite
    /// holds no handle on the instance that encodes. These suites run one
    /// current-thread runtime, so the worker encodes on the thread that drives
    /// it and this total includes what the worker serialized. Read it as a
    /// difference around one dispatch, never as an absolute.
    static SERIALIZES: Cell<usize> = const { Cell::new(0) };
}

/// A codec over one handler result, encoded as a single byte.
///
/// It cannot reuse the response layer's `CountingCodec`: that codec's payload
/// is `Vec<u8>` and its visibility stops inside the response module.
#[derive(Default)]
struct ResultProbeCodec;

/// The same codec, except that its error arm cannot fit the smallest frame cap.
///
/// The success arm stays one byte, so a suite can hold a control response and
/// an over-cap response to the same fixture and differ in one dimension.
#[derive(Default)]
struct OversizedProbeCodec;

/// One fleet, one loopback transport, and one responder over them.
struct Fixture<C: Codec<Payload = Result<(), TestError>>> {
    router: TestRouter,
    responder: Arc<Responder<C>>,
    deliveries: UnboundedReceiver<Delivery>,
}

impl Codec for ResultProbeCodec {
    type Error = ProbeCodecError;
    type Payload = Result<(), TestError>;

    const FORMAT_ID: &'static str = "test-result";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, ProbeCodecError> {
        decode_result(buf)
    }

    fn serialize(
        &mut self,
        payload: Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), ProbeCodecError> {
        SERIALIZES.set(SERIALIZES.get() + 1);
        buf.push(discriminant(&payload));
        Ok(())
    }
}

impl Codec for OversizedProbeCodec {
    type Error = ProbeCodecError;
    type Payload = Result<(), TestError>;

    const FORMAT_ID: &'static str = "test-oversized";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, ProbeCodecError> {
        decode_result(buf)
    }

    fn serialize(
        &mut self,
        payload: Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), ProbeCodecError> {
        SERIALIZES.set(SERIALIZES.get() + 1);
        let byte = discriminant(&payload);
        if payload.is_ok() {
            buf.push(byte);
        } else {
            buf.resize(FrameCap::MIN_BYTES, byte);
        }
        Ok(())
    }
}

impl<C: Codec<Payload = Result<(), TestError>>> Fixture<C> {
    /// A fixture whose fleet holds `max_destinations` cells of `slots_each`
    /// slots, encoding against `CAP_BYTES`.
    fn new(max_destinations: usize, slots_each: usize) -> Result<Self> {
        Self::with_cap(max_destinations, slots_each, FrameCap::new(CAP_BYTES)?)
    }

    fn with_cap(max_destinations: usize, slots_each: usize, cap: FrameCap) -> Result<Self> {
        let (router, deliveries) = TestRouter::new(config(max_destinations, slots_each))?;
        let responder = Arc::new(Responder::new(
            &router,
            cap,
            SubsystemName::try_new(SUBSYSTEM)?,
        )?);
        Ok(Self {
            router,
            responder,
            deliveries,
        })
    }

    /// The composed stack a cascade suite drives: log outside retry, retry
    /// outside respond, respond directly around `leaf`.
    ///
    /// Log sits outermost so retry runs mid-stack with a retry ceiling. That is
    /// the only composition in which a transient error can exhaust its retries,
    /// which is the row these suites exist to pin.
    fn stack<H>(&self, leaf: H, max_retries: u32) -> Result<impl EventHandler<Payload = Value>>
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
        let provider = responding_provider(
            &retry.layer(LogMiddleware::new()),
            leaf,
            Arc::clone(&self.responder),
        );
        Ok(provider.handler_for_partition(TOPIC.into(), PARTITION))
    }

    /// How many reservations the fleet has refused for want of capacity.
    fn refused(&self) -> u64 {
        self.router.fleet().refused()
    }

    /// How many destinations the fleet has admitted. A dispatch that reserved
    /// no slot leaves this at zero, which is observable before any drain.
    fn admitted(&self) -> u64 {
        self.router.fleet().admitted()
    }

    /// Joins every delivery worker and collects what the transport recorded.
    ///
    /// Every handler built from this fixture must be dropped first: the drain
    /// consumes the responder, so a surviving handle fails the test rather than
    /// skipping the wait.
    async fn drain(self) -> Result<Drained> {
        let Self {
            router,
            responder,
            mut deliveries,
            ..
        } = self;
        let counters = responder.counters();
        let Some(responder) = Arc::into_inner(responder) else {
            bail!("a responder handle outlived the stack");
        };
        responder.drain().await;
        drop(router);

        Ok(Drained {
            deliveries: collect_deliveries(&mut deliveries).await,
            sent: counters.sent(),
            dropped: counters.dropped(),
        })
    }
}

/// A message asking node `index` for a response to request `request_byte`.
fn tagged(index: u8, request_byte: u8, key: &str) -> Result<ConsumerMessage<Value>> {
    tagged_under(index, request_byte, key, Span::current())
}

/// [`tagged`] with the record's own span named, for a suite whose claim is
/// which trace the answer lands in.
fn tagged_under(
    index: u8,
    request_byte: u8,
    key: &str,
    span: Span,
) -> Result<ConsumerMessage<Value>> {
    create_message(
        key,
        Some(RequestTag::new(
            RequestId::from_bytes([request_byte; 16]),
            node(index),
        )),
        span,
    )
}

/// A message that asks for nothing — ordinary traffic.
fn untagged(key: &str) -> Result<ConsumerMessage<Value>> {
    create_message(key, None, Span::current())
}

fn create_message(
    key: &str,
    request: Option<RequestTag>,
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

/// The frame ceiling the ordinary fixtures encode against, so a decode in a
/// suite cannot disagree with the encode in a worker.
fn cap() -> Result<FrameCap> {
    Ok(FrameCap::new(CAP_BYTES)?)
}

/// The byte one result encodes to: zero for a success, else the category's own
/// wire discriminant.
fn discriminant(payload: &Result<(), TestError>) -> u8 {
    match payload {
        Ok(()) => 0,
        Err(TestError(category)) => i32::from(*category) as u8,
    }
}

fn decode_result(buf: &[u8]) -> Result<Result<(), TestError>, ProbeCodecError> {
    let Some(byte) = buf.first().copied() else {
        return Err(ProbeCodecError::Empty);
    };
    if byte == 0 {
        return Ok(Ok(()));
    }
    Ok(Err(TestError(ErrorCategory::try_from(i32::from(byte))?)))
}

/// Why a probe codec could not read a payload.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
enum ProbeCodecError {
    /// The payload carries no result byte.
    #[error("the result payload is empty")]
    Empty,

    /// The result byte names no error category.
    #[error(transparent)]
    Category(#[from] UnknownErrorCategory),
}
