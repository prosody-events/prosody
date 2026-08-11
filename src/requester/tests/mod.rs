//! What the requester suites share: codecs, a registry, a mock requester,
//! response frames, and produce futures.
//!
//! Every suite runs on a paused-time runtime, so a virtual second costs
//! nothing and every schedule is the same on every machine.

use super::registry::{PendingRegistry, Registration};
use crate::codec::Codec;
use crate::error::ErrorCategory;
use crate::producer::{ProducerConfiguration, ProsodyProducer};
use crate::requester::{ProsodyRequester, ResponseError};
use crate::response::frame::{FrameResult, HandlerError, ResponseFrame, ResponseSuccess};
use crate::response::headers::{RequestDeadline, RequestTag};
use crate::response::{FormatToken, RequestId};
use crate::router::PeerId;
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use crate::{EventIdentity, Topic};
use bytes::Bytes;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use quickcheck::{Arbitrary, Gen};
use std::future::{Future, poll_fn};
use std::iter::empty;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use thiserror::Error;

mod flat_map;
mod metrics;
mod request;
mod trace;

/// Bytes one successful test response occupies.
const RESPONSE_BYTES: usize = 4;

/// The peer every suite answers to.
const PEER: PeerId = PeerId::from_bytes([7_u8; 16]);

/// Longest timeout the suites may ask for.
const MAX_TIMEOUT: Duration = Duration::from_mins(1);

/// Subsystem names the property generators draw from.
const POOL: [&str; 6] = ["billing", "ledger", "audit", "search", "mailer", "a,b"];

/// The topic, key and subsystem [`unanswered_call`] names.
pub(super) const TOPIC: &str = "requests";
pub(super) const KEY: &str = "the-key";
pub(super) const SUBSYSTEM: &str = "billing";

/// The request payload every suite that reaches the real `request` body sends.
#[derive(Debug)]
pub(super) struct RequestPayload;

/// The codec that request is produced with.
#[derive(Debug, Default)]
pub(super) struct RequestCodec;

/// The response codec every requester suite speaks.
///
/// Four bytes carry the successful value.
#[derive(Debug, Default)]
pub(super) struct TestCodec;

/// Why the test codec could not read a response.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum TestCodecError {
    /// The buffer is not one tag byte and one `u32`, or names no arm.
    #[error("the test payload is malformed")]
    Malformed,
}

impl Codec for TestCodec {
    type Error = TestCodecError;
    type Payload = u32;

    const FORMAT_ID: &'static str = "requester-test";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, TestCodecError> {
        let Ok(value) = <[u8; 4]>::try_from(buf) else {
            return Err(TestCodecError::Malformed);
        };
        Ok(u32::from_le_bytes(value))
    }

    fn serialize_ref(
        &mut self,
        payload: &Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), TestCodecError> {
        buf.extend_from_slice(&payload.to_le_bytes());
        Ok(())
    }
}

impl EventIdentity for RequestPayload {
    fn event_id(&self) -> Option<&str> {
        None
    }
}

impl Codec for RequestCodec {
    type Error = TestCodecError;
    type Payload = RequestPayload;

    const FORMAT_ID: &'static str = "requester-test-request";

    fn deserialize(&mut self, _buf: &mut [u8]) -> Result<RequestPayload, TestCodecError> {
        Ok(RequestPayload)
    }

    fn serialize_ref(
        &mut self,
        _payload: &RequestPayload,
        buf: &mut Vec<u8>,
    ) -> Result<(), TestCodecError> {
        buf.push(0);
        Ok(())
    }
}

/// Builds an empty registry.
pub(super) fn registry() -> Arc<PendingRegistry> {
    PendingRegistry::new()
}

/// Registers one request through the validation a real call goes through.
///
/// Every suite speaks [`TestCodec`], so the awaited format is not a parameter.
pub(super) fn register(
    registry: &Arc<PendingRegistry>,
    awaited: &[SubsystemName],
    timeout: Duration,
) -> Result<Registration> {
    let deadline = RequestDeadline::after(timeout)
        .ok_or_else(|| eyre!("the test deadline was out of range"))?;
    Ok(registry.register::<TestCodecError>(awaited, deadline)?)
}

/// A requester over a mock cluster, so a case reaches the real `request` body.
pub(super) fn requester(
    registry: Arc<PendingRegistry>,
) -> Result<ProsodyRequester<RequestCodec, TestCodec>> {
    let config = ProducerConfiguration::builder()
        .bootstrap_servers(vec!["localhost:9094".to_owned()])
        .source_system("requester-tests")
        .mock(true)
        .build()?;
    let producer = ProsodyProducer::new(&config, Telemetry::new().sender())?;
    Ok(ProsodyRequester::new(producer, PEER, registry))
}

/// Drives one real call that nothing answers, to its deadline.
///
/// Run it on a paused clock: the clock then walks past the deadline, so the one
/// outcome is a timeout the body computed rather than a constant. What the call
/// leaves behind — its span, and the latency it recorded — is each suite's own
/// assertion.
///
/// # Errors
///
/// Returns an error when the call fails or answers anything but a timeout.
pub(super) async fn unanswered_call() -> Result<()> {
    let registry = registry();
    let requester = requester(registry)?;
    let awaited = names(&[SUBSYSTEM])?;
    let results = requester
        .request::<_, u32>(
            empty(),
            Topic::from(TOPIC),
            KEY,
            RequestPayload,
            &awaited,
            MAX_TIMEOUT,
        )
        .await?;
    ensure!(
        results == vec![Err(ResponseError::Timeout)],
        "nothing answered this call, so its one result must be a timeout"
    );
    Ok(())
}

/// Builds subsystem names, refusing any the crate would refuse.
pub(super) fn names(names: &[&str]) -> Result<Vec<SubsystemName>> {
    names
        .iter()
        .map(|name| Ok(SubsystemName::try_new(name)?))
        .collect()
}

/// Chooses `count` distinct indices from `0..length` in random order.
pub(super) fn distinct_indices(g: &mut Gen, length: usize, count: usize) -> Vec<usize> {
    let mut pool: Vec<usize> = (0..length).collect();
    let mut chosen = Vec::with_capacity(count);
    for _ in 0..count {
        chosen.push(pool.swap_remove(usize::arbitrary(g) % pool.len()));
    }
    chosen
}

/// Encodes one response body through the codec that reads it back.
pub(super) fn body(payload: u32) -> Result<Bytes> {
    let mut buf = Vec::with_capacity(RESPONSE_BYTES);
    TestCodec::with_cached_local(|codec| codec.serialize(payload, &mut buf))?;
    Ok(Bytes::from(buf))
}

/// Builds one response frame for `subsystem`, in the format the waiter reads.
pub(super) fn frame(id: RequestId, subsystem: &SubsystemName, payload: Bytes) -> ResponseFrame {
    ResponseFrame {
        header: RequestTag::new(
            id,
            PEER,
            RequestDeadline::from_unix_micros(1_700_000_000_000_000),
        )
        .header(subsystem.clone()),
        result: FrameResult::Success(ResponseSuccess {
            format: FormatToken::make(TestCodec::FORMAT_ID),
            payload,
        }),
    }
}

/// Builds one successful response frame carrying `value`.
pub(super) fn success(
    id: RequestId,
    subsystem: &SubsystemName,
    value: u32,
) -> Result<ResponseFrame> {
    Ok(frame(id, subsystem, body(value)?))
}

/// Builds one handler failure frame.
pub(super) fn failure(
    id: RequestId,
    subsystem: &SubsystemName,
    category: ErrorCategory,
    message: &'static str,
) -> ResponseFrame {
    ResponseFrame {
        header: RequestTag::new(
            id,
            PEER,
            RequestDeadline::from_unix_micros(1_700_000_000_000_000),
        )
        .header(subsystem.clone()),
        result: FrameResult::HandlerError(HandlerError {
            category,
            message: Bytes::from_static(message.as_bytes()),
        }),
    }
}

/// Polls `future` exactly once and reports what that one poll returned.
///
/// A zero timeout would not do: it does not promise exactly one inner poll.
pub(super) async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}
