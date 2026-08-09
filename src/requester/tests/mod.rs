//! What the requester suites share: a request codec and its payload, one
//! response codec, a registry factory, a requester over a mock cluster, and the
//! frames and produce futures the delivery race is driven with.
//!
//! Every suite runs on a paused-time runtime, so a virtual second costs
//! nothing and every schedule is the same on every machine.

use super::registry::{PendingRegistry, Registration};
use crate::codec::Codec;
use crate::error::{ClassifyError, ErrorCategory};
use crate::producer::{ProducerConfiguration, ProsodyProducer};
use crate::requester::{Outcome, ProsodyRequester, ResponseFailure};
use crate::response::frame::ResponseFrame;
use crate::response::headers::{RequestDeadline, RequestTag};
use crate::response::{FormatToken, RequestId, ResponseStatus};
use crate::router::NodeId;
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use crate::{EventIdentity, Topic};
use bytes::BytesMut;
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

/// Bytes one test response occupies: one arm tag and one little-endian `u32`.
const RESPONSE_BYTES: usize = 5;

/// Arm tag of a successful response.
const OK_TAG: u8 = 0;

/// Arm tag of a transient handler failure.
const TRANSIENT_TAG: u8 = 1;

/// Arm tag of a permanent handler failure.
const PERMANENT_TAG: u8 = 2;

/// Arm tag of a terminal handler failure.
const TERMINAL_TAG: u8 = 3;

/// The node every suite answers to.
const NODE: NodeId = NodeId::from_bytes([7_u8; 16]);

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
/// One tag byte names the arm and the four bytes after it carry the value. The
/// tag also names the category a failed arm classifies as, so a suite can build
/// a frame whose wire status agrees with its payload and one whose status does
/// not.
#[derive(Debug, Default)]
pub(super) struct TestCodec;

/// The handler error a test response carries.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("test handler error {value}")]
pub(super) struct TestError {
    /// The value the failing arm carries.
    pub(super) value: u32,
    /// What the error classifies as.
    pub(super) category: ErrorCategory,
}

/// Why the test codec could not read a response.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum TestCodecError {
    /// The buffer is not one tag byte and one `u32`, or names no arm.
    #[error("the test payload is malformed")]
    Malformed,
}

impl Codec for TestCodec {
    type Error = TestCodecError;
    type Payload = Result<u32, TestError>;

    const FORMAT_ID: &'static str = "requester-test";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, TestCodecError> {
        let Some((tag, rest)) = buf.split_first() else {
            return Err(TestCodecError::Malformed);
        };
        let Ok(value) = <[u8; 4]>::try_from(rest) else {
            return Err(TestCodecError::Malformed);
        };
        let value = u32::from_le_bytes(value);
        match *tag {
            OK_TAG => Ok(Ok(value)),
            TRANSIENT_TAG | PERMANENT_TAG | TERMINAL_TAG => {
                let Some(category) = tagged_category(*tag) else {
                    return Err(TestCodecError::Malformed);
                };
                Ok(Err(TestError { value, category }))
            }
            _ => Err(TestCodecError::Malformed),
        }
    }

    fn serialize_ref(
        &mut self,
        payload: &Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), TestCodecError> {
        let (tag, value) = match payload {
            Ok(value) => (OK_TAG, *value),
            Err(error) => (category_tag(error.category), error.value),
        };
        buf.push(tag);
        buf.extend_from_slice(&value.to_le_bytes());
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

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        self.category
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
    Ok(ProsodyRequester::new(producer, NODE, registry))
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
    let outcomes = requester
        .request::<_, u32, TestError>(
            empty(),
            Topic::from(TOPIC),
            KEY,
            RequestPayload,
            &awaited,
            MAX_TIMEOUT,
        )
        .await?;
    ensure!(
        outcomes == vec![Outcome::Failed(ResponseFailure::Timeout)],
        "nothing answered this call, so its one outcome must be a timeout"
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
pub(super) fn body(payload: Result<u32, TestError>) -> Result<BytesMut> {
    let mut buf = Vec::with_capacity(RESPONSE_BYTES);
    TestCodec::with_cached_local(|codec| codec.serialize(payload, &mut buf))?;
    Ok(BytesMut::from(buf.as_slice()))
}

/// Builds one response frame for `subsystem`, in the format the waiter reads.
pub(super) fn frame(
    id: RequestId,
    subsystem: &SubsystemName,
    status: ResponseStatus,
    payload: BytesMut,
) -> ResponseFrame {
    formatted_frame(id, subsystem, status, payload, TestCodec::FORMAT_ID)
}

/// Builds one response frame in an arbitrary format token.
pub(super) fn formatted_frame(
    id: RequestId,
    subsystem: &SubsystemName,
    status: ResponseStatus,
    payload: BytesMut,
    format: &str,
) -> ResponseFrame {
    ResponseFrame {
        header: RequestTag::new(
            id,
            NODE,
            RequestDeadline::from_unix_micros(1_700_000_000_000_000),
        )
        .header(subsystem.clone(), status),
        format: FormatToken::make(format),
        payload,
    }
}

/// Builds one successful response frame carrying `value`.
pub(super) fn success(
    id: RequestId,
    subsystem: &SubsystemName,
    value: u32,
) -> Result<ResponseFrame> {
    Ok(frame(
        id,
        subsystem,
        ResponseStatus::Success,
        body(Ok(value))?,
    ))
}

/// Polls `future` exactly once and reports what that one poll returned.
///
/// A zero timeout would not do: it does not promise exactly one inner poll.
pub(super) async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

/// The category one arm tag names.
const fn tagged_category(tag: u8) -> Option<ErrorCategory> {
    match tag {
        TRANSIENT_TAG => Some(ErrorCategory::Transient),
        PERMANENT_TAG => Some(ErrorCategory::Permanent),
        TERMINAL_TAG => Some(ErrorCategory::Terminal),
        _ => None,
    }
}

/// The arm tag one category is written as.
const fn category_tag(category: ErrorCategory) -> u8 {
    match category {
        ErrorCategory::Transient => TRANSIENT_TAG,
        ErrorCategory::Permanent => PERMANENT_TAG,
        ErrorCategory::Terminal => TERMINAL_TAG,
    }
}
