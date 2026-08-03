//! What the requester suites share: one response codec, one registry factory,
//! and the frames and produce futures the delivery race is driven with.
//!
//! Every suite runs on a paused-time runtime, so a virtual second costs
//! nothing and every schedule is the same on every machine.

use super::config::RequesterConfiguration;
use super::registry::PendingRegistry;
use crate::codec::Codec;
use crate::error::{ClassifyError, ErrorCategory};
use crate::response::frame::ResponseFrame;
use crate::response::headers::RequestTag;
use crate::response::{FormatToken, RequestId, ResponseStatus};
use crate::router::NodeId;
use crate::subsystem::SubsystemName;
use bytes::BytesMut;
use color_eyre::Result;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use thiserror::Error;

mod config;
mod lifecycle;
mod race;
mod registry;
mod request;
mod sweep;

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

/// Grace the suites configure.
///
/// It is long enough that the registry's own sweep task never ticks inside a
/// suite, so every sweep a suite observes is one it called itself.
const SWEEP_GRACE: Duration = Duration::from_mins(10);

/// Subsystem names the property generators draw from.
const POOL: [&str; 6] = ["billing", "ledger", "audit", "search", "mailer", "a,b"];

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

    fn serialize(
        &mut self,
        payload: Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), TestCodecError> {
        let (tag, value) = match payload {
            Ok(value) => (OK_TAG, value),
            Err(error) => (category_tag(error.category), error.value),
        };
        buf.push(tag);
        buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }
}

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        self.category
    }
}

/// Builds a registry and starts its sweep task.
///
/// Call this inside a runtime, because the registry spawns its sweep.
pub(super) fn registry(max_in_flight: usize, max_awaited: usize) -> Result<Arc<PendingRegistry>> {
    Ok(PendingRegistry::new(&RequesterConfiguration {
        max_in_flight,
        max_awaited,
        max_timeout: MAX_TIMEOUT,
        sweep_grace: SWEEP_GRACE,
    })?)
}

/// Builds subsystem names, refusing any the crate would refuse.
pub(super) fn names(names: &[&str]) -> Result<Vec<SubsystemName>> {
    names
        .iter()
        .map(|name| Ok(SubsystemName::try_new(name)?))
        .collect()
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
        header: RequestTag::new(id, NODE).header(subsystem.clone(), status),
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
