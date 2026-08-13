//! Converts one generated Protobuf frame into Prosody's domain types.

use super::{
    FrameHeader, FrameResult, HandlerError, ResponseFrame, ResponseSuccess as DomainSuccess,
};
use crate::error::{ErrorCategory, UnknownErrorCategory};
use crate::peer::response::{FormatToken, RequestId};
use crate::peer::router::PeerId;
use crate::peer::router::grpc::generated::{
    DeliverResultRequest, HandlerError as WireHandlerError, ResponseSuccess,
    deliver_result_request::Result as WireResult,
};
use crate::subsystem::{SubsystemName, SubsystemNameError};
use bytes::Bytes;
use prost::DecodeError;
use std::mem::size_of;
use std::str::{Utf8Error, from_utf8};
use thiserror::Error;
use uuid::Bytes as UuidBytes;

impl TryFrom<DeliverResultRequest> for ResponseFrame {
    type Error = FrameDecodeError;

    fn try_from(wire: DeliverResultRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            header: FrameHeader {
                target: PeerId::from_bytes(required_id(&wire.target_peer, "target_peer")?),
                request: RequestId::from_bytes(required_id(&wire.request_id, "request_id")?),
                subsystem: decode_subsystem(&wire.subsystem)?,
                relay: optional_id(&wire.relay_peer, "relay_peer")?.map(PeerId::from_bytes),
            },
            result: decode_result(wire.result)?,
        })
    }
}

fn decode_result(result: Option<WireResult>) -> Result<FrameResult, FrameDecodeError> {
    match result.ok_or(FrameDecodeError::MissingField("result"))? {
        WireResult::Success(success) => success.try_into(),
        WireResult::HandlerError(error) => error.try_into(),
    }
}

impl TryFrom<ResponseSuccess> for FrameResult {
    type Error = FrameDecodeError;

    fn try_from(success: ResponseSuccess) -> Result<Self, Self::Error> {
        if success.format.is_empty() {
            return Err(FrameDecodeError::MissingField("success.format"));
        }
        Ok(Self::Success(DomainSuccess {
            format: FormatToken::try_from_bytes(success.format)?,
            payload: success.payload,
        }))
    }
}

impl TryFrom<WireHandlerError> for FrameResult {
    type Error = FrameDecodeError;

    fn try_from(error: WireHandlerError) -> Result<Self, Self::Error> {
        from_utf8(&error.message)?;
        Ok(Self::HandlerError(HandlerError {
            category: ErrorCategory::try_from(error.category)?,
            message: error.message,
        }))
    }
}

fn decode_subsystem(value: &Bytes) -> Result<SubsystemName, FrameDecodeError> {
    if value.is_empty() {
        return Err(FrameDecodeError::MissingField("subsystem"));
    }
    Ok(SubsystemName::try_new(from_utf8(value)?)?)
}

fn required_id(value: &Bytes, field: &'static str) -> Result<UuidBytes, FrameDecodeError> {
    optional_id(value, field)?.ok_or(FrameDecodeError::MissingField(field))
}

fn optional_id(value: &Bytes, field: &'static str) -> Result<Option<UuidBytes>, FrameDecodeError> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() != size_of::<UuidBytes>() {
        return Err(FrameDecodeError::MalformedId {
            field,
            bytes: value.len(),
        });
    }
    let mut id = UuidBytes::default();
    id.copy_from_slice(value);
    Ok(Some(id))
}

/// Why a decoded Protobuf frame is not a valid Prosody response.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum FrameDecodeError {
    #[error("frame is missing {0}")]
    MissingField(&'static str),
    #[error("{field} is {bytes} bytes, not 16")]
    MalformedId { field: &'static str, bytes: usize },
    #[error(transparent)]
    UnknownCategory(#[from] UnknownErrorCategory),
    #[error(transparent)]
    InvalidText(#[from] Utf8Error),
    #[error(transparent)]
    InvalidSubsystem(#[from] SubsystemNameError),
    #[error(transparent)]
    Protobuf(#[from] DecodeError),
}

impl FrameDecodeError {
    /// Returns a fixed message safe for an unauthenticated peer.
    pub(crate) const fn message(&self) -> &'static str {
        match self {
            Self::MissingField(_) => "the frame omits a field it must carry",
            Self::MalformedId { .. } => "a frame identifier is not 16 bytes",
            Self::UnknownCategory(_) => "the frame states no known error category",
            Self::InvalidText(_) => "a frame text field is not UTF-8",
            Self::InvalidSubsystem(_) => "the frame names no valid subsystem",
            Self::Protobuf(_) => "the frame is not well-formed protobuf",
        }
    }
}
