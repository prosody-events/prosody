//! Reading one response frame.

use super::{
    FIELD_ERROR_CATEGORY, FIELD_ERROR_MESSAGE, FIELD_HANDLER_ERROR, FIELD_RELAY_PEER,
    FIELD_REQUEST_ID, FIELD_SUBSYSTEM, FIELD_SUCCESS, FIELD_SUCCESS_FORMAT, FIELD_SUCCESS_PAYLOAD,
    FIELD_TARGET_PEER, FrameHeader, FrameResult, HandlerError, ID_BYTES, ResponseFrame,
    ResponseSuccess,
};
use crate::error::{ErrorCategory, UnknownErrorCategory};
use crate::peer::response::{FormatToken, RequestId};
use crate::peer::router::PeerId;
use crate::subsystem::{SubsystemName, SubsystemNameError};
use bytes::{Buf, Bytes};
use fixedstr::Flexstr;
use prost::DecodeError;
use prost::encoding::{
    DecodeContext, WireType, check_wire_type, decode_key, decode_varint, skip_field,
};
use std::str::{Utf8Error, from_utf8};
use thiserror::Error;

const fn known_field(tag: u32) -> Option<(&'static str, u8)> {
    Some(match tag {
        FIELD_TARGET_PEER => ("target_peer", 0b0000_0001),
        FIELD_REQUEST_ID => ("request_id", 0b0000_0010),
        FIELD_SUBSYSTEM => ("subsystem", 0b0000_0100),
        FIELD_SUCCESS => ("success", 0b0000_1000),
        FIELD_HANDLER_ERROR => ("handler_error", 0b0000_1000),
        FIELD_RELAY_PEER => ("relay_peer", 0b0001_0000),
        _ => return None,
    })
}

/// Decodes one frame and rejects repeated singular fields.
pub(crate) fn decode_frame<B: Buf>(src: &mut B) -> Result<ResponseFrame, FrameDecodeError> {
    let mut target = None;
    let mut request = None;
    let mut subsystem = None;
    let mut result = None;
    let mut relay = None;
    let mut seen = 0u8;

    while src.has_remaining() {
        let (tag, wire_type) = decode_key(src)?;
        if let Some((field, bit)) = known_field(tag) {
            if seen & bit != 0 {
                return Err(FrameDecodeError::RepeatedField(field));
            }
            seen |= bit;
        }
        match tag {
            FIELD_TARGET_PEER => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                target = decode_id(src, "target_peer")?.map(PeerId::from_bytes);
            }
            FIELD_REQUEST_ID => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                request = decode_id(src, "request_id")?.map(RequestId::from_bytes);
            }
            FIELD_SUBSYSTEM => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                subsystem = decode_text::<B, { SubsystemName::MAX_BYTES + 1 }>(src, "subsystem")?
                    .map(SubsystemName::try_new)
                    .transpose()?;
            }
            FIELD_SUCCESS => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                result = Some(decode_success(&mut decode_bytes(src, "success")?)?);
            }
            FIELD_HANDLER_ERROR => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                result = Some(decode_error(&mut decode_bytes(src, "handler_error")?)?);
            }
            FIELD_RELAY_PEER => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                relay = decode_id(src, "relay_peer")?.map(PeerId::from_bytes);
            }
            _ => skip_field(wire_type, tag, src, DecodeContext::default())?,
        }
    }

    Ok(ResponseFrame {
        header: FrameHeader {
            target: target.ok_or(FrameDecodeError::MissingField("target_peer"))?,
            request: request.ok_or(FrameDecodeError::MissingField("request_id"))?,
            subsystem: subsystem.ok_or(FrameDecodeError::MissingField("subsystem"))?,
            relay,
        },
        result: result.ok_or(FrameDecodeError::MissingField("result"))?,
    })
}

fn decode_success(src: &mut Bytes) -> Result<FrameResult, FrameDecodeError> {
    let mut format = None;
    let mut payload = None;
    let mut seen = 0u8;
    while src.has_remaining() {
        let (tag, wire_type) = decode_key(src)?;
        match tag {
            FIELD_SUCCESS_FORMAT => {
                repeated(&mut seen, 1, "success.format")?;
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                let bytes = decode_bytes(src, "success.format")?;
                if bytes.is_empty() {
                    return Err(FrameDecodeError::MissingField("success.format"));
                }
                format = Some(FormatToken::try_from_bytes(bytes)?);
            }
            FIELD_SUCCESS_PAYLOAD => {
                repeated(&mut seen, 2, "success.payload")?;
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                payload = Some(decode_bytes(src, "success.payload")?);
            }
            _ => skip_field(wire_type, tag, src, DecodeContext::default())?,
        }
    }
    Ok(FrameResult::Success(ResponseSuccess {
        format: format.ok_or(FrameDecodeError::MissingField("success.format"))?,
        payload: match payload {
            Some(payload) => payload,
            None => Bytes::new(),
        },
    }))
}

fn decode_error(src: &mut Bytes) -> Result<FrameResult, FrameDecodeError> {
    let mut category = None;
    let mut message = None;
    let mut seen = 0u8;
    while src.has_remaining() {
        let (tag, wire_type) = decode_key(src)?;
        match tag {
            FIELD_ERROR_CATEGORY => {
                repeated(&mut seen, 1, "handler_error.category")?;
                check_wire_type(WireType::Varint, wire_type)?;
                let raw = decode_varint(src)?;
                let raw = i32::try_from(raw).map_err(|_| FrameDecodeError::CategoryTooWide(raw))?;
                category = Some(ErrorCategory::try_from(raw)?);
            }
            FIELD_ERROR_MESSAGE => {
                repeated(&mut seen, 2, "handler_error.message")?;
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                let bytes = decode_bytes(src, "handler_error.message")?;
                from_utf8(&bytes)?;
                message = Some(bytes);
            }
            _ => skip_field(wire_type, tag, src, DecodeContext::default())?,
        }
    }
    Ok(FrameResult::HandlerError(HandlerError {
        category: category.ok_or(FrameDecodeError::MissingField("handler_error.category"))?,
        message: match message {
            Some(message) => message,
            None => Bytes::new(),
        },
    }))
}

fn repeated(seen: &mut u8, bit: u8, field: &'static str) -> Result<(), FrameDecodeError> {
    if *seen & bit != 0 {
        return Err(FrameDecodeError::RepeatedField(field));
    }
    *seen |= bit;
    Ok(())
}

fn decode_id<B: Buf>(
    src: &mut B,
    field: &'static str,
) -> Result<Option<[u8; ID_BYTES]>, FrameDecodeError> {
    let len = decode_varint(src)?;
    if len == 0 {
        return Ok(None);
    }
    if len != ID_BYTES as u64 {
        return Err(FrameDecodeError::MalformedId { field, bytes: len });
    }
    require(src, field, len)?;
    let mut id = [0u8; ID_BYTES];
    src.copy_to_slice(&mut id);
    Ok(Some(id))
}

fn decode_text<B: Buf, const N: usize>(
    src: &mut B,
    field: &'static str,
) -> Result<Option<Flexstr<N>>, FrameDecodeError> {
    let len = decode_varint(src)?;
    if len == 0 {
        return Ok(None);
    }
    if len >= N as u64 {
        return Err(FrameDecodeError::StringTooLong {
            field,
            bytes: len,
            limit: N - 1,
        });
    }
    require(src, field, len)?;
    let mut buf = [0u8; N];
    let len = len as usize;
    src.copy_to_slice(&mut buf[..len]);
    Ok(Some(Flexstr::make(from_utf8(&buf[..len])?)))
}

fn decode_bytes<B: Buf>(src: &mut B, field: &'static str) -> Result<Bytes, FrameDecodeError> {
    let len = decode_varint(src)?;
    require(src, field, len)?;
    Ok(src.copy_to_bytes(len as usize))
}

fn require<B: Buf>(src: &B, field: &'static str, bytes: u64) -> Result<(), FrameDecodeError> {
    if bytes > src.remaining() as u64 {
        return Err(FrameDecodeError::Truncated {
            field,
            bytes,
            remaining: src.remaining(),
        });
    }
    Ok(())
}

/// Why a peer frame could not be read.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum FrameDecodeError {
    #[error("{field} claims {bytes} bytes with {remaining} left in the frame")]
    Truncated {
        field: &'static str,
        bytes: u64,
        remaining: usize,
    },
    #[error("frame is missing {0}")]
    MissingField(&'static str),
    #[error("frame repeats {0}")]
    RepeatedField(&'static str),
    #[error("{field} is {bytes} bytes, not 16")]
    MalformedId { field: &'static str, bytes: u64 },
    #[error("{field} is {bytes} bytes, over the {limit}-byte limit")]
    StringTooLong {
        field: &'static str,
        bytes: u64,
        limit: usize,
    },
    #[error("handler_error.category value {0} does not fit int32")]
    CategoryTooWide(u64),
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
            Self::Truncated { .. } => "a frame field claims more bytes than the frame carries",
            Self::MissingField(_) => "the frame omits a field it must carry",
            Self::RepeatedField(_) => "the frame repeats a field it may carry once",
            Self::MalformedId { .. } => "a frame identifier is not 16 bytes",
            Self::StringTooLong { .. } => "a frame text field is over its limit",
            Self::CategoryTooWide(_) | Self::UnknownCategory(_) => {
                "the frame states no known error category"
            }
            Self::InvalidText(_) => "a frame text field is not UTF-8",
            Self::InvalidSubsystem(_) => "the frame names no valid subsystem",
            Self::Protobuf(_) => "the frame is not well-formed protobuf",
        }
    }
}
