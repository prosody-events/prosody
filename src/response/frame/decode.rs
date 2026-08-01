//! Reading one frame a peer sent, bounding it before anything is allocated on
//! behalf of a length that peer merely claimed.

use super::{
    FIELD_CATEGORY, FIELD_FORMAT, FIELD_PAYLOAD, FIELD_PROTOCOL_VERSION, FIELD_RELAY_NODE,
    FIELD_REQUEST_ID, FIELD_SUBSYSTEM, FIELD_TARGET_NODE, FrameCap, FrameHeader, ID_BYTES,
    ResponseFrame,
};
use crate::error::{ErrorCategory, UnknownErrorCategory};
use crate::response::{FORMAT_MAX_BYTES, RESPONSE_PROTOCOL_VERSION, RequestId};
use crate::router::NodeId;
use crate::subsystem::SubsystemName;
use bytes::{Buf, BufMut, BytesMut};
use fixedstr::Flexstr;
use prost::DecodeError;
use prost::encoding::{
    DecodeContext, WireType, check_wire_type, decode_key, decode_varint, skip_field,
};
use std::str::{Utf8Error, from_utf8};
use thiserror::Error;

/// Every field this frame defines, paired with the bit that records having read
/// it. A tag absent from here belongs to a later protocol version and is
/// skipped.
const fn known_field(tag: u32) -> Option<(&'static str, u8)> {
    Some(match tag {
        FIELD_PROTOCOL_VERSION => ("protocol_version", 0b0000_0001),
        FIELD_TARGET_NODE => ("target_node", 0b0000_0010),
        FIELD_REQUEST_ID => ("request_id", 0b0000_0100),
        FIELD_SUBSYSTEM => ("subsystem", 0b0000_1000),
        FIELD_FORMAT => ("format", 0b0001_0000),
        FIELD_CATEGORY => ("category", 0b0010_0000),
        FIELD_PAYLOAD => ("payload", 0b0100_0000),
        FIELD_RELAY_NODE => ("relay_node", 0b1000_0000),
        _ => return None,
    })
}

/// Decodes one frame, bounding the whole encoded message before any per-field
/// work so nothing is allocated on behalf of a length a peer merely claimed.
///
/// Each field may appear once. Protobuf lets a sender repeat a singular field
/// and take the last, but every repeat here would be either a contradiction or
/// an amplifier — a capped frame packed with tiny `payload` fields would buy
/// millions of allocate-and-discard pairs for a few bytes each. Refusing the
/// repeat is the same posture this decoder already takes toward an empty
/// `subsystem` or a zero `category`.
///
/// # Errors
///
/// Returns a [`FrameDecodeError`] naming the field that made the frame
/// unreadable.
pub(crate) fn decode_frame<B: Buf>(
    src: &mut B,
    cap: FrameCap,
) -> Result<ResponseFrame, FrameDecodeError> {
    if src.remaining() > cap.bytes() {
        return Err(FrameDecodeError::FrameTooLarge {
            bytes: src.remaining(),
            limit: cap.bytes(),
        });
    }

    let mut version = None;
    let mut target = None;
    let mut request = None;
    let mut subsystem = None;
    let mut format = None;
    let mut category = None;
    // A codec may legally serialize to zero bytes, and a frame no relay has
    // touched carries no relay node, so a peer that omits these proto3 defaults
    // is sending a well-formed frame. The six fields above exclude their default
    // by construction — the version is at least 1, the ids are 16 bytes, a
    // response is never for an unnamed subsystem or in an unnamed format, and
    // the category reserves 0 — so their absence is malformed. That is stricter
    // than the `.proto` can state, which is the point: a schema cannot say "not
    // the default", so the decoder does.
    let mut payload = BytesMut::new();
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
            FIELD_PROTOCOL_VERSION => {
                check_wire_type(WireType::Varint, wire_type)?;
                version = Some(decode_varint(src)?);
            }
            FIELD_TARGET_NODE => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                target = decode_id(src, "target_node")?.map(NodeId::from_bytes);
            }
            FIELD_REQUEST_ID => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                request = decode_id(src, "request_id")?.map(RequestId::from_bytes);
            }
            FIELD_SUBSYSTEM => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                subsystem = decode_text::<B, { SubsystemName::MAX_BYTES + 1 }>(src, "subsystem")?;
            }
            FIELD_FORMAT => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                format = decode_text::<B, { FORMAT_MAX_BYTES + 1 }>(src, "format")?;
            }
            FIELD_CATEGORY => {
                check_wire_type(WireType::Varint, wire_type)?;
                category = Some(ErrorCategory::try_from(decode_varint(src)? as u32 as i32)?);
            }
            FIELD_PAYLOAD => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                payload = decode_payload(src)?;
            }
            FIELD_RELAY_NODE => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                relay = decode_id(src, "relay_node")?.map(NodeId::from_bytes);
            }
            _ => skip_field(wire_type, tag, src, DecodeContext::default())?,
        }
    }

    // Compared as the raw varint rather than narrowed to the field's `uint32`:
    // narrowing would fold a value too wide to be any version onto one this
    // build believes it speaks, and refusing it costs nothing a real peer needs.
    let version = version.ok_or(FrameDecodeError::MissingField("protocol_version"))?;
    if version != u64::from(RESPONSE_PROTOCOL_VERSION) {
        return Err(FrameDecodeError::UnsupportedVersion(version));
    }
    Ok(ResponseFrame {
        header: FrameHeader {
            target: target.ok_or(FrameDecodeError::MissingField("target_node"))?,
            request: request.ok_or(FrameDecodeError::MissingField("request_id"))?,
            subsystem: subsystem.ok_or(FrameDecodeError::MissingField("subsystem"))?,
            category: category.ok_or(FrameDecodeError::MissingField("category"))?,
            relay,
        },
        format: format.ok_or(FrameDecodeError::MissingField("format"))?,
        payload,
    })
}

/// Reads an identifier field. A zero-length field is protobuf's way of spelling
/// "absent", which only `relay_node` legitimately is; for the others the caller
/// turns `None` into a missing-field error.
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
    if src.remaining() < ID_BYTES {
        return Err(FrameDecodeError::Truncated {
            field,
            bytes: len,
            remaining: src.remaining(),
        });
    }
    let mut id = [0u8; ID_BYTES];
    src.copy_to_slice(&mut id);
    Ok(Some(id))
}

/// Reads a bounded UTF-8 field into an inline string. The claimed length is
/// checked against the bound before a byte is copied, so the stack buffer is
/// always large enough and no peer-claimed length ever sizes an allocation.
///
/// An empty string is protobuf's spelling of the default, which neither of this
/// frame's text fields may be, so it reads as absent and the caller turns
/// `None` into a missing-field error.
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
    if len > src.remaining() as u64 {
        return Err(FrameDecodeError::Truncated {
            field,
            bytes: len,
            remaining: src.remaining(),
        });
    }
    let mut buf = [0u8; N];
    let len = len as usize;
    src.copy_to_slice(&mut buf[..len]);
    Ok(Some(Flexstr::make(from_utf8(&buf[..len])?)))
}

/// Copies the payload into one allocation sized to the length the frame states,
/// refusing a length the frame does not actually carry.
fn decode_payload<B: Buf>(src: &mut B) -> Result<BytesMut, FrameDecodeError> {
    let len = decode_varint(src)?;
    if len > src.remaining() as u64 {
        return Err(FrameDecodeError::Truncated {
            field: "payload",
            bytes: len,
            remaining: src.remaining(),
        });
    }
    let len = len as usize;
    let mut payload = BytesMut::with_capacity(len);
    payload.put(src.take(len));
    Ok(payload)
}

/// Why a frame a peer sent could not be read.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum FrameDecodeError {
    /// The encoded frame is larger than the configured ceiling.
    #[error("frame is {bytes} bytes, over the {limit}-byte cap")]
    FrameTooLarge {
        /// The encoded frame's length.
        bytes: usize,
        /// The configured ceiling.
        limit: usize,
    },

    /// A field states a length the frame does not carry.
    #[error("{field} claims {bytes} bytes with {remaining} left in the frame")]
    Truncated {
        /// The field that made the claim.
        field: &'static str,
        /// The claimed length.
        bytes: u64,
        /// What the frame actually had left.
        remaining: usize,
    },

    /// A field whose absence cannot be a legal default is absent.
    #[error("frame is missing {0}")]
    MissingField(&'static str),

    /// A field appears more than once.
    #[error("frame repeats {0}")]
    RepeatedField(&'static str),

    /// The frame states a protocol version this build does not speak.
    #[error("unsupported response protocol version {0}")]
    UnsupportedVersion(u64),

    /// An identifier field is present but is not 16 bytes.
    #[error("{field} is {bytes} bytes, not 16")]
    MalformedId {
        /// The identifier field.
        field: &'static str,
        /// The length it claimed.
        bytes: u64,
    },

    /// A string field is longer than the frame permits.
    #[error("{field} is {bytes} bytes, over the {limit}-byte limit")]
    StringTooLong {
        /// The string field.
        field: &'static str,
        /// The claimed length.
        bytes: u64,
        /// The longest value the field may carry.
        limit: usize,
    },

    /// A string field is not valid UTF-8.
    #[error(transparent)]
    InvalidUtf8(#[from] Utf8Error),

    /// The category field names no error category.
    #[error(transparent)]
    Category(#[from] UnknownErrorCategory),

    /// The bytes are not well-formed protobuf.
    #[error(transparent)]
    Wire(#[from] DecodeError),
}
