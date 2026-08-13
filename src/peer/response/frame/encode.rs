//! Turning one borrowed response into owned frame data.

use super::{
    DELIVER_RESULT_HANDLER_ERROR_TAG, DELIVER_RESULT_RELAY_PEER_TAG, DELIVER_RESULT_REQUEST_ID_TAG,
    DELIVER_RESULT_SUBSYSTEM_TAG, DELIVER_RESULT_SUCCESS_TAG, DELIVER_RESULT_TARGET_PEER_TAG,
    FrameHeader, FrameResult, HANDLER_ERROR_CATEGORY_TAG, HANDLER_ERROR_MESSAGE_TAG, HandlerError,
    RESPONSE_SUCCESS_FORMAT_TAG, RESPONSE_SUCCESS_PAYLOAD_TAG, ResponseFrame, ResponseSuccess,
};
use crate::codec::{Codec, SerializeBufGuard};
use crate::error::ErrorCategory;
use crate::peer::response::FormatToken;
use crate::peer::router::{Framed, PeerId};
use bytes::{BufMut, Bytes};
use prost::encoding::{WireType, encode_key, encode_varint, encoded_len_varint, key_len};
use std::error::Error;
use std::mem::size_of;
use thiserror::Error;
use uuid::Bytes as UuidBytes;

/// An encoded response ready for routing.
#[derive(Clone)]
pub struct Staged {
    header: FrameHeader,
    result: FrameResult,
    bytes: usize,
}

/// One frame on its way to its target, with this relay's identifier.
#[derive(Clone)]
pub(crate) struct Forwarded(ResponseFrame);

/// Serializes one successful response through the standard codec resources.
pub(crate) fn stage_success<C: Codec>(
    header: &FrameHeader,
    payload: &C::Payload,
) -> Result<Staged, EncodeError<C::Error>> {
    const {
        assert!(
            !C::FORMAT_ID.is_empty(),
            "a response codec must name a format"
        );
    }
    let mut scratch = SerializeBufGuard::acquire();
    C::with_cached_local(|codec| codec.serialize_ref(payload, &mut scratch))
        .map_err(EncodeError::Codec)?;
    Ok(stage(
        header,
        FrameResult::Success(ResponseSuccess {
            format: FormatToken::make(C::FORMAT_ID),
            payload: Bytes::copy_from_slice(&scratch),
        }),
    ))
}

/// Stages one handler failure in Prosody's protocol format.
pub(crate) fn stage_error(
    header: &FrameHeader,
    category: ErrorCategory,
    message: String,
) -> Staged {
    stage(
        header,
        FrameResult::HandlerError(HandlerError {
            category,
            message: Bytes::from(message),
        }),
    )
}

fn stage(header: &FrameHeader, result: FrameResult) -> Staged {
    Staged {
        bytes: frame_len(header, &result) as usize,
        header: header.clone(),
        result,
    }
}

impl Framed for Staged {
    fn bytes(&self) -> usize {
        self.bytes
    }

    fn write<B: BufMut>(&self, dst: &mut B) {
        write_frame(&self.header, &self.result, dst);
    }
}

impl Staged {
    pub(in crate::peer::response) const fn header(&self) -> &FrameHeader {
        &self.header
    }

    pub(in crate::peer::response) const fn target(&self) -> PeerId {
        self.header.target
    }

    pub(in crate::peer::response) fn into_local_frame(self) -> ResponseFrame {
        ResponseFrame {
            header: self.header,
            result: self.result,
        }
    }
}

impl Forwarded {
    pub(crate) fn new(mut frame: ResponseFrame, relay: PeerId) -> Self {
        frame.header.relay = Some(relay);
        Self(frame)
    }
}

impl Framed for Forwarded {
    fn bytes(&self) -> usize {
        frame_len(&self.0.header, &self.0.result) as usize
    }

    fn write<B: BufMut>(&self, dst: &mut B) {
        write_frame(&self.0.header, &self.0.result, dst);
    }
}

fn write_frame<B: BufMut>(header: &FrameHeader, result: &FrameResult, dst: &mut B) {
    write_bytes_field(
        DELIVER_RESULT_TARGET_PEER_TAG,
        &header.target.into_bytes(),
        dst,
    );
    write_bytes_field(
        DELIVER_RESULT_REQUEST_ID_TAG,
        &header.request.into_bytes(),
        dst,
    );
    write_bytes_field(
        DELIVER_RESULT_SUBSYSTEM_TAG,
        header.subsystem.as_str().as_bytes(),
        dst,
    );
    match result {
        FrameResult::Success(ResponseSuccess { format, payload }) => {
            let len = success_len(format.as_bytes().len(), payload.len());
            write_message_key(DELIVER_RESULT_SUCCESS_TAG, len, dst);
            write_bytes_field(RESPONSE_SUCCESS_FORMAT_TAG, format.as_bytes(), dst);
            write_bytes_field(RESPONSE_SUCCESS_PAYLOAD_TAG, payload, dst);
        }
        FrameResult::HandlerError(HandlerError { category, message }) => {
            let len = error_len(*category, message.len());
            write_message_key(DELIVER_RESULT_HANDLER_ERROR_TAG, len, dst);
            write_varint_field(HANDLER_ERROR_CATEGORY_TAG, i32::from(*category) as u64, dst);
            write_bytes_field(HANDLER_ERROR_MESSAGE_TAG, message, dst);
        }
    }
    if let Some(relay) = header.relay {
        write_bytes_field(DELIVER_RESULT_RELAY_PEER_TAG, &relay.into_bytes(), dst);
    }
}

fn frame_len(header: &FrameHeader, result: &FrameResult) -> u64 {
    bytes_field_len(DELIVER_RESULT_TARGET_PEER_TAG, size_of::<UuidBytes>())
        + bytes_field_len(DELIVER_RESULT_REQUEST_ID_TAG, size_of::<UuidBytes>())
        + bytes_field_len(
            DELIVER_RESULT_SUBSYSTEM_TAG,
            header.subsystem.as_str().len(),
        )
        + match result {
            FrameResult::Success(ResponseSuccess { format, payload }) => message_field_len(
                DELIVER_RESULT_SUCCESS_TAG,
                success_len(format.as_bytes().len(), payload.len()),
            ),
            FrameResult::HandlerError(HandlerError { category, message }) => message_field_len(
                DELIVER_RESULT_HANDLER_ERROR_TAG,
                error_len(*category, message.len()),
            ),
        }
        + if header.relay.is_some() {
            bytes_field_len(DELIVER_RESULT_RELAY_PEER_TAG, size_of::<UuidBytes>())
        } else {
            0
        }
}

fn success_len(format: usize, payload: usize) -> u64 {
    bytes_field_len(RESPONSE_SUCCESS_FORMAT_TAG, format)
        + bytes_field_len(RESPONSE_SUCCESS_PAYLOAD_TAG, payload)
}

fn error_len(category: ErrorCategory, message: usize) -> u64 {
    varint_field_len(HANDLER_ERROR_CATEGORY_TAG, i32::from(category) as u64)
        + bytes_field_len(HANDLER_ERROR_MESSAGE_TAG, message)
}

const fn message_field_len(tag: u32, len: u64) -> u64 {
    key_len(tag) as u64 + encoded_len_varint(len) as u64 + len
}

const fn varint_field_len(tag: u32, value: u64) -> u64 {
    key_len(tag) as u64 + encoded_len_varint(value) as u64
}

const fn bytes_field_len(tag: u32, len: usize) -> u64 {
    key_len(tag) as u64 + encoded_len_varint(len as u64) as u64 + len as u64
}

fn write_message_key<B: BufMut>(tag: u32, len: u64, dst: &mut B) {
    encode_key(tag, WireType::LengthDelimited, dst);
    encode_varint(len, dst);
}

fn write_varint_field<B: BufMut>(tag: u32, value: u64, dst: &mut B) {
    encode_key(tag, WireType::Varint, dst);
    encode_varint(value, dst);
}

fn write_bytes_field<B: BufMut>(tag: u32, value: &[u8], dst: &mut B) {
    encode_key(tag, WireType::LengthDelimited, dst);
    encode_varint(value.len() as u64, dst);
    dst.put_slice(value);
}

/// Why a successful response could not be encoded.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum EncodeError<E: Error> {
    /// The response codec failed.
    #[error(transparent)]
    Codec(E),
}
