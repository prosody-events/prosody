//! Turning one response into frame bytes, against a scratch buffer that is
//! allocated once and never grows in steady state.

use super::{
    FIELD_CATEGORY, FIELD_FORMAT, FIELD_PAYLOAD, FIELD_PROTOCOL_VERSION, FIELD_RELAY_NODE,
    FIELD_REQUEST_ID, FIELD_SUBSYSTEM, FIELD_TARGET_NODE, FrameCap, FrameHeader, ID_BYTES,
};
use crate::codec::Codec;
use crate::error::ErrorCategory;
use crate::response::{FORMAT_MAX_BYTES, RESPONSE_PROTOCOL_VERSION, SUBSYSTEM_MAX_BYTES};
use bytes::BufMut;
use prost::encoding::{WireType, encode_key, encode_varint, encoded_len_varint, key_len};
use std::error::Error;
use thiserror::Error;

/// Encodes responses into frames against one bounded scratch buffer, allocated
/// once at construction and reused for every response.
///
/// Staging and framing are two steps because a protobuf `bytes` field writes
/// its varint length *before* its contents: the payload must be serialized
/// somewhere before that length is known. [`FrameEncoder::stage`] serializes
/// into the scratch and refuses anything whose complete frame would exceed the
/// cap; only a [`Staged`] can be written, so "framed before the cap was
/// checked" is unrepresentable.
pub(crate) struct FrameEncoder<C> {
    codec: C,
    scratch: Vec<u8>,
    cap: FrameCap,
}

/// A response whose complete frame length has been checked against the cap.
/// Writing one cannot fail and cannot exceed that cap.
pub(crate) struct Staged<'a> {
    header: &'a FrameHeader,
    format: &'static str,
    payload: &'a [u8],
    bytes: usize,
}

impl<C: Codec> FrameEncoder<C> {
    /// Builds an encoder whose scratch is sized once, to the cap.
    pub(crate) fn new(codec: C, cap: FrameCap) -> Self {
        Self {
            codec,
            scratch: Vec::with_capacity(cap.bytes()),
            cap,
        }
    }

    /// Serializes one response into the scratch and checks the complete frame
    /// length against the cap.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError::UnusableSubsystem`] for a name no decoder would
    /// accept, [`EncodeError::Codec`] when the codec fails, and
    /// [`EncodeError::TooLarge`] when the framed response would exceed the cap.
    pub(crate) fn stage<'a>(
        &'a mut self,
        header: &'a FrameHeader,
        payload: C::Payload,
    ) -> Result<Staged<'a>, EncodeError<C::Error>> {
        const {
            assert!(
                !C::FORMAT_ID.is_empty(),
                "a codec used for responses must name a format"
            );
            assert!(
                C::FORMAT_ID.len() <= FORMAT_MAX_BYTES,
                "a codec used for responses must have a FORMAT_ID a frame can carry"
            );
        }
        if header.subsystem.is_empty() || header.subsystem.len() > SUBSYSTEM_MAX_BYTES {
            return Err(EncodeError::UnusableSubsystem {
                bytes: header.subsystem.len(),
                limit: SUBSYSTEM_MAX_BYTES,
            });
        }

        // The scratch enters every response at exactly the cap. Only a response
        // the cap will refuse can have grown it — `Codec::serialize` writes into
        // a `Vec`, so nothing can stop it growing one — and that memory is given
        // back here. Reclaiming at the top rather than on each failure arm is
        // one site instead of three, at the cost of holding an oversized buffer
        // until this encoder's next response.
        self.scratch.clear();
        self.scratch.shrink_to(self.cap.bytes());
        self.codec
            .serialize(payload, &mut self.scratch)
            .map_err(EncodeError::Codec)?;

        let bytes = frame_len(header, C::FORMAT_ID, self.scratch.len());
        if bytes > self.cap.bytes() as u64 {
            return Err(EncodeError::TooLarge {
                bytes,
                limit: self.cap.bytes(),
            });
        }
        Ok(Staged {
            header,
            format: C::FORMAT_ID,
            payload: &self.scratch,
            bytes: bytes as usize,
        })
    }

    /// The scratch's live capacity, for the tests that pin it to the cap.
    #[cfg(test)]
    pub(crate) fn scratch_capacity(&self) -> usize {
        self.scratch.capacity()
    }
}

impl Staged<'_> {
    /// The exact number of bytes [`Staged::write`] will produce.
    pub(crate) const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Writes the frame in field order. Field order is this encoder's choice,
    /// not a protobuf requirement; the decoder accepts any order.
    ///
    /// `dst` must be able to take [`Staged::bytes`] more bytes, because
    /// [`BufMut`]'s writes are infallible and panic instead of failing. Every
    /// destination this crate writes into — tonic's encode buffer, a
    /// [`BytesMut`](bytes::BytesMut) — reserves on demand, and sizing one at
    /// the frame cap keeps it from ever having to.
    pub(crate) fn write<B: BufMut>(&self, dst: &mut B) {
        write_varint_field(
            FIELD_PROTOCOL_VERSION,
            u64::from(RESPONSE_PROTOCOL_VERSION),
            dst,
        );
        write_bytes_field(FIELD_TARGET_NODE, &self.header.target.into_bytes(), dst);
        write_bytes_field(FIELD_REQUEST_ID, &self.header.request.into_bytes(), dst);
        write_bytes_field(
            FIELD_SUBSYSTEM,
            self.header.subsystem.to_str().as_bytes(),
            dst,
        );
        write_bytes_field(FIELD_FORMAT, self.format.as_bytes(), dst);
        write_varint_field(FIELD_CATEGORY, category_varint(self.header.category), dst);
        write_bytes_field(FIELD_PAYLOAD, self.payload, dst);
        if let Some(relay) = self.header.relay {
            write_bytes_field(FIELD_RELAY_NODE, &relay.into_bytes(), dst);
        }
    }
}

/// The exact length [`Staged::write`] produces, summed in `u64` so no addition
/// can overflow whatever a codec emitted.
fn frame_len(header: &FrameHeader, format: &str, payload: usize) -> u64 {
    varint_field_len(FIELD_PROTOCOL_VERSION, u64::from(RESPONSE_PROTOCOL_VERSION))
        + bytes_field_len(FIELD_TARGET_NODE, ID_BYTES)
        + bytes_field_len(FIELD_REQUEST_ID, ID_BYTES)
        + bytes_field_len(FIELD_SUBSYSTEM, header.subsystem.len())
        + bytes_field_len(FIELD_FORMAT, format.len())
        + varint_field_len(FIELD_CATEGORY, category_varint(header.category))
        + bytes_field_len(FIELD_PAYLOAD, payload)
        + if header.relay.is_some() {
            bytes_field_len(FIELD_RELAY_NODE, ID_BYTES)
        } else {
            0
        }
}

const fn varint_field_len(tag: u32, value: u64) -> u64 {
    key_len(tag) as u64 + encoded_len_varint(value) as u64
}

const fn bytes_field_len(tag: u32, len: usize) -> u64 {
    key_len(tag) as u64 + encoded_len_varint(len as u64) as u64 + len as u64
}

/// An `int32` sign-extends to 64 bits on the wire. Every category is positive,
/// so this is always the one- or two-byte form.
fn category_varint(category: ErrorCategory) -> u64 {
    i64::from(i32::from(category)) as u64
}

pub(super) fn write_varint_field<B: BufMut>(tag: u32, value: u64, dst: &mut B) {
    encode_key(tag, WireType::Varint, dst);
    encode_varint(value, dst);
}

pub(super) fn write_bytes_field<B: BufMut>(tag: u32, value: &[u8], dst: &mut B) {
    encode_key(tag, WireType::LengthDelimited, dst);
    encode_varint(value.len() as u64, dst);
    dst.put_slice(value);
}

/// Why one response could not be turned into a frame.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum EncodeError<E: Error> {
    /// The subsystem name is one no decoder would accept: a frame carries no
    /// unnamed subsystem, and none longer than the limit.
    #[error("subsystem name is {bytes} bytes, outside the 1..={limit} a frame carries")]
    UnusableSubsystem {
        /// The name's length.
        bytes: usize,
        /// The longest name a frame may carry.
        limit: usize,
    },

    /// The application's codec failed to serialize the response.
    #[error(transparent)]
    Codec(E),

    /// The complete frame would exceed the configured ceiling.
    #[error("framed response is {bytes} bytes, over the {limit}-byte cap")]
    TooLarge {
        /// The length the complete frame would have had.
        bytes: u64,
        /// The configured ceiling.
        limit: usize,
    },
}
