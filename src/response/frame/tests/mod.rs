use super::{
    FIELD_FORMAT, FIELD_PAYLOAD, FIELD_PROTOCOL_VERSION, FIELD_RELAY_NODE, FIELD_REQUEST_ID,
    FIELD_STATUS, FIELD_SUBSYSTEM, FIELD_TARGET_NODE, FrameCap, FrameCapError, FrameHeader,
    RELAY_FIELD_BYTES,
};
use crate::codec::Codec;
use crate::response::{RequestId, ResponseStatus};
use crate::router::NodeId;
use crate::subsystem::SubsystemName;
use bytes::BytesMut;
use color_eyre::Result;
use prost::encoding::{WireType, encode_key, encode_varint};
use std::cell::Cell;
use std::convert::Infallible;

mod decode;
mod encode;

/// A tag no field of this protocol uses, so a decoder must skip it.
const UNKNOWN_TAG: u32 = 99;

/// A well-formed 16-byte identifier.
const RAW_ID: [u8; 16] = [0x11; 16];

thread_local! {
    static CACHE_USES: Cell<usize> = const { Cell::new(0) };
    static SERIALIZE_CAPACITY: Cell<usize> = const { Cell::new(0) };
}

/// A codec whose payload is simply its bytes.
///
/// Thread-local counters observe calls through the cached codec instance.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CountingCodec;

/// A frame assembled field by field, so a case can omit one field or make one
/// malformed. Every field defaults to a well-formed value, and `None` leaves
/// the field out of the encoding entirely.
pub(crate) struct RawFrame<'a> {
    pub(crate) version: Option<u64>,
    pub(crate) target: Option<&'a [u8]>,
    pub(crate) request: Option<&'a [u8]>,
    pub(crate) subsystem: Option<&'a [u8]>,
    pub(crate) format: Option<&'a [u8]>,
    pub(crate) status: Option<u64>,
    pub(crate) payload: Option<&'a [u8]>,
    pub(crate) relay: Option<&'a [u8]>,
    /// Stands in for a field a later protocol version might add.
    pub(crate) unknown: Option<u64>,
}

impl Codec for CountingCodec {
    type Error = Infallible;
    type Payload = Vec<u8>;

    const FORMAT_ID: &'static str = "test-bytes";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Vec<u8>, Infallible> {
        Ok(buf.to_vec())
    }

    fn deserialize_owned(&mut self, buf: BytesMut) -> Result<Vec<u8>, Infallible> {
        Ok(buf.into())
    }

    fn serialize(&mut self, payload: Vec<u8>, buf: &mut Vec<u8>) -> Result<(), Infallible> {
        if buf.is_empty() {
            *buf = payload;
        } else {
            buf.extend_from_slice(&payload);
        }
        Ok(())
    }

    fn serialize_ref(&mut self, payload: &Vec<u8>, buf: &mut Vec<u8>) -> Result<(), Infallible> {
        SERIALIZE_CAPACITY.set(buf.capacity());
        buf.extend_from_slice(payload);
        Ok(())
    }

    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        CACHE_USES.set(CACHE_USES.get() + 1);
        f(&mut Self)
    }
}

impl Default for RawFrame<'_> {
    fn default() -> Self {
        Self {
            version: Some(1),
            target: Some(&RAW_ID),
            request: Some(&RAW_ID),
            subsystem: Some(b"billing"),
            format: Some(CountingCodec::FORMAT_ID.as_bytes()),
            status: Some(2),
            payload: Some(b"hi"),
            relay: None,
            unknown: None,
        }
    }
}

pub(crate) fn cache_uses_on_this_thread() -> usize {
    CACHE_USES.get()
}

pub(crate) fn serialize_capacity_on_this_thread() -> usize {
    SERIALIZE_CAPACITY.get()
}

/// Writes one protobuf field, without borrowing the encoder's writers, so a
/// fixture frame is a second opinion on the wire form rather than a mirror of
/// the code under test.
fn raw_varint_field(tag: u32, value: u64, dst: &mut BytesMut) {
    encode_key(tag, WireType::Varint, dst);
    encode_varint(value, dst);
}

fn raw_bytes_field(tag: u32, value: &[u8], dst: &mut BytesMut) {
    encode_key(tag, WireType::LengthDelimited, dst);
    encode_varint(value.len() as u64, dst);
    dst.extend_from_slice(value);
}

impl RawFrame<'_> {
    pub(crate) fn encode(&self) -> BytesMut {
        let mut dst = BytesMut::new();
        if let Some(version) = self.version {
            raw_varint_field(FIELD_PROTOCOL_VERSION, version, &mut dst);
        }
        if let Some(target) = self.target {
            raw_bytes_field(FIELD_TARGET_NODE, target, &mut dst);
        }
        if let Some(request) = self.request {
            raw_bytes_field(FIELD_REQUEST_ID, request, &mut dst);
        }
        if let Some(subsystem) = self.subsystem {
            raw_bytes_field(FIELD_SUBSYSTEM, subsystem, &mut dst);
        }
        if let Some(format) = self.format {
            raw_bytes_field(FIELD_FORMAT, format, &mut dst);
        }
        if let Some(status) = self.status {
            raw_varint_field(FIELD_STATUS, status, &mut dst);
        }
        if let Some(payload) = self.payload {
            raw_bytes_field(FIELD_PAYLOAD, payload, &mut dst);
        }
        if let Some(relay) = self.relay {
            raw_bytes_field(FIELD_RELAY_NODE, relay, &mut dst);
        }
        if let Some(unknown) = self.unknown {
            raw_varint_field(UNKNOWN_TAG, unknown, &mut dst);
        }
        dst
    }
}

/// A header whose fixed fields are deterministic, so a frozen-bytes assertion
/// and a boundary calculation can both name exact numbers.
fn header(subsystem: &str, status: ResponseStatus, relay: Option<NodeId>) -> Result<FrameHeader> {
    Ok(FrameHeader {
        target: NodeId::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
        request: RequestId::from_bytes([
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        ]),
        subsystem: SubsystemName::try_new(subsystem)?,
        status,
        relay,
    })
}

/// The ceiling is validated once, at construction, so no later code can hold an
/// unusable one.
#[test]
fn a_frame_cap_outside_the_supported_range_is_refused() {
    for bytes in [0, FrameCap::MIN_BYTES - 1, FrameCap::MAX_BYTES + 1] {
        assert!(
            matches!(FrameCap::new(bytes), Err(FrameCapError::OutOfRange { .. })),
            "a cap of {bytes} bytes must be refused"
        );
    }
    for bytes in [FrameCap::MIN_BYTES, FrameCap::MAX_BYTES] {
        assert!(
            FrameCap::new(bytes).is_ok(),
            "a cap of {bytes} bytes must be accepted"
        );
    }
}
