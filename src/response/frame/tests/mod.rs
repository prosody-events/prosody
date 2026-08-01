use super::encode::{write_bytes_field, write_varint_field};
use super::{
    FIELD_CATEGORY, FIELD_FORMAT, FIELD_PAYLOAD, FIELD_PROTOCOL_VERSION, FIELD_RELAY_NODE,
    FIELD_REQUEST_ID, FIELD_SUBSYSTEM, FIELD_TARGET_NODE, FrameCap, FrameCapError, FrameHeader,
};
use crate::codec::Codec;
use crate::error::ErrorCategory;
use crate::response::{RequestId, Subsystem};
use crate::router::NodeId;
use bytes::BytesMut;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

mod decode;
mod encode;

/// A tag no field of this protocol uses, so a decoder must skip it.
const UNKNOWN_TAG: u32 = 99;

/// A well-formed 16-byte identifier.
const RAW_ID: [u8; 16] = [0x11; 16];

/// What a relay node costs a frame: key + length + the 16 identifier bytes.
const RELAY_FIELD_BYTES: usize = 18;

/// A codec whose payload is simply its bytes.
///
/// The call counters are what make "the payload was serialized exactly once"
/// and "a mismatched frame never reached the codec" observable; a clone shares
/// them with the encoder that owns the codec.
#[derive(Clone, Debug, Default)]
struct CountingCodec {
    serializes: Arc<AtomicUsize>,
    deserializes: Arc<AtomicUsize>,
}

/// A frame assembled field by field, so a case can omit one field or make one
/// malformed. Every field defaults to a well-formed value, and `None` leaves
/// the field out of the encoding entirely.
struct RawFrame {
    version: Option<u64>,
    target: Option<&'static [u8]>,
    request: Option<&'static [u8]>,
    subsystem: Option<&'static [u8]>,
    format: Option<&'static [u8]>,
    category: Option<u64>,
    payload: Option<&'static [u8]>,
    relay: Option<&'static [u8]>,
    /// Stands in for a field a later protocol version might add.
    unknown: Option<u64>,
}

impl Codec for CountingCodec {
    type Error = Infallible;
    type Payload = Vec<u8>;

    const FORMAT_ID: &'static str = "test-bytes";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Vec<u8>, Infallible> {
        self.deserializes.fetch_add(1, Relaxed);
        Ok(buf.to_vec())
    }

    fn serialize(&mut self, payload: Vec<u8>, buf: &mut Vec<u8>) -> Result<(), Infallible> {
        self.serializes.fetch_add(1, Relaxed);
        buf.extend_from_slice(&payload);
        Ok(())
    }
}

impl CountingCodec {
    fn serializes(&self) -> usize {
        self.serializes.load(Relaxed)
    }

    fn deserializes(&self) -> usize {
        self.deserializes.load(Relaxed)
    }
}

impl Default for RawFrame {
    fn default() -> Self {
        Self {
            version: Some(1),
            target: Some(&RAW_ID),
            request: Some(&RAW_ID),
            subsystem: Some(b"billing"),
            format: Some(CountingCodec::FORMAT_ID.as_bytes()),
            category: Some(2),
            payload: Some(b"hi"),
            relay: None,
            unknown: None,
        }
    }
}

impl RawFrame {
    fn encode(&self) -> BytesMut {
        let mut dst = BytesMut::new();
        if let Some(version) = self.version {
            write_varint_field(FIELD_PROTOCOL_VERSION, version, &mut dst);
        }
        if let Some(target) = self.target {
            write_bytes_field(FIELD_TARGET_NODE, target, &mut dst);
        }
        if let Some(request) = self.request {
            write_bytes_field(FIELD_REQUEST_ID, request, &mut dst);
        }
        if let Some(subsystem) = self.subsystem {
            write_bytes_field(FIELD_SUBSYSTEM, subsystem, &mut dst);
        }
        if let Some(format) = self.format {
            write_bytes_field(FIELD_FORMAT, format, &mut dst);
        }
        if let Some(category) = self.category {
            write_varint_field(FIELD_CATEGORY, category, &mut dst);
        }
        if let Some(payload) = self.payload {
            write_bytes_field(FIELD_PAYLOAD, payload, &mut dst);
        }
        if let Some(relay) = self.relay {
            write_bytes_field(FIELD_RELAY_NODE, relay, &mut dst);
        }
        if let Some(unknown) = self.unknown {
            write_varint_field(UNKNOWN_TAG, unknown, &mut dst);
        }
        dst
    }
}

/// A header whose fixed fields are deterministic, so a frozen-bytes assertion
/// and a boundary calculation can both name exact numbers.
fn header(subsystem: &str, category: ErrorCategory, relay: Option<NodeId>) -> FrameHeader {
    FrameHeader {
        target: NodeId::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
        request: RequestId::from_bytes([
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        ]),
        subsystem: Subsystem::make(subsystem),
        category,
        relay,
    }
}

/// An independent model of the framed length, spelled out as the wire costs a
/// reader can check by hand rather than by reusing the encoder's arithmetic.
/// It assumes the subsystem and format each fit a one-byte length varint, which
/// every value used in these tests does.
fn expected_frame_len(subsystem: &str, payload: usize, relay: bool) -> usize {
    let format = CountingCodec::FORMAT_ID.len();
    2                                                 // protocol_version: key + value
        + 18 + 18                                     // target_node, request_id: key + len + 16
        + 2 + subsystem.len()                         // subsystem: key + len + bytes
        + 2 + format                                  // format: key + len + bytes
        + 2                                           // category: key + value
        + 1 + varint_len(payload) + payload           // payload: key + len + bytes
        + if relay { RELAY_FIELD_BYTES } else { 0 }
}

/// Bytes a protobuf varint of `value` occupies.
fn varint_len(value: usize) -> usize {
    let mut len = 1;
    let mut rest = value >> 7_u32;
    while rest > 0 {
        len += 1;
        rest >>= 7_u32;
    }
    len
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
