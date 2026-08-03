use super::{
    FIELD_FORMAT, FIELD_PAYLOAD, FIELD_PROTOCOL_VERSION, FIELD_RELAY_NODE, FIELD_REQUEST_ID,
    FIELD_STATUS, FIELD_SUBSYSTEM, FIELD_TARGET_NODE, FrameCap, FrameCapError, FrameHeader,
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

thread_local! {
    /// Payloads serialized on this thread, by every [`CountingCodec`] on it.
    ///
    /// A delivery worker builds its own codec through `Default`. A suite that
    /// drives delivery therefore holds no handle on the instance that encodes,
    /// and cannot read that instance's own counter. Those suites run one
    /// current-thread runtime, so the worker encodes on the thread that drives
    /// it and this total includes what the worker serialized. Read it through
    /// [`serialized_on_this_thread`] as a difference, never as an absolute:
    /// every other codec on the same thread counts here too.
    static SERIALIZED_HERE: Cell<usize> = const { Cell::new(0) };
}

/// A codec whose payload is simply its bytes.
///
/// The call counters are what make "the payload was serialized exactly once"
/// and "a mismatched frame never reached the codec" observable; a clone shares
/// them with the encoder that owns the codec.
///
/// `moves` picks between the two shapes [`Codec::serialize`] sanctions: append
/// into the caller's buffer, or — when that buffer is empty — hand the
/// payload's own buffer over instead, as [`BinaryCodec`](crate::BinaryCodec)
/// does.
#[derive(Clone, Debug, Default)]
pub(crate) struct CountingCodec {
    serializes: Arc<AtomicUsize>,
    deserializes: Arc<AtomicUsize>,
    moves: bool,
}

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
        self.deserializes.fetch_add(1, Relaxed);
        Ok(buf.to_vec())
    }

    fn serialize(&mut self, payload: Vec<u8>, buf: &mut Vec<u8>) -> Result<(), Infallible> {
        self.serializes.fetch_add(1, Relaxed);
        SERIALIZED_HERE.set(SERIALIZED_HERE.get() + 1);
        if self.moves && buf.is_empty() {
            *buf = payload;
        } else {
            buf.extend_from_slice(&payload);
        }
        Ok(())
    }
}

impl CountingCodec {
    /// The move-into-an-empty-buffer shape.
    fn moving() -> Self {
        Self {
            moves: true,
            ..Self::default()
        }
    }

    /// The scratch capacity the encoder is left holding once this codec has
    /// serialized: the payload's own buffer when it moved one in, otherwise the
    /// buffer the encoder built.
    fn expected_scratch(&self, handed: usize, built: usize) -> usize {
        if self.moves { handed } else { built }
    }

    fn serializes(&self) -> usize {
        self.serializes.load(Relaxed)
    }

    fn deserializes(&self) -> usize {
        self.deserializes.load(Relaxed)
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

/// How many payloads [`CountingCodec`] has serialized on this thread.
pub(crate) fn serialized_on_this_thread() -> usize {
    SERIALIZED_HERE.get()
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
        + 2                                           // status: key + value
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
