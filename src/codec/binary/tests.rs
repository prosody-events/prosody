use std::convert::Infallible;
use std::ptr;
use std::str;

use super::*;
use bytes::BytesMut;

/// Test format marker: an opaque token for the prefix-framed test payloads.
struct PrefixFormat;

impl BinaryFormat for PrefixFormat {
    const FORMAT_ID: &'static str = "test-prefix";
}

/// Test extractor: reads the first 4 bytes as a big-endian length, then
/// returns the next `len` bytes interpreted as UTF-8 as the event id. The
/// `event_type` is left unset.
#[derive(Default)]
struct PrefixExtractor;

impl BinaryExtractor for PrefixExtractor {
    type Error = Infallible;

    fn extract<'a>(&mut self, buf: &'a mut [u8]) -> Result<BinaryMetadata<'a>, Self::Error> {
        if buf.len() < 4 {
            return Ok(BinaryMetadata::default());
        }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let Some(end) = 4_usize.checked_add(len) else {
            return Ok(BinaryMetadata::default());
        };
        if end > buf.len() {
            return Ok(BinaryMetadata::default());
        }
        Ok(BinaryMetadata {
            event_id: str::from_utf8(&buf[4..end]).ok(),
            event_type: None,
        })
    }

    fn with_cached_local<R>(f: impl FnOnce(Self) -> (Self, R)) -> R {
        f(Self).1
    }
}

fn frame(id: &[u8]) -> color_eyre::Result<Vec<u8>> {
    let len = u32::try_from(id.len())?;
    let mut out = len.to_be_bytes().to_vec();
    out.extend_from_slice(id);
    Ok(out)
}

#[test]
fn deserialize_preserves_verbatim_bytes() -> color_eyre::Result<()> {
    let original = frame(b"abc")?;
    let mut wire = original.clone();

    let mut codec = BinaryCodec::<PrefixExtractor, PrefixFormat>::default();
    let payload = codec.deserialize(&mut wire)?;

    assert_eq!(payload.bytes, original, "Vec must hold the verbatim copy");
    assert_eq!(payload.event_id(), Some("abc"));
    Ok(())
}

#[test]
fn serialize_round_trips_bytes() -> color_eyre::Result<()> {
    let payload = BinaryPayload::new(
        b"hello world".to_vec(),
        Some("hello".to_owned()),
        None::<String>,
    );
    let mut buf = Vec::new();
    let mut codec = BinaryCodec::<PrefixExtractor, PrefixFormat>::default();
    codec.serialize_ref(&payload, &mut buf)?;
    assert_eq!(buf, b"hello world");
    let mut owned = Vec::new();
    codec.serialize(payload, &mut owned)?;
    assert_eq!(owned, buf);
    Ok(())
}

#[test]
fn owned_and_borrowed_decode_preserve_the_same_bytes() -> color_eyre::Result<()> {
    let original = frame(b"abc")?;
    let mut borrowed = original.clone();
    let mut codec = BinaryCodec::<PrefixExtractor, PrefixFormat>::default();
    let borrowed_payload = codec.deserialize(&mut borrowed)?;
    let owned_payload = codec.deserialize_owned(BytesMut::from(original.as_slice()))?;
    assert_eq!(owned_payload.bytes, borrowed_payload.bytes);
    assert_eq!(owned_payload.event_id(), borrowed_payload.event_id());
    assert_eq!(owned_payload.event_type(), borrowed_payload.event_type());
    Ok(())
}

#[test]
fn serialize_swaps_into_empty_buffer_without_copy() -> color_eyre::Result<()> {
    let bytes = b"zero-copy payload".to_vec();
    let bytes_ptr = bytes.as_ptr();
    let payload = BinaryPayload::new(bytes, None::<String>, None::<String>);
    let mut buf = Vec::new();
    let mut codec = BinaryCodec::<PrefixExtractor, PrefixFormat>::default();
    codec.serialize(payload, &mut buf)?;
    assert_eq!(buf, b"zero-copy payload");
    assert!(
        ptr::eq(buf.as_ptr(), bytes_ptr),
        "empty buf must adopt the payload's allocation, not memcpy"
    );
    Ok(())
}

#[test]
fn serialize_appends_when_buffer_non_empty() -> color_eyre::Result<()> {
    let payload = BinaryPayload::new(b" world".to_vec(), None::<String>, None::<String>);
    let mut buf = b"hello".to_vec();
    let mut codec = BinaryCodec::<PrefixExtractor, PrefixFormat>::default();
    codec.serialize(payload, &mut buf)?;
    assert_eq!(buf, b"hello world");
    Ok(())
}

#[test]
fn missing_event_id_yields_none() -> color_eyre::Result<()> {
    // Buffer shorter than the length prefix — PrefixExtractor's short-circuit
    // branch returns BinaryMetadata::default().
    let mut wire = b"ab".to_vec();
    let mut codec = BinaryCodec::<PrefixExtractor, PrefixFormat>::default();
    let payload = codec.deserialize(&mut wire)?;
    assert!(payload.event_id().is_none());
    assert!(payload.event_type().is_none());
    Ok(())
}

fn json_id(input: &[u8]) -> Result<Option<String>, JsonExtractError> {
    let mut buf = input.to_vec();
    Ok(JsonExtractor::default()
        .extract(&mut buf)?
        .event_id
        .map(str::to_owned))
}

#[test]
fn json_id_rejects_invalid_json() {
    assert!(json_id(b"").is_err());
    assert!(json_id(b"   ").is_err());
}
