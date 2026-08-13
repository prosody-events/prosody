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

fn json_type(input: &[u8]) -> Result<Option<String>, JsonExtractError> {
    let mut buf = input.to_vec();
    Ok(JsonExtractor::default()
        .extract(&mut buf)?
        .event_type
        .map(str::to_owned))
}

#[test]
fn json_id_finds_top_level_field_regardless_of_noise() -> Result<(), JsonExtractError> {
    assert_eq!(json_id(br#"{"id":"abc"}"#)?.as_deref(), Some("abc"));
    assert_eq!(
        json_id(b"  {\n  \"id\" : \"abc-123\"  ,\n  \"x\": 1\n}")?.as_deref(),
        Some("abc-123")
    );
    assert_eq!(
        json_id(br#"{"name":"Alice","kind":"user","id":"42"}"#)?.as_deref(),
        Some("42")
    );
    // Earlier value contains the literal characters `"id":` — must not
    // false-match.
    assert_eq!(
        json_id(br#"{"note":"\"id\": fake","id":"real"}"#)?.as_deref(),
        Some("real")
    );
    assert_eq!(
        json_id(br#"{"data":{"id":"nested"},"id":"top"}"#)?.as_deref(),
        Some("top")
    );
    assert_eq!(json_id(br#"{"data":{"id":"nested"}}"#)?, None);
    assert_eq!(
        json_id(br#"{"a":[1,2,{"id":"inner"}],"b":3.14,"c":true,"d":null,"id":"x"}"#)?.as_deref(),
        Some("x")
    );
    Ok(())
}

#[test]
fn json_id_value_with_escapes_is_decoded() -> Result<(), JsonExtractError> {
    // serde/simd_json decode escape sequences in place; we get the
    // unescaped string back.
    let input = br#"{"id":"a\"b"}"#;
    assert_eq!(json_id(input)?.as_deref(), Some("a\"b"));
    Ok(())
}

#[test]
fn json_id_null_value_yields_none() -> Result<(), JsonExtractError> {
    // `null` deserializes to Option::None — a successful extraction with
    // no id present.
    assert_eq!(json_id(br#"{"id":null}"#)?, None);
    Ok(())
}

#[test]
fn json_id_non_string_value_propagates_error() {
    // Non-null, non-string values for `id` are a parse error.
    assert!(json_id(br#"{"id":123}"#).is_err());
}

#[test]
fn json_id_non_object_propagates_error() {
    // Inputs that aren't a JSON object cannot be parsed as the metadata
    // view — the error propagates.
    assert!(json_id(b"[1,2,3]").is_err());
    assert!(json_id(b"").is_err());
    assert!(json_id(b"   ").is_err());
}

#[test]
fn json_id_via_binary_codec() -> color_eyre::Result<()> {
    let mut wire = br#"{"id":"evt-1","payload":{"x":1}}"#.to_vec();
    let original = wire.clone();
    let mut codec = JsonBinaryMessageCodec::default();
    let payload = codec.deserialize(&mut wire)?;
    assert_eq!(payload.bytes, original);
    assert_eq!(payload.event_id(), Some("evt-1"));
    Ok(())
}

#[test]
fn json_type_extracts_field() -> Result<(), JsonExtractError> {
    assert_eq!(
        json_type(br#"{"type":"user.created"}"#)?.as_deref(),
        Some("user.created")
    );
    Ok(())
}

#[test]
fn json_type_with_id_field_present() -> Result<(), JsonExtractError> {
    let input = br#"{"id":"evt-1","type":"order.placed"}"#;
    assert_eq!(json_id(input)?.as_deref(), Some("evt-1"));
    assert_eq!(json_type(input)?.as_deref(), Some("order.placed"));
    Ok(())
}

#[test]
fn json_type_missing_returns_none() -> Result<(), JsonExtractError> {
    assert_eq!(json_type(br#"{"id":"x"}"#)?, None);
    Ok(())
}

#[test]
fn json_type_via_binary_codec() -> color_eyre::Result<()> {
    let mut wire = br#"{"id":"evt-1","type":"user.created","data":{}}"#.to_vec();
    let original = wire.clone();
    let mut codec = JsonBinaryMessageCodec::default();
    let payload = codec.deserialize(&mut wire)?;
    assert_eq!(payload.bytes, original);
    assert_eq!(payload.event_id(), Some("evt-1"));
    assert_eq!(payload.event_type(), Some("user.created"));
    Ok(())
}
