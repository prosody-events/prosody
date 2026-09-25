use super::{
    BinaryPayload, Codec, JsonBinaryCodec, JsonBinaryMessageCodec, JsonCodec, owned_bytes,
};
use crate::test_util::ArbJson;
use crate::{EventIdentity, EventType};
use bytes::{Bytes, BytesMut};
use quickcheck::{QuickCheck, TestResult};
use serde_json::Value;

/// The format tokens are persisted in keyed-state identity rows; changing
/// one orphans every cell written under it. Frozen by construction. The JSON
/// codecs are deliberately format-equal — that equality is what lets
/// differently-implemented consumers (and the erased state seam's binary
/// codec) share a collection.
#[test]
fn format_ids_are_stable() {
    assert_eq!(JsonCodec::FORMAT_ID, "json");
    assert_eq!(JsonBinaryCodec::FORMAT_ID, "json");
    assert_eq!(JsonBinaryMessageCodec::FORMAT_ID, "json");
}

/// Serializes `value` through a fresh [`JsonCodec`].
fn json_bytes(value: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut codec = JsonCodec::default();
    // The float-free `ArbJson` domain always encodes.
    assert!(
        codec.serialize(value.clone(), &mut buf).is_ok(),
        "JsonCodec must encode a float-free value"
    );
    buf
}

/// The `"json"` format-id promises mutually decodable bytes: whatever
/// [`JsonCodec`] writes, [`JsonBinaryCodec`]
/// reads back byte-for-byte (it never parses), and re-decoding those bytes
/// through `JsonCodec` reproduces the original value — for every JSON shape,
/// including `null`, scalars, arrays, and objects. This pins the cross-client
/// byte-compatibility law the shared format-id asserts.
///
/// Every serializer writes the same bytes, and owned binary serialization
/// returns the payload's own allocation.
///
/// Falsify: make [`NoopExtractor`](super::NoopExtractor) / the binary
/// codec drop or mutate a byte and the recovered bytes / re-decoded value
/// diverge.
#[test]
fn binary_json_codec_is_byte_compatible_with_json() {
    fn prop(ArbJson(value): ArbJson) -> TestResult {
        let bytes = json_bytes(&value);
        let mut borrowed_bytes = Vec::new();
        let mut json = JsonCodec::default();
        if json.serialize_ref(&value, &mut borrowed_bytes).is_err() || borrowed_bytes != bytes {
            return TestResult::error("JSON serializers wrote different bytes");
        }
        match json.serialize_bytes(value.clone()) {
            Ok(owned) if owned == bytes => {}
            Ok(_) => return TestResult::error("JSON serialize_bytes wrote different bytes"),
            Err(_) => return TestResult::error("JSON serialize_bytes failed"),
        }
        let mut mutable_bytes = bytes.clone();
        let borrowed_decode = json.deserialize(&mut mutable_bytes);
        let owned_decode = json.deserialize_owned(BytesMut::from(bytes.as_slice()));
        match (borrowed_decode, owned_decode) {
            (Ok(borrowed), Ok(owned)) if borrowed == value && owned == value => {}
            (Ok(_), Ok(_)) => return TestResult::error("JSON decoders produced different values"),
            _ => return TestResult::error("JSON decoding failed"),
        }

        // JsonCodec bytes -> binary deserialize -> verbatim bytes.
        let mut binary = JsonBinaryCodec::default();
        let mut scratch = bytes.clone();
        match binary.deserialize(&mut scratch) {
            Ok(payload) if payload.bytes == bytes => {}
            Ok(_) => return TestResult::error(format!("binary codec altered bytes for {value}")),
            Err(_) => return TestResult::error("binary codec deserialize failed"),
        }

        // The message codec accepts the same JSON domain. It extracts string
        // metadata from objects and leaves all other JSON values untagged.
        let mut message = JsonBinaryMessageCodec::default();
        let mut scratch = bytes.clone();
        match message.deserialize(&mut scratch) {
            Ok(payload)
                if payload.bytes == bytes
                    && payload.event_id() == value.get("id").and_then(Value::as_str)
                    && payload.event_type() == value.get("type").and_then(Value::as_str) => {}
            Ok(_) => return TestResult::error(format!("message codec altered {value}")),
            Err(_) => return TestResult::error(format!("message codec rejected {value}")),
        }

        // Every JSON shape is also valid in a metadata field. Only strings
        // become metadata.
        let tagged = serde_json::json!({ "id": value, "type": value });
        let tagged_bytes = json_bytes(&tagged);
        let mut scratch = tagged_bytes.clone();
        match message.deserialize(&mut scratch) {
            Ok(payload)
                if payload.bytes == tagged_bytes
                    && payload.event_id() == value.as_str()
                    && payload.event_type() == value.as_str() => {}
            Ok(_) => return TestResult::error(format!("message codec altered tagged {value}")),
            Err(_) => return TestResult::error(format!("message codec rejected tagged {value}")),
        }

        // Owned binary serialization returns the payload's own allocation.
        let mut binary = JsonBinaryCodec::default();
        let owned = bytes.clone();
        let origin = owned.as_ptr();
        match binary.serialize_bytes(BinaryPayload::new(owned, None::<String>, None::<String>)) {
            Ok(encoded) if encoded == bytes && encoded.as_ptr() == origin => {}
            Ok(_) => return TestResult::error("binary serialize_bytes copied or altered bytes"),
            Err(_) => return TestResult::error("binary serialize_bytes failed"),
        }

        // Binary serialize -> JsonCodec deserialize -> original value.
        let mut out = Vec::new();
        if binary
            .serialize(
                BinaryPayload::new(bytes, None::<String>, None::<String>),
                &mut out,
            )
            .is_err()
        {
            return TestResult::error("binary codec serialize failed");
        }
        let mut json = JsonCodec::default();
        match json.deserialize(&mut out) {
            Ok(decoded) if decoded == value => TestResult::passed(),
            Ok(decoded) => TestResult::error(format!("re-decoded {decoded} != {value}")),
            Err(_) => TestResult::error(format!("JsonCodec could not re-decode {value}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(ArbJson) -> TestResult);
}

/// [`owned_bytes`] keeps the input bytes and drops all spare capacity. A
/// vector with no spare capacity keeps its allocation.
///
/// Falsify: convert through `Bytes::from(Vec)` and the spare capacity stays.
#[test]
fn owned_bytes_keeps_no_spare_capacity() {
    fn prop(input: Vec<u8>, spare: u8) -> TestResult {
        let input = Bytes::from(input);
        let mut encoding = Vec::with_capacity(input.len() + usize::from(spare));
        encoding.extend_from_slice(&input);
        let origin = encoding.as_ptr();
        let bytes = owned_bytes(encoding);
        if bytes != input {
            return TestResult::error("owned_bytes altered bytes");
        }
        if input.is_empty() {
            return TestResult::passed();
        }
        if spare == 0 && bytes.as_ptr() != origin {
            return TestResult::error("owned_bytes copied a vector with no spare capacity");
        }
        match bytes.try_into_mut() {
            Ok(unique) if unique.capacity() == input.len() => TestResult::passed(),
            Ok(_) => TestResult::error("owned_bytes kept spare capacity"),
            Err(_) => TestResult::error("owned_bytes shared its allocation"),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<u8>, u8) -> TestResult);
}
