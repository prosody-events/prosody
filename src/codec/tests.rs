use super::{BinaryPayload, Codec, JsonBinaryCodec, JsonBinaryMessageCodec, JsonCodec};
use crate::test_util::ArbJson;
use bytes::BytesMut;
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

        // Binary serialize -> JsonCodec deserialize -> original value.
        let mut out = Vec::new();
        let mut binary = JsonBinaryCodec::default();
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
