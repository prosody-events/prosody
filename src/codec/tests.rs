use super::{BinaryPayload, Codec, JsonBinaryCodec, JsonCodec, JsonPassthroughStateCodec};
use crate::test_util::ArbJson;
use quickcheck::{QuickCheck, TestResult};
use serde_json::Value;

/// The format tokens are persisted in keyed-state identity rows; changing
/// one orphans every cell written under it. Frozen by construction. The JSON
/// codecs are deliberately format-equal — that equality is what lets
/// differently-implemented consumers (and the erased state seam's passthrough
/// codec) share a collection.
#[test]
fn format_ids_are_stable() {
    assert_eq!(JsonCodec::FORMAT_ID, "json");
    assert_eq!(JsonBinaryCodec::FORMAT_ID, "json");
    assert_eq!(JsonPassthroughStateCodec::FORMAT_ID, "json");
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
/// [`JsonCodec`] writes, the erased state seam's [`JsonPassthroughStateCodec`]
/// reads back byte-for-byte (it never parses), and re-decoding those bytes
/// through `JsonCodec` reproduces the original value — for every JSON shape,
/// including `null`, scalars, arrays, and objects. This pins the cross-client
/// byte-compatibility law the shared format-id asserts.
///
/// Falsify: make [`NoopExtractor`](super::NoopExtractor) / the passthrough
/// codec drop or mutate a byte and the recovered bytes / re-decoded value
/// diverge.
#[test]
fn passthrough_state_codec_byte_compatible_with_json() {
    fn prop(ArbJson(value): ArbJson) -> TestResult {
        let bytes = json_bytes(&value);

        // JsonCodec bytes -> passthrough deserialize -> verbatim bytes.
        let mut passthrough = JsonPassthroughStateCodec::default();
        let mut scratch = bytes.clone();
        match passthrough.deserialize(&mut scratch) {
            Ok(payload) if payload.bytes == bytes => {}
            Ok(_) => return TestResult::error(format!("passthrough altered bytes for {value}")),
            Err(_) => return TestResult::error("passthrough deserialize failed"),
        }

        // passthrough serialize -> JsonCodec deserialize -> original value.
        let mut out = Vec::new();
        let mut passthrough = JsonPassthroughStateCodec::default();
        if passthrough
            .serialize(
                BinaryPayload::new(bytes, None::<String>, None::<String>),
                &mut out,
            )
            .is_err()
        {
            return TestResult::error("passthrough serialize failed");
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
