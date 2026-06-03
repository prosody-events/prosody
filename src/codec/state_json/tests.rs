//! Property tests for the JSON state codec.

use super::{CodecId, JsonStateCodec, StateCodec};
use crate::error::{ClassifyError, ErrorCategory};
use crate::test_util::ArbJson;
use color_eyre::eyre::bail;
use quickcheck::{QuickCheck, TestResult};
use serde_json::Value;

/// Invariant: `decode ∘ encode` is the identity for every JSON-representable
/// value.
#[test]
fn prop_json_state_codec_roundtrip() {
    fn prop(value: ArbJson) -> TestResult {
        let ArbJson(value) = value;
        let encoded = match JsonStateCodec::encode(&value) {
            Ok(bytes) => bytes,
            Err(error) => return TestResult::error(error.to_string()),
        };
        match JsonStateCodec::decode::<Value>(&encoded) {
            Ok(decoded) => TestResult::from_bool(decoded == value),
            Err(error) => TestResult::error(error.to_string()),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(ArbJson) -> TestResult);
}

/// Invariant: the codec discriminator persisted in structural identity is a
/// fixed constant — drift would falsify every frozen identity row.
#[test]
fn json_state_codec_id_is_stable() {
    assert_eq!(JsonStateCodec::CODEC_ID.as_i16(), 1);
    assert_eq!(CodecId::from_i16(0), Some(CodecId::None));
    assert_eq!(CodecId::from_i16(1), Some(CodecId::Json));
    assert_eq!(CodecId::from_i16(7), None);
}

/// Invariant: undecodable cell bytes surface a Permanent error — the row is
/// skipped, never retried.
#[test]
fn decode_garbage_classifies_permanent() -> color_eyre::Result<()> {
    let Err(error) = JsonStateCodec::decode::<Value>(b"\xff not json") else {
        bail!("expected decode failure");
    };
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}
