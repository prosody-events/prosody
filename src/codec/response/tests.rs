use super::{ResultCodec, ResultCodecError};
use crate::codec::{Codec, I64Codec, JsonCodec};
use color_eyre::Result;
use color_eyre::eyre::bail;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use serde_json::Value;

/// A composed codec over two different formats: the arms are told apart by the
/// discriminant, never by their shape.
type Composed = ResultCodec<I64Codec, JsonCodec>;

/// Failure payloads come from a fixed vocabulary, so a generated one is always
/// a value the JSON codec round-trips exactly.
fn error_payload(index: i64) -> Value {
    match index.rem_euclid(3) {
        0 => Value::Null,
        1 => Value::String("rejected".to_owned()),
        _ => Value::Bool(true),
    }
}

/// Both arms survive the composition, whichever one a result took.
#[quickcheck]
fn a_composed_result_round_trips(is_ok: bool, value: i64) -> TestResult {
    let payload = if is_ok {
        Ok(value)
    } else {
        Err(error_payload(value))
    };

    let mut buffer = Vec::new();
    let mut codec = Composed::default();
    if let Err(error) = codec.serialize(payload.clone(), &mut buffer) {
        return TestResult::error(format!("serializing failed: {error}"));
    }
    match codec.deserialize(&mut buffer) {
        Ok(decoded) => {
            assert_eq!(decoded, payload, "the result must survive the round trip");
            TestResult::passed()
        }
        Err(error) => TestResult::error(format!("deserializing failed: {error}")),
    }
}

/// The composed identity names both components, so a composed codec asserts a
/// durable identity distinct from either of theirs.
#[test]
fn the_composed_format_id_names_both_arms() {
    assert_eq!(
        Composed::FORMAT_ID,
        "result(i64-be,json)",
        "the composed identity is built from the components' ids"
    );
}

/// The discriminant leads, and the arm's own bytes follow it unchanged.
#[test]
fn each_arm_is_written_behind_its_discriminant() -> Result<()> {
    let mut codec = Composed::default();
    let mut buffer = Vec::new();

    codec.serialize(Ok(1), &mut buffer)?;
    assert_eq!(
        buffer,
        [0x00, 0, 0, 0, 0, 0, 0, 0, 1],
        "a success is the ok tag followed by the output codec's bytes"
    );

    buffer.clear();
    codec.serialize(Err(Value::Bool(true)), &mut buffer)?;
    assert_eq!(
        buffer.first(),
        Some(&0x01),
        "a failure is the error tag followed by the error codec's bytes"
    );
    Ok(())
}

/// A buffer that names no arm is refused rather than guessed at.
#[test]
fn a_buffer_naming_no_arm_is_refused() -> Result<()> {
    let mut codec = Composed::default();

    match codec.deserialize(&mut []) {
        Err(ResultCodecError::Empty) => {}
        other => bail!("an empty buffer must be refused, got {other:?}"),
    }
    match codec.deserialize(&mut [0x02, 0x00]) {
        Err(ResultCodecError::UnknownDiscriminant(0x02)) => {}
        other => bail!("an unknown discriminant must be refused, got {other:?}"),
    }
    Ok(())
}
