//! Fixed-width codec composition: the derived composite id, the frozen pair
//! bytes, and the wire-form length check.

use super::*;
use quickcheck::{QuickCheck, TestResult};

/// The composed [`Codec::FORMAT_ID`] is `"(a,b)"` from the components' ids, and
/// the composed width is the sum — both derived at compile time.
#[test]
fn pair_format_id_and_width_are_derived() {
    assert_eq!(<(I64Codec, I64Codec)>::FORMAT_ID, "(i64-be,i64-be)");
    assert_eq!(<(I64Codec, I64Codec) as FixedCodec>::WIDTH, 16);
}

/// Frozen-bytes golden: `(I64Codec, I64Codec)` writes exactly `a ‖ b` as two
/// big-endian `i64`s (16 bytes) — the deque's head/tail meta frame. Covers a
/// sign-crossing first component (`push_front` drives an index negative).
#[test]
fn i64_pair_bytes_are_frozen() -> color_eyre::Result<()> {
    let mut codec = <(I64Codec, I64Codec)>::default();

    let mut buf = Vec::new();
    codec.serialize((1, 258), &mut buf)?;
    assert_eq!(buf, [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 2]);

    let mut buf = Vec::new();
    codec.serialize((-1, 2), &mut buf)?;
    assert_eq!(
        buf,
        [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 2
        ]
    );
    Ok(())
}

/// The pair round-trips any `(i64, i64)`, and a buffer that is not the combined
/// fixed width — short or long — is rejected as [`PairCodecError::Length`]
/// rather than silently misread. The long side is load-bearing: components are
/// not required to reject trailing bytes, so the pair's exact-width check is
/// the only guard against a tail-ignoring future component.
#[test]
fn prop_i64_pair_round_trip_and_length_reject() {
    fn prop(first: i64, second: i64) -> TestResult {
        let mut codec = <(I64Codec, I64Codec)>::default();
        let mut buf = Vec::new();
        if let Err(error) = codec.serialize((first, second), &mut buf) {
            return TestResult::error(format!("serialize failed: {error}"));
        }
        let round_trips = codec.deserialize(&mut buf.clone()) == Ok((first, second));
        let short = matches!(
            codec.deserialize(&mut buf[..15].to_vec()),
            Err(PairCodecError::Length {
                expected: 16,
                actual: 15
            })
        );
        let mut extended = buf.clone();
        extended.push(0);
        let long = matches!(
            codec.deserialize(&mut extended),
            Err(PairCodecError::Length {
                expected: 16,
                actual: 17
            })
        );
        TestResult::from_bool(round_trips && short && long)
    }
    QuickCheck::new().quickcheck(prop as fn(i64, i64) -> TestResult);
}
