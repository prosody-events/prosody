//! Order-preserving key codec invariants.
//!
//! Proves the codec half of the order-preserving invariant (clustering
//! byte-order == logical key order): a per-codec monotonicity differential
//! over random key pairs plus round-trip, and the frozen-bytes golden for the
//! Deque sign-flip index — a durable wire-format-freeze contract.

use super::{
    I64KeyCodec, KeyCodecError, OrderedKeyCodec, U64KeyCodec, UnitKey, Utf8KeyCodec,
    order_preserving_i64, order_preserving_i64_decode,
};
use crate::error::{ClassifyError, ErrorCategory};
use quickcheck::{QuickCheck, TestResult};

/// Asserts both halves of the order-preserving invariant for one codec over a
/// key pair: monotonicity (`a.cmp(b) == encode(a).cmp(encode(b))`) and
/// round-trip (`decode(encode(k)) == k`).
fn check_codec<C>(a: C::Key, b: C::Key) -> bool
where
    C: OrderedKeyCodec,
    C::Key: Clone,
{
    let ea = C::encode(&a);
    let eb = C::encode(&b);
    let monotone = a.cmp(&b) == ea.as_bytes().cmp(eb.as_bytes());
    let round_trips =
        C::decode(ea.as_bytes()).as_ref() == Ok(&a) && C::decode(eb.as_bytes()).as_ref() == Ok(&b);
    monotone && round_trips
}

/// `Utf8KeyCodec`: UTF-8 byte order matches `String` order; bytes round-trip.
#[test]
fn utf8_codec_is_monotone() {
    fn prop(a: String, b: String) -> TestResult {
        TestResult::from_bool(check_codec::<Utf8KeyCodec>(a, b))
    }
    QuickCheck::new().quickcheck(prop as fn(String, String) -> TestResult);
}

/// `I64KeyCodec`: the sign-flip makes memcmp match signed order; round-trips.
#[test]
fn i64_codec_is_monotone() {
    fn prop(a: i64, b: i64) -> TestResult {
        TestResult::from_bool(check_codec::<I64KeyCodec>(a, b))
    }
    QuickCheck::new().quickcheck(prop as fn(i64, i64) -> TestResult);
}

/// `U64KeyCodec`: big-endian bytes match unsigned order; round-trips.
#[test]
fn u64_codec_is_monotone() {
    fn prop(a: u64, b: u64) -> TestResult {
        TestResult::from_bool(check_codec::<U64KeyCodec>(a, b))
    }
    QuickCheck::new().quickcheck(prop as fn(u64, u64) -> TestResult);
}

/// A decode of the wrong byte width is rejected with a `BadLength` that reports
/// the actual width and classifies `Permanent`, never silently misread.
#[test]
fn fixed_width_codecs_reject_bad_length() {
    fn rejects_bad_length<C: OrderedKeyCodec>(bytes: &[u8]) -> bool {
        matches!(
            C::decode(bytes),
            Err(error @ KeyCodecError::BadLength { expected: 8, actual })
                if actual == bytes.len()
                && error.classify_error() == ErrorCategory::Permanent
        )
    }
    fn prop(len: u8) -> TestResult {
        let len = usize::from(len) % 16;
        if len == 8 {
            return TestResult::discard();
        }
        let bytes = vec![0u8; len];
        TestResult::from_bool(
            rejects_bad_length::<I64KeyCodec>(&bytes) && rejects_bad_length::<U64KeyCodec>(&bytes),
        )
    }
    QuickCheck::new().quickcheck(prop as fn(u8) -> TestResult);
}

/// A `Utf8KeyCodec` decode of non-UTF-8 bytes is rejected with an `InvalidUtf8`
/// that classifies `Permanent` — the only `KeyCodecError` arm the fixed-width
/// codecs cannot produce.
#[test]
fn utf8_codec_rejects_invalid_utf8() {
    // 0xFF is never a valid UTF-8 byte (continuation/leading-byte rules forbid it).
    let result = Utf8KeyCodec::decode(&[0xFF]);
    assert!(matches!(result, Err(KeyCodecError::InvalidUtf8(_))));
    if let Err(error) = result {
        assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    }
}

/// Frozen-bytes golden for the Deque sign-flip index: these anchors are a
/// durable wire-format contract, so a change must fail loudly here. `i64::MIN`
/// maps to all-zero, `i64::MAX` to all-ones, `0` to the midpoint, and `-1` to
/// just below it — proving memcmp == signed order across the sign boundary.
#[test]
fn deque_index_anchors_are_frozen() {
    assert_eq!(order_preserving_i64(i64::MIN), [0x00; 8]);
    assert_eq!(
        order_preserving_i64(-1),
        [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
    );
    assert_eq!(
        order_preserving_i64(0),
        [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    );
    assert_eq!(order_preserving_i64(i64::MAX), [0xFF; 8]);
}

/// Frozen-bytes goldens for the other Map key codecs — durable wire contracts
/// for the entry coordinate. `Utf8KeyCodec` is the raw UTF-8 bytes;
/// `U64KeyCodec` is plain big-endian (unsigned, so no sign flip — `0` is
/// all-zero, unlike the signed `i64` codec).
#[test]
fn map_key_coordinate_bytes_are_frozen() {
    assert_eq!(Utf8KeyCodec::encode(&"cart".to_owned()).as_bytes(), b"cart");
    assert_eq!(Utf8KeyCodec::encode(&String::new()).as_bytes(), b"");
    assert_eq!(
        U64KeyCodec::encode(&0).as_bytes(),
        &[0, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        U64KeyCodec::encode(&258).as_bytes(),
        &[0, 0, 0, 0, 0, 0, 1, 2]
    );
    assert_eq!(
        U64KeyCodec::encode(&u64::MAX).as_bytes(),
        &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
    );
}

/// The sign-flip encoding round-trips over every `i64`.
#[test]
fn deque_index_round_trips() {
    fn prop(value: i64) -> TestResult {
        TestResult::from_bool(order_preserving_i64_decode(order_preserving_i64(value)) == value)
    }
    QuickCheck::new().quickcheck(prop as fn(i64) -> TestResult);
}

/// `UnitKey` addresses exactly one cell: it encodes the empty coordinate and
/// decodes *only* the empty coordinate — non-empty bytes in a unit-addressed
/// section are a corrupt address, rejected as [`KeyCodecError::BadLength`]
/// rather than silently read as `()`.
#[test]
fn unit_key_round_trips_only_the_empty_coordinate() {
    assert_eq!(UnitKey::encode(&()).as_bytes(), b"");
    assert!(UnitKey::decode(&[]).is_ok());
    assert!(matches!(
        UnitKey::decode(&[0]),
        Err(KeyCodecError::BadLength {
            expected: 0,
            actual: 1
        })
    ));
}

/// Byte-identity law: every key codec is its own payload codec — `serialize`
/// writes exactly `encode`'s bytes and `deserialize` agrees with `decode` —
/// which is what lets a key ride as a cell payload with no
/// adapter. Held by construction today (the `Codec` impls delegate); this
/// property guards against a future impl drifting the two byte forms apart.
#[test]
fn prop_key_codec_payload_bytes_are_coordinate_bytes() {
    fn agrees<KC>(key: KC::Key) -> bool
    where
        KC: OrderedKeyCodec,
        KC::Key: Clone + PartialEq,
    {
        let mut codec = KC::default();
        let mut buf = Vec::new();
        if codec.serialize(key.clone(), &mut buf).is_err() {
            return false;
        }
        buf == KC::encode(&key).as_bytes() && codec.deserialize(&mut buf.clone()) == Ok(key)
    }
    fn prop(s: String, i: i64, u: u64) -> bool {
        agrees::<Utf8KeyCodec>(s)
            && agrees::<I64KeyCodec>(i)
            && agrees::<U64KeyCodec>(u)
            && agrees::<UnitKey>(())
    }
    QuickCheck::new().quickcheck(prop as fn(String, i64, u64) -> bool);
}
