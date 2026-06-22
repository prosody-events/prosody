//! Order-preserving key codec invariants.
//!
//! Covers the codec half of invariant 3 (clustering byte-order == logical key
//! order): a per-codec monotonicity differential over random key pairs plus
//! round-trip, and the frozen-bytes golden for the Deque sign-flip index — a
//! durable wire-format-freeze contract.

use super::{
    I64KeyCodec, OrderedKeyCodec, U64KeyCodec, Utf8KeyCodec, order_preserving_i64,
    order_preserving_i64_decode,
};
use quickcheck::{QuickCheck, TestResult};

/// Asserts the two halves of invariant 3 for one codec over a key pair:
/// monotonicity (`a.cmp(b) == encode(a).cmp(encode(b))`) and round-trip
/// (`decode(encode(k)) == k`).
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

/// A decode of the wrong byte width is rejected (a `Permanent` `BadLength`),
/// never silently misread.
#[test]
fn fixed_width_codecs_reject_bad_length() {
    fn prop(len: u8) -> TestResult {
        let len = usize::from(len) % 16;
        if len == 8 {
            return TestResult::discard();
        }
        let bytes = vec![0u8; len];
        TestResult::from_bool(
            I64KeyCodec::decode(&bytes).is_err() && U64KeyCodec::decode(&bytes).is_err(),
        )
    }
    QuickCheck::new().quickcheck(prop as fn(u8) -> TestResult);
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

/// The sign-flip encoding round-trips over every `i64`.
#[test]
fn deque_index_round_trips() {
    fn prop(value: i64) -> TestResult {
        TestResult::from_bool(order_preserving_i64_decode(order_preserving_i64(value)) == value)
    }
    QuickCheck::new().quickcheck(prop as fn(i64) -> TestResult);
}
