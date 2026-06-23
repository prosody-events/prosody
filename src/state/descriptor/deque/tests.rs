//! Deque section-freeze and frozen-byte goldens.
//!
//! The behavioral invariants (the dense window, crash atomicity, residue
//! skipping) are proven by the memory-backed `run_deque_trace` property in
//! [`crate::state::tests`]. These pin the durable wire contracts: the section
//! discriminants and the `META_BOUNDS` byte layout, so a change fails loudly.

use super::*;
use crate::error::ErrorCategory;
use quickcheck::{QuickCheck, TestResult};

/// Inv 7: the `Meta`/`Entries` discriminants round-trip through `i8` and every
/// other value is rejected as a `Permanent` error (never coerced to a variant).
#[test]
fn prop_deque_section_round_trip() {
    fn prop(value: i8) -> TestResult {
        match value {
            0 | 1 => TestResult::from_bool(
                DequeNs::try_from(value).is_ok_and(|ns| i8::from(ns) == value),
            ),
            _ => TestResult::from_bool(matches!(
                DequeNs::try_from(value),
                Err(error @ MetaDecodeError::UnexpectedSection(v))
                    if v == value && error.classify_error() == ErrorCategory::Permanent
            )),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(i8) -> TestResult);
}

/// The frozen discriminants and the `META_BOUNDS` cell address.
#[test]
fn deque_layout_is_frozen() {
    assert_eq!(DequeNs::Meta as i8, 0);
    assert_eq!(DequeNs::Entries as i8, 1);
    assert_eq!(i8::from(META_BOUNDS.section), 0);
    assert!(META_BOUNDS.coordinate.as_bytes().is_empty());
}

/// Frozen-bytes golden for the `head ‖ tail` bounds payload (16 bytes,
/// big-endian) — a durable wire-format contract. Covers a positive pair and a
/// sign-crossing `head` (`push_front` drives the index negative).
#[test]
fn deque_meta_bounds_bytes_are_frozen() {
    assert_eq!(
        encode_bounds(1, 258),
        [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 2]
    );
    assert_eq!(
        encode_bounds(-1, 2),
        [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 2
        ]
    );
}

/// The bounds payload round-trips, and a wrong width or a disordered pair is a
/// `Permanent` error rather than a silent misread.
#[test]
fn prop_deque_bounds_round_trip_and_reject() {
    fn prop(head: i32, delta: u16) -> TestResult {
        // `tail = head + delta` keeps the pair ordered; the `i32`/`u16` widths
        // keep `head + delta` from overflowing `i64`.
        let head = i64::from(head);
        let tail = head + i64::from(delta);
        let encoded = encode_bounds(head, tail);
        let round_trips = decode_bounds(&encoded) == Ok((head, tail));
        let bad_length = matches!(
            decode_bounds(&encoded[..15]),
            Err(MetaDecodeError::BadLength {
                expected: 16,
                actual: 15
            })
        );
        // A disordered pair (only when `delta > 0`, so `tail < head` is genuine).
        let disordered = delta == 0
            || matches!(
                decode_bounds(&encode_bounds(tail, head)),
                Err(MetaDecodeError::Disordered { .. })
            );
        TestResult::from_bool(round_trips && bad_length && disordered)
    }
    QuickCheck::new().quickcheck(prop as fn(i32, u16) -> TestResult);
}
