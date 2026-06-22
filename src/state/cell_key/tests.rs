//! Cell-addressing invariants.
//!
//! Covers the namespace wire-stability half of invariant 7 (`From`/`TryFrom`
//! round-trip, frozen discriminants, unknown-discriminator classification) and
//! the `CellKey` ordering contract `(namespace, order_key)`.

use super::{CellKey, Direction, Namespace, OrderKey, Scan, UnknownNamespace};
use crate::error::{ClassifyError, ErrorCategory};
use quickcheck::{QuickCheck, TestResult};

/// Every [`Namespace`] variant, for exhaustive round-trip coverage.
const ALL_NAMESPACES: [Namespace; 2] = [Namespace::Meta, Namespace::Entries];

fn namespace_from_parity(value: u8) -> Namespace {
    if value.is_multiple_of(2) {
        Namespace::Meta
    } else {
        Namespace::Entries
    }
}

/// Wire-format freeze: the durable `namespace` discriminants are a contract, so
/// a change must fail loudly here, not silently brick existing cells.
#[test]
fn namespace_discriminants_are_frozen() {
    assert_eq!(i8::from(Namespace::Meta), 0);
    assert_eq!(i8::from(Namespace::Entries), 1);
}

/// `From`/`TryFrom<i8>` round-trips over every variant.
#[test]
fn namespace_round_trips_every_variant() {
    for namespace in ALL_NAMESPACES {
        assert_eq!(Namespace::try_from(i8::from(namespace)), Ok(namespace));
    }
}

/// Any `i8` outside the closed set decodes to [`UnknownNamespace`] carrying the
/// offending value and classifying `Permanent`.
#[test]
fn namespace_rejects_unknown_discriminant() {
    fn prop(value: i8) -> TestResult {
        match Namespace::try_from(value) {
            Ok(_) => TestResult::from_bool(value == 0 || value == 1),
            Err(error) => TestResult::from_bool(
                value != 0
                    && value != 1
                    && error == UnknownNamespace(value)
                    && error.classify_error() == ErrorCategory::Permanent,
            ),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(i8) -> TestResult);
}

/// [`CellKey`] orders by `(namespace, order_key)`: namespace dominates, then
/// the unsigned-lexicographic order-key bytes break ties.
#[test]
fn cell_key_orders_by_namespace_then_order_key() {
    fn prop(a_ns: u8, a_key: Vec<u8>, b_ns: u8, b_key: Vec<u8>) -> TestResult {
        let a = CellKey {
            namespace: namespace_from_parity(a_ns),
            order_key: OrderKey::from_bytes(a_key.clone()),
        };
        let b = CellKey {
            namespace: namespace_from_parity(b_ns),
            order_key: OrderKey::from_bytes(b_key.clone()),
        };
        let expected = (i8::from(a.namespace), a_key).cmp(&(i8::from(b.namespace), b_key));
        TestResult::from_bool(a.cmp(&b) == expected)
    }
    QuickCheck::new().quickcheck(prop as fn(u8, Vec<u8>, u8, Vec<u8>) -> TestResult);
}

/// `OrderKey::empty()` is the least key and round-trips its bytes.
#[test]
fn order_key_empty_is_least() {
    assert!(OrderKey::empty().as_bytes().is_empty());
    assert!(OrderKey::empty() <= OrderKey::from_bytes(vec![0u8]));
}

/// Construction smoke for [`Scan`]: the required `namespace`/`start` fields and
/// the optional `end`/`limit` line up and read back. (The *requiredness* of
/// `start`/`namespace` is enforced by the field types, not provable at
/// runtime.)
#[test]
fn scan_construction_carries_namespace_and_start() {
    let start = OrderKey::empty();
    let scan = Scan {
        namespace: Namespace::Entries,
        start: &start,
        dir: Direction::Forward,
        end: None,
        limit: Some(10),
    };
    assert_eq!(scan.namespace, Namespace::Entries);
    assert_eq!(scan.dir, Direction::Forward);
}
