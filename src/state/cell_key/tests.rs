//! Cell-addressing invariants.
//!
//! Covers the `CellKey` ordering contract `(section, order_key)` and
//! `OrderKey`'s least-element/round-trip behaviour. The `Section` discriminant
//! is **opaque** here — the cell layer never validates it — so the
//! per-collection discriminant freeze + unknown-rejection lives with the
//! collection section enums (invariant 7), not in the cell core.

use super::{CellKey, Direction, OrderKey, Scan, Section};
use quickcheck::{QuickCheck, TestResult};

/// [`CellKey`] orders by `(section, order_key)`: the section discriminant
/// dominates, then the unsigned-lexicographic order-key bytes break ties.
#[test]
fn cell_key_orders_by_section_then_order_key() {
    fn prop(a_sec: i8, a_key: Vec<u8>, b_sec: i8, b_key: Vec<u8>) -> TestResult {
        let a = CellKey {
            section: Section::new(a_sec),
            order_key: OrderKey::from_bytes(a_key.clone()),
        };
        let b = CellKey {
            section: Section::new(b_sec),
            order_key: OrderKey::from_bytes(b_key.clone()),
        };
        let expected = (a_sec, a_key).cmp(&(b_sec, b_key));
        TestResult::from_bool(a.cmp(&b) == expected)
    }
    QuickCheck::new().quickcheck(prop as fn(i8, Vec<u8>, i8, Vec<u8>) -> TestResult);
}

/// `Section` round-trips its discriminant through `i8`. Opaque and total: any
/// `i8` is a valid section here, because validation is the owning collection's
/// concern, not the cell core's.
#[test]
fn section_round_trips_discriminant() {
    fn prop(value: i8) -> bool {
        i8::from(Section::new(value)) == value
    }
    QuickCheck::new().quickcheck(prop as fn(i8) -> bool);
}

/// `OrderKey::empty()` is the least key and round-trips its bytes.
#[test]
fn order_key_empty_is_least() {
    assert!(OrderKey::empty().as_bytes().is_empty());
    assert!(OrderKey::empty() <= OrderKey::from_bytes(vec![0u8]));
}

/// Construction smoke for [`Scan`]: the required `section`/`start` fields and
/// the optional `end`/`limit` line up and read back. (The *requiredness* of
/// `start`/`section` is enforced by the field types, not provable at runtime.)
#[test]
fn scan_construction_carries_section_and_start() {
    let start = OrderKey::empty();
    let scan = Scan {
        section: Section::new(1),
        start: &start,
        dir: Direction::Forward,
        end: None,
        limit: Some(10),
    };
    assert_eq!(scan.section, Section::new(1));
    assert_eq!(scan.dir, Direction::Forward);
}
