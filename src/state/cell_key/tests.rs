//! Cell-addressing invariants.
//!
//! Covers the `CellKey` ordering contract `(section, coordinate)` and
//! `Coordinate`'s least-element/round-trip behaviour. The [`Section`]
//! discriminant is **opaque** here — the cell layer never validates it — so the
//! per-collection discriminant freeze and unknown-rejection lives with the
//! collection section enums, not in the cell core.

use super::{CellKey, Coordinate, Direction, Scan, Section};
use quickcheck::QuickCheck;
use std::ops::Bound;

/// [`CellKey`] orders by `(section, coordinate)`: the section discriminant
/// dominates, then the unsigned-lexicographic coordinate bytes break ties.
#[test]
fn cell_key_orders_by_section_then_coordinate() {
    fn prop(a_sec: i8, a_key: Vec<u8>, b_sec: i8, b_key: Vec<u8>) -> bool {
        let a = CellKey {
            section: Section::new(a_sec),
            coordinate: Coordinate::from_bytes(a_key.clone()),
        };
        let b = CellKey {
            section: Section::new(b_sec),
            coordinate: Coordinate::from_bytes(b_key.clone()),
        };
        let expected = (a_sec, a_key).cmp(&(b_sec, b_key));
        a.cmp(&b) == expected
    }
    QuickCheck::new().quickcheck(prop as fn(i8, Vec<u8>, i8, Vec<u8>) -> bool);
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

/// `Coordinate::empty()` is the least coordinate and round-trips its bytes.
#[test]
fn coordinate_empty_is_least() {
    assert!(Coordinate::empty().as_bytes().is_empty());
    assert!(Coordinate::empty() <= Coordinate::from_bytes(vec![0u8]));
}

/// Construction smoke for [`Scan`]: the required `section` field and the
/// direction-relative `start`/`end` bounds line up and read back.
#[test]
fn scan_construction_carries_section_and_start() {
    let start = Coordinate::empty();
    let scan = Scan {
        section: Section::new(1),
        start: Bound::Included(&start),
        dir: Direction::Forward,
        end: Bound::Unbounded,
        limit: Some(10),
    };
    assert_eq!(scan.section, Section::new(1));
    assert_eq!(scan.dir, Direction::Forward);
    assert_eq!(scan.start, Bound::Included(&start));
    assert_eq!(scan.end, Bound::Unbounded);
    assert_eq!(scan.limit, Some(10));
}
