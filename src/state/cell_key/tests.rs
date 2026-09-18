//! Cell-addressing invariants.
//!
//! Proves the `CellKey` ordering contract `(section, coordinate)` and
//! `Coordinate`'s least-element/round-trip behaviour. The [`Section`]
//! discriminant is **opaque** here — the cell layer never validates it — so the
//! per-collection discriminant freeze and unknown-rejection lives with the
//! collection section enums, not in the cell core.

use super::{CellKey, Coordinate, Section};
use quickcheck::QuickCheck;

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

/// `Coordinate::empty()` is the least coordinate: it never exceeds any other
/// coordinate and is strictly less than every non-empty one, and it round-trips
/// as empty bytes.
#[test]
fn coordinate_empty_is_least() {
    fn prop(bytes: Vec<u8>) -> bool {
        let empty = Coordinate::empty();
        let was_empty = bytes.is_empty();
        let other = Coordinate::from_bytes(bytes);
        empty <= other && (was_empty || empty < other)
    }
    assert!(Coordinate::empty().as_bytes().is_empty());
    QuickCheck::new().quickcheck(prop as fn(Vec<u8>) -> bool);
}

/// A prefix range contains exactly the coordinates that start with its bytes.
#[test]
fn coordinate_prefix_range_matches_bytes() {
    fn prop(mut c: Vec<u8>, mut p: Vec<u8>, shared: bool) -> bool {
        for byte in c.iter_mut().chain(&mut p) {
            *byte = [0, 1, 254, 255][usize::from(*byte % 4)];
        }
        if shared {
            c.splice(..0, p.iter().copied());
        }
        let starts_with = c.starts_with(&p);
        let coordinate = Coordinate::from_bytes(c);
        let prefix = Coordinate::from_bytes(p);
        starts_with
            == (prefix <= coordinate && prefix.prefix_end().is_none_or(|end| coordinate < end))
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<u8>, Vec<u8>, bool) -> bool);
}
