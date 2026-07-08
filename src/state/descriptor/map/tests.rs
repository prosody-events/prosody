//! Map section-freeze and frozen-byte goldens.
//!
//! The behavioral invariants (key ordering, the loose-superset bounds, the
//! missing-bound fallback, crash atomicity) are proven by the memory-backed
//! `run_map_trace` property and the missing-bound directed test in
//! [`crate::state::tests`]. These pin the durable wire contracts: the section
//! discriminants and the `Meta` cell addresses.

use super::*;
use quickcheck::{QuickCheck, TestResult};

/// Inv 7: the `Meta`/`Entries` discriminants round-trip through `i8` and every
/// other value is rejected (never coerced to a variant).
#[test]
fn prop_map_section_round_trip() {
    fn prop(value: i8) -> TestResult {
        match value {
            0 | 1 => {
                TestResult::from_bool(MapNs::try_from(value).is_ok_and(|ns| i8::from(ns) == value))
            }
            _ => TestResult::from_bool(matches!(
                MapNs::try_from(value),
                Err(UnknownMapSection(v)) if v == value
            )),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(i8) -> TestResult);
}

/// The frozen discriminants and the two distinct `Meta` cell addresses (a
/// durable contract — both live in the `Meta` section at fixed coordinates).
#[test]
fn map_layout_is_frozen() {
    assert_eq!(MapNs::Meta as i8, 0);
    assert_eq!(MapNs::Entries as i8, 1);

    let min = meta_min_cell();
    let max = meta_max_cell();
    assert_eq!(i8::from(min.section), 0);
    assert_eq!(i8::from(max.section), 0);
    assert_eq!(min.coordinate.as_bytes(), &[0]);
    assert_eq!(max.coordinate.as_bytes(), &[1]);
    // The two bounds must address distinct cells.
    assert_ne!(min.coordinate, max.coordinate);
}
