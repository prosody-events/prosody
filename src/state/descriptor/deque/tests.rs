//! Deque section-freeze and window validation.
//!
//! The behavioral invariants (the dense window, crash atomicity, residue
//! skipping) are proven by the memory-backed `run_deque_trace` property in
//! [`crate::state::tests`]; the frozen `head ‖ tail` frame bytes are pinned by
//! the pair codec's own goldens (`crate::codec`). These pin the durable cell
//! addresses and the collection-owned `head ≤ tail` window check.

use super::*;
use quickcheck::{QuickCheck, TestResult};

/// The frozen cell addresses (a durable contract — the bounds family lowers to
/// section `0` at the empty coordinate, the entries family to section `1`) and
/// the reset domain a `clear` covers.
///
/// The declared ids, format tokens, and section count are additionally pinned
/// by the `const` assertion beside the layout; this test pins what the two
/// cell-address helpers resolve to, which is the address every seeded-cell test
/// writes through.
#[test]
fn deque_layout_is_frozen() {
    let sections = <FrozenLayout as CollectionLayout>::SECTIONS;
    assert_eq!(
        sections.iter().map(|s| i8::from(*s)).collect::<Vec<_>>(),
        vec![0, 1],
        "a whole-layout reset covers both declared sections"
    );
    assert_eq!(i8::from(meta_cell().section), 0);
    assert!(
        meta_cell().coordinate.as_bytes().is_empty(),
        "the bounds cell is unit-addressed at the empty coordinate"
    );
    assert_eq!(
        i8::from(entry_cell_for(&I64KeyCodec::encode(&7)).section),
        1
    );
}

/// [`Window::new`] lifts an ordered `(head, tail)` pair (its length is the
/// span) and rejects a reversed pair as a `Permanent`
/// [`MetaDecodeError::Disordered`] rather than a silent misread — the
/// collection owning the *meaning* the pair codec cannot.
#[test]
fn prop_deque_window_orders_head_and_tail() {
    fn prop(head: i32, delta: u16) -> TestResult {
        // The `i32`/`u16` widths keep `head + delta` from overflowing `i64`.
        let head = i64::from(head);
        let tail = head + i64::from(delta);

        let ordered =
            matches!(Window::new(head, tail), Ok(window) if window.len() == Ok(usize::from(delta)));
        // A genuinely reversed pair (only when `delta > 0`) is rejected.
        let disordered = delta == 0
            || matches!(
                Window::new(tail, head),
                Err(error @ MetaDecodeError::Disordered { head: h, tail: t })
                    if h == tail && t == head && error.classify_error() == ErrorCategory::Permanent
            );
        TestResult::from_bool(ordered && disordered)
    }
    QuickCheck::new().quickcheck(prop as fn(i32, u16) -> TestResult);
}
