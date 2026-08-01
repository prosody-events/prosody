//! Deque section-freeze and window validation.
//!
//! The behavioral invariants (the dense window, crash atomicity, residue
//! skipping) are proven by the memory-backed `run_deque_trace` property in
//! [`crate::state::tests`]; the frozen `head ‖ tail` frame bytes are pinned by
//! the pair codec's own goldens (`crate::codec`). These pin the durable section
//! discriminants and the collection-owned `head ≤ tail` window check.

use super::*;
use quickcheck::{QuickCheck, TestResult};

/// The frozen section discriminants — a durable wire contract (the sections
/// lower to `0`/`1`).
#[test]
fn deque_layout_is_frozen() {
    assert_eq!(DequeNs::Meta as i8, 0);
    assert_eq!(DequeNs::Entries as i8, 1);
    assert_eq!(i8::from(META_SECTION), 0);
    assert_eq!(i8::from(ENTRY_SECTION), 1);
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
