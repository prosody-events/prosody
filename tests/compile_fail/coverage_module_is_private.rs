//! The `coverage` module is private to `crate::state::cached`, so `IntervalSet`
//! and its `pub(in crate::state::cached)` `cover` are unnameable downstream — no
//! external context can obtain an `IntervalSet` to fabricate a "covered"
//! interval. This module privacy *is* the load-bearing seal, and no property can
//! prove an unnameable method's absence, so it is a compile-fail proof. The
//! `use` below fails at the module path (`E0603`), before `cover` can be named.

use prosody::state::cached::coverage::IntervalSet;

fn main() {
    // `coverage` is a private module: `IntervalSet` (and `cover`) are unnameable
    // outside `crate::state::cached`.
    let _set = IntervalSet::default();
}
