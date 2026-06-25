//! Scan coverage cannot be fabricated from outside the cache. The `coverage`
//! module is private to `crate::state::cached` and `IntervalSet::cover` is
//! `pub(in crate::state::cached)`, so no downstream crate can name `IntervalSet`
//! or `cover` to mint a "covered" interval. Coverage is born only from the
//! scan-drain, after the lower store oracle-resolved the gap (`CovBuild`) — no
//! property can prove this method's *absence*, so it is a compile-fail proof.

use prosody::state::cached::coverage::IntervalSet;

fn main() {
    // `coverage` is a private module: `IntervalSet` (and `cover`) are unnameable
    // outside `crate::state::cached`.
    let _set = IntervalSet::default();
}
