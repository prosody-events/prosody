//! Compile-fail proof that a cell scan cannot be unbounded.
//!
//! `ScanEdge` has only `Included`/`Excluded` variants — no `Unbounded` — so a
//! scan is always pinned to a concrete start and end coordinate and can never
//! walk past a collection's known-live extent into a tombstone field. Naming
//! the missing variant fails to compile, which *is* the enforcement.

use prosody::state::{Coordinate, Direction, Scan, ScanEdge, Section};

fn main() {
    let c = Coordinate::empty();
    let _ = Scan {
        section: Section::new(0),
        start: ScanEdge::Included(&c),
        dir: Direction::Forward,
        end: ScanEdge::Unbounded,
        limit: None,
    };
}
