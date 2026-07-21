//! `CellScope` pins collection-partition containment: it is `pub` only
//! because it names a parameter of the public `CollectionSpec::handle`, but
//! its constructor is `pub(in crate::state::descriptor)` and its fields are
//! private, so downstream code can hold one only where the framework hands
//! it in and can never mint one. The expected `.stderr` is pinned
//! (regenerate with `TRYBUILD=overwrite`).

use prosody::state::descriptor::CellScope;
use prosody::state::{StateName, StateType};

fn mint(state_type: StateType, name: StateName) -> CellScope<()> {
    // `CellScope::new` is `pub(in crate::state::descriptor)` — unreachable
    // downstream.
    CellScope::new((), state_type, name)
}

fn main() {}
