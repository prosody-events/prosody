use super::*;
use crate::state::cell_key::{Coordinate, Section};
use crate::state::identity::StateKey;
use color_eyre::eyre::Result;

fn collection() -> Result<CollectionId> {
    Ok(CollectionId::new(
        StateKey::new(uuid::Uuid::from_u128(1), Key::from("k")),
        StateType::Application,
        StateName::try_new("c")?,
    ))
}

/// Regression: a `lookup` after a same-collection drain must return `None`
/// promptly, never hang. `remove_collection` is the mid-handler flush path
/// (the session drains the collection's buffered cells after writing them
/// through); a subsequent `get` re-reads the dirty leg via `lookup`. The
/// lock-free `peek_with` read makes this terminate — the lock-based
/// `read_sync` spins forever here under single-threaded execution
/// (scc 3.7.0).
#[test]
fn lookup_after_remove_returns_none_without_spinning() -> Result<()> {
    let store = DirtyStore::new();
    let c = collection()?;
    let cell = CellKey {
        section: Section::new(0),
        coordinate: Coordinate::empty(),
    };
    store.set(&c, &cell, b"x");
    assert!(store.lookup(&c, &cell).is_some());
    store.remove_collection(&c);
    assert!(store.lookup(&c, &cell).is_none());
    // Re-set after the drain must be visible again (no stale tombstone).
    store.set(&c, &cell, b"y");
    assert_eq!(
        store.lookup(&c, &cell),
        Some(DirtyVal::Set(Bytes::from_static(b"y")))
    );
    Ok(())
}
