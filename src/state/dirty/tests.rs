use super::*;
use crate::state::cell_key::{Coordinate, Section};
use crate::state::order_codec::{I64KeyCodec, OrderedKeyCodec};
use crate::state::tests::support::fresh_collection;
use color_eyre::eyre::Result;

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
    let c = fresh_collection("c")?;
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

/// Regression: a section snapshot must return **every** cell in that section,
/// including the ones at the front of the coordinate order, even when a cell in
/// a lower section is present.
///
/// The scan bounds the dirty tree by a `(key, state_type, name, section)`
/// prefix. A bound that compares `Equal` to the whole section span lets scc's
/// range start-seek land in the *middle* of the span whenever a neighbouring
/// section shifts the tree's leaf layout, silently dropping the section's
/// leading cells. A deque triggers exactly this: its head/tail meta cell sits
/// in a lower section than its entries, so streaming a deque within the same
/// event that mutated it dropped the front of the queue. The strict-separator
/// bounds (see [`Edge`]) keep the seek at the span's true edge.
#[test]
fn section_snapshot_returns_whole_section_beside_a_lower_section() -> Result<()> {
    let store = DirtyStore::new();
    let c = fresh_collection("c")?;

    // A single meta cell in the lower section, like a deque's head/tail bounds.
    store.set(
        &c,
        &CellKey {
            section: Section::new(0),
            coordinate: Coordinate::empty(),
        },
        b"meta",
    );

    // Entry cells in the upper section spanning the i64 sign boundary, inserted
    // out of order (a deque's interleaved push-front/push-back). The sign-flipped
    // encoding keeps byte order equal to signed order, so the negatives sort
    // first — exactly the cells the buggy seek dropped. The population spans
    // many scc leaf nodes (capacity 15), so a mid-span seek lands well inside
    // the section and drops a prefix — the fat-bound regression fails loudly.
    let mut indices: Vec<i64> = Vec::new();
    for step in 0..60_i64 {
        indices.push(step);
        indices.push(-step - 1);
    }
    for &index in &indices {
        store.set(
            &c,
            &CellKey {
                section: Section::new(1),
                coordinate: I64KeyCodec::encode(&index),
            },
            b"e",
        );
    }

    let snapshot = store.section_snapshot(&c, Section::new(1));
    let got = snapshot
        .iter()
        .map(|(cell, _)| I64KeyCodec::decode(cell.coordinate.as_bytes()))
        .collect::<Result<Vec<i64>, _>>()?;
    let mut expected = indices.clone();
    expected.sort_unstable();
    assert_eq!(
        got, expected,
        "section snapshot dropped or misordered cells"
    );
    Ok(())
}
