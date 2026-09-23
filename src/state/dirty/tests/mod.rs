use super::*;
use crate::state::StateKey;
use crate::state::cell_key::Coordinate;
use crate::state::order_codec::{I64KeyCodec, OrderedKeyCodec};
use crate::state::tests::support::fresh_collection;
use color_eyre::eyre::Result;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use uuid::Uuid;

mod model;

/// Regression: a `lookup` after a same-collection drain must return `None`
/// promptly, never hang. `remove_collection` is the mid-handler `commit()`
/// path (the session drains the collection's buffered cells after writing
/// them through); a subsequent `get` re-reads the dirty leg via `lookup`. The
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
    assert!(store.lookup(&c, cell.as_ref()).is_some());
    store.remove_collection(&c);
    assert!(store.lookup(&c, cell.as_ref()).is_none());
    // Re-set after the drain must be visible again (no stale tombstone).
    store.set(&c, &cell, b"y");
    assert_eq!(
        store.lookup(&c, cell.as_ref()),
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

/// Regression: after `clear_section` sweeps one section, range reads over the
/// **sibling** section must still see every buffered cell.
///
/// With `clear_section` built on [`scc::TreeIndex::remove_range_sync`] (scc
/// 3.8.4), this exact insert order left the tree in a shape where a later
/// `range` seek through the strict-separator bounds ([`Edge`]) answered
/// **empty** for the surviving sibling section — `section_snapshot` and
/// `touched` both missed all seven cells while point `lookup`s still found
/// them, so `finalize` would silently stage nothing: a lost write. The fix is
/// [`remove_span`]'s snapshot-then-point-remove; this pin replays the shrunk
/// trace that exposed it (from `prop_memory_cached_overlay_view`).
#[test]
fn clear_section_keeps_sibling_section_ranges() -> Result<()> {
    let store = DirtyStore::new();
    let c = fresh_collection("entries")?;
    let cell = |section: i8, coord: u8| CellKey {
        section: Section::new(section),
        coordinate: Coordinate::from_bytes(vec![coord]),
    };
    // The exact buffered-op order that shaped the failing tree; `None` is a
    // buffered clear, `Some` a buffered set.
    let ops: &[(i8, u8, Option<u8>)] = &[
        (1, 0, None),
        (1, 7, None),
        (0, 10, Some(211)),
        (0, 4, Some(83)),
        (1, 1, Some(106)),
        (0, 11, Some(119)),
        (1, 7, Some(94)),
        (0, 7, None),
        (1, 2, Some(174)),
        (0, 5, None),
        (1, 0, None),
        (1, 5, None),
        (0, 2, None),
        (1, 9, Some(139)),
        (1, 4, None),
        (0, 3, Some(219)),
    ];
    for &(section, coord, value) in ops {
        match value {
            Some(byte) => store.set(&c, &cell(section, coord), &[byte]),
            None => store.clear(&c, &cell(section, coord)),
        }
    }

    store.clear_section(&c, Section::new(0));

    // Section 1's seven buffered cells survive the sibling sweep — for the
    // scan leg (section_snapshot) and the finalize work-list (touched) alike.
    let survivors: Vec<u8> = store
        .section_snapshot(&c, Section::new(1))
        .iter()
        .filter_map(|(key, _)| key.coordinate.as_bytes().first().copied())
        .collect();
    assert_eq!(
        survivors,
        [0, 1, 2, 4, 5, 7, 9],
        "the sibling section's snapshot lost cells after clear_section"
    );
    let touched = store.touched(&c.state_key().key);
    let [((state_type, name), cleared, cells)] = touched.as_slice() else {
        color_eyre::eyre::bail!("expected exactly one touched collection: {touched:?}");
    };
    assert_eq!((*state_type, name), (StateType::Application, c.name()));
    assert_eq!(cleared.as_slice(), [Section::new(0)]);
    let staged: Vec<u8> = cells
        .iter()
        .filter_map(|(key, _)| key.coordinate.as_bytes().first().copied())
        .collect();
    assert_eq!(
        staged,
        [0, 1, 2, 4, 5, 7, 9],
        "the finalize work-list lost the sibling section's cells: {cells:?}"
    );
    Ok(())
}

/// Regression, the marker tree's [`Edge`] fat-bound seek hazard (the analogue
/// of [`section_snapshot_returns_whole_section_beside_a_lower_section`] for
/// the cell tree): with the marker tree spanning multiple scc leaf nodes and a
/// neighbouring key populated below, `touched` must report every marker in the
/// key's sub-range, `remove_collection` (the mid-handler `commit()` drain)
/// must remove exactly its collection's markers, and `clear_event` must remove
/// exactly the key's. A fat collection bound seeks mid-span and leaves a
/// leading dirty clear marker standing after the `commit()` wrote the erasure
/// through — later same-event reads keep answering "cleared" for repopulated
/// state.
#[test]
fn marker_scopes_span_multiple_leaf_nodes() -> Result<()> {
    let store = DirtyStore::new();
    let segment = Uuid::new_v4();
    let coll = |key: &str, name: &str| -> Result<CollectionId> {
        Ok(CollectionId::new(
            StateKey::new(segment, Arc::from(key)),
            StateType::Application,
            StateName::try_new(name)?,
        ))
    };

    // 40 collections × 2 sections under "kb", plus 40 markers under the
    // neighbouring "ka" — 120 markers span many scc leaf nodes (capacity 15),
    // so a fat range bound would seek mid-span and drop leading markers.
    for i in 0..40_u8 {
        let name = format!("c{i:02}");
        store.clear_section(&coll("ka", &name)?, Section::new(0));
        store.clear_section(&coll("kb", &name)?, Section::new(0));
        store.clear_section(&coll("kb", &name)?, Section::new(1));
    }

    let kb: Key = Arc::from("kb");
    let touched = store.touched(&kb);
    assert_eq!(touched.len(), 40, "every kb collection is reported");
    assert!(
        touched
            .iter()
            .all(|(_, cleared, cells)| cleared.len() == 2 && cells.is_empty()),
        "each kb collection reports both cleared sections and no cells"
    );

    // The collection scope's strict-`Edge` bounds are load-bearing for scc
    // range *seeks*: a fat bound comparing `Equal` to a span that itself
    // straddles leaf nodes lands mid-span and skips leading markers. Probe
    // the seek contract directly through a range read over one wide span,
    // populated out of order like the cell tree's seek regression so the
    // leaf layout diverges from rank order (the drains below exercise the
    // same seek through `remove_span`'s doomed-key snapshot, but a read
    // asserts the yielded set explicitly).
    let wide = coll("kc", "wide")?;
    for step in 0..60_i8 {
        store.clear_section(&wide, Section::new(step));
        store.clear_section(&wide, Section::new(-step - 1));
    }
    // `cleared_sections` IS the `MarkerCollectionScope` range walk (the
    // `commit()` drain's clear half), so probing through it pins both the
    // scope's seek contract and the accessor.
    let sections: Vec<i8> = store
        .cleared_sections(&wide)
        .into_iter()
        .map(i8::from)
        .collect();
    let expected: Vec<i8> = (-60..60).collect();
    assert_eq!(
        sections, expected,
        "the collection scope must span exactly its own markers"
    );
    // The key scope walks the same straddling span, so a fat key bound also
    // lands mid-span here — deterministically, unlike the narrow kb spans.
    let kc: Key = Arc::from("kc");
    let kc_touched = store.touched(&kc);
    assert!(
        kc_touched.len() == 1 && kc_touched[0].1.len() == 120,
        "the key scope must span every kc marker"
    );

    // Drain every even-numbered kb collection so the drains land throughout
    // the multi-leaf layout, and `touched` proves each drain removed exactly
    // its collection's markers.
    for i in (0..40_u8).step_by(2) {
        store.remove_collection(&coll("kb", &format!("c{i:02}"))?);
    }
    let survivors = store.touched(&kb);
    let names: Vec<&str> = survivors
        .iter()
        .map(|((_, name), ..)| name.as_str())
        .collect();
    let expected: Vec<String> = (1..40_u8).step_by(2).map(|i| format!("c{i:02}")).collect();
    assert_eq!(
        names, expected,
        "exactly the drained collections' markers vanish"
    );
    assert!(
        survivors
            .iter()
            .all(|(_, cleared, cells)| cleared.len() == 2 && cells.is_empty()),
        "each surviving kb collection keeps both cleared sections"
    );

    store.clear_event(&kb);
    assert!(store.touched(&kb).is_empty(), "kb's markers are swept");
    let ka: Key = Arc::from("ka");
    assert_eq!(
        store.touched(&ka).len(),
        40,
        "the neighbouring key's markers are untouched"
    );
    Ok(())
}
