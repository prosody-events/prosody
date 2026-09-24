//! The dirty store tracks a model over generated operation traces.

use super::*;

/// The bounded op pools the dirty-store trace ranges over: 2 keys × 2
/// collections × 2 sections × 3 coordinates, small enough that ops collide.
const OP_KEYS: u8 = 2;

const OP_COLLS: u8 = 2;

const OP_SECTIONS: i8 = 2;

const OP_COORDS: u8 = 3;

/// One dirty-store op over the bounded pools.
#[derive(Clone, Copy, Debug)]
enum DirtyOp {
    Set(u8, u8, i8, u8, u8),
    Clear(u8, u8, i8, u8),
    ClearSection(u8, u8, i8),
    RemoveCollection(u8, u8),
    ClearEvent(u8),
}

impl Arbitrary for DirtyOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let key = u8::arbitrary(g) % OP_KEYS;
        let coll = u8::arbitrary(g) % OP_COLLS;
        let section = i8::arbitrary(g).rem_euclid(OP_SECTIONS);
        let coord = u8::arbitrary(g) % OP_COORDS;
        match u8::arbitrary(g) % 7 {
            0 | 1 => Self::Set(key, coll, section, coord, u8::arbitrary(g)),
            2 => Self::Clear(key, coll, section, coord),
            3 | 4 => Self::ClearSection(key, coll, section),
            5 => Self::RemoveCollection(key, coll),
            _ => Self::ClearEvent(key),
        }
    }
}

/// A shrinkable dirty-store trace.
#[derive(Clone, Debug)]
struct DirtyTrace(Vec<DirtyOp>);

impl Arbitrary for DirtyTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(Vec::<DirtyOp>::arbitrary(g).into_iter().take(40).collect())
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(Self))
    }
}

/// The plain dirty-store model: cells and markers keyed by pool indices.
#[derive(Default)]
struct DirtyModel {
    cells: BTreeMap<(u8, u8, i8, u8), DirtyVal>,
    markers: BTreeSet<(u8, u8, i8)>,
}

/// One canonicalized `touched` shape for comparison: collection index →
/// (cleared sections, cells with outcomes).
type Touched = BTreeMap<u8, (BTreeSet<i8>, Vec<(u8, DirtyVal)>)>;

/// The pool cell at `(section, coord)` — single-byte coordinates, so byte
/// order is numeric order.
fn pool_cell(section: i8, coord: u8) -> CellKey {
    CellKey {
        section: Section::new(section),
        coordinate: Coordinate::from_bytes(vec![coord]),
    }
}

/// Asserts every read surface over the whole pool: `section_cleared`,
/// `section_snapshot`, and `lookup` against the model, then `touched` per key
/// (collected into [`Touched`]; a collection appearing twice violates
/// `touched`'s grouping contract and fails outright — duplicate entries are
/// never merged). Returns `false` on the first divergence.
fn dirty_matches(
    store: &DirtyStore,
    ids: &[Vec<CollectionId>],
    model: &DirtyModel,
) -> Result<bool> {
    for k in 0..OP_KEYS {
        for c in 0..OP_COLLS {
            let id = &ids[k as usize][c as usize];
            // The `commit()` drain's clear half: exactly the model's markers.
            let cleared: BTreeSet<i8> = store
                .cleared_sections(id)
                .into_iter()
                .map(i8::from)
                .collect();
            let expected_cleared: BTreeSet<i8> = model
                .markers
                .iter()
                .filter(|&&(k2, c2, _)| k2 == k && c2 == c)
                .map(|&(.., s)| s)
                .collect();
            if cleared != expected_cleared {
                return Ok(false);
            }
            // `collection_dirty` is the rollback Applied/NoOp probe: it must
            // agree with the model's emptiness for the collection.
            let model_dirty = !expected_cleared.is_empty()
                || model.cells.keys().any(|&(k2, c2, ..)| k2 == k && c2 == c);
            if store.collection_dirty(id) != model_dirty {
                return Ok(false);
            }
            for s in 0..OP_SECTIONS {
                if store.section_cleared(id, Section::new(s)) != model.markers.contains(&(k, c, s))
                {
                    return Ok(false);
                }
                let snapshot: Vec<(u8, DirtyVal)> = store
                    .section_snapshot(id, Section::new(s))
                    .into_iter()
                    .map(|(cell, val)| (cell.coordinate.as_bytes()[0], val))
                    .collect();
                let expected: Vec<(u8, DirtyVal)> = model
                    .cells
                    .iter()
                    .filter(|&(&(k2, c2, s2, _), _)| k2 == k && c2 == c && s2 == s)
                    .map(|(&(.., x), val)| (x, val.clone()))
                    .collect();
                if snapshot != expected {
                    return Ok(false);
                }
                for x in 0..OP_COORDS {
                    if store.lookup(id, pool_cell(s, x).as_ref())
                        != model.cells.get(&(k, c, s, x)).cloned()
                    {
                        return Ok(false);
                    }
                }
            }
        }
    }

    for k in 0..OP_KEYS {
        let mut got: Touched = BTreeMap::new();
        for ((state_type, name), cleared, cells) in
            store.touched(&ids[k as usize][0].state_key().key)
        {
            if state_type != StateType::Application {
                return Ok(false);
            }
            let c: u8 = name.as_str().trim_start_matches('c').parse()?;
            let cleared = cleared.into_iter().map(i8::from).collect();
            let cells = cells
                .into_iter()
                .map(|(cell, val)| (cell.coordinate.as_bytes()[0], val))
                .collect();
            // `touched` groups by collection — one entry carrying the
            // collection's marker and cells together. A duplicate key is a
            // split entry: a grouping-contract violation, never merged away.
            if got.insert(c, (cleared, cells)).is_some() {
                return Ok(false);
            }
        }
        let mut expected: Touched = BTreeMap::new();
        for (k2, c, s) in &model.markers {
            if *k2 == k {
                expected.entry(*c).or_default().0.insert(*s);
            }
        }
        for ((k2, c, _, x), val) in &model.cells {
            if *k2 == k {
                expected.entry(*c).or_default().1.push((*x, val.clone()));
            }
        }
        if got != expected {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Drives a random op trace over both trees against a plain
/// `BTreeMap`/`BTreeSet` model, asserting every read surface after every op
/// ([`dirty_matches`]) — marker/set/clear interleavings, `clear_section`
/// wiping exactly its section's cells, and `remove_collection`/`clear_event`
/// sweeping exactly their scope in **both** trees.
fn run_dirty_trace(DirtyTrace(ops): DirtyTrace) -> Result<bool> {
    let segment = Uuid::new_v4();
    let ids: Vec<Vec<CollectionId>> = (0..OP_KEYS)
        .map(|k| {
            (0..OP_COLLS)
                .map(|c| {
                    Ok(CollectionId::new(
                        StateKey::new(segment, Arc::from(format!("k{k}").as_str())),
                        StateType::Application,
                        StateName::try_new(format!("c{c}"))?,
                    ))
                })
                .collect::<Result<_>>()
        })
        .collect::<Result<_>>()?;
    let store = DirtyStore::new();
    let mut model = DirtyModel::default();

    for op in ops {
        match op {
            DirtyOp::Set(key, coll, section, coord, value) => {
                store.set(
                    &ids[key as usize][coll as usize],
                    &pool_cell(section, coord),
                    &[value],
                );
                model.cells.insert(
                    (key, coll, section, coord),
                    DirtyVal::Set(Bytes::copy_from_slice(&[value])),
                );
            }
            DirtyOp::Clear(key, coll, section, coord) => {
                store.clear(
                    &ids[key as usize][coll as usize],
                    &pool_cell(section, coord),
                );
                model
                    .cells
                    .insert((key, coll, section, coord), DirtyVal::Cleared);
            }
            DirtyOp::ClearSection(key, coll, section) => {
                store.clear_section(&ids[key as usize][coll as usize], Section::new(section));
                model.markers.insert((key, coll, section));
                // The marker supersedes every buffered outcome of its
                // section: the durable clear's gap erase subsumes them all.
                model
                    .cells
                    .retain(|&(k2, c2, s2, _), _| !(k2 == key && c2 == coll && s2 == section));
            }
            DirtyOp::RemoveCollection(key, coll) => {
                store.remove_collection(&ids[key as usize][coll as usize]);
                model
                    .cells
                    .retain(|&(k2, c2, ..), _| !(k2 == key && c2 == coll));
                model
                    .markers
                    .retain(|&(k2, c2, _)| !(k2 == key && c2 == coll));
            }
            DirtyOp::ClearEvent(key) => {
                store.clear_event(&ids[key as usize][0].state_key().key);
                model.cells.retain(|&(k2, ..), _| k2 != key);
                model.markers.retain(|&(k2, ..)| k2 != key);
            }
        }
        if !dirty_matches(&store, &ids, &model)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The dirty store tracks the plain model over random
/// set/clear/clear-section/remove/clear-event interleavings across 2 keys × 2
/// collections × 2 sections — every read surface asserted after every op.
#[test]
fn prop_dirty_store_tracks_model() {
    fn property(trace: DirtyTrace) -> TestResult {
        match run_dirty_trace(trace) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("dirty store diverged from the model"),
            Err(error) => TestResult::error(format!("trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(property as fn(DirtyTrace) -> TestResult);
}
