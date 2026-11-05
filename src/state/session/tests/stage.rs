//! Stage operations and their independent dirty-store model, shared by the
//! stage property tests.

use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::dirty::DirtyVal;
use crate::state::session::{KeyedStateSession, StateBackend};
use crate::state::{StateName, StateType};
use bytes::Bytes;
use quickcheck::{Arbitrary, Gen};
use std::collections::{HashMap, HashSet};

/// The cell at `(section, coord)` — a single-byte coordinate, so byte order is
/// numeric order. Lets one Value session buffer across many sections.
pub(super) fn cell_in(section: i8, coord: u8) -> CellKey {
    CellKey {
        section: Section::new(section),
        coordinate: Coordinate::from_bytes(vec![coord]),
    }
}

/// One staging op: a set / clear at `(section, coord)` or a whole-section
/// clear. `section` is already reduced to a small index so a handful of
/// sections actually collide (multi-section grouping and section-clear
/// subsumption both need repeats).
#[derive(Clone, Copy, Debug)]
pub(super) enum StageOp {
    Set { section: u8, coord: u8, byte: u8 },
    Clear { section: u8, coord: u8 },
    ClearSection { section: u8 },
}

impl Arbitrary for StageOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 | 1 => Self::Set {
                section: u8::arbitrary(g) % 3,
                coord: u8::arbitrary(g),
                byte: u8::arbitrary(g),
            },
            2 | 3 => Self::Clear {
                section: u8::arbitrary(g) % 3,
                coord: u8::arbitrary(g),
            },
            _ => Self::ClearSection {
                section: u8::arbitrary(g) % 3,
            },
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match *self {
            Self::Set {
                section,
                coord,
                byte,
            } => Box::new((coord, byte).shrink().map(move |(coord, byte)| Self::Set {
                section,
                coord,
                byte,
            })),
            Self::Clear { section, coord } => Box::new(
                coord
                    .shrink()
                    .map(move |coord| Self::Clear { section, coord }),
            ),
            Self::ClearSection { .. } => quickcheck::empty_shrinker(),
        }
    }
}

/// Replays `ops` into a plain model mirroring the dirty store's
/// last-writer-wins semantics: `set`/`clear` upsert a cell, `clear_section`
/// marks the section and drops its buffered cells (exactly
/// `DirtyStore::clear_section`). Returns the per-cell outcomes and the cleared
/// sections — the stage's input, modeled independently of `stage_collection`.
pub(super) fn replay_dirty(ops: &[StageOp]) -> (HashMap<CellKey, DirtyVal>, HashSet<Section>) {
    let mut cells: HashMap<CellKey, DirtyVal> = HashMap::new();
    let mut cleared: HashSet<Section> = HashSet::new();
    for op in ops {
        match *op {
            StageOp::Set {
                section,
                coord,
                byte,
            } => {
                cells.insert(
                    cell_in(section as i8, coord),
                    DirtyVal::Set(Bytes::copy_from_slice(&[byte])),
                );
            }
            StageOp::Clear { section, coord } => {
                cells.insert(cell_in(section as i8, coord), DirtyVal::Cleared);
            }
            StageOp::ClearSection { section } => {
                let s = Section::new(section as i8);
                cleared.insert(s);
                cells.retain(|c, _| c.section != s);
            }
        }
    }
    (cells, cleared)
}

/// Applies `ops` to `session` in order — the same sequence [`replay_dirty`]
/// models. Generic over the backend so both fixtures drive it.
pub(super) async fn apply_stage_ops<B: StateBackend>(
    session: &KeyedStateSession<B, ()>,
    name: &StateName,
    ops: &[StageOp],
) {
    for op in ops {
        match *op {
            StageOp::Set {
                section,
                coord,
                byte,
            } => {
                session
                    .seed(
                        StateType::Application,
                        name,
                        &cell_in(section as i8, coord),
                        Some(&[byte]),
                    )
                    .await;
            }
            StageOp::Clear { section, coord } => {
                session
                    .seed(
                        StateType::Application,
                        name,
                        &cell_in(section as i8, coord),
                        None,
                    )
                    .await;
            }
            StageOp::ClearSection { section } => {
                session
                    .seed_section_clear(StateType::Application, name, Section::new(section as i8))
                    .await;
            }
        }
    }
}

/// Every cell a set/clear op names (a section-clear names no cell).
pub(super) fn touched_cells(ops: &[StageOp]) -> HashSet<CellKey> {
    ops.iter()
        .filter_map(|op| match *op {
            StageOp::Set { section, coord, .. } | StageOp::Clear { section, coord } => {
                Some(cell_in(section as i8, coord))
            }
            StageOp::ClearSection { .. } => None,
        })
        .collect()
}
