//! Overlay batch reads answer from the dirty store before the lower store.

use super::Overlay;
use crate::state::cell::Values;
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::dirty::DirtyStore;
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::store::{CELL_BATCH, ReadBatch};
use crate::state::tests::support::CountingCellStore;
use crate::state::{CollectionId, StateKey, StateName, StateType};
use crate::test_util::TEST_RUNTIME;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail};
use quickcheck::QuickCheck;
use std::collections::HashMap;
use std::iter::once;
use std::sync::Arc;
use uuid::Uuid;

const SECTION: Section = Section::new(0);

/// A batch read over an empty lower store answers each position from the
/// dirty model. It reads the lower store once exactly when a position has no
/// dirty answer. A cleared section never reaches it.
#[test]
fn prop_lower_batch_reads_only_untouched_positions() {
    fn property(
        cleared: bool,
        dirty: Vec<(u8, Option<u8>)>,
        first: u8,
        reads: Vec<u8>,
    ) -> Result<()> {
        TEST_RUNTIME.block_on(check(cleared, dirty, first, reads))
    }
    QuickCheck::new()
        .quickcheck(property as fn(bool, Vec<(u8, Option<u8>)>, u8, Vec<u8>) -> Result<()>);
}

async fn check(
    cleared: bool,
    dirty: Vec<(u8, Option<u8>)>,
    first: u8,
    reads: Vec<u8>,
) -> Result<()> {
    let id = CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("key")),
        StateType::Application,
        StateName::try_new("overlay")?,
    );
    let lower = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
    let overlay = Overlay::new(Arc::new(DirtyStore::new()), lower.clone());

    // The model applies the section clear first. Later dirty writes repopulate
    // the section, and the last write to a coordinate wins.
    if cleared {
        overlay.dirty().clear_section(&id, SECTION);
    }
    let mut model = HashMap::new();
    for (coordinate, value) in dirty {
        let cell = CellKey {
            section: SECTION,
            coordinate: Coordinate::from_bytes(vec![coordinate]),
        };
        match value {
            Some(byte) => overlay.dirty().set(&id, &cell, &[byte]),
            None => overlay.dirty().clear(&id, &cell),
        }
        model.insert(coordinate, value);
    }

    let coordinates: Vec<[u8; 1]> = once(first)
        .chain(reads)
        .take(CELL_BATCH.get())
        .map(|coordinate| [coordinate])
        .collect();
    let Some(batch) = ReadBatch::chunks(coordinates.iter().map(<[u8; 1]>::as_slice)).next() else {
        bail!("a read batch with a first coordinate is never empty");
    };
    let reaches_lower = !cleared
        && coordinates
            .iter()
            .any(|[coordinate]| !model.contains_key(coordinate));

    let answers = overlay.get_many::<Values>(&id, SECTION, &batch).await?;
    if lower.batch_reads() != usize::from(reaches_lower) {
        bail!(
            "{} lower batch reads, expected {}",
            lower.batch_reads(),
            usize::from(reaches_lower)
        );
    }
    let expected: Vec<_> = coordinates
        .iter()
        .map(|[coordinate]| {
            model
                .get(coordinate)
                .copied()
                .flatten()
                .map(|byte| Bytes::copy_from_slice(&[byte]))
        })
        .collect();
    if *answers != *expected {
        bail!("answers {answers:?}, expected {expected:?}");
    }
    Ok(())
}
