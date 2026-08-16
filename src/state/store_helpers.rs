use super::cell::ProvisionalCell;
use super::cell_key::{CellKey, Coordinate, Section};
use super::identity::CollectionId;
use super::store::{CellBuffer, CellStore, CoordinateBatch};
use smallvec::SmallVec;

/// Splits a batch into unique coordinates and an input-position plan.
pub(crate) fn dedupe(batch: &CoordinateBatch) -> (CellBuffer<&Coordinate>, CellBuffer<usize>) {
    let mut uniques: CellBuffer<&Coordinate> = SmallVec::with_capacity(batch.len());
    let mut plan: CellBuffer<usize> = SmallVec::with_capacity(batch.len());
    for coordinate in batch.iter() {
        let index = if let Some(index) = uniques.iter().position(|unique| *unique == coordinate) {
            index
        } else {
            uniques.push(coordinate);
            uniques.len() - 1
        };
        plan.push(index);
    }
    (uniques, plan)
}

/// Expands unique answers through a deduplication plan.
pub(crate) fn realign<T: Clone>(plan: &[usize], answers: &[T]) -> CellBuffer<T> {
    debug_assert!(
        plan.iter().all(|&index| index < answers.len()),
        "batch read must answer every input position"
    );
    plan.iter().map(|&index| answers[index].clone()).collect()
}

/// Returns the sorted, distinct coordinates for one bounded batch.
pub(crate) fn sorted_unique_coordinates(batch: &CoordinateBatch) -> CellBuffer<&Coordinate> {
    let mut coordinates: CellBuffer<&Coordinate> = SmallVec::with_capacity(batch.len());
    coordinates.extend(batch.iter());
    coordinates.sort_unstable();
    coordinates.dedup();
    coordinates
}

/// Groups sorted cell keys into bounded batches for each section.
pub(crate) fn section_batches(keys: &[CellKey]) -> Vec<(Section, CoordinateBatch)> {
    keys.chunk_by(|a, b| a.section == b.section)
        .flat_map(|run| {
            let section = run[0].section;
            CoordinateBatch::chunks(run.iter().map(|key| key.coordinate.clone()))
                .map(move |batch| (section, batch))
        })
        .collect()
}

/// Reads distinct provisional cells in ascending coordinate order.
pub(crate) async fn provisional_point_loop<S: CellStore>(
    store: &S,
    collection: &CollectionId,
    section: Section,
    batch: &CoordinateBatch,
) -> Result<CellBuffer<(Coordinate, ProvisionalCell)>, S::Error> {
    let uniques = sorted_unique_coordinates(batch);
    let mut out: CellBuffer<(Coordinate, ProvisionalCell)> = SmallVec::with_capacity(uniques.len());
    for coordinate in uniques {
        let cell = CellKey {
            section,
            coordinate: coordinate.clone(),
        };
        if let Some(provisional) = store.provisional_cell_at(collection, &cell).await? {
            out.push((coordinate.clone(), provisional));
        }
    }
    Ok(out)
}
