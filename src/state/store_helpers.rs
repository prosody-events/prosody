use super::cell::ProvisionalCell;
use super::cell_key::{CellKey, Coordinate, Section};
use super::identity::CollectionId;
use super::store::{CellBuffer, CellStore, CoordinateBatch};
use smallvec::SmallVec;

/// Returns unique coordinates and each input coordinate's unique index.
pub(crate) fn dedupe(batch: &CoordinateBatch) -> (CellBuffer<&Coordinate>, CellBuffer<usize>) {
    let mut unique_coordinates: CellBuffer<&Coordinate> = SmallVec::with_capacity(batch.len());
    let mut input_indices: CellBuffer<usize> = SmallVec::with_capacity(batch.len());
    for coordinate in batch.iter() {
        let index = if let Some(index) = unique_coordinates
            .iter()
            .position(|unique| *unique == coordinate)
        {
            index
        } else {
            unique_coordinates.push(coordinate);
            unique_coordinates.len() - 1
        };
        input_indices.push(index);
    }
    (unique_coordinates, input_indices)
}

/// Expands unique answers to the original input order.
pub(crate) fn expand_to_input_order<T: Clone>(
    input_indices: &[usize],
    unique_answers: &[T],
) -> CellBuffer<T> {
    debug_assert!(
        input_indices
            .iter()
            .all(|&index| index < unique_answers.len()),
        "batch read must answer every input position"
    );
    input_indices
        .iter()
        .map(|&index| unique_answers[index].clone())
        .collect()
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
    let unique_coordinates = sorted_unique_coordinates(batch);
    let mut out: CellBuffer<(Coordinate, ProvisionalCell)> =
        SmallVec::with_capacity(unique_coordinates.len());
    for coordinate in unique_coordinates {
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
