use super::cell::ProvisionalCell;
use super::cell_key::{CellKey, Coordinate, Section};
use super::identity::CollectionId;
use super::store::{CELL_BATCH, CellBuffer, CellStore, CoordinateBatch, ReadBatch};
use smallvec::SmallVec;

/// Returns the distinct coordinates of `batch` in first-occurrence order.
pub(crate) fn distinct<'a>(batch: &ReadBatch<'a>) -> SmallVec<[&'a [u8]; CELL_BATCH.get()]> {
    let mut coordinates: SmallVec<[&[u8]; CELL_BATCH.get()]> = SmallVec::new();
    for &coordinate in batch.iter() {
        if !coordinates.contains(&coordinate) {
            coordinates.push(coordinate);
        }
    }
    coordinates
}

/// Returns the answer of the first earlier position that holds `coordinate`.
///
/// A batch read answers positions in input order and calls this before it
/// reads each one. `answers` then holds exactly the earlier positions, so a
/// repeated coordinate reuses its first answer and shares one read.
pub(crate) fn repeated<T: Clone>(
    batch: &ReadBatch<'_>,
    answers: &[T],
    coordinate: &[u8],
) -> Option<T> {
    batch
        .iter()
        .zip(answers)
        .find_map(|(&earlier, answer)| (earlier == coordinate).then(|| answer.clone()))
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
