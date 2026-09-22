//! Bounded coordinate encoding and aligned batch reads.

use super::{
    BorrowedKeyOf, CellBuffer, CellCodecError, CellStateError, CellType, Coordinate, Mutation,
    OrderedKeyCodec, Projection, Section, Staged, StateAccessError, StateName, StateSession,
    StateType, sealed, staged,
};
use crate::codec::{Codec, SerializeBufGuard};
use crate::state::cell_key::CellRef;
use crate::state::order_codec::KeyCodecError;
use crate::state::store::{CELL_BATCH, CoordinateBatch, ReadBatch};
use smallvec::SmallVec;

/// Encodes one key before its borrow ends. The future owns the buffer guard.
pub(super) fn encode_key<K: OrderedKeyCodec>(
    key: &K::Borrowed,
) -> Result<SerializeBufGuard, KeyCodecError> {
    let mut buffer = SerializeBufGuard::acquire();
    K::with_cached_local(|codec| codec.serialize_key(key, &mut buffer))?;
    Ok(buffer)
}

/// Encodes one bounded batch at a time and preserves every input position.
/// The read completes before the next batch reuses the encoding buffer.
pub(super) async fn read_keys<'a, S, T, P: Projection>(
    session: &S,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    state_type: StateType,
    name: &StateName,
    section: Section,
    journal: &[Mutation],
    mut keys: impl Iterator<Item = &'a BorrowedKeyOf<T>> + Send,
) -> Result<CellBuffer<Option<P::Payload>>, CellStateError<CellCodecError<T>>>
where
    S: StateSession,
    S::Engine: sealed::Reads<S, P>,
    T: CellType,
{
    let mut answers = CellBuffer::with_capacity(keys.size_hint().0);
    let mut buffer = SerializeBufGuard::acquire();
    loop {
        buffer.clear();
        let (batch, positions) = {
            let mut ends: SmallVec<[usize; CELL_BATCH.get()]> = SmallVec::new();
            T::Key::with_cached_local(|codec| {
                for key in keys.by_ref().take(CELL_BATCH.get()) {
                    codec.serialize_key(key, &mut buffer)?;
                    ends.push(buffer.len());
                }
                Ok(())
            })
            .map_err(CellStateError::Key)?;
            if ends.is_empty() {
                return Ok(answers);
            }
            let mut pending: SmallVec<[&[u8]; CELL_BATCH.get()]> = SmallVec::new();
            let mut positions: SmallVec<[usize; CELL_BATCH.get()]> = SmallVec::new();
            let mut start = 0;
            for &end in &ends {
                let cell = CellRef {
                    section,
                    coordinate: &buffer[start..end],
                };
                start = end;
                let value = match staged(journal, cell) {
                    Some(Staged::Present(bytes)) => Some(P::from_value(bytes)),
                    Some(Staged::Absent) => None,
                    None => {
                        pending.push(cell.coordinate);
                        positions.push(answers.len());
                        None
                    }
                };
                answers.push(value);
            }
            (ReadBatch::from_buffer(pending), positions)
        };
        if let Some(batch) = &batch {
            let loaded = <S::Engine as sealed::Reads<S, P>>::read_batch(
                session, inner, state_type, name, section, batch,
            )
            .await?;
            if loaded.len() != positions.len() {
                return Err(
                    StateAccessError::misaligned_batch(loaded.len(), positions.len()).into(),
                );
            }
            for (position, value) in positions.iter().zip(loaded) {
                answers[*position] = value;
            }
        }
    }
}

/// Reads one projected answer per coordinate in input order.
/// Batches run sequentially because owner reads can repair the same collection.
pub(in crate::state::collection) async fn read_coordinates<S, P: Projection>(
    session: &S,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    state_type: StateType,
    name: &StateName,
    section: Section,
    coordinates: impl IntoIterator<Item = Coordinate>,
) -> Result<CellBuffer<Option<P::Payload>>, StateAccessError>
where
    S: StateSession,
    S::Engine: sealed::Reads<S, P>,
{
    let coordinates = coordinates.into_iter();
    // Unknown input lengths can grow the result buffer. Storage batches stay
    // bounded.
    let mut answers = CellBuffer::with_capacity(coordinates.size_hint().0);
    for batch in CoordinateBatch::chunks(coordinates) {
        let batch = batch.as_ref();
        let values = <S::Engine as sealed::Reads<S, P>>::read_batch(
            session, inner, state_type, name, section, &batch,
        )
        .await?;
        debug_assert_eq!(
            values.len(),
            batch.len(),
            "batch read answers every input position"
        );
        answers.extend(values);
    }
    Ok(answers)
}
