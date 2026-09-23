//! Bounded coordinate encoding and aligned batch reads.

use super::{
    BorrowedKeyOf, CellBuffer, CellCodecError, CellStateError, CellType, Coordinate, Mutation,
    OrderedKeyCodec, Projection, Section, Staged, StateAccessError, StateName, StateSession,
    StateType, sealed, staged,
};
use crate::codec::{Codec, SerializeBufGuard};
use crate::state::cell_key::CellRef;
use crate::state::order_codec::KeyCodecError;
use crate::state::store::{CELL_BATCH, ReadBatch};
use smallvec::SmallVec;

/// Encodes one key before its borrow ends.
/// The pooled buffer returns at once, so concurrent point reads share it.
/// The copy stays inline for coordinates of 32 bytes or fewer.
pub(super) fn encode_key<K: OrderedKeyCodec>(
    key: &K::Borrowed,
) -> Result<SmallVec<[u8; 32]>, KeyCodecError> {
    let mut buffer = SerializeBufGuard::acquire();
    K::with_cached_local(|codec| codec.serialize_key(key, &mut buffer))?;
    Ok(SmallVec::from_slice(&buffer))
}

/// Encodes one bounded batch at a time and preserves every input position.
/// The read completes before the next batch reuses the encoding buffer.
/// Reads pass an empty journal, so one path serves reads and writes.
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
    let mut answers = CellBuffer::new();
    let mut buffer = SerializeBufGuard::acquire();
    loop {
        buffer.clear();
        let mut ends: SmallVec<[usize; CELL_BATCH.get()]> = SmallVec::new();
        T::Key::with_cached_local(|codec| {
            for key in keys.by_ref().take(CELL_BATCH.get()) {
                codec.serialize_key(key, &mut buffer)?;
                ends.push(buffer.len());
            }
            Ok(())
        })
        .map_err(CellStateError::Key)?;
        // The loop above takes at most one batch of keys, so the first chunk
        // holds every encoded coordinate.
        let coordinates = ends.iter().scan(0, |start, &end| {
            let coordinate = &buffer[*start..end];
            *start = end;
            Some(coordinate)
        });
        let Some(batch) = ReadBatch::chunks(coordinates).next() else {
            return Ok(answers);
        };
        let inner = &mut *inner;
        // Coordinates that the journal does not answer read from the store.
        let merged = batch
            .merge(
                |coordinate| {
                    let cell = CellRef {
                        section,
                        coordinate,
                    };
                    staged(journal, cell).map(|staged| match staged {
                        Staged::Present(bytes) => Some(P::from_value(bytes)),
                        Staged::Absent => None,
                    })
                },
                move |pending| async move {
                    <S::Engine as sealed::Reads<S, P>>::read_batch(
                        session, inner, state_type, name, section, &pending,
                    )
                    .await
                },
            )
            .await?;
        // The first batch moves in, so a read of one batch allocates once.
        if answers.is_empty() {
            answers = merged.into();
        } else {
            answers.extend(merged);
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
    coordinates: &[Coordinate],
) -> Result<CellBuffer<Option<P::Payload>>, StateAccessError>
where
    S: StateSession,
    S::Engine: sealed::Reads<S, P>,
{
    let mut answers = CellBuffer::with_capacity(coordinates.len());
    for batch in ReadBatch::chunks(coordinates.iter().map(Coordinate::as_bytes)) {
        let values = <S::Engine as sealed::Reads<S, P>>::read_batch(
            session, inner, state_type, name, section, &batch,
        )
        .await?;
        answers.extend(values);
    }
    Ok(answers)
}
