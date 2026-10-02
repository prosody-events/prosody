//! Bounded coordinate encoding and aligned batch reads.

use super::{
    BorrowedKeyOf, CellBuffer, CellCodecError, CellStateError, CellType, Collection, ContextOf,
    Coordinate, FromSession, Mutation, OrderedKeyCodec, Presence, Projection, ResolvedOf, Section,
    Staged, StateAccessError, StateName, StateSession, StateType, Values, resolve_batch, sealed,
    staged,
};
use crate::codec::{Codec, SerializeBufGuard};
use crate::state::cell_key::CellRef;
use crate::state::order_codec::KeyCodecError;
use crate::state::store::{CELL_BATCH, ReadBatch};
use smallvec::SmallVec;
use std::convert::identity;

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

/// Reads and resolves one value per key in input order.
/// A read passes an empty journal. A write passes its own journal.
pub(super) async fn get_keys<'a, S, L, T>(
    collection: &Collection<S, L>,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    section: Section,
    journal: &[Mutation],
    keys: impl Iterator<Item = &'a BorrowedKeyOf<T>> + Send,
) -> Result<CellBuffer<Option<ResolvedOf<T>>>, CellStateError<CellCodecError<T>>>
where
    S: StateSession,
    T: CellType,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    let bytes =
        read_keys::<S, L, T, Values, _>(collection, inner, section, journal, keys, identity)
            .await?;
    resolve_batch::<S, T>(collection.session(), bytes).await
}

/// Tests each key for presence in input order.
/// The journal follows [`get_keys`].
pub(super) async fn contains_keys<'a, S, L, T>(
    collection: &Collection<S, L>,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    section: Section,
    journal: &[Mutation],
    keys: impl Iterator<Item = &'a BorrowedKeyOf<T>> + Send,
) -> Result<CellBuffer<bool>, CellStateError<CellCodecError<T>>>
where
    S: StateSession,
    T: CellType,
{
    read_keys::<S, L, T, Presence, _>(
        collection,
        inner,
        section,
        journal,
        keys,
        |present: Option<()>| present.is_some(),
    )
    .await
}

/// Encodes one bounded batch at a time and preserves every input position.
/// The read completes before the next batch reuses the encoding buffer.
/// `answer` maps each projected cell into the one answer buffer.
async fn read_keys<'a, S, L, T, P: Projection, A: Default>(
    collection: &Collection<S, L>,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    section: Section,
    journal: &[Mutation],
    mut keys: impl Iterator<Item = &'a BorrowedKeyOf<T>> + Send,
    answer: impl Fn(Option<P::Payload>) -> A + Copy + Send,
) -> Result<CellBuffer<A>, CellStateError<CellCodecError<T>>>
where
    S: StateSession,
    S::Engine: sealed::Reads<S, P>,
    T: CellType,
{
    let mut answers = CellBuffer::with_capacity(keys.size_hint().0);
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
        batch
            .merge_into(
                &mut answers,
                |coordinate| {
                    let cell = CellRef {
                        section,
                        coordinate,
                    };
                    staged(journal, cell).map(|staged| {
                        answer(match staged {
                            Staged::Present(bytes) => Some(P::from_value(bytes)),
                            Staged::Absent => None,
                        })
                    })
                },
                move |pending| async move {
                    <S::Engine as sealed::Reads<S, P>>::read_batch(
                        collection.session(),
                        inner,
                        collection.state_type(),
                        collection.name(),
                        section,
                        &pending,
                    )
                    .await
                },
                answer,
            )
            .await?;
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
