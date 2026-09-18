//! Aligned batch reads and typed value resolution.

use super::{
    BorrowedKeyOf, CellBuffer, CellType, Coordinate, OrderedKeyCodec, Projection, Section,
    StateAccessError, StateName, StateSession, StateType, sealed,
};
use crate::state::store::CoordinateBatch;

/// One position of an aligned batch read: either already answered from the
/// invocation's journal, or awaiting the engine at its coordinate.
pub(super) enum Slot<P: Projection> {
    /// The journal already answers this position.
    Answered(Option<P::Payload>),
    /// The engine must read this coordinate.
    Pending(Coordinate),
}

/// Fills every pending slot from the engine and returns the answers aligned to
/// `slots` — the journal-aware batch read a write invocation performs, where
/// only the journal-silent positions reach the engine. It reads them through
/// [`read_coordinates`].
pub(super) async fn batched<S: StateSession, P: Projection>(
    session: &S,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    state_type: StateType,
    name: &StateName,
    section: Section,
    slots: CellBuffer<Slot<P>>,
) -> Result<CellBuffer<Option<P::Payload>>, StateAccessError>
where
    S::Engine: sealed::Reads<S, P>,
{
    let pending: CellBuffer<Coordinate> = slots
        .iter()
        .filter_map(|slot| match slot {
            Slot::Pending(coordinate) => Some(coordinate.clone()),
            Slot::Answered(_) => None,
        })
        .collect();
    let answers =
        read_coordinates::<S, P>(session, inner, state_type, name, section, pending).await?;
    let mut answers = answers.into_iter();
    Ok(slots
        .into_iter()
        .map(|slot| match slot {
            Slot::Answered(payload) => payload,
            // The engine answers every batched position in order, so the
            // answers line up with the pending slots.
            Slot::Pending(_) => answers.next().flatten(),
        })
        .collect())
}

/// Reads one projected answer per key in input order.
///
/// # Errors
///
/// An access error from the engine.
pub(super) async fn read_keys<'a, S, T, P: Projection>(
    session: &S,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    state_type: StateType,
    name: &StateName,
    section: Section,
    keys: impl Iterator<Item = &'a BorrowedKeyOf<T>> + Send,
) -> Result<CellBuffer<Option<P::Payload>>, StateAccessError>
where
    S: StateSession,
    S::Engine: sealed::Reads<S, P>,
    T: CellType,
{
    // Mapped as a function item, so the lowering carries no closure whose
    // higher-ranked capture would defeat the future's `Send` proof.
    let coordinates = keys.map(<T::Key as OrderedKeyCodec>::encode);
    read_coordinates::<S, P>(session, inner, state_type, name, section, coordinates).await
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
