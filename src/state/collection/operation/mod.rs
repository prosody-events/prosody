//! The two scoped operation types and the invocation-local write journal.

use super::stream::PlanBase;
use super::{
    CellAddress, CellFamily, Collection, CollectionLayout, CollectionRead, CollectionWrite, Plan,
    StateSession, WritableStateSession, encode_cell, resolve_batch, resolve_cell, sealed,
    sealed_ops,
};
use crate::state::access::StateAccessError;
use crate::state::cell::{Presence, Projection, Values};
use crate::state::cell_key::{CellKey, CellRef, Coordinate, Direction, Section};
use crate::state::descriptor::{
    BorrowedKeyOf, CellCodecError, CellResolver, CellStateError, CellType, ContextOf, FromSession,
    ResolvedOf, WriteOf,
};
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::store::CellBuffer;
use crate::state::{StateName, StateType};
use std::ops::Bound;

mod batch;
mod read;
use batch::encode_key;
pub(super) use batch::read_coordinates;
use bytes::Bytes;
use smallvec::SmallVec;
use std::future::Future;

/// Inline capacity of one invocation's mutation journal.
///
/// Four is the current maximum across the built-in collections. A Deque push
/// stages one entry, at most `TRIM_MAX` point clears, and one bounds set. Map
/// needs two, Value one. Each collection declares its own maximum beside its
/// layout, and a compile-time assertion there checks the declaration against
/// this budget. Widening a collection past four is therefore a build error,
/// not a silent steady-state allocation. A spill is still correct: Rust cannot
/// derive a sound maximum from an arbitrary async body, so the inline bound is
/// an allocation budget, never a limit.
pub const JOURNAL_INLINE: usize = 4;

/// One invocation's staged mutations, in authored order.
pub type MutationJournal = SmallVec<[Mutation; JOURNAL_INLINE]>;

/// One staged mutation. `Set` carries its encoded payload. The command encodes
/// the payload once, and merge moves that one copy into the overlay.
pub enum Mutation {
    /// Stage `bytes` at `cell`.
    Set {
        /// The addressed cell.
        cell: CellKey,
        /// The encoded payload.
        bytes: Bytes,
    },
    /// Stage an absence at `cell`.
    Clear {
        /// The addressed cell.
        cell: CellKey,
    },
    /// Stage an absence over every section of the collection's layout. One
    /// payload-free entry: the sections are the layout's own static set, read
    /// at the command, where the layout is still in scope.
    Reset {
        /// Every active and reserved section of the declaring layout.
        sections: &'static [Section],
    },
}

/// What this invocation has already staged for one cell. This is the
/// journal's answer to a read. A cell the journal does not mention falls
/// through to the engine.
enum Staged {
    /// The cell was written in this invocation.
    Present(Bytes),
    /// The cell was cleared in this invocation.
    Absent,
}

/// One admitted read invocation, valid only inside the scope that created it.
///
/// The scope lends `&mut ReadOperation` through a higher-ranked closure, so the
/// caller cannot name the operation's borrow lifetime. The caller can therefore
/// neither return the operation from the scope nor store it in a slot declared
/// outside it. Only owned data crosses the boundary.
///
/// The type is neither `Clone` nor `Copy`. Its only constructor acquires engine
/// state and builds the complete value. No API pairs a separately obtained
/// guard, permit, or inner value with a collection.
pub struct ReadOperation<'a, S: StateSession, L> {
    collection: &'a Collection<S, L>,
    inner: <S::Engine as sealed::ReadEngine<S>>::ReadInner<'a>,
}

impl<'a, S: StateSession, L> ReadOperation<'a, S, L> {
    /// Acquires read admission for one invocation over `collection`.
    pub(super) async fn new(collection: &'a Collection<S, L>) -> Self {
        let inner = <S::Engine as sealed::ReadEngine<S>>::begin_read(collection.session()).await;
        Self { collection, inner }
    }

    /// Plans a managed point-get stream over `coordinates` in `family`, in the
    /// given order. The plan freezes this invocation's engine state. Each
    /// reader chunk then resumes on the same source, and each owner chunk
    /// reacquires the gate, without a second planning command.
    pub(crate) fn coordinates<T: CellType, B: AsRef<[u8]> + Send>(
        &self,
        family: CellFamily<L, T>,
        coordinates: Vec<Coordinate>,
    ) -> Plan<S, T, B> {
        Plan::coordinates(self.plan_base(family.section()), coordinates)
    }

    /// Plans a scan over encoded edges in the declared family.
    /// The edges follow `dir` and can include, exclude, or omit an endpoint.
    pub(crate) fn range<T: CellType, B: AsRef<[u8]> + Send>(
        &self,
        family: CellFamily<L, T>,
        start: Bound<B>,
        dir: Direction,
        end: Bound<B>,
    ) -> Plan<S, T, B> {
        Plan::range(self.plan_base(family.section()), start, dir, end)
    }

    /// The binding and captured engine state every managed plan carries.
    fn plan_base(&self, section: Section) -> PlanBase<S> {
        PlanBase::new(
            self.collection.session().clone(),
            self.collection.state_type(),
            self.collection.name().clone(),
            section,
            <S::Engine as sealed::ReadEngine<S>>::capture(&self.inner),
        )
    }
}

/// One admitted write invocation with its own mutation journal. The
/// non-escape and construction guarantees are [`ReadOperation`]'s.
pub struct WriteOperation<'a, S: WritableStateSession, L> {
    collection: &'a Collection<S, L>,
    inner: <S::Engine as sealed::WriteEngine<S>>::WriteInner<'a>,
    journal: MutationJournal,
}

impl<'a, S: WritableStateSession, L> WriteOperation<'a, S, L> {
    /// Acquires write admission for one invocation over `collection`.
    ///
    /// # Errors
    ///
    /// Whatever the engine's admission refuses.
    pub(super) async fn new(collection: &'a Collection<S, L>) -> Result<Self, StateAccessError> {
        let inner =
            <S::Engine as sealed::WriteEngine<S>>::begin_write(collection.session()).await?;
        Ok(Self {
            collection,
            inner,
            journal: MutationJournal::new(),
        })
    }

    /// Ends the invocation by consuming the operation: revalidates admission,
    /// then replays the journal into the event overlay in authored order with
    /// no suspension point.
    ///
    /// Taking `self` is the invariant, not a convenience. A merged operation
    /// is moved, so a write after merge is a use-after-move that the compiler
    /// rejects. No runtime state check is needed.
    ///
    /// The held write admission excludes the settle boundary's close and the
    /// attempt boundary's reset between the fence and the replay. No partially
    /// replayed invocation is observable. The fence samples termination once.
    /// Teardown is not gated, and
    /// [`EventStateScope`](crate::state::manager::EventStateScope) owns that
    /// residual.
    ///
    /// # Errors
    ///
    /// The final fence's refusal: a stale attempt, a closed session, or
    /// termination. Nothing is replayed in that case.
    pub(super) fn merge(self) -> Result<(), StateAccessError> {
        let session = self.collection.session();
        let (state_type, name) = (self.collection.state_type(), self.collection.name());
        <S::Engine as sealed::WriteEngine<S>>::validate_write(session, &self.inner)?;
        let journal = self.journal;
        <S::Engine as sealed::WriteEngine<S>>::apply(
            session,
            state_type,
            name,
            &self.inner,
            journal,
        );
        Ok(())
    }

    /// Reads the journal answer or one projected engine answer.
    async fn staged_or_read<P: Projection>(
        &mut self,
        cell: CellRef<'_>,
    ) -> Result<Option<P::Payload>, StateAccessError>
    where
        S::Engine: sealed::Reads<S, P>,
    {
        match staged(&self.journal, cell) {
            Some(Staged::Present(bytes)) => Ok(Some(P::from_value(bytes))),
            Some(Staged::Absent) => Ok(None),
            None => {
                <S::Engine as sealed::Reads<S, P>>::read_point(
                    self.collection.session(),
                    &mut *self.inner,
                    self.collection.state_type(),
                    self.collection.name(),
                    cell,
                )
                .await
            }
        }
    }

    /// How many mutations this invocation has staged.
    #[cfg(test)]
    pub(crate) fn journal_len(&self) -> usize {
        self.journal.len()
    }

    /// Whether the journal has outgrown its inline capacity.
    #[cfg(test)]
    pub(crate) fn journal_spilled(&self) -> bool {
        self.journal.spilled()
    }
}

impl<S: StateSession, L> sealed_ops::CollectionOperation for ReadOperation<'_, S, L> {}

impl<S: WritableStateSession, L> sealed_ops::CollectionOperation for WriteOperation<'_, S, L> {}

impl<'c, S: WritableStateSession, L> CollectionWrite for WriteOperation<'c, S, L> {
    fn take<'a, T>(
        &'a mut self,
        family: CellFamily<L, T>,
        key: &BorrowedKeyOf<T>,
    ) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>>
    + Send
    + use<'a, 'c, S, L, T>
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let encoded = encode_key::<T::Key>(key);
        async move {
            let buffer = encoded.map_err(CellStateError::Key)?;
            let cell = CellRef {
                section: family.section(),
                coordinate: &buffer,
            };
            let value = match self.staged_or_read::<Values>(cell).await? {
                Some(bytes) => Some(resolve_cell::<S, T>(self.collection.session(), bytes).await?),
                None => None,
            };
            // A failed read leaves the journal unchanged.
            self.journal.push(Mutation::Clear {
                cell: cell.into_owned(),
            });
            Ok(value)
        }
    }

    fn set<T: CellType>(
        &mut self,
        address: CellAddress<L, T>,
        value: WriteOf<'_, T>,
    ) -> Result<(), CellStateError<CellCodecError<T>>> {
        let stored = <T::Resolver as CellResolver>::stored_from(value);
        let buffer = encode_cell::<T::Codec>(stored).map_err(CellStateError::Codec)?;
        self.journal.push(Mutation::Set {
            cell: address.cell,
            bytes: Bytes::copy_from_slice(&buffer),
        });
        Ok(())
    }

    fn clear<T: CellType>(&mut self, address: CellAddress<L, T>) {
        self.journal.push(Mutation::Clear { cell: address.cell });
    }

    fn clear_collection(&mut self)
    where
        L: CollectionLayout,
    {
        self.journal.push(Mutation::Reset {
            sections: L::SECTIONS,
        });
    }
}

/// Returns the last staged answer without a coordinate copy.
fn staged(journal: &[Mutation], cell: CellRef<'_>) -> Option<Staged> {
    journal.iter().rev().find_map(|mutation| match mutation {
        Mutation::Set {
            cell: staged,
            bytes,
        } if staged.as_ref() == cell => Some(Staged::Present(bytes.clone())),
        Mutation::Clear { cell: staged } if staged.as_ref() == cell => Some(Staged::Absent),
        // A staged reset hides every cell of the layout, so a read
        // after `clear_collection` sees the same absence the merge will
        // replay.
        Mutation::Reset { sections } if sections.contains(&cell.section) => Some(Staged::Absent),
        _ => None,
    })
}
