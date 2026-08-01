//! The two scoped operation types and the invocation-local write journal.

use super::{
    CellFamily, Collection, CollectionRead, CollectionWrite, StateSession, WritableStateSession,
    cell_key, encode_cell, resolve_cell, sealed, sealed_ops,
};
use crate::state::access::StateAccessError;
use crate::state::cell_key::CellKey;
use crate::state::descriptor::{
    CellCodecError, CellResolver, CellStateError, CellType, ContextOf, FromSession, KeyOf,
    ResolvedOf, WriteOf,
};
use bytes::Bytes;
use smallvec::SmallVec;
use std::future::Future;

/// Inline capacity of one invocation's mutation journal.
///
/// Four is the current maximum across the built-in collections: a Deque push
/// stages one entry, at most `TRIM_MAX` point clears, and one bounds set; Map
/// needs two, Value one. Each collection proves its own statically bounded
/// maximum fits with a compile-time assertion beside its layout, so a method or
/// bound change cannot silently add a steady-state allocation. A spill is still
/// semantically correct — Rust cannot derive a sound maximum from an arbitrary
/// async body, so the inline bound is an allocation budget, never a limit.
pub const JOURNAL_INLINE: usize = 4;

/// One invocation's staged mutations, in authored order.
pub type MutationJournal = SmallVec<[Mutation; JOURNAL_INLINE]>;

/// One staged mutation. `Set` carries its already-encoded payload: the single
/// owned copy the dirty store requires either way, made once at the command and
/// moved into the overlay at merge.
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
}

/// What this invocation has already staged for one cell — the journal's answer
/// to a read, distinct from "the journal says nothing" (which falls through to
/// the engine).
enum Staged {
    /// The cell was written in this invocation.
    Present(Bytes),
    /// The cell was cleared in this invocation.
    Absent,
}

/// One admitted read invocation, valid only inside the scope that created it.
///
/// The scope lends `&mut ReadOperation` through a higher-ranked closure, so the
/// operation's borrow lifetime is unnameable by the caller: the operation can
/// be neither returned from the scope nor stored in a slot declared outside it,
/// and only owned data crosses the boundary. The type is neither `Clone` nor
/// `Copy`, and its only constructor acquires engine state and builds the
/// complete value — there is no API pairing an independently obtained guard,
/// permit, or inner value with a collection.
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
    /// Taking `self` is the invariant, not a convenience — a merged operation
    /// is moved, so "merge, then keep writing" is a use-after-move the compiler
    /// rejects rather than a runtime state check.
    ///
    /// The held write admission excludes the settle boundary's close and the
    /// attempt boundary's reset between the fence and the replay, so no
    /// partially replayed invocation is observable. Termination is *sampled*
    /// at the fence: teardown is ungated by design, and that residual is owned
    /// by [`EventStateScope`](crate::state::manager::EventStateScope).
    ///
    /// # Errors
    ///
    /// The final fence's refusal — a stale attempt, a closed session, or
    /// termination — in which case nothing is replayed.
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

    /// This invocation's staged view of `cell`, or `None` when the journal
    /// says nothing about it.
    ///
    /// Reverse order is what gives staged mutations ordinary last-write-wins
    /// and read-your-writes semantics; forward replay at merge reproduces
    /// exactly the same result.
    fn staged(&self, cell: &CellKey) -> Option<Staged> {
        self.journal
            .iter()
            .rev()
            .find_map(|mutation| match mutation {
                Mutation::Set {
                    cell: staged,
                    bytes,
                } if staged == cell => Some(Staged::Present(bytes.clone())),
                Mutation::Clear { cell: staged } if staged == cell => Some(Staged::Absent),
                _ => None,
            })
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

impl<S: StateSession, L> CollectionRead for ReadOperation<'_, S, L> {
    type Layout = L;
    type Session = S;

    fn get<T>(
        &mut self,
        family: CellFamily<L, T>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>> + Send
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        // The key is lowered before the async block, so only the owned
        // coordinate crosses the engine await.
        let cell = cell_key(family, key);
        let Self { collection, inner } = self;
        let session = collection.session();
        async move {
            let bytes = <S::Engine as sealed::ReadEngine<S>>::read_point(
                session,
                inner,
                collection.state_type(),
                collection.name(),
                &cell,
            )
            .await?;
            match bytes {
                Some(bytes) => Ok(Some(resolve_cell::<S, T>(session, bytes).await?)),
                None => Ok(None),
            }
        }
    }
}

impl<S: WritableStateSession, L> CollectionRead for WriteOperation<'_, S, L> {
    type Layout = L;
    type Session = S;

    fn get<T>(
        &mut self,
        family: CellFamily<L, T>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>> + Send
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let cell = cell_key(family, key);
        let staged = self.staged(&cell);
        let Self {
            collection, inner, ..
        } = self;
        let session = collection.session();
        async move {
            let bytes = match staged {
                Some(Staged::Present(bytes)) => Some(bytes),
                Some(Staged::Absent) => None,
                // The write state derefs to the read state, so the write
                // operation reuses the read driver unchanged.
                None => {
                    <S::Engine as sealed::ReadEngine<S>>::read_point(
                        session,
                        &mut **inner,
                        collection.state_type(),
                        collection.name(),
                        &cell,
                    )
                    .await?
                }
            };
            match bytes {
                Some(bytes) => Ok(Some(resolve_cell::<S, T>(session, bytes).await?)),
                None => Ok(None),
            }
        }
    }
}

impl<S: WritableStateSession, L> CollectionWrite for WriteOperation<'_, S, L> {
    fn set<T: CellType>(
        &mut self,
        family: CellFamily<L, T>,
        key: &KeyOf<T>,
        value: WriteOf<'_, T>,
    ) -> Result<(), CellStateError<CellCodecError<T>>> {
        let cell = cell_key(family, key);
        let stored = <T::Resolver as CellResolver>::stored_from(value);
        let buffer = encode_cell::<T::Codec>(stored).map_err(CellStateError::Codec)?;
        self.journal.push(Mutation::Set {
            cell,
            bytes: Bytes::copy_from_slice(&buffer),
        });
        Ok(())
    }

    fn clear<T: CellType>(&mut self, family: CellFamily<L, T>, key: &KeyOf<T>) {
        self.journal.push(Mutation::Clear {
            cell: cell_key(family, key),
        });
    }
}
