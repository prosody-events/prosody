//! The two scoped operation types and the invocation-local write journal.

use super::stream::{PlanBase, read_keys_presence};
use super::{
    CellFamily, Collection, CollectionLayout, CollectionRead, CollectionWrite, CoordinatePlan,
    RangePlan, StateSession, WritableStateSession, cell_key, encode_cell, resolve_batch,
    resolve_cell, sealed, sealed_ops,
};
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Coordinate, Direction, ScanEdge, Section};
use crate::state::descriptor::{
    CellCodecError, CellResolver, CellStateError, CellType, ContextOf, FromSession, KeyOf,
    ResolvedOf, WriteOf,
};
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state::{StateName, StateType};
use bytes::Bytes;
use smallvec::SmallVec;
use std::future::Future;
use std::num::NonZeroUsize;

/// Inline capacity of one invocation's mutation journal.
///
/// Four is the current maximum across the built-in collections: a Deque push
/// stages one entry, at most `TRIM_MAX` point clears, and one bounds set; Map
/// needs two, Value one. Each collection *declares* its own maximum beside its
/// layout, and a compile-time assertion there pins that declaration against
/// this budget — so widening a collection past four is a build error, not a
/// silent steady-state allocation. A spill is still semantically correct —
/// Rust cannot derive a sound maximum from an arbitrary async body, so the
/// inline bound is an allocation budget, never a limit.
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
    /// Stage an absence over every section of the collection's layout. One
    /// payload-free entry: the sections are the layout's own static set, read
    /// at the command, where the layout is still in scope.
    Reset {
        /// Every active and reserved section of the declaring layout.
        sections: &'static [Section],
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
/// caller cannot name the operation's borrow lifetime. The caller can therefore
/// neither return the operation from the scope nor store it in a slot declared
/// outside it. Only owned data crosses the boundary.
///
/// The type is neither `Clone` nor `Copy`. Its only constructor acquires engine
/// state and builds the complete value, so no API pairs an independently
/// obtained guard, permit, or inner value with a collection.
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

    /// Plans a managed point-get stream over `keys` in `family`, in the given
    /// order. Freezing this invocation's engine state into the plan is what
    /// lets each chunk resume on the same source (reader) or reacquire the gate
    /// (owner) without re-running the planning command.
    pub(crate) fn coordinates<T: CellType>(
        &self,
        family: CellFamily<L, T>,
        keys: Vec<KeyOf<T>>,
    ) -> CoordinatePlan<S, T> {
        CoordinatePlan::new(self.plan_base(family.section()), keys)
    }

    /// Plans a managed durable range over the whole of `family`'s section, in
    /// `dir` order — the fallback for a collection with no coordinate
    /// enumeration to point-get.
    pub(crate) fn range<T: CellType>(
        &self,
        family: CellFamily<L, T>,
        dir: Direction,
    ) -> RangePlan<S, T> {
        RangePlan::new(
            self.plan_base(family.section()),
            ScanEdge::Unbounded,
            dir,
            ScanEdge::Unbounded,
            None,
        )
    }

    /// Plans a managed durable range over one inclusive typed span of
    /// `family`'s section. The plan walks `[start, end]` in `dir` order and
    /// yields at most `limit` cells.
    ///
    /// A collection with a contiguous coordinate window takes this plan instead
    /// of an enumeration of every coordinate in the window. `start` and `end`
    /// are direction-relative, exactly as
    /// [`Scan`](crate::state::cell_key::Scan) defines them. Only inclusive
    /// edges exist here: a collection that knows its window also knows both of
    /// its occupied endpoints.
    pub(crate) fn range_within<T: CellType>(
        &self,
        family: CellFamily<L, T>,
        start: &KeyOf<T>,
        dir: Direction,
        end: &KeyOf<T>,
        limit: NonZeroUsize,
    ) -> RangePlan<S, T> {
        RangePlan::new(
            self.plan_base(family.section()),
            ScanEdge::Included(<T::Key as OrderedKeyCodec>::encode(start)),
            dir,
            ScanEdge::Included(<T::Key as OrderedKeyCodec>::encode(end)),
            Some(limit),
        )
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
                // A staged reset hides every cell of the layout, so a read
                // after `clear_collection` sees the same absence the merge will
                // replay.
                Mutation::Reset { sections } if sections.contains(&cell.section) => {
                    Some(Staged::Absent)
                }
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

    fn name(&self) -> &StateName {
        self.collection.name()
    }

    fn has_ttl(&self) -> bool {
        self.collection.def().ttl.is_some()
    }

    fn keyset_limit(&self) -> usize {
        self.collection.def().keyset_limit
    }

    fn capacity(&self) -> Option<NonZeroUsize> {
        self.collection.def().capacity
    }

    fn get_many<T>(
        &mut self,
        family: CellFamily<L, T>,
        keys: &[KeyOf<T>],
    ) -> impl Future<
        Output = Result<CellBuffer<Option<ResolvedOf<T>>>, CellStateError<CellCodecError<T>>>,
    > + Send
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let section = family.section();
        let Self { collection, inner } = self;
        let session = collection.session();
        read_keys_resolved::<S, T>(
            session,
            inner,
            collection.state_type(),
            collection.name(),
            section,
            keys,
        )
    }

    fn contains<T: CellType>(
        &mut self,
        family: CellFamily<L, T>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<bool, StateAccessError>> + Send {
        let section = family.section();
        let coordinate = <T::Key as OrderedKeyCodec>::encode(key);
        let Self { collection, inner } = self;
        let session = collection.session();
        async move {
            read_presence(
                session,
                inner,
                collection.state_type(),
                collection.name(),
                section,
                coordinate,
            )
            .await
        }
    }

    fn contains_many<T: CellType>(
        &mut self,
        family: CellFamily<L, T>,
        keys: &[KeyOf<T>],
    ) -> impl Future<Output = Result<CellBuffer<bool>, StateAccessError>> + Send {
        let section = family.section();
        let Self { collection, inner } = self;
        read_keys_presence::<S, T>(
            collection.session(),
            inner,
            collection.state_type(),
            collection.name(),
            section,
            keys,
        )
    }

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

    fn name(&self) -> &StateName {
        self.collection.name()
    }

    fn has_ttl(&self) -> bool {
        self.collection.def().ttl.is_some()
    }

    fn keyset_limit(&self) -> usize {
        self.collection.def().keyset_limit
    }

    fn capacity(&self) -> Option<NonZeroUsize> {
        self.collection.def().capacity
    }

    fn get_many<T>(
        &mut self,
        family: CellFamily<L, T>,
        keys: &[KeyOf<T>],
    ) -> impl Future<
        Output = Result<CellBuffer<Option<ResolvedOf<T>>>, CellStateError<CellCodecError<T>>>,
    > + Send
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let section = family.section();
        // Fold the journal per position first: a staged answer never reaches
        // the engine, and only the journal-silent positions enter the batch.
        let slots: CellBuffer<Slot> = keys
            .iter()
            .map(|key| match self.staged(&cell_key(family, key)) {
                Some(Staged::Present(bytes)) => Slot::Answered(Some(bytes)),
                Some(Staged::Absent) => Slot::Answered(None),
                None => Slot::Pending(<T::Key as OrderedKeyCodec>::encode(key)),
            })
            .collect();
        let Self {
            collection, inner, ..
        } = self;
        let session = collection.session();
        async move {
            let bytes = batched_bytes::<S>(
                session,
                &mut **inner,
                collection.state_type(),
                collection.name(),
                section,
                slots,
            )
            .await?;
            resolve_batch::<S, T>(session, bytes).await
        }
    }

    fn contains<T: CellType>(
        &mut self,
        family: CellFamily<L, T>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<bool, StateAccessError>> + Send {
        let cell = cell_key(family, key);
        let staged = self.staged(&cell);
        let Self {
            collection, inner, ..
        } = self;
        let session = collection.session();
        async move {
            match staged {
                Some(Staged::Present(_)) => Ok(true),
                Some(Staged::Absent) => Ok(false),
                None => {
                    read_presence(
                        session,
                        &mut **inner,
                        collection.state_type(),
                        collection.name(),
                        cell.section,
                        cell.coordinate,
                    )
                    .await
                }
            }
        }
    }

    fn contains_many<T: CellType>(
        &mut self,
        family: CellFamily<L, T>,
        keys: &[KeyOf<T>],
    ) -> impl Future<Output = Result<CellBuffer<bool>, StateAccessError>> + Send {
        let section = family.section();
        let mut slots = CellBuffer::with_capacity(keys.len());
        let mut pending = CellBuffer::with_capacity(keys.len());
        for key in keys {
            match self.staged(&cell_key(family, key)) {
                Some(Staged::Present(_)) => slots.push(Some(true)),
                Some(Staged::Absent) => slots.push(Some(false)),
                None => {
                    slots.push(None);
                    pending.push(<T::Key as OrderedKeyCodec>::encode(key));
                }
            }
        }
        let Self {
            collection, inner, ..
        } = self;
        async move {
            let expected = pending.len();
            let mut answers = CellBuffer::with_capacity(expected);
            for batch in CoordinateBatch::chunks(pending) {
                answers.extend(
                    <S::Engine as sealed::ReadEngine<S>>::read_presence_batch(
                        collection.session(),
                        &mut **inner,
                        collection.state_type(),
                        collection.name(),
                        section,
                        &batch,
                    )
                    .await?,
                );
            }
            let received = answers.len();
            let mut answers = answers.into_iter();
            slots
                .into_iter()
                .map(|slot| slot.or_else(|| answers.next()))
                .collect::<Option<CellBuffer<bool>>>()
                .ok_or_else(|| StateAccessError::misaligned_batch(received, expected))
        }
    }

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
        // The owned cell moves into the future. Only the cell crosses the
        // engine await, never the borrowed key.
        async move { read_staged::<S, T, L>(collection, &mut **inner, staged, &cell).await }
    }
}

/// Reads one coordinate through the engine's presence batch.
async fn read_presence<S: StateSession>(
    session: &S,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    state_type: StateType,
    name: &StateName,
    section: Section,
    coordinate: Coordinate,
) -> Result<bool, StateAccessError> {
    let batch = CoordinateBatch::one(coordinate);
    let answers = <S::Engine as sealed::ReadEngine<S>>::read_presence_batch(
        session, inner, state_type, name, section, &batch,
    )
    .await?;
    answers
        .first()
        .copied()
        .ok_or_else(|| StateAccessError::misaligned_batch(answers.len(), batch.len()))
}

impl<S: WritableStateSession, L> CollectionWrite for WriteOperation<'_, S, L> {
    fn take<T>(
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
            collection,
            inner,
            journal,
        } = self;
        async move {
            let value = read_staged::<S, T, L>(collection, &mut **inner, staged, &cell).await?;
            // Only a completed read stages the clear. A read error leaves the
            // journal silent. `Ok(None)` still clears the addressed residue.
            journal.push(Mutation::Clear { cell });
            Ok(value)
        }
    }

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

    fn clear_collection(&mut self)
    where
        L: CollectionLayout,
    {
        self.journal.push(Mutation::Reset {
            sections: L::SECTIONS,
        });
    }
}

/// The typed point read of a write invocation. It returns the journal's answer
/// when the journal has one. If not, it does one engine point read. Both paths
/// then run the shared decode and resolve.
///
/// This function uses the desugared `-> impl Future + Send` form for the reason
/// [`resolve_cell`] states: the future holds the resolver's [`ContextOf`]
/// projection across the resolve await.
///
/// # Errors
///
/// An access error from the engine, a codec error (Permanent), or a resolution
/// error.
fn read_staged<'a, S, T, L>(
    collection: &'a Collection<S, L>,
    inner: &'a mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    staged: Option<Staged>,
    cell: &'a CellKey,
) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>> + Send + 'a
where
    S: StateSession,
    T: CellType,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    let session = collection.session();
    async move {
        let bytes = match staged {
            Some(Staged::Present(bytes)) => Some(bytes),
            Some(Staged::Absent) => None,
            // The write state derefs to the read state, so the write operation
            // reuses the read driver unchanged.
            None => {
                <S::Engine as sealed::ReadEngine<S>>::read_point(
                    session,
                    inner,
                    collection.state_type(),
                    collection.name(),
                    cell,
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

/// One position of an aligned batch read: either already answered from the
/// invocation's journal, or awaiting the engine at its coordinate.
enum Slot {
    /// The journal already answers this position.
    Answered(Option<Bytes>),
    /// The engine must read this coordinate.
    Pending(Coordinate),
}

/// Fills every pending slot from the engine and returns the answers aligned to
/// `slots` — the journal-aware batch read a write invocation performs, where
/// only the journal-silent positions reach the engine. It reads them through
/// [`read_coordinate_bytes`].
async fn batched_bytes<S: StateSession>(
    session: &S,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    state_type: StateType,
    name: &StateName,
    section: Section,
    slots: CellBuffer<Slot>,
) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
    let pending: CellBuffer<Coordinate> = slots
        .iter()
        .filter_map(|slot| match slot {
            Slot::Pending(coordinate) => Some(coordinate.clone()),
            Slot::Answered(_) => None,
        })
        .collect();
    let expected = pending.len();
    let answers =
        read_coordinate_bytes(session, inner, state_type, name, section, pending, expected).await?;
    let mut answers = answers.into_iter();
    Ok(slots
        .into_iter()
        .map(|slot| match slot {
            Slot::Answered(bytes) => bytes,
            // The engine answers every batched position in order, so the
            // answers line up with the pending slots.
            Slot::Pending(_) => answers.next().flatten(),
        })
        .collect())
}

/// Reads visible committed bytes for typed entry reads.
/// The result aligns with `keys` and does not decode values.
///
/// # Errors
///
/// An access error from the engine.
pub(super) async fn read_keys_bytes<S, T>(
    session: &S,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    state_type: StateType,
    name: &StateName,
    section: Section,
    keys: &[KeyOf<T>],
) -> Result<CellBuffer<Option<Bytes>>, StateAccessError>
where
    S: StateSession,
    T: CellType,
{
    // Mapped as a function item, so the lowering carries no closure whose
    // higher-ranked capture would defeat the future's `Send` proof.
    let coordinates = keys.iter().map(<T::Key as OrderedKeyCodec>::encode);
    read_coordinate_bytes(
        session,
        inner,
        state_type,
        name,
        section,
        coordinates,
        keys.len(),
    )
    .await
}

/// Reads `coordinates` from the engine and returns the bytes, index-aligned to
/// the input. `expected` is the coordinate count, known to every caller.
///
/// The read is split into maximal batches and the batches are issued
/// **sequentially**: two repair-capable owner reads over one collection must
/// not overlap.
async fn read_coordinate_bytes<S, I>(
    session: &S,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    state_type: StateType,
    name: &StateName,
    section: Section,
    coordinates: I,
    expected: usize,
) -> Result<CellBuffer<Option<Bytes>>, StateAccessError>
where
    S: StateSession,
    I: IntoIterator<Item = Coordinate>,
{
    let mut bytes: CellBuffer<Option<Bytes>> = SmallVec::with_capacity(expected);
    for batch in CoordinateBatch::chunks(coordinates) {
        bytes.extend(
            <S::Engine as sealed::ReadEngine<S>>::read_batch(
                session, inner, state_type, name, section, &batch,
            )
            .await?,
        );
    }
    debug_assert_eq!(
        bytes.len(),
        expected,
        "batch read answers every input position"
    );
    Ok(bytes)
}

/// [`read_keys_bytes`] plus the typed decode and resolution — the whole of a
/// journal-free batch get, performed under the invocation's admission.
///
/// # Errors
///
/// An access error from the engine, a codec error (Permanent), or a resolution
/// error.
pub(super) async fn read_keys_resolved<S, T>(
    session: &S,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    state_type: StateType,
    name: &StateName,
    section: Section,
    keys: &[KeyOf<T>],
) -> Result<CellBuffer<Option<ResolvedOf<T>>>, CellStateError<CellCodecError<T>>>
where
    S: StateSession,
    T: CellType,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    let bytes = read_keys_bytes::<S, T>(session, inner, state_type, name, section, keys).await?;
    resolve_batch::<S, T>(session, bytes).await
}
