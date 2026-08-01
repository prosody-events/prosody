//! The collection-operation core: one public collection invocation is one
//! typed scoped operation.
//!
//! A collection handle owns a [`Collection<S, L>`] — a session already
//! validated against one registered collection, branded with that collection's
//! zero-sized layout `L`. Calling a handle method opens exactly one scoped
//! operation over that binding, runs the authored algorithm against it, and
//! closes it. The operation is where admission, backend mechanics, and raw
//! bytes live; the authored body sees only the read and write collection
//! commands, over typed keys and application values.
//!
//! # Two engines, one command API
//!
//! The session type selects the engine at compile time through the sealed
//! `S::Engine` projection. The owner engine drives the per-event session: its
//! read state is the session gate's read permit, its write state the mutate
//! permit, and its reads see this event's overlay. The published-reader engine
//! drives a `StateReader` operation: its read state is that invocation's own
//! source selection, and it has no write engine at all, so a reader handle
//! cannot express a mutation. Collection code never branches on which.
//!
//! # The byte boundary
//!
//! Cell bytes are spoken here and in the descriptor's cell view only: one
//! decode/encode pair is what every collection's values pass through, and a
//! command's typed key is lowered to its
//! order-preserving coordinate before any engine sees it. Collection code
//! names no `Bytes`, `CellKey`, permit, or source.

use crate::codec::{Codec, SerializeBufGuard};
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Section};
use crate::state::descriptor::{
    CellCodecError, CellResolver, CellStateError, CellType, ContextOf, FromSession, KeyOf,
    ResolvedOf, StructuralIdentity, WriteOf,
};
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::session::CellRead;
use crate::state::{StateName, StateType, StoreOutcome};
use bytes::Bytes;
use educe::Educe;
use std::future::Future;
use std::marker::PhantomData;

mod operation;
pub(crate) mod owner;

#[cfg(test)]
mod tests;

pub(crate) use operation::{
    JOURNAL_INLINE, Mutation, MutationJournal, ReadOperation, WriteOperation,
};
pub(crate) use prosody_macros::{collection_layout, collection_methods};

/// Framework-internal engine authority: admission, raw cell commands, mutation
/// replay, and durable repair.
///
/// These traits are `pub` only so the public session bounds above them do not
/// trip `private_bounds`; the module's own `pub(crate)` visibility is the seal.
/// Downstream code can project and bound `S::Engine`, but cannot name the
/// traits, so their associated functions are uncallable and no outside type can
/// claim to have acquired owner admission. Putting callable commands on a
/// private *supertrait* of a public trait would not seal them — Rust permits
/// those calls through the public subtrait — so every command carrying
/// authority lives one layer below anything a caller can name.
pub(crate) mod sealed {
    use super::{
        Bytes, CellKey, MutationJournal, StateAccessError, StateName, StateType, StoreOutcome,
    };
    use std::future::Future;
    use std::ops::DerefMut;

    /// The engine a session type binds. Selecting the engine is what makes
    /// owner and published-reader behavior a compile-time choice rather than a
    /// runtime branch.
    pub trait Session: Sized {
        /// This session's engine.
        type Engine: ReadEngine<Self>;
    }

    /// A session whose engine can also mutate. `WriteOperation` exists only
    /// for these, so "mutate through read admission" is unrepresentable.
    pub trait WritableSession: Session<Engine: WriteEngine<Self>> {}

    /// The read half of one engine: how an invocation acquires its state and
    /// how it reads one cell's visible committed bytes through it.
    pub trait ReadEngine<S: ?Sized> {
        /// The per-invocation state: the owner's gate permit, or the reader's
        /// operation-local source selection.
        type ReadInner<'a>: Send
        where
            S: 'a;

        /// Acquires this invocation's read state.
        fn begin_read(session: &S) -> impl Future<Output = Self::ReadInner<'_>> + Send;

        /// Reads one cell's visible committed bytes, advancing the
        /// invocation's state (the reader pins its source here).
        fn read_point(
            session: &S,
            inner: &mut Self::ReadInner<'_>,
            state_type: StateType,
            name: &StateName,
            cell: &CellKey,
        ) -> impl Future<Output = Result<Option<Bytes>, StateAccessError>> + Send;
    }

    /// The write half of one engine: admission, the final fence, journal
    /// replay, and the mid-handler durable pair.
    pub trait WriteEngine<S: ?Sized>: ReadEngine<S> {
        /// The per-invocation write state. `DerefMut` to the read state is the
        /// one-way relation from write admission to the read admission it
        /// subsumes — which is how a write operation reuses the read driver
        /// unchanged, with no runtime variant and no inverse conversion.
        type WriteInner<'a>: DerefMut<Target = Self::ReadInner<'a>> + Send
        where
            S: 'a;

        /// Acquires this invocation's write state.
        ///
        /// # Errors
        ///
        /// Whatever the engine's admission refuses — for the owner, a stale
        /// attempt, a closed session, or termination.
        fn begin_write(
            session: &S,
        ) -> impl Future<Output = Result<Self::WriteInner<'_>, StateAccessError>> + Send;

        /// Rechecks admission at the end of the invocation, immediately before
        /// replay.
        ///
        /// # Errors
        ///
        /// As [`Self::begin_write`].
        fn validate_write(
            session: &S,
            inner: &Self::WriteInner<'_>,
        ) -> Result<(), StateAccessError>;

        /// Replays a validated journal into the event overlay. Synchronous and
        /// infallible by contract: there is no suspension point between the
        /// fence and the last staged mutation.
        fn apply(
            session: &S,
            state_type: StateType,
            name: &StateName,
            inner: &Self::WriteInner<'_>,
            journal: MutationJournal,
        );

        /// Durably commits the collection's buffered changes mid-invocation.
        ///
        /// # Errors
        ///
        /// Admission refusal, or a store failure.
        fn commit(
            session: &S,
            state_type: StateType,
            name: &StateName,
        ) -> impl Future<Output = Result<StoreOutcome, StateAccessError>> + Send;

        /// Discards the collection's buffered changes mid-invocation.
        fn rollback(
            session: &S,
            state_type: StateType,
            name: &StateName,
        ) -> impl Future<Output = StoreOutcome> + Send;
    }
}

/// Seals [`CollectionSpec`](crate::state::descriptor::CollectionSpec): the
/// layout macro emits this marker, so a collection kind cannot exist without a
/// declared durable layout.
pub(crate) mod sealed_spec {
    /// The seal marker; see the module item's doc.
    pub trait SealedSpec {}
}

/// A session that can be bound to a collection.
///
/// The public half of the session surface: a harmless loader query plus the
/// sealed engine projection. Implementing it requires implementing the sealed
/// session trait one layer below, which no downstream crate can name — so a
/// downstream crate can bound on `StateSession` but can never supply one.
///
/// `Engine` is a reserved associated-item name across every public session
/// bound: adding a second nameable trait that also declares `Engine` would
/// break the bare `S::Engine` projection irrecoverably, since the
/// disambiguating form is unwritable outside this crate.
pub trait StateSession: sealed::Session + Clone + Send + Sync + 'static {
    /// Opaque per-session capability slot. The keyed-state machinery never
    /// interprets it; a
    /// [`crate::state::descriptor::CellResolver`] living outside
    /// `src/state` reads it from the session at resolve time.
    type Loader: Clone + Send + Sync + 'static;

    /// Returns the session's capability slot for a resolver to read.
    fn loader(&self) -> &Self::Loader;
}

/// A session that can also mutate its collections. The write scope and every
/// mutating command are reachable only through this bound, so a read-only
/// session's handle has no mutation to refuse at runtime.
pub trait WritableStateSession: StateSession + sealed::WritableSession {}

/// One collection's declared durable layout: the canonical section set a
/// whole-layout reset covers, and the generated descriptor the frozen layout
/// tests pin.
///
/// Emitted by
/// [`collection_layout!`](crate::state::collection::collection_layout), never
/// hand-written: `SECTIONS` must be every active *and* reserved id, so a family
/// added or removed by hand could otherwise silently leave stale rows behind.
pub(crate) trait CollectionLayout {
    /// Every active and reserved section, id-sorted.
    const SECTIONS: &'static [Section];

    /// One entry per active family, id-sorted.
    const DESCRIPTOR: &'static [LayoutEntry];

    /// Ids of removed families, id-sorted. They stay in [`Self::SECTIONS`] so
    /// a reset keeps erasing their legacy rows, and can never be reused.
    const RESERVED: &'static [i8];
}

/// One active family in a generated layout descriptor: its durable id and the
/// key and payload format tokens its cells are addressed and encoded with.
///
/// No type can compare this crate against yesterday's schema, so the
/// descriptor is what a frozen test pins instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LayoutEntry {
    id: i8,
    key_format: &'static str,
    format: &'static str,
}

impl LayoutEntry {
    /// Builds one descriptor entry. Called only from generated code.
    pub(crate) const fn new(id: i8, key_format: &'static str, format: &'static str) -> Self {
        Self {
            id,
            key_format,
            format,
        }
    }

    /// The family's durable section id.
    pub(crate) const fn id(self) -> i8 {
        self.id
    }

    /// The family's durable key-encoding token.
    pub(crate) const fn key_format(self) -> &'static str {
        self.key_format
    }

    /// The family's durable payload-encoding token.
    pub(crate) const fn format(self) -> &'static str {
        self.format
    }
}

/// Compile-time string equality, so a collection can freeze its layout
/// descriptor's format tokens as a `const` assertion rather than a test that
/// could be skipped.
pub(crate) const fn same_token(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// A declared cell family: the layout it belongs to, the durable section it
/// addresses, and the cell type it stores.
///
/// A command's family argument is checked against the operation's own layout,
/// so a family borrowed from another collection does not compile even when its
/// section and cell type happen to match. Tokens carry no collection or
/// session identity, allocate nothing, and are mintable only by the layout
/// macro — an undeclared section is unaddressable.
pub(crate) struct CellFamily<L, T> {
    section: Section,
    _type: PhantomData<fn() -> (L, T)>,
}

// Manual, so a family token does not inherit `L: Copy` / `T: Copy` bounds from
// a derive.
impl<L, T> Copy for CellFamily<L, T> {}

impl<L, T> Clone for CellFamily<L, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<L, T> CellFamily<L, T> {
    /// Declares the family at durable section `id`. Called only from generated
    /// layout code.
    pub(crate) const fn declare(id: i8) -> Self {
        Self {
            section: Section::new(id),
            _type: PhantomData,
        }
    }

    /// The durable section this family addresses.
    const fn section(self) -> Section {
        self.section
    }
}

/// A session bound to exactly one registered collection with layout `L`.
///
/// Construction is the validation: the owner path checks registration and
/// structural identity, and the published-reader path consumes the validation
/// its source acquisition already performed. Because the whole value is built
/// at once from private fields, there is no binding token that could be paired
/// with another session, and a collection for another session type or layout is
/// a different type.
///
/// The binding deliberately does *not* capture the collection's
/// [`CollectionDef`](crate::state::registry::CollectionDef): every
/// configuration query still goes to the session per call. Capturing it waits
/// for the first caller that reads configuration inside a scoped operation.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct Collection<S, L> {
    session: S,
    state_type: StateType,
    name: StateName,
    _layout: PhantomData<fn() -> L>,
}

impl<S, L> Collection<S, L> {
    /// The collection's canonical name — the operation-span field every
    /// handle method records.
    pub(crate) fn name(&self) -> &StateName {
        &self.name
    }

    /// The bound session.
    fn session(&self) -> &S {
        &self.session
    }

    /// The collection's state namespace.
    fn state_type(&self) -> StateType {
        self.state_type
    }

    /// Bridge: the binding's parts, for a collection kind that still builds a
    /// `CellScope` from them. Dies with the old cell-command surface, once
    /// every kind runs through the engine.
    pub(in crate::state) fn into_parts(self) -> (S, StateType, StateName) {
        (self.session, self.state_type, self.name)
    }
}

impl<S: CellRead, L> Collection<S, L> {
    /// Validates `session` against the registered collection named `name` and
    /// binds it. The sole owner-side constructor.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session,
    /// [`StateAccessError::Unregistered`] for an unknown name, or
    /// [`StateAccessError::IdentityMismatch`] when the registered identity
    /// differs from the asserted one.
    pub(in crate::state) fn bind(
        session: &S,
        name: &'static str,
        state_type: StateType,
        identity: &StructuralIdentity,
    ) -> Result<Self, StateAccessError> {
        let name = session.verify_state_registration(name, state_type, identity)?;
        Ok(Self {
            session: session.clone(),
            state_type,
            name,
            _layout: PhantomData,
        })
    }
}

impl<S: StateSession, L> Collection<S, L> {
    /// Runs `f` as one scoped read invocation.
    ///
    /// Both of the operation's lifetimes are higher-ranked: the closure must
    /// work for *any* borrow of the operation and for *any* binding lifetime
    /// the operation could have had. Neither is therefore nameable by the
    /// caller, so the operation can be neither returned from the scope nor
    /// stored in a slot declared outside it, and only owned data crosses back
    /// out. Quantifying the binding lifetime too is also what keeps the
    /// resulting future's `Send` provable from a generic caller.
    pub(crate) async fn read<R, F>(&self, f: F) -> R
    where
        F: for<'op, 'scope> AsyncFnOnce(&'op mut ReadOperation<'scope, S, L>) -> R,
    {
        let mut op = ReadOperation::new(self).await;
        f(&mut op).await
    }
}

impl<S: WritableStateSession, L> Collection<S, L> {
    /// Runs `f` as one scoped write invocation.
    ///
    /// Success ends in the consuming merge, which revalidates admission and
    /// then replays the journal with no suspension point. Every other exit —
    /// an authored `Err`, a `?`, or a dropped future — drops the journal
    /// unreplayed, which is what makes an authored method failure-atomic.
    ///
    /// # Errors
    ///
    /// Admission refusal, the merge's final fence, or whatever `f` returns.
    pub(crate) async fn write<R, E, F>(&self, f: F) -> Result<R, E>
    where
        E: From<StateAccessError>,
        F: for<'op, 'scope> AsyncFnOnce(&'op mut WriteOperation<'scope, S, L>) -> Result<R, E>,
    {
        let mut op = WriteOperation::new(self).await?;
        let value = f(&mut op).await?;
        op.merge()?;
        Ok(value)
    }

    /// Durably commits this collection's buffered changes mid-handler.
    ///
    /// # Errors
    ///
    /// Admission refusal, or a store failure.
    pub(crate) async fn commit(&self) -> Result<StoreOutcome, StateAccessError> {
        <S::Engine as sealed::WriteEngine<S>>::commit(&self.session, self.state_type, &self.name)
            .await
    }

    /// Discards this collection's buffered changes mid-handler.
    pub(crate) async fn rollback(&self) -> StoreOutcome {
        <S::Engine as sealed::WriteEngine<S>>::rollback(&self.session, self.state_type, &self.name)
            .await
    }
}

/// The read commands every scoped operation offers. Implemented by both
/// operation types, so one collection algorithm serves the owner session and
/// the published reader.
///
/// Every command takes `&mut self`: one public invocation is one explicit
/// top-to-bottom algorithm, and overlapping commands do not compile. Commands
/// that need concurrency provide it internally, after taking the one borrow.
pub(crate) trait CollectionRead: sealed_ops::CollectionOperation {
    /// The bound session type, which the resolver context is extracted from.
    type Session: StateSession;

    /// The layout brand every family argument is checked against.
    type Layout;

    /// Reads, decodes, and resolves the visible value at `key`.
    ///
    /// # Errors
    ///
    /// An access error from the engine, a codec error (Permanent) when the
    /// cell bytes do not decode, or a resolution error from the resolver.
    fn get<T>(
        &mut self,
        family: CellFamily<Self::Layout, T>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>> + Send
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, Self::Session>;
}

/// The mutation commands, implemented only by the write operation.
///
/// Both are synchronous: encoding and staging perform no I/O, so representing
/// them as futures would add suspension points without work. `set` is fallible
/// only at typed encoding; a point clear cannot fail after admission.
pub(crate) trait CollectionWrite: CollectionRead {
    /// Stages a write of `value` at `key`.
    ///
    /// # Errors
    ///
    /// A codec error (Permanent) when the value fails to encode.
    fn set<T: CellType>(
        &mut self,
        family: CellFamily<Self::Layout, T>,
        key: &KeyOf<T>,
        value: WriteOf<'_, T>,
    ) -> Result<(), CellStateError<CellCodecError<T>>>;

    /// Stages a clear of the cell at `key`.
    fn clear<T: CellType>(&mut self, family: CellFamily<Self::Layout, T>, key: &KeyOf<T>);
}

/// Seals the author-facing command traits: they are implemented for the two
/// operation types and nothing else, so a helper bounded by them can only ever
/// receive real admission.
pub(crate) mod sealed_ops {
    /// The seal marker; see the module item's doc.
    pub trait CollectionOperation {}
}

/// The full cell address for `key` in `family` — the sole place a collection's
/// typed key is lowered to its order-preserving coordinate.
fn cell_key<L, T: CellType>(family: CellFamily<L, T>, key: &KeyOf<T>) -> CellKey {
    CellKey {
        section: family.section(),
        coordinate: <T::Key as OrderedKeyCodec>::encode(key),
    }
}

/// Decodes and resolves raw cell bytes into the exposed application value.
///
/// Written in the desugared `-> impl Future + Send` form so the `Send` bound is
/// *stated* rather than inferred: the future holds the resolver's
/// [`ContextOf`] projection across the resolve await, which rustc cannot infer
/// `Send` for through an `async fn`.
///
/// # Errors
///
/// A codec error (Permanent) when the bytes do not decode, or a resolution
/// error from the resolver.
pub(in crate::state) fn resolve_cell<'a, S, T>(
    session: &'a S,
    bytes: Bytes,
) -> impl Future<Output = Result<ResolvedOf<T>, CellStateError<CellCodecError<T>>>> + Send + 'a
where
    S: StateSession,
    T: CellType,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    let stored = decode_cell::<T::Codec>(bytes);
    async move {
        let stored = stored.map_err(CellStateError::Codec)?;
        let ctx = <ContextOf<'a, T> as FromSession<'a, S>>::from_session(session);
        Ok(<T::Resolver as CellResolver>::resolve(ctx, stored).await?)
    }
}

/// Decodes a cell's bytes as `C::Payload`. Parses in place when the `Bytes` is
/// uniquely owned (zero-copy, the production path — every backend decode mints
/// a fresh `Bytes`); falls back to a copy for a shared clone (the in-memory
/// test backend). The single decode path every typed cell read shares.
pub(in crate::state) fn decode_cell<C: Codec>(cell: Bytes) -> Result<C::Payload, C::Error> {
    match cell.try_into_mut() {
        Ok(mut buf) => C::with_cached_local(|codec| codec.deserialize(&mut buf)),
        Err(cell) => {
            let mut buf = cell.to_vec();
            C::with_cached_local(|codec| codec.deserialize(&mut buf))
        }
    }
}

/// Encodes `payload` into the pooled, reusable serialize buffer, returning the
/// guard so the caller hands its bytes on before the guard drops (returning the
/// buffer to the pool). The guard owns its buffer, so it is `Send` and rides a
/// write across an await. The single encode path every typed cell write shares.
pub(in crate::state) fn encode_cell<C: Codec>(
    payload: C::Payload,
) -> Result<SerializeBufGuard, C::Error> {
    let mut buf = SerializeBufGuard::acquire();
    C::with_cached_local(|codec| codec.serialize(payload, &mut buf))?;
    Ok(buf)
}

/// Guards every cell operation on either session kind: a session whose
/// partition is shutting down, whose event is cancelled, or whose pinned
/// attempt epoch no longer matches the live one (a handle or stream leaked past
/// its dispatch attempt) refuses state access with
/// [`StateAccessError::Terminated`]. Only the per-event session can be in any
/// of those states, so the guard is vacuous on the published reader.
///
/// # Errors
///
/// [`StateAccessError::Terminated`], as above.
pub(in crate::state) fn ensure_live<S>(session: &S) -> Result<(), StateAccessError>
where
    S: CellRead,
{
    if session.is_terminated() || !session.attempt_current() {
        return Err(StateAccessError::Terminated);
    }
    Ok(())
}
