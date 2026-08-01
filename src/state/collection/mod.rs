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
//! This module speaks cell bytes, and no other module does. One decode/encode
//! pair carries every collection's values. A command lowers its typed key to
//! the order-preserving coordinate before any engine sees the key. Collection
//! code names no `Bytes`, `CellKey`, permit, or source.
//!
//! # Mid-handler durability
//!
//! Every collection handle exposes `commit()` and `rollback()` — Value, Map,
//! and Deque all do, and future collection kinds must too. This is their one
//! contract; the handles' docs link back here.
//!
//! `commit()` durably commits the collection's buffered changes mid-handler, so
//! they survive a restart after failure. This is why it exists: a complex or
//! large handler (a materialization handler fanning one message into thousands
//! of writes) commits incremental progress and resumes from it on retry or
//! redelivery instead of starting from scratch. Handler idempotence across the
//! resume is the contract.
//!
//! Every currently-buffered op of the collection is written straight to
//! committed state (`write_resolved`) and dropped from the dirty buffer, so
//! multi-cell kinds commit data and bookkeeping together (a Map's entries and
//! keyset, a Deque's entries and window bounds). Within the batch budget those
//! cells ride one atomic same-partition batch; an over-budget commit splits
//! into the fewest fitting batches, and — `write_resolved` being marker-free —
//! a crash mid-split can leave a torn committed write the store cannot
//! reconstruct (the over-budget residual on the collection-grain atomicity
//! invariant, on the crate-internal `CellStore` trait), reconstructed
//! only when the idempotent handler re-run re-issues the same ops. The
//! guarantee is **at-least-once**: a `commit()`-landed write is durable and
//! visible immediately — never provisional, never listed in any event marker,
//! never rolled back (the bottom store resolves any standing clears-bearing
//! marker before the write lands — the write-side committed-unapplied window on
//! `write_resolved` — ordering the write after that resolution so a stale
//! clear's replay cannot erase it, subject to the concurrent-resolver residual
//! noted there). Ops buffered *after* the commit ride the collection's normal
//! stage→settle path; reads already see buffered writes without committing.
//!
//! **Orthogonal to [`CommitMode`](crate::state::CommitMode):** the mode governs
//! how *un-committed* writes settle at the event boundary — staged
//! provisionally for `ReadCommitted` (external readers observe committed values
//! only after the event commits), applied immediately for `ReadUncommitted`.
//! `commit()` bypasses that staging entirely: a `commit()`-landed write on a
//! `ReadCommitted` collection is externally visible at once and survives an
//! event abort.
//!
//! `rollback()` discards the collection's buffered uncommitted ops mid-handler
//! — cells *and* dirty clear markers — reverting reads to the last `commit()`,
//! or the pre-event committed value if none. It is `commit()` minus the durable
//! write: the same whole-collection drain, to nothing.
//!
//! **It cannot cross a `commit()` floor.** A `commit()`-landed row is durable
//! and unreachable by rollback — only ops buffered since the last `commit()`
//! are discarded.
//!
//! `rollback()` is async because it joins the session operation gate: a buffer
//! drain racing `commit()`'s snapshot→write→drain could otherwise persist a
//! partial set no serial order explains. It is still infallible: it touches
//! only the in-memory dirty buffer and cannot fail. A terminated session
//! (partition shutting down, or the event cancelled) and a **closed** session
//! (the settle boundary already snapshotted it) both discard nothing and return
//! [`StoreOutcome::NoOp`] — the containment every other command gets from the
//! live guard and the gate's closure check, expressed as a `NoOp` because the
//! infallible signature cannot surface an error. It keeps a stale clone that
//! outlived its event from draining a later same-key event's buffer.
//!
//! This is distinct from the settle boundary's rollback of staged provisional
//! cells (framework-only, after the handler returns): this is the
//! handler-facing mid-flight discard.

use crate::codec::{Codec, SerializeBufGuard};
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor::{
    CellCodecError, CellResolver, CellStateError, CellType, ContextOf, FromSession, KeyOf,
    ResolvedOf, StructuralIdentity, WriteOf,
};
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::registry::CollectionDef;
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state::{RESOLVE_FANOUT, StateName, StateType, StoreOutcome};
use bytes::Bytes;
use educe::Educe;
use futures::stream::{Stream, StreamExt, TryStreamExt, iter};
use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use tokio::task::coop::cooperative;

mod operation;
pub(crate) mod owner;
mod stream;

#[cfg(test)]
mod tests;

pub(crate) use operation::{
    JOURNAL_INLINE, Mutation, MutationJournal, ReadOperation, WriteOperation,
};
pub(crate) use prosody_macros::{collection_layout, collection_methods};
pub(crate) use stream::{CoordinatePlan, KeyItem, RangePlan, ScanItem};

/// Framework-internal engine authority: admission, the raw byte reads,
/// mutation replay, and the mid-handler durable pair.
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
        Bytes, CellBuffer, CellKey, CollectionDef, CoordinateBatch, MutationJournal, Scan, Section,
        StateAccessError, StateName, StateType, StoreOutcome, Stream, StructuralIdentity,
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

        /// The owned state a managed stream plan carries out of the invocation
        /// that built it. The owner keeps nothing (each chunk reacquires
        /// admission); the reader keeps its selected source, so a chunk resumes
        /// on exactly the source the planning command chose.
        type Plan: Clone + Send + Sync + 'static;

        /// Validates the collection named `name` against this engine's
        /// authority and returns its canonical name. The owner validates
        /// registration and structural identity against the registry; the
        /// published reader consumes the validation its source acquisition
        /// already performed.
        ///
        /// # Errors
        ///
        /// Whatever the engine's validation refuses — for the owner, an
        /// unregistered name or a structural-identity mismatch.
        fn verify_registration(
            session: &S,
            name: &'static str,
            state_type: StateType,
            identity: &StructuralIdentity,
        ) -> Result<StateName, StateAccessError>;

        /// The collection's operational settings **as this engine sees them**;
        /// each impl documents its own source. Captured once at bind, so every
        /// configuration query inside a scoped operation answers from one
        /// snapshot.
        fn collection_def(session: &S, state_type: StateType, name: &StateName) -> CollectionDef;

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

        /// Reads one section's `batch` in one lower hop, index-aligned to
        /// `batch`, advancing the invocation's state exactly as
        /// [`Self::read_point`] does.
        fn read_batch(
            session: &S,
            inner: &mut Self::ReadInner<'_>,
            state_type: StateType,
            name: &StateName,
            section: Section,
            batch: &CoordinateBatch,
        ) -> impl Future<Output = Result<CellBuffer<Option<Bytes>>, StateAccessError>> + Send;

        /// Freezes this invocation's state into the plan a managed stream
        /// driver runs on. Total: there is no unplannable invocation, so no
        /// driver carries an unreachable arm.
        fn capture(inner: &Self::ReadInner<'_>) -> Self::Plan;

        /// Re-enters an invocation under a captured plan — one coordinate
        /// chunk's admission. The owner reacquires the gate here, which is what
        /// keeps a coordinate stream free of a gate hold across a yield.
        fn resume<'a>(
            session: &'a S,
            plan: &Self::Plan,
        ) -> impl Future<Output = Self::ReadInner<'a>> + Send;

        /// Pages a durable range under a captured plan, gate-free — the range
        /// driver's only lower hop, and the one command that cannot repair.
        fn page<'a>(
            session: &'a S,
            plan: &'a Self::Plan,
            state_type: StateType,
            name: &'a StateName,
            scan: Scan<'a>,
        ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a;

        /// The per-emission fence a managed stream runs after every source
        /// completion, before the item or error escapes. Vacuous on the
        /// published reader, which has no attempt to leak past.
        ///
        /// # Errors
        ///
        /// [`StateAccessError::Terminated`] once the stream outlived its
        /// dispatch attempt.
        fn fence(session: &S) -> Result<(), StateAccessError>;
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
    pub(crate) const fn section(self) -> Section {
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
/// The binding also captures the collection's [`CollectionDef`] **as the bound
/// engine sees it** — the registry definition for a per-event session, the
/// descriptor's own for a published reader. Registration is immutable for the
/// session's lifetime, so one capture answers every configuration query a
/// scoped operation makes, and a stream's arm cannot change under it
/// mid-flight.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct Collection<S, L> {
    session: S,
    state_type: StateType,
    name: StateName,
    def: CollectionDef,
    _layout: PhantomData<fn() -> L>,
}

impl<S, L> Collection<S, L> {
    /// The collection's canonical name — the operation-span field every
    /// handle method records.
    pub(crate) fn name(&self) -> &StateName {
        &self.name
    }

    /// The collection's captured operational settings; see the type doc.
    fn def(&self) -> &CollectionDef {
        &self.def
    }

    /// The bound session.
    fn session(&self) -> &S {
        &self.session
    }

    /// The collection's state namespace.
    fn state_type(&self) -> StateType {
        self.state_type
    }
}

impl<S: StateSession, L> Collection<S, L> {
    /// Validates `session` against the collection named `name` and binds it.
    /// The sole constructor for either engine: the owner validates registration
    /// and structural identity against the registry, while the published reader
    /// consumes the validation its source acquisition already performed. Which
    /// happens is the engine's choice, never the caller's.
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
        let name = <S::Engine as sealed::ReadEngine<S>>::verify_registration(
            session, name, state_type, identity,
        )?;
        let def = <S::Engine as sealed::ReadEngine<S>>::collection_def(session, state_type, &name);
        Ok(Self {
            session: session.clone(),
            state_type,
            name,
            def,
            _layout: PhantomData,
        })
    }

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

    /// The collection's canonical name — the operation-span field and the
    /// subject of a collection's degrade warnings.
    fn name(&self) -> &StateName;

    /// Whether the collection carries a durable TTL. Read from the binding's
    /// captured settings; no I/O.
    fn has_ttl(&self) -> bool;

    /// The Map keyset bound: how many live distinct keys a map tracks before
    /// overflowing to the full-section scan. Read from the binding's captured
    /// settings; no I/O.
    fn keyset_limit(&self) -> usize;

    /// The Deque push cap. A push evicts from the far end above this many
    /// window slots. `None` means unbounded. Reads the binding's captured
    /// settings, with no I/O.
    ///
    /// Only a write calls it today, as with [`has_ttl`](Self::has_ttl). Both
    /// stay here so the three binding-config accessors read as one group, and
    /// a read does call [`keyset_limit`](Self::keyset_limit).
    fn capacity(&self) -> Option<NonZeroUsize>;

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

    /// Reads, decodes, and resolves `keys` as one aligned batch: `result[i]`
    /// answers `keys[i]`, duplicates are answered per position, and an absent
    /// cell reads `None`.
    ///
    /// The lower reads are sub-batched and sequential (two repair-capable owner
    /// reads must not race one collection's marker); the typed resolves fan out
    /// across the whole call in an order-preserving window.
    ///
    /// # Errors
    ///
    /// As [`Self::get`].
    fn get_many<T>(
        &mut self,
        family: CellFamily<Self::Layout, T>,
        keys: &[KeyOf<T>],
    ) -> impl Future<
        Output = Result<CellBuffer<Option<ResolvedOf<T>>>, CellStateError<CellCodecError<T>>>,
    > + Send
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, Self::Session>;

    /// Whether a stored cell exists at `key`, **without decoding its value or
    /// running the resolver**. The guarantee is "no decode, no resolve", not
    /// "no I/O": a cold cache still reaches the store.
    ///
    /// # Errors
    ///
    /// An access error from the engine.
    fn contains<T: CellType>(
        &mut self,
        family: CellFamily<Self::Layout, T>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<bool, StateAccessError>> + Send;
}

/// The mutation commands, implemented only by the write operation.
///
/// `set` and `clear` are synchronous. They encode and stage, and do no I/O, so
/// a future would add a suspension point without work. `set` can fail only at
/// typed encoding, and a point clear cannot fail after admission.
/// [`take`](Self::take) is the one exception, because it reads first.
pub(crate) trait CollectionWrite: CollectionRead {
    /// Reads, decodes, and resolves the value at `key`, then stages a clear of
    /// that cell. This is the one supported read-then-mutate composite.
    ///
    /// The read completes first. A read error stages nothing. `Ok(None)` still
    /// clears the addressed residue.
    ///
    /// The trait declares this method and gives no default body. A default body
    /// over an opaque `Self` cannot prove the returned future `Send` for its
    /// `&mut Self` and `&KeyOf<T>` captures.
    ///
    /// # Errors
    ///
    /// As [`CollectionRead::get`].
    fn take<T>(
        &mut self,
        family: CellFamily<Self::Layout, T>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>> + Send
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, Self::Session>;

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

    /// Stages an absence over the collection's **whole declared layout** — one
    /// payload-free journal entry that expands to every active and reserved
    /// section at merge, so a removed family's legacy rows are erased too. From
    /// this program point the collection reads empty, and later commands in the
    /// same invocation repopulate it.
    fn clear_collection(&mut self)
    where
        Self::Layout: CollectionLayout;
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

/// Decodes and resolves an aligned batch of raw cell slots into the exposed
/// application values, preserving input order. The resolves — the expensive
/// half, potentially a loader read per cell — fan out across the WHOLE batch
/// through an ordered [`buffered`](StreamExt::buffered) window of
/// [`RESOLVE_FANOUT`], so a batch's resolves overlap instead of serializing per
/// sub-batch.
///
/// # Errors
///
/// A codec error (Permanent) when a cell's bytes do not decode, or a resolution
/// error from the resolver.
pub(in crate::state) async fn resolve_batch<S, T>(
    session: &S,
    bytes: CellBuffer<Option<Bytes>>,
) -> Result<CellBuffer<Option<ResolvedOf<T>>>, CellStateError<CellCodecError<T>>>
where
    S: StateSession,
    T: CellType,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    iter(bytes)
        .map(|slot| {
            cooperative(async move {
                match slot {
                    Some(raw) => Ok::<_, CellStateError<CellCodecError<T>>>(Some(
                        resolve_cell::<S, T>(session, raw).await?,
                    )),
                    None => Ok(None),
                }
            })
        })
        .buffered(RESOLVE_FANOUT)
        .try_collect()
        .await
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
