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
//! Every collection handle exposes `commit()` and `rollback()`. Value, Map,
//! Set, and Deque all do, and every future collection kind must too. The
//! contract stays here rather than on `Collection::commit` and
//! `Collection::rollback`: those two are `pub(crate)`, so the public handle
//! docs cannot link to them.
//!
//! `commit()` durably commits the collection's buffered changes mid-handler, so
//! they survive a restart after failure. A large or complex handler keeps
//! incremental progress with it. It writes every currently-buffered op straight
//! to committed state and drops it from the dirty buffer, so a multi-cell kind
//! commits data and bookkeeping together. Handler idempotence across the resume
//! is the contract.
//!
//! The guarantee is **at-least-once**. A committed write is durable and visible
//! at once, and no rollback reaches it. Ops buffered *after* the commit ride
//! the normal stage→settle path. This is **orthogonal to
//! [`CommitMode`](crate::state::CommitMode)**, which governs only how
//! *un-committed* writes settle at the event boundary. A committed write on a
//! `ReadCommitted` collection is externally visible at once, and it survives an
//! event abort.
//!
//! `rollback()` discards the collection's buffered uncommitted ops — cells and
//! dirty clear markers alike. Reads revert to the last `commit()`, or to the
//! pre-event committed value if there was none. **It cannot cross a `commit()`
//! floor.** The settle boundary also rolls back staged provisional cells, but
//! that is a different, framework-only step after the handler returns.

use crate::codec::Codec;
use crate::state::access::StateAccessError;
use crate::state::cell_key::Section;
use crate::state::descriptor::{
    CellCodecError, CellResolver, CellStateError, CellType, CollectionSpec, ContextOf, FanoutOf,
    FromSession, ResolvedOf, StructuralIdentity,
};
use crate::state::fanout::Fanout;
use crate::state::registry::CollectionDef;
use crate::state::store::CellBuffer;
use crate::state::{RESOLVE_FANOUT, StateName, StateType, StoreOutcome};
use bytes::Bytes;
use educe::Educe;
use futures::stream::{StreamExt, TryStreamExt, iter};
use std::future::{Future, ready};
use std::marker::PhantomData;
use tokio::task::coop::cooperative;

mod address;
mod operation;
pub(crate) mod owner;
mod stream;

#[cfg(test)]
mod tests;

pub(crate) use address::{CellAddress, CellFamily};
pub(crate) use operation::{
    JOURNAL_INLINE, Mutation, MutationJournal, ReadOperation, WriteOperation,
};
pub(crate) use prosody_macros::{collection_layout, collection_methods};
pub(crate) use stream::{Plan, StreamProjection};

pub(crate) mod sealed;

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

/// True when `S`'s cell type both addresses and encodes `entry`. Every
/// collection's frozen-layout block asserts this over its entries family, so
/// the spec's `Cell` can never drift from the family it declares.
pub(crate) const fn spec_matches<S: CollectionSpec>(entry: LayoutEntry) -> bool {
    same_token(
        <<S::Cell as CellType>::Key as Codec>::FORMAT_ID,
        entry.key_format(),
    ) && same_token(
        <<S::Cell as CellType>::Codec as Codec>::FORMAT_ID,
        entry.format(),
    )
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

    /// Durably commits this collection's buffered changes mid-handler. The
    /// module's mid-handler durability section states the contract.
    ///
    /// Within the batch budget the drained cells ride one atomic
    /// same-partition batch. An over-budget commit splits into the fewest
    /// batches that fit. The write is marker-free, so a crash mid-split can
    /// leave a torn committed write that the store cannot reconstruct. That is
    /// the over-budget residual on the collection-grain atomicity invariant,
    /// stated in the [`store`](crate::state::store) module. Only the idempotent
    /// handler re-run repairs it, by re-issuing the same ops. The bottom store
    /// resolves any unsettled section clear before the write lands, so a
    /// stale clear's replay cannot erase it.
    ///
    /// # Errors
    ///
    /// Admission refusal, or a store failure.
    pub(crate) async fn commit(&self) -> Result<StoreOutcome, StateAccessError> {
        <S::Engine as sealed::WriteEngine<S>>::commit(&self.session, self.state_type, &self.name)
            .await
    }

    /// Discards this collection's buffered uncommitted ops mid-handler. It is
    /// [`commit`](Self::commit) minus the durable write: the same
    /// whole-collection drain, to nothing. The module's mid-handler durability
    /// section states the contract.
    ///
    /// It is async because it joins the session operation gate. A buffer drain
    /// that raced the commit's snapshot→write→drain could otherwise persist a
    /// partial set that no serial order explains.
    ///
    /// It is still infallible, because it touches only the in-memory dirty
    /// buffer. Two sessions discard nothing and return
    /// [`StoreOutcome::NoOp`]: a terminated one (the partition shuts down, or
    /// the event is cancelled) and a **closed** one (the settle boundary
    /// already snapshotted it). That is the containment every other command
    /// gets from the live guard and the gate's closure check. The infallible
    /// signature cannot surface an error, so it reads as a `NoOp`. It stops a
    /// stale clone that outlived its event from draining a later same-key
    /// event's buffer.
    pub(crate) async fn rollback(&self) -> StoreOutcome {
        <S::Engine as sealed::WriteEngine<S>>::rollback(&self.session, self.state_type, &self.name)
            .await
    }
}

mod commands;
pub(crate) use commands::{CollectionRead, CollectionWrite, sealed_ops};

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
) -> impl Future<Output = Result<ResolvedOf<T>, CellStateError<CellCodecError<T>>>> + Send + use<'a, S, T>
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
/// application values, preserving input order. The cell type's
/// [`FanoutOf`] runs the resolves across the whole batch. The answer buffer
/// is sized once to the batch length.
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
    let len = bytes.len();
    let futures = iter(bytes).map(|slot| {
        cooperative(async move {
            match slot {
                Some(raw) => Ok::<_, CellStateError<CellCodecError<T>>>(Some(
                    resolve_cell::<S, T>(session, raw).await?,
                )),
                None => Ok(None),
            }
        })
    });
    <FanoutOf<T> as Fanout>::drive(futures, RESOLVE_FANOUT)
        .try_fold(CellBuffer::with_capacity(len), |mut values, value| {
            values.push(value);
            ready(Ok(values))
        })
        .await
}

/// Decodes a cell's bytes as `C::Payload` through
/// [`Codec::deserialize_bytes`]. A codec that reads shared bytes avoids a
/// copy, and the default copies only shared bytes. The single decode path
/// every typed cell read shares.
pub(in crate::state) fn decode_cell<C: Codec>(cell: Bytes) -> Result<C::Payload, C::Error> {
    C::with_cached_local(|codec| codec.deserialize_bytes(cell))
}

/// Encodes `payload` into owned cell bytes through
/// [`Codec::serialize_bytes`]. A codec that owns its encoded bytes returns
/// them without a copy. The single encode path every typed cell write shares.
pub(in crate::state) fn encode_cell<C: Codec>(payload: C::Payload) -> Result<Bytes, C::Error> {
    C::with_cached_local(|codec| codec.serialize_bytes(payload))
}
