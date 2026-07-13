//! The byte boundary of keyed state — and the ONLY place `src/state` speaks a
//! cell's raw bytes. [`CellScope`] forwards a collection partition's raw cell
//! ops and [`CellView`] wraps them in a [`CellType`]'s codecs and resolver; the
//! raw sinks and the `decode`/`encode` helpers are **private to this module**,
//! so a collection kind (a sibling module) can reach a cell only through the
//! typed surface — the codec (bytes ↔ stored) and resolver (stored ↔ exposed)
//! are the only things that speak a cell's bytes.

use super::{
    CellCodecError, CellResolver, CellStateError, CellType, ContextOf, FromSession, KeyOf,
    ResolvedOf, WriteOf,
};
use crate::codec::{Codec, SerializeBufGuard};
use crate::state::StateAccessError;
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge, Section};
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::session::{CellSession, MutatePermit, OpPermit};
use crate::state::{SHARD_FANOUT_CONCURRENCY, StateName, StateType, StoreOutcome};
use async_stream::try_stream;
use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use std::future::Future;
use std::marker::PhantomData;
use tokio::task::coop::cooperative;

/// One item [`CellView::scan`] yields: a decoded key paired with its resolved
/// value, or the error that ended the stream.
pub(crate) type ScanItem<T> = Result<(KeyOf<T>, ResolvedOf<T>), CellStateError<CellCodecError<T>>>;

/// A cell store scoped to ONE collection partition (the unit of atomicity).
///
/// Pins the collection's `(state_type, name)` once at bind and forwards by
/// [`CellKey`], so a collection handle addresses only cells within its own
/// partition and **cannot escape it** (the `CollectionScopeContainment`
/// invariant — the segment/key are injected by the session, the wrapped
/// session is private). A kind projects the typed views it needs from a scope
/// with `Self::typed`; the raw byte ops stay module-private, so a cell's
/// bytes are only ever spoken through its codecs — an [`OrderedKeyCodec`] for
/// the address, a [`Codec`] for the value. Cheap `Clone`.
///
/// The type is `pub` because it names a parameter of the public
/// [`CollectionSpec`](super::CollectionSpec)`::handle`, but it is *sealed*: its
/// constructor is crate-internal and its fields private, so downstream code can
/// hold one only where the framework hands it in and can never mint one — the
/// containment invariant survives exposure.
#[derive(Clone)]
pub struct CellScope<S> {
    session: S,
    state_type: StateType,
    name: StateName,
}

impl<S> CellScope<S> {
    /// Binds a scope to one collection partition (see the type doc for the
    /// `CollectionScopeContainment` invariant this establishes). Bound-free —
    /// construction reads nothing.
    pub(in crate::state::descriptor) fn new(
        session: S,
        state_type: StateType,
        name: StateName,
    ) -> Self {
        Self {
            session,
            state_type,
            name,
        }
    }
}

impl<S: CellSession> CellScope<S> {
    /// Projects a typed view over this scope's cells in `section` for cell type
    /// `T`. A kind projects one view per cell family, each in its own section
    /// (a Map projects its entries and its meta cells from the same scope
    /// into different sections).
    pub(in crate::state::descriptor) fn typed<T>(&self, section: Section) -> CellView<S, T> {
        CellView {
            scope: self.clone(),
            section,
            _marker: PhantomData,
        }
    }

    /// The bound session, for a typed view to extract a resolver context from.
    fn session(&self) -> &S {
        &self.session
    }

    /// Whether this collection carries a TTL — a cheap, allocation-free
    /// registry lookup the Map keyset write consults per `set` so the keyset
    /// cell co-expires with its entries (`KeysetPresence`).
    pub(in crate::state::descriptor) fn has_ttl(&self) -> bool {
        self.session.collection_has_ttl(self.state_type, &self.name)
    }

    /// This collection's Map keyset bound — a cheap registry lookup the Map
    /// keyset transition consults per `set`/`stream`.
    pub(in crate::state::descriptor) fn keyset_limit(&self) -> usize {
        self.session
            .collection_keyset_limit(self.state_type, &self.name)
    }

    /// Reads one cell's visible committed bytes. Demands a read permit
    /// (`GateWitness`); `_permit` is a terminal token — the borrow is not
    /// threaded into the unwitnessed [`CellSession`] trait, but the returned
    /// future's edition-2024 lifetime capture still binds it to the gate.
    async fn raw_get(
        &self,
        _permit: &OpPermit<'_>,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        ensure_live(&self.session)?;
        self.session.get(self.state_type, &self.name, cell).await
    }

    /// Scans this collection's cells in `coordinate` order. Unwitnessed by
    /// design: a scan drives gate-free (the stream takes the gate only for its
    /// init metadata read; see
    /// [`SessionGate`](crate::state::session::sealed::SessionGate)'s
    /// chunked stream contract).
    fn raw_scan<'a>(
        &'a self,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        self.session.scan(self.state_type, &self.name, scan)
    }

    /// Buffers a set of one cell's bytes. Demands a mutate permit
    /// (`GateWitness`); see [`Self::raw_get`] for the `_permit` token.
    async fn raw_set(
        &self,
        _permit: &MutatePermit<'_>,
        cell: &CellKey,
        value: &[u8],
    ) -> Result<(), StateAccessError> {
        ensure_live(&self.session)?;
        self.session
            .set(self.state_type, &self.name, cell, value)
            .await
    }

    /// Buffers a clear of one cell. Demands a mutate permit (`GateWitness`).
    async fn raw_clear(
        &self,
        _permit: &MutatePermit<'_>,
        cell: &CellKey,
    ) -> Result<(), StateAccessError> {
        ensure_live(&self.session)?;
        self.session.clear(self.state_type, &self.name, cell).await
    }

    /// Buffers a dirty clear marker over one section; see
    /// [`CellSession::clear_section`]. Demands a mutate permit (`GateWitness`).
    async fn clear_section(
        &self,
        _permit: &MutatePermit<'_>,
        section: Section,
    ) -> Result<(), StateAccessError> {
        ensure_live(&self.session)?;
        self.session
            .clear_section(self.state_type, &self.name, section)
            .await
    }

    /// Durably commits this collection's buffered ops mid-handler.
    /// At-least-once; see [`CellSession::commit`] for the contract. Demands a
    /// mutate permit (`GateWitness`).
    async fn raw_commit(
        &self,
        _permit: &MutatePermit<'_>,
    ) -> Result<StoreOutcome, StateAccessError> {
        ensure_live(&self.session)?;
        self.session.commit(self.state_type, &self.name).await
    }

    /// Discards this collection's uncommitted buffered ops mid-handler; see
    /// [`CellSession::rollback`] for the contract. Unwitnessed by design: the
    /// session owns rollback's gate acquire, so a handle-held permit would
    /// re-enter the non-reentrant mutex and deadlock. The terminated- and
    /// closed-session guards live in the session impl (as a `NoOp`), not
    /// here: the infallible signature cannot surface an error the way
    /// `ensure_live` does for the fallible ops.
    async fn raw_rollback(&self) -> StoreOutcome {
        self.session.rollback(self.state_type, &self.name).await
    }
}

/// A typed cell interface over one section of one collection partition: the
/// [`OrderedKeyCodec`] + [`Codec`] + [`CellResolver`] of a [`CellType`] `T`
/// applied to a [`CellScope`]'s raw bytes. It owns both byte codecs, so a kind
/// never speaks a key or value byte: `get`/`set`/`clear` encode the typed key
/// to its coordinate; `get` then decodes and resolves the cell, `set` lowers
/// then encodes it; `scan` decodes each yielded key and resolves each value.
/// Every op guards on session termination.
///
/// The one op bound `for<'s> ContextOf<'s, T>: FromSession<'s, S>` sits on the
/// op impl block: it is what lets `get`/`scan` extract the resolver's context
/// from the session for any lifetime.
pub(crate) struct CellView<S, T> {
    scope: CellScope<S>,
    section: Section,
    _marker: PhantomData<fn() -> T>,
}

impl<S: Clone, T> Clone for CellView<S, T> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            section: self.section,
            _marker: PhantomData,
        }
    }
}

impl<S: CellSession, T: CellType> CellView<S, T> {
    /// The collection's name, for the handles' operation spans.
    pub(in crate::state::descriptor) fn name(&self) -> &StateName {
        &self.scope.name
    }

    /// Acquires the session operation gate for a read — the top of every
    /// gated public read wrapper (the handles' `get`/`len`/stream inits). The
    /// returned [`OpPermit`] is the witness the view's read sinks demand.
    pub(in crate::state::descriptor) async fn read_permit(&self) -> OpPermit<'_> {
        self.scope.session().gate().read().await
    }

    /// Acquires the session operation gate for a mutator, erroring
    /// [`StateAccessError::SessionClosed`] once the settle boundary closed the
    /// session. The returned [`MutatePermit`] is the witness the view's
    /// mutating sinks demand.
    pub(in crate::state::descriptor) async fn mutate_permit(
        &self,
    ) -> Result<MutatePermit<'_>, StateAccessError> {
        self.scope.session().gate().mutate().await
    }

    /// The full cell address for `key` in this view's section — the sole place
    /// a typed key is lowered to its order-preserving coordinate.
    fn cell(&self, key: &KeyOf<T>) -> CellKey {
        CellKey {
            section: self.section,
            coordinate: <T::Key as OrderedKeyCodec>::encode(key),
        }
    }

    /// Whether this view's collection carries a TTL (see
    /// [`CellScope::has_ttl`]).
    pub(in crate::state::descriptor) fn has_ttl(&self) -> bool {
        self.scope.has_ttl()
    }

    /// This view's collection Map keyset bound (see
    /// [`CellScope::keyset_limit`]).
    pub(in crate::state::descriptor) fn keyset_limit(&self) -> usize {
        self.scope.keyset_limit()
    }

    /// Buffers a dirty clear marker over this view's whole section: every
    /// cell reads as deleted from this program point, and later `set`s
    /// repopulate. See [`CellSession::clear_section`] for the transactional
    /// contract.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub(in crate::state::descriptor) async fn clear_all(
        &self,
        permit: &MutatePermit<'_>,
    ) -> Result<(), CellStateError<CellCodecError<T>>> {
        Ok(self.scope.clear_section(permit, self.section).await?)
    }

    /// Discards this collection's uncommitted buffered ops mid-handler — every
    /// typed view over the scope, not just this view's cells; the discard twin
    /// of [`Self::commit`]. See [`CellSession::rollback`] for the contract
    /// (the session owns the gate acquire, so this stays permit-free).
    pub(in crate::state::descriptor) async fn rollback(&self) -> StoreOutcome {
        self.scope.raw_rollback().await
    }
}

impl<S, T> CellView<S, T>
where
    S: CellSession,
    T: CellType,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    /// Reads, decodes, and resolves the visible committed value at `key` — the
    /// point-op read surface: the overlay check → `raw_get` → cache-fill under
    /// the permit, then decode + resolve through [`Self::resolve_bytes`]. The
    /// point-get streams compose this per chunk under one chunk permit.
    ///
    /// Written in the desugared `-> impl Future + Send` form for two reasons an
    /// `async fn` could not express:
    /// - the future holds the resolver's [`ContextOf`] GAT projection across
    ///   the resolve await, which rustc issue #100013 cannot infer `Send` for
    ///   through an `async fn`;
    /// - the key is lowered to its [`CellKey`] coordinate *before* the async
    ///   block, so only the owned coordinate — never the borrowed `&KeyOf<T>` —
    ///   is captured into the future.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session, a codec error (Permanent)
    /// when the cell bytes do not decode, or a resolution error from the
    /// resolver.
    pub(in crate::state::descriptor) fn get<'a>(
        &'a self,
        permit: &'a OpPermit<'_>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>> + Send + 'a
    {
        let cell = self.cell(key);
        async move {
            match self.scope.raw_get(permit, &cell).await? {
                Some(bytes) => Ok(Some(self.resolve_bytes(bytes).await?)),
                None => Ok(None),
            }
        }
    }

    /// Decodes and resolves raw cell bytes into the exposed value — the private
    /// helper [`Self::get`] and [`Self::scan`] share. `get` decodes and
    /// resolves under its point permit; `scan` resolves each yielded
    /// `(CellKey, Bytes)` pair gate-free. Takes no permit: a resolver's only
    /// session capability is `()` or `&Loader` ([`FromSession`]), never a cell
    /// op, so resolution touches no cell state and needs no gate (which is why
    /// a resolver that re-entered the gate would deadlock — see
    /// [`CellResolver`]).
    ///
    /// Desugared `-> impl Future + Send + 'a` (with the synchronous decode
    /// hoisted before the async block) so the `Send` bound is **stated**, not
    /// inferred: a `.map(|item| cooperative(...resolve_bytes...))` fan-out
    /// buffered under `.buffered(N)` requires the per-item futures `Send` for a
    /// higher-ranked lifetime so the whole collection stream stays `Send` (it
    /// is driven under `KeyManager`'s `buffer_unordered`), which an `async
    /// fn`'s inferred `Send` is "not general enough" to satisfy. The
    /// explicit bound also removes the need for the `manual_async_fn` shape
    /// a single-async-block `async fn` would trip.
    ///
    /// # Errors
    ///
    /// Returns a codec error (Permanent) when the bytes do not decode, or a
    /// resolution error from the resolver.
    fn resolve_bytes<'a>(
        &'a self,
        bytes: Bytes,
    ) -> impl Future<Output = Result<ResolvedOf<T>, CellStateError<CellCodecError<T>>>> + Send + 'a
    {
        let stored = decode_cell::<T::Codec>(bytes);
        async move {
            let stored = stored.map_err(CellStateError::Codec)?;
            let ctx = <ContextOf<'a, T> as FromSession<'a, S>>::from_session(self.scope.session());
            Ok(<T::Resolver as CellResolver>::resolve(ctx, stored).await?)
        }
    }

    /// Lowers `value` through the resolver, encodes it, and buffers a set at
    /// `key`.
    ///
    /// Desugared like [`Self::get`]: the key is lowered to its coordinate and
    /// the value through the resolver *before* the async block, so only owned
    /// values cross the buffering await (a borrowed `&KeyOf<T>` never does).
    ///
    /// # Errors
    ///
    /// Returns a codec error (Permanent) when the cell fails to encode, or an
    /// access error from the session.
    pub(in crate::state::descriptor) fn set<'a>(
        &'a self,
        permit: &'a MutatePermit<'_>,
        key: &KeyOf<T>,
        value: WriteOf<'_, T>,
    ) -> impl Future<Output = Result<(), CellStateError<CellCodecError<T>>>> + Send + 'a {
        let cell = self.cell(key);
        let stored = <T::Resolver as CellResolver>::stored_from(value);
        async move {
            let buf = encode_cell::<T::Codec>(stored).map_err(CellStateError::Codec)?;
            Ok(self.scope.raw_set(permit, &cell, &buf).await?)
        }
    }

    /// Buffers a clear of the cell at `key`.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub(in crate::state::descriptor) fn clear<'a>(
        &'a self,
        permit: &'a MutatePermit<'_>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<(), CellStateError<CellCodecError<T>>>> + Send + 'a {
        let cell = self.cell(key);
        async move { Ok(self.scope.raw_clear(permit, &cell).await?) }
    }

    /// Durably commits this collection's buffered ops mid-handler — the
    /// single `commit()` home, draining the whole collection's buffered ops
    /// (every typed view over the scope), not just this view's cells.
    /// At-least-once; see [`CellSession::commit`] for the contract.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub(in crate::state::descriptor) async fn commit(
        &self,
        permit: &MutatePermit<'_>,
    ) -> Result<StoreOutcome, CellStateError<CellCodecError<T>>> {
        Ok(self.scope.raw_commit(permit).await?)
    }

    /// Scans this section's cells in key order over the typed range
    /// `[start, end]` (direction-relative; see [`Scan`]), decoding each key and
    /// resolving each value, yielding `(KeyOf<T>, ResolvedOf<T>)`. The borrowed
    /// bound keys are encoded to owned coordinates on the stream's first poll
    /// and never touched again.
    ///
    /// Items may be prefetched and resolved up to
    /// [`SHARD_FANOUT_CONCURRENCY`]
    /// ahead of the consumer; the window is ordered (`buffered`, not
    /// `buffer_unordered`), so cells arrive in key order. The stream terminates
    /// at the first error.
    pub(in crate::state::descriptor) fn scan<'a>(
        &'a self,
        start: ScanEdge<&'a KeyOf<T>>,
        dir: Direction,
        end: ScanEdge<&'a KeyOf<T>>,
        limit: Option<usize>,
    ) -> impl Stream<Item = ScanItem<T>> + Send + 'a {
        try_stream! {
            ensure_live(self.scope.session())?;
            let this = self;
            // Encode the direction-relative edges once, here — the owned
            // coordinates outlive the scan the generator drives to completion
            // below. (`Scan::start`/`end` follow `dir`; `Scan` itself derives
            // the byte-order low/high.)
            let start = encode_edge::<T::Key>(start);
            let end = encode_edge::<T::Key>(end);
            let scan = Scan {
                section: self.section,
                start: start.as_ref(),
                dir,
                end: end.as_ref(),
                limit,
            };
            // `cooperative` inline in the producing closure (a `.map(cooperative)`
            // stage trips a higher-ranked-lifetime error on the non-`'static`
            // per-item futures); `buffered` keeps key order. Each item is
            // decoded and resolved through the shared `resolve_bytes`; the scan
            // runs gate-free (`raw_scan` is unwitnessed).
            let inner = self
                .scope
                .raw_scan(scan)
                .map(|item| {
                    cooperative(async move {
                        let (cell, bytes) = item?;
                        let key = <T::Key as OrderedKeyCodec>::decode(cell.coordinate.as_bytes())
                            .map_err(CellStateError::Key)?;
                        let resolved = this.resolve_bytes(bytes).await?;
                        Ok::<_, CellStateError<CellCodecError<T>>>((key, resolved))
                    })
                })
                .buffered(SHARD_FANOUT_CONCURRENCY);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item?;
            }
        }
    }
}

/// Decodes a cell's bytes as `C::Payload`. Parses in place when the `Bytes` is
/// uniquely owned (zero-copy, the production path — every backend decode mints
/// a fresh `Bytes`); falls back to a copy for a shared clone (the in-memory
/// test backend). The single decode path every typed cell view shares.
fn decode_cell<C: Codec>(cell: Bytes) -> Result<C::Payload, C::Error> {
    match cell.try_into_mut() {
        Ok(mut buf) => C::with_cached_local(|codec| codec.deserialize(&mut buf)),
        Err(cell) => {
            let mut buf = cell.to_vec();
            C::with_cached_local(|codec| codec.deserialize(&mut buf))
        }
    }
}

/// Encodes `payload` into the pooled, reusable serialize buffer, returning the
/// guard so the caller hands its bytes to a cell `set` before the guard drops
/// (returning the buffer to the pool). The guard owns its buffer, so it is
/// `Send` and rides the write across an await. The single encode path every
/// typed cell view shares.
fn encode_cell<C: Codec>(payload: C::Payload) -> Result<SerializeBufGuard, C::Error> {
    let mut buf = SerializeBufGuard::acquire();
    C::with_cached_local(|codec| codec.serialize(payload, &mut buf))?;
    Ok(buf)
}

/// Lowers a typed scan edge to its order-preserving coordinate edge. Called
/// once per scan, on the stream's first poll; the owned coordinate — not the
/// borrowed key — is what the running scan holds.
fn encode_edge<K: OrderedKeyCodec>(edge: ScanEdge<&K::Key>) -> ScanEdge<Coordinate> {
    edge.map(K::encode)
}

/// Guards every cell operation: a session whose partition is shutting
/// down or whose event is cancelled refuses state access with
/// [`StateAccessError::Terminated`].
fn ensure_live<S>(session: &S) -> Result<(), StateAccessError>
where
    S: CellSession,
{
    if session.is_terminated() {
        return Err(StateAccessError::Terminated);
    }
    Ok(())
}
