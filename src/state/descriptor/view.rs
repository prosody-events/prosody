//! Bridge: the raw cell-command byte boundary of the collection kinds that
//! still thread permits by hand. [`CellScope`] forwards a collection
//! partition's raw cell ops and [`CellView`] wraps them in a [`CellType`]'s
//! codecs and resolver; the raw sinks are **private to this module**, so a
//! collection kind (a sibling module) can reach a cell only through the typed
//! surface. Decoding and resolving is the *same* pair the scoped-operation
//! boundary in [`crate::state::collection`] uses; this module dies once Deque
//! runs through that engine too.

use super::{
    CellCodecError, CellResolver, CellStateError, CellType, ContextOf, FromSession, KeyOf,
    ResolvedOf, STREAM_CHUNK, WriteOf,
};
use crate::state::StateAccessError;
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge, Section};
use crate::state::collection::{ScanItem, encode_cell, ensure_live, resolve_batch, resolve_cell};
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::session::{CellRead, CellWrite};
use crate::state::store::{CELL_BATCH, CellBuffer, CoordinateBatch};
use crate::state::{SHARD_FANOUT_CONCURRENCY, StateName, StateType, StoreOutcome};
use async_stream::try_stream;
use bytes::Bytes;
use futures::stream::{Stream, StreamExt, unfold};
use smallvec::SmallVec;
use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use tokio::task::coop::cooperative;

/// A cell store scoped to ONE collection partition (the unit of atomicity).
///
/// Pins the collection's `(state_type, name)` once at bind and forwards by
/// [`CellKey`], so a collection handle addresses only cells within its own
/// partition and **cannot escape it** (the `CollectionScopeContainment`
/// invariant — the segment/key are injected by the session, the wrapped
/// session is private). A kind projects the typed views it needs from a scope
/// with `Self::typed`; the raw byte ops stay module-private, so a cell's
/// bytes are only ever spoken through its codecs — an [`OrderedKeyCodec`] for
/// the address, a [`Codec`](crate::codec::Codec) for the value. Cheap `Clone`.
#[derive(Clone)]
pub(crate) struct CellScope<S> {
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

    /// Projects a typed view over this scope's cells in `section` for cell type
    /// `T`. A kind projects one view per cell family, each in its own section
    /// (a Map projects its entries and its meta cells from the same scope
    /// into different sections).
    pub(in crate::state::descriptor) fn typed<T>(&self, section: Section) -> CellView<S, T>
    where
        S: Clone,
    {
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
}

impl<S: CellRead> CellScope<S> {
    /// This collection's Deque push capacity (`None` unbounded) — a cheap
    /// settings lookup the Deque `push_*` enforcement consults per push.
    pub(in crate::state::descriptor) fn capacity(&self) -> Option<NonZeroUsize> {
        self.session
            .collection_def(self.state_type, &self.name)
            .capacity
    }

    /// Reads one cell's visible committed bytes. Demands a read permit. The
    /// `_permit` is unused in the body: the [`CellRead`] call does not take it.
    /// The returned future still captures the borrow through its edition-2024
    /// lifetime, so the gate stays held while the read runs.
    async fn raw_get(
        &self,
        _permit: &S::Permit<'_>,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        ensure_live(&self.session)?;
        self.session.get(self.state_type, &self.name, cell).await
    }

    /// Batch twin of [`Self::raw_get`]: reads `section`'s `batch` in one lower
    /// hop, aligned. Demands the same read-permit witness and runs the same
    /// `ensure_live` guard as [`Self::raw_get`].
    async fn raw_get_many(
        &self,
        _permit: &S::Permit<'_>,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        ensure_live(&self.session)?;
        self.session
            .get_many(self.state_type, &self.name, section, batch)
            .await
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
}

impl<S: CellWrite> CellScope<S> {
    /// Buffers a set of one cell's bytes. Demands a mutate permit; see
    /// [`Self::raw_get`] for the `_permit` token.
    async fn raw_set(
        &self,
        _permit: &S::MutatePermit<'_>,
        cell: &CellKey,
        value: &[u8],
    ) -> Result<(), StateAccessError> {
        ensure_live(&self.session)?;
        self.session
            .set(self.state_type, &self.name, cell, value)
            .await
    }

    /// Buffers a clear of one cell. Demands a mutate permit.
    async fn raw_clear(
        &self,
        _permit: &S::MutatePermit<'_>,
        cell: &CellKey,
    ) -> Result<(), StateAccessError> {
        ensure_live(&self.session)?;
        self.session.clear(self.state_type, &self.name, cell).await
    }

    /// Buffers a dirty clear marker over one section; see
    /// [`CellWrite::clear_section`]. Demands a mutate permit.
    async fn clear_section(
        &self,
        _permit: &S::MutatePermit<'_>,
        section: Section,
    ) -> Result<(), StateAccessError> {
        ensure_live(&self.session)?;
        self.session
            .clear_section(self.state_type, &self.name, section)
            .await
    }

    /// Durably commits this collection's buffered ops mid-handler.
    /// At-least-once; see [`CellWrite::commit`] for the contract. Demands a
    /// mutate permit.
    async fn raw_commit(
        &self,
        _permit: &S::MutatePermit<'_>,
    ) -> Result<StoreOutcome, StateAccessError> {
        ensure_live(&self.session)?;
        self.session.commit(self.state_type, &self.name).await
    }

    /// Discards this collection's uncommitted buffered ops mid-handler; see
    /// [`CellWrite::rollback`] for the contract. Unwitnessed by design: the
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
/// resolving read impl block (`S: CellRead`): it is what lets `get`/`scan`
/// extract the resolver's context from the session for any lifetime. The
/// mutators need no such bound — `set` only lowers through the resolver's
/// `stored_from`, and `clear`/`commit` do not resolve at all.
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

impl<S: CellRead, T: CellType> CellView<S, T> {
    /// The collection's name, for the handles' operation spans.
    pub(in crate::state::descriptor) fn name(&self) -> &StateName {
        &self.scope.name
    }

    /// Acquires the session operation gate for a read — the top of every Map
    /// and Deque public read wrapper (their `get`/`len`/stream inits). The
    /// returned `S::Permit` is the witness the view's read sinks demand.
    pub(in crate::state::descriptor) async fn read_permit(&self) -> S::Permit<'_> {
        self.scope.session().permit().await
    }

    /// The full cell address for `key` in this view's section — the sole place
    /// a typed key is lowered to its order-preserving coordinate.
    fn cell(&self, key: &KeyOf<T>) -> CellKey {
        CellKey {
            section: self.section,
            coordinate: <T::Key as OrderedKeyCodec>::encode(key),
        }
    }

    /// This view's collection Deque push capacity (see
    /// [`CellScope::capacity`]).
    pub(in crate::state::descriptor) fn capacity(&self) -> Option<NonZeroUsize> {
        self.scope.capacity()
    }

    /// The scan shell's fence adapter — the shell's one uniform piece and the
    /// SOLE home of a scan's per-emission attempt fence. Wraps a source's item
    /// stream (the resolving range and coordinate sources of [`Self::scan`] /
    /// [`Self::scan_at`]) and runs [`ensure_live`] after EVERY `inner.next()`
    /// completion — `Some`, `Err`, and the exhaustion `None` alike — BEFORE
    /// matching it, so a scan leaked past its handler attempt (a spawned task,
    /// an un-awaited future, a foreign promise) errors
    /// [`StateAccessError::Terminated`] at its next emission; the source's own
    /// buffer sits below the fence, so no item whose emission check follows the
    /// bump crosses. Empty sources still pass the fence on
    /// exhaustion, so a leaked empty-plan stream errors rather than reporting a
    /// clean end.
    ///
    /// # Invariant — no await, no buffering between the fence and the caller
    ///
    /// Every source buffer (a coordinate chunk's buffer, the range source's
    /// `buffered` resolution window) sits BELOW this adapter; a collection adds
    /// only synchronous per-item transforms above it (`map_err` into its
    /// collection error, dropping the coordinate for a deque). The check is a
    /// LINEARIZATION point, not a wall-clock wall: a completion whose
    /// synchronous `ensure_live` passed linearized before any concurrent
    /// attempt boundary. It holds no permit (the check is sync); a concurrent
    /// reset — which needs the gate exclusively and, for the coordinate source,
    /// queues behind the whole chunk's permit — is ordered relative to a
    /// completion by whether its bump landed before that completion's check.
    fn fenced<'a, X: Send + 'a>(
        &'a self,
        inner: impl Stream<Item = Result<X, CellStateError<CellCodecError<T>>>> + Send + 'a,
    ) -> impl Stream<Item = Result<X, CellStateError<CellCodecError<T>>>> + Send + 'a {
        let session = self.scope.session();
        // Heap-hold the source's state machine (the chunk unfold or the
        // `buffered` resolution window): it is the large part, so boxing it
        // keeps the fence adapter — and every collection stream that embeds it
        // — a small future (large-future stack bloat, not a per-item cost).
        // One bounded allocation per stream construction (a per-read entry
        // point, never the steady-state per-item path); `Pin<Box<_>>` is
        // `Unpin`, so no `pin_mut!`. The boxed type is the concrete source, not
        // a `dyn Stream`.
        let mut inner = Box::pin(inner);
        try_stream! {
            loop {
                let item = inner.next().await;
                // Pin compare + termination, BEFORE matching the completion —
                // `Some`, `Err`, and the exhaustion `None` alike.
                ensure_live(session)?;
                match item {
                    Some(item) => yield item?,
                    None => break,
                }
            }
        }
    }

    /// [`Self::get_many`]'s PHASE 1: lower each key to its coordinate in input
    /// order, split via [`CoordinateBatch::chunks`], and read the sub-batches
    /// SEQUENTIALLY. Aligned to `keys` (`result[i]` answers `keys[i]`). **No
    /// decode, no resolver** — the result is raw `Option<Bytes>`, so it sits in
    /// the resolver-free impl block.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    async fn read_bytes(
        &self,
        permit: &S::Permit<'_>,
        keys: &[KeyOf<T>],
    ) -> Result<CellBuffer<Option<Bytes>>, CellStateError<CellCodecError<T>>> {
        // The per-chunk coordinate buffer stays inline (`≤ CELL_BATCH`); only
        // the owned coordinates cross the store await, never a borrow of `self`.
        let mut bytes: CellBuffer<Option<Bytes>> = SmallVec::with_capacity(keys.len());
        for key_chunk in keys.chunks(CELL_BATCH) {
            let coords: CellBuffer<Coordinate> =
                key_chunk.iter().map(|k| self.cell(k).coordinate).collect();
            for batch in CoordinateBatch::chunks(coords) {
                bytes.extend(
                    self.scope
                        .raw_get_many(permit, self.section, &batch)
                        .await?,
                );
            }
        }
        debug_assert_eq!(
            bytes.len(),
            keys.len(),
            "batch read answers every input position"
        );
        Ok(bytes)
    }
}

impl<S: CellWrite, T: CellType> CellView<S, T> {
    /// Acquires the session operation gate for a mutator and applies the total
    /// admission order (pin → closed → termination) — a thin forwarder to
    /// [`CellWrite::mutate_permit`], which owns the sequence. The returned
    /// `S::MutatePermit` is the witness the view's mutating sinks demand.
    pub(in crate::state::descriptor) async fn mutate_permit(
        &self,
    ) -> Result<S::MutatePermit<'_>, StateAccessError> {
        self.scope.session().mutate_permit().await
    }

    /// Buffers a dirty clear marker over this view's whole section: every
    /// cell reads as deleted from this program point, and later `set`s
    /// repopulate. See [`CellWrite::clear_section`] for the transactional
    /// contract.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub(in crate::state::descriptor) async fn clear_all(
        &self,
        permit: &S::MutatePermit<'_>,
    ) -> Result<(), CellStateError<CellCodecError<T>>> {
        Ok(self.scope.clear_section(permit, self.section).await?)
    }

    /// Discards this collection's uncommitted buffered ops mid-handler — every
    /// typed view over the scope, not just this view's cells; the discard twin
    /// of [`Self::commit`]. See [`CellWrite::rollback`] for the contract
    /// (the session owns the gate acquire, so this stays permit-free).
    pub(in crate::state::descriptor) async fn rollback(&self) -> StoreOutcome {
        self.scope.raw_rollback().await
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
        permit: &'a S::MutatePermit<'_>,
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
        permit: &'a S::MutatePermit<'_>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<(), CellStateError<CellCodecError<T>>>> + Send + 'a {
        let cell = self.cell(key);
        async move { Ok(self.scope.raw_clear(permit, &cell).await?) }
    }

    /// Durably commits this collection's buffered ops mid-handler — the
    /// single `commit()` home, draining the whole collection's buffered ops
    /// (every typed view over the scope), not just this view's cells.
    /// At-least-once; see [`CellWrite::commit`] for the contract.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub(in crate::state::descriptor) async fn commit(
        &self,
        permit: &S::MutatePermit<'_>,
    ) -> Result<StoreOutcome, CellStateError<CellCodecError<T>>> {
        Ok(self.scope.raw_commit(permit).await?)
    }
}

impl<S, T> CellView<S, T>
where
    S: CellRead,
    T: CellType,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    /// Reads, decodes, and resolves the visible committed value at `key` — the
    /// point-op read surface: the overlay check → `raw_get` → cache-fill under
    /// the permit, then decode + resolve through [`Self::resolve_bytes`]. The
    /// point-op handles compose this under a single read permit.
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
        permit: &'a S::Permit<'_>,
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

    /// Reads, decodes, and resolves the visible committed values for `keys` as
    /// one aligned batch (`result[i]` answers `keys[i]`; duplicate keys are
    /// answered per position under the observation rules; absent → `None`).
    /// Owns the chunking and takes the read permit, exactly as [`Self::get`]
    /// does. The scan coordinate source is its only caller, composing it per
    /// chunk under one chunk permit.
    ///
    /// Runs in two decoupled phases. PHASE 1 — the sub-batched store reads (the
    /// cheap part; carries marker-help + cache-fill writes beneath the cache,
    /// whose cross-sub-batch concurrency safety is asserted nowhere): the
    /// [`Self::read_bytes`] loop. PHASE 2 — the typed resolves (the expensive
    /// part: a loader read from Kafka; a pure read, no cache write, no marker
    /// help): the shared [`resolve_batch`], whose ordered fan-out keeps the
    /// output aligned to `keys`.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session, a codec error (Permanent) when
    /// a cell's bytes do not decode, or a resolution error from the resolver.
    pub(in crate::state::descriptor) async fn get_many(
        &self,
        permit: &S::Permit<'_>,
        keys: &[KeyOf<T>],
    ) -> Result<CellBuffer<Option<ResolvedOf<T>>>, CellStateError<CellCodecError<T>>> {
        let bytes = self.read_bytes(permit, keys).await?;
        resolve_batch::<S, T>(self.scope.session(), bytes).await
    }

    /// Decodes and resolves raw cell bytes into the exposed value — the
    /// private helper [`Self::get`] and [`Self::scan`] share, delegating to the
    /// single decode/resolve pair in [`crate::state::collection`]. `get`
    /// resolves under its point permit; `scan` resolves each yielded
    /// `(CellKey, Bytes)` pair gate-free. Neither passes a permit: a resolver's
    /// only session capability is `()` or `&Loader` ([`FromSession`]), never a
    /// cell op, so resolution touches no cell state and needs no gate.
    ///
    /// # Errors
    ///
    /// Returns a codec error (Permanent) when the bytes do not decode, or a
    /// resolution error from the resolver.
    fn resolve_bytes(
        &self,
        bytes: Bytes,
    ) -> impl Future<Output = Result<ResolvedOf<T>, CellStateError<CellCodecError<T>>>> + Send {
        resolve_cell::<S, T>(self.scope.session(), bytes)
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
    /// at the first error. This is the **range source** of the scan shell,
    /// fenced per emission by [`Self::fenced`].
    pub(in crate::state::descriptor) fn scan<'a>(
        &'a self,
        start: ScanEdge<&'a KeyOf<T>>,
        dir: Direction,
        end: ScanEdge<&'a KeyOf<T>>,
        limit: Option<usize>,
    ) -> impl Stream<Item = ScanItem<T>> + Send + 'a {
        self.fenced(self.range_source(start, dir, end, limit))
    }

    /// The coordinate source of the scan shell: resolves the cells addressed by
    /// `coords` (a draining coordinate iterator) in INPUT ORDER, yielding
    /// `(KeyOf<T>, ResolvedOf<T>)` and skipping absent cells uniformly (TTL
    /// holes, popped positions, membership races). Each `≤ STREAM_CHUNK` chunk
    /// is read gate-witnessed by ONE [`Self::get_many`] call — one read permit
    /// per chunk, one lower batch read on any miss (a chunk is one
    /// [`CoordinateBatch`] since `STREAM_CHUNK == CELL_BATCH`) — with the
    /// permit released before the first yield (`StreamYieldFree`). The
    /// permit spans the whole chunk's fetch and resolve, so an attempt
    /// boundary (which needs the gate exclusively) serializes AFTER a chunk
    /// and never tears it. Fenced per emission by [`Self::fenced`]; an
    /// empty `coords` yields nothing but still passes the fence on
    /// exhaustion.
    pub(in crate::state::descriptor) fn scan_at<'a, I>(
        &'a self,
        coords: I,
    ) -> impl Stream<Item = ScanItem<T>> + Send + 'a
    where
        I: Iterator<Item = KeyOf<T>> + Send + 'a,
    {
        self.fenced(self.coordinate_source(coords))
    }

    /// The unfenced body of [`Self::scan_at`]. Split out so [`Self::fenced`]
    /// wraps the same shape as the range source.
    fn coordinate_source<'a, I>(&'a self, coords: I) -> impl Stream<Item = ScanItem<T>> + Send + 'a
    where
        I: Iterator<Item = KeyOf<T>> + Send + 'a,
    {
        try_stream! {
            // Chunk source: the draining coordinate iterator is the unfold
            // state. `peek()` ends the unfold at drain (alloc-free; works for
            // `iter::empty()`).
            let chunks = unfold(coords.peekable(), |mut coords| async move {
                coords.peek()?; // exhausted ⇒ unfold ends
                // One read permit per chunk, dropped with this future's scope —
                // never held across a yield (StreamYieldFree). It spans the
                // whole batch fetch + resolve.
                let permit = self.read_permit().await;
                // Collect the chunk's ≤ STREAM_CHUNK keys, then ONE batch
                // fetch + resolve; `get_many` keeps `vals` aligned to `keys`.
                let keys: CellBuffer<KeyOf<T>> =
                    coords.by_ref().take(STREAM_CHUNK).collect();
                let chunk = self.get_many(&permit, &keys).await.map(|vals| {
                    // A `None` is an absent cell: skipped, never an error.
                    keys.into_iter()
                        .zip(vals)
                        .filter_map(|(key, value)| value.map(|v| (key, v)))
                        .collect::<CellBuffer<_>>()
                });
                Some((chunk, coords))
            });
            futures::pin_mut!(chunks);
            while let Some(chunk) = chunks.next().await {
                for entry in chunk? {
                    yield entry;
                }
            }
        }
    }

    /// The range source of the scan shell — see [`Self::scan`], which wraps
    /// this in [`Self::fenced`]. Drives the gate-free `raw_scan` through an
    /// ordered `buffered` resolution window and terminates at the first
    /// error.
    fn range_source<'a>(
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

/// Lowers a typed scan edge to its order-preserving coordinate edge. Called
/// once per scan, on the stream's first poll; the owned coordinate — not the
/// borrowed key — is what the running scan holds.
fn encode_edge<K: OrderedKeyCodec>(edge: ScanEdge<&K::Key>) -> ScanEdge<Coordinate> {
    edge.map(K::encode)
}
