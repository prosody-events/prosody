//! Index-addressed double-ended queue collection.
//!
//! A Deque is a window of cells over a monotonic `i64` index space. Every
//! [`DequeHandle`] method runs as one scoped operation over the bound
//! collection. There is no Deque-specific store or session, and no branch on
//! the bound engine.
//!
//! To use one, build a descriptor with [`deque_state`], register it with the
//! consumer, then bind the [`Registered`](super::Registered) handle through
//! [`EventContext::state`](crate::consumer::event_context::EventContext::state).
//!
//! # Layout
//!
//! Two declared cell families (see [`DequeKind`]):
//!
//! * `BOUNDS` holds one unit-addressed cell: `head ‖ tail` as two big-endian
//!   `i64`s, encoded by the [`(I64Codec, I64Codec)`](crate::codec::FixedCodec)
//!   pair codec with no framing. `head == tail` (and the absent cell, read as
//!   the empty window `[0, 0)`) is empty.
//! * `ENTRIES` holds one cell per live element, addressed by the sign-flipped
//!   big-endian index ([`I64KeyCodec`]) so the clustering byte order is the
//!   signed index order, and typed by the element cell type `T`.
//!
//! # Invariant: monotonic window within its lifetime; dense without a TTL
//!
//! `[head, tail)` is a contiguous window with `head ≤ tail`, and indices are
//! monotonic and never reused **within a window's lifetime** — a pop advances
//! `head`/`tail` past the freed index, never back into it. So `len` is
//! `tail − head` (O(1) from the bounds cell), `get(i)` reads the single cell at
//! `head + i`, and iteration point-reads each index in `[head, tail)`, never a
//! popped tombstone (which sits below `head` or at/above `tail`).
//! [`DequeHandle::clear`] ends the window's lifetime and
//! **resets the index space**: the erased bounds cell reads `[0, 0)`, so the
//! next push writes index 0. Reuse is safe — every pre-clear row is erased by
//! the clear, and a later write to a reused coordinate out-stamps any earlier
//! tombstone (single writer, monotonic timestamps).
//!
//! Co-stamping keeps the window move and its entry mutation together. One
//! invocation stages both into one journal. They buffer as one op, and they
//! stage under one settle marker with one write TS/TTL (see
//! [`KeyedStateSession::finalize`](crate::state::session)). Recovery therefore
//! restores both together, whatever the batching does.
//!
//! A mid-handler [`DequeHandle::commit`] drains them resolved and marker-free,
//! as one atomic batch within the batch budget. An over-budget commit can crash
//! mid-split. That is the collection-grain over-budget residual on `CellStore`,
//! which the Map keyset shares.
//!
//! **Without a TTL the window is also dense**: every index in `[head, tail)`
//! maps to a present entry cell, so `len` is exact and iteration yields exactly
//! `len` elements. **With a TTL** an entry's expiry is anchored at its push, so
//! entries can expire *inside* the window while it stays put — the window
//! develops holes. Every mutating op rewrites the bounds cell, so the bounds
//! cell outlives the entries and holes do not move `head` or `tail`.
//!
//! Under holes `len` is an **upper bound** on the live count. `get` and
//! `stream` **skip** an expired index: an absent cell resolves as skipped
//! (`get` → `None`, `stream` omits it), never as an error. These are the
//! time-window semantics a TTL asks for. A TTL'd deque is a sliding window of
//! elements that have not expired.
//!
//! # Invariant: capacity
//!
//! A registered `capacity` is a runtime-only cap on window slots — never
//! persisted, not part of identity, freely changed across redeploys. It is
//! enforced **lazily, on push only**: reads, `len`, iteration, `pop`, and
//! `clear` never enforce it. A bounded [`DequeHandle::push_back`] evicts from
//! the **front** and [`DequeHandle::push_front`] from the **back**, at most
//! `TRIM_MAX` slots per push and decode-free / resolver-free. Each eviction is
//! one single-cell clear, staged beside the append and the bounds move. So a
//! persisted window may exceed the cap; for a **measurable** window a reduction
//! of excess `D` converges in `⌈D / (TRIM_MAX − 1)⌉` pushes. An unmeasurable
//! span (only reachable from a corrupt or hand-seeded bounds cell) under an
//! absurd (`≈ 2^63`) cap deliberately under-evicts and may not converge — the
//! safe direction, never erasing in-capacity cells (see `evictions`).

use super::{
    CellCodecError, CellStateError, CellType, CollectionSpec, ContextOf, Descriptor, FromSession,
    Keyed, ResolvedOf, WriteOf,
};
use crate::codec::{Codec, I64Codec, I64CodecError, JsonCodec, PairCodecError};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell_key::Direction;
#[cfg(test)]
use crate::state::cell_key::{CellKey, Coordinate};
use crate::state::collection::{
    Collection, CollectionLayout, CollectionRead, CollectionWrite, CoordinatePlan, JOURNAL_INLINE,
    RangePlan, ScanItem, StateSession, WritableStateSession, collection_layout, collection_methods,
    same_token,
};
#[cfg(test)]
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::order_codec::{I64KeyCodec, UnitKey};
use crate::state::{CollectionKindId, StateAccessError, StoreOutcome};
use async_stream::try_stream;
use educe::Educe;
use futures::future::Either;
use futures::stream::{Stream, StreamExt};
use std::error::Error;
use std::num::NonZeroUsize;
use thiserror::Error;
use tracing::{Instrument, Span, field::Empty, info_span, instrument};

collection_layout! {
    /// The Deque collection kind: one head/tail bounds cell, plus one cell per
    /// live element at the sign-flipped big-endian index.
    ///
    /// The kind pins the index encoding to [`I64KeyCodec`]. A registration
    /// cannot choose it. That encoding rides the identity's key-codec token,
    /// like every other key axis.
    pub struct DequeKind<T> {
        /// The head/tail bounds cell (see the module's window invariant).
        #[id(0)]
        BOUNDS: MetaCodec,
        /// One cell per live element.
        #[id(1)]
        ENTRIES: Keyed<I64KeyCodec, T>,
    }
}

/// The deque's head/tail meta codec: two big-endian `i64`s composed with no
/// framing, byte-identical to the frozen 16-byte `head ‖ tail` frame.
type MetaCodec = (I64Codec, I64Codec);

/// The [`MetaCodec`] decode error — a corrupt (wrong-width) bounds frame.
type MetaCodecError = PairCodecError<I64CodecError, I64CodecError>;

/// The instantiation that the frozen-layout pin and the test-only cell-address
/// helpers read their sections and format tokens from. The layout supplies a
/// family's durable section and its declared codecs, never the type parameters,
/// so every instantiation answers the same.
type FrozenLayout = DequeKind<JsonCodec>;

/// Iteration shape threshold: a window of at most this many entries streams
/// by per-index point gets — each a cacheable committed point read — while a
/// wider window pays one durable range scan instead of `len` point reads. A
/// read-shape choice, not configuration.
pub(crate) const DEQUE_POINT_ITERATION_MAX: usize = 128;

/// The most window slots a single bounded push evicts, bounding per-event
/// eviction work. `>= 2` so a full push nets a `TRIM_MAX − 1` window reduction:
/// a push appends one slot, so at `1` it would evict one and append one and
/// never shrink an over-wide window. This net reduction is what converges a
/// measurable over-wide window toward the cap (rate and the unmeasurable-span
/// caveat are on the module's capacity invariant; see `evictions`).
pub(crate) const TRIM_MAX: usize = 2;

const _: () = assert!(TRIM_MAX >= 2, "a bounded push must net a window reduction");

const _: () = assert!(
    TRIM_MAX <= u8::MAX as usize,
    "an eviction count is returned as a `u8`"
);

/// Deque's declared per-invocation mutation maximum. A bounded push stages one
/// entry set, at most [`TRIM_MAX`] point clears, and one bounds set. A pop
/// stages one clear and one bounds set. `clear` stages one whole-layout reset.
///
/// The assertion below pins this declaration against [`JOURNAL_INLINE`]'s
/// budget. `prop_deque_capacity_convergence` pins the runtime half: a push
/// never stages more than `TRIM_MAX` clears.
const DEQUE_MAX_MUTATIONS: usize = TRIM_MAX + 2;

const _: () = assert!(
    DEQUE_MAX_MUTATIONS <= JOURNAL_INLINE,
    "a Deque invocation must stay inside the journal's inline capacity"
);

/// Deque's durable layout, frozen. The ids and the bounds family's format
/// tokens below address every Deque cell ever written. A change to one silently
/// re-points the existing rows, and no type can compare this crate against
/// yesterday's schema.
///
/// The entries family's *payload* token is the user's choice, and it rides the
/// collection's structural identity instead. Its *key* token belongs to the
/// kind, so this block pins it. The pin is a compile-time assertion, not a
/// test, so no run can filter it out.
const _: () = {
    let families = <FrozenLayout as CollectionLayout>::DESCRIPTOR;
    assert!(
        families.len() == 2,
        "Deque declares exactly two cell families"
    );
    assert!(
        families[0].id() == 0,
        "Deque's bounds family is durably section 0"
    );
    assert!(
        same_token(families[0].key_format(), "unit.v1"),
        "the bounds cell is durably unit-addressed"
    );
    assert!(
        same_token(families[0].format(), "(i64-be,i64-be)"),
        "the bounds cell is durably the head ‖ tail big-endian pair"
    );
    assert!(
        families[1].id() == 1,
        "Deque's entries family is durably section 1"
    );
    assert!(
        same_token(families[1].key_format(), "i64.v1"),
        "Deque entries are durably addressed by the kind's index codec"
    );
    assert!(
        same_token(
            <<<FrozenLayout as CollectionSpec>::Cell as CellType>::Key as Codec>::FORMAT_ID,
            families[1].key_format()
        ),
        "the spec's cell type addresses the entries family"
    );
    assert!(
        same_token(
            <<<FrozenLayout as CollectionSpec>::Cell as CellType>::Codec as Codec>::FORMAT_ID,
            families[1].format()
        ),
        "the spec's cell type encodes the entries family"
    );
    assert!(
        <FrozenLayout as CollectionLayout>::SECTIONS.len() == 2,
        "Deque's reset domain is its two families"
    );
    assert!(
        <FrozenLayout as CollectionLayout>::RESERVED.is_empty(),
        "Deque has never removed a family"
    );
};

/// Descriptor for a codec-backed deque collection. Generic over an element
/// [`CellType`] `T` — a plain [`Codec`] (JSON by default) or a codec paired
/// with a resolver via [`WithResolver`](super::WithResolver).
/// There is no key-codec parameter: the index encoding is fixed by the kind.
/// Declare via [`deque_state`].
pub type DequeDescriptor<T = JsonCodec> = Descriptor<DequeKind<T>>;

impl<T: CellType<Key = UnitKey>> CollectionSpec for DequeKind<T> {
    type Cell = Keyed<I64KeyCodec, T>;
    type Handle<S: StateSession> = DequeHandle<S, T>;

    const KIND: CollectionKindId = CollectionKindId::Deque;

    fn handle<S: StateSession>(collection: Collection<S, Self>) -> DequeHandle<S, T> {
        DequeHandle { cells: collection }
    }
}

/// Typed, owned handle over a codec-backed deque.
///
/// The handle owns the bound collection. That collection's session clone is
/// `Clone + Send + Sync + 'static`, which FFI requires. Each method opens
/// exactly one scoped operation. [`stream`](Self::stream) runs a short planning
/// operation, then drives the plan it returns. `Clone` is cheap.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct DequeHandle<S, T> {
    cells: Collection<S, DequeKind<T>>,
}

/// The arm that [`DequeHandle::stream`] takes, as the owned plan that its
/// planning invocation captured. One arm point-gets each position's absolute
/// index. The other arm runs one durable range scan over exactly
/// `[head, tail − 1]`, and serves a window wider than
/// [`DEQUE_POINT_ITERATION_MAX`].
enum DequePlan<S: StateSession, T: CellType<Key = UnitKey>> {
    /// Point-get each planned absolute index. A backward stream reverses the
    /// index list at plan time.
    Points(CoordinatePlan<S, Keyed<I64KeyCodec, T>>),

    /// One durable range scan anchored on the window.
    Range(RangePlan<S, Keyed<I64KeyCodec, T>>),
}

impl<S, T> DequePlan<S, T>
where
    S: StateSession,
    T: CellType<Key = UnitKey>,
{
    /// Drives the planned arm and resolves each live entry.
    fn entries(self) -> impl Stream<Item = ScanItem<Keyed<I64KeyCodec, T>>> + Send
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        match self {
            Self::Points(plan) => Either::Left(plan.entries()),
            Self::Range(plan) => Either::Right(plan.entries()),
        }
    }
}

#[collection_methods(field = cells, session = S)]
impl<S, T> DequeHandle<S, T>
where
    S: StateSession,
    T: CellType<Key = UnitKey>,
{
    /// The number of live elements (`tail − head`, O(1) from the bounds cell).
    ///
    /// # Errors
    ///
    /// Returns a `Permanent` [`DequeStateError`] when the bounds cell is
    /// corrupt or the count exceeds `usize`, or an access error from the
    /// session.
    #[instrument(name = "deque.len", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[read(op)]
    pub async fn len(&self) -> Result<usize, DequeStateError<CellCodecError<T>>> {
        Ok(bounds(op).await?.len()?)
    }

    /// Whether the deque holds no live elements (`head == tail`).
    ///
    /// # Errors
    ///
    /// Returns a `Permanent` [`DequeStateError`] when the bounds cell is
    /// corrupt, or an access error from the session.
    #[instrument(name = "deque.is_empty", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[read(op)]
    pub async fn is_empty(&self) -> Result<bool, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        Ok(window.head == window.tail)
    }

    /// Reads and resolves the element at front-relative position `index`
    /// (`VecDeque::get` semantics): position `0` is the front, a single cell
    /// read at `head + index`, `None` when `index >= len`.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the cell does not decode, a
    /// `Permanent` meta error when the bounds cell is corrupt, or an access
    /// error from the session.
    #[instrument(
        name = "deque.get",
        skip_all,
        fields(collection = self.cells.name().as_str(), deque.index = Empty),
        err
    )]
    #[read(op)]
    pub async fn get(
        &self,
        index: usize,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        // Recorded as i64: the OTel layer exports signed ints as typed Int
        // attributes but stringifies unsigned values; a beyond-i64 index is
        // out of window anyway and stays unrecorded.
        if let Ok(index) = i64::try_from(index) {
            Span::current().record("deque.index", index);
        }
        let window = bounds(op).await?;
        if index >= window.len()? {
            return Ok(None);
        }
        let absolute = window.absolute(index)?;
        Ok(op.get(DequeKind::<T>::ENTRIES, &absolute).await?)
    }

    /// Reads and resolves the front element (position `0`) — exactly
    /// [`get(0)`](Self::get), reading the front slot `head` directly instead of
    /// deriving it from a position, so `None` when the deque is empty.
    ///
    /// # Endpoint-slot semantics
    ///
    /// A peek is an endpoint-*slot* read and never searches inward. Under a TTL
    /// the window can hold holes (see the module's window invariant): an
    /// expired endpoint slot yields `None` **even when [`len`](Self::len)
    /// `> 0` and live interior elements exist**, matching what a `get` at
    /// that position returns. [`peek_back`](Self::peek_back) is the
    /// symmetric back-endpoint read and shares this contract. Parity with
    /// `get` is total: an over-wide window whose span exceeds `usize` errors
    /// [`IndexOverflow`](MetaDecodeError::IndexOverflow) here exactly as it
    /// does through `get`'s length check — the span validation is pure
    /// arithmetic on the bounds cell already in hand, adding no read.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the entry does not decode, a
    /// `Permanent` meta error when the bounds cell is corrupt, or an access
    /// error from the session.
    #[instrument(name = "deque.peek_front", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[read(op)]
    pub async fn peek_front(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        if window.len()? == 0 {
            return Ok(None);
        }
        Ok(op.get(DequeKind::<T>::ENTRIES, &window.head).await?)
    }

    /// Reads and resolves the back element (position `len - 1`) — exactly
    /// [`get(len - 1)`](Self::get) reading the back slot `tail − 1` directly,
    /// and without the empty-deque negative-index error that a manual
    /// `len`-then-`get` incurs: `None` when the deque is empty. Shares
    /// [`peek_front`](Self::peek_front)'s endpoint-slot / TTL-hole contract and
    /// its total parity with `get`.
    ///
    /// # Errors
    ///
    /// See [`peek_front`](Self::peek_front).
    #[instrument(name = "deque.peek_back", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[read(op)]
    pub async fn peek_back(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        if window.len()? == 0 {
            return Ok(None);
        }
        let last = window
            .tail
            .checked_sub(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        Ok(op.get(DequeKind::<T>::ENTRIES, &last).await?)
    }

    /// Reads the bounds cell and captures the stream's arm as an owned plan.
    ///
    /// A window of at most [`DEQUE_POINT_ITERATION_MAX`] entries gives the
    /// chunked point-get arm over the window's absolute indices in `dir` order.
    /// A wider window gives one durable range scan over exactly
    /// `[head, tail − 1]`, under the window's own limit. That scan therefore
    /// reads no row outside the window.
    ///
    /// An empty window gives an empty point-get plan. It does zero reads, and
    /// its exhaustion still passes the stream fence.
    #[read(op)]
    async fn stream_plan(
        &self,
        dir: Direction,
    ) -> Result<DequePlan<S, T>, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        let len = window.len()?;
        if len > DEQUE_POINT_ITERATION_MAX {
            // Wide window: one durable range scan, anchored on the window.
            // It runs from the front `head` to the back `tail − 1`, and
            // mirrors backward. `len > 0` proves `tail − 1` does not underflow.
            let last = window
                .tail
                .checked_sub(1)
                .ok_or(MetaDecodeError::IndexOverflow)?;
            let (start, end) = match dir {
                Direction::Forward => (window.head, last),
                Direction::Backward => (last, window.head),
            };
            return Ok(DequePlan::Range(op.range_within(
                DequeKind::<T>::ENTRIES,
                &start,
                dir,
                &end,
                len,
            )));
        }
        // Point-get arm. `absolute` is monotone in the position. One check of
        // the extreme index therefore proves that every position in `[0, len)`
        // is in range, and that the coordinate list cannot fail.
        if len > 0 {
            window.absolute(len - 1)?;
        }
        let head = window.head;
        // `DEQUE_POINT_ITERATION_MAX` bounds this buffer at 128 × 8 B ≈ 1 KiB.
        // This code sizes it once and pays it once per stream construction,
        // never in the per-item steady state. Owned indices are what let one
        // driver serve the owner and the reader with no runtime branch.
        let mut indices: Vec<i64> = Vec::with_capacity(len);
        indices.extend((0..len).map(|position| head + position as i64));
        if dir == Direction::Backward {
            indices.reverse();
        }
        Ok(DequePlan::Points(
            op.coordinates(DequeKind::<T>::ENTRIES, indices),
        ))
    }

    /// Streams the live elements in index order — front to back for
    /// [`Direction::Forward`], back to front for [`Direction::Backward`]. Each
    /// element is resolved as it is yielded.
    ///
    /// # Per-arm consistency (position identity, a paged read, not a snapshot)
    ///
    /// The one bounds read at init snapshots the **position window**
    /// `[head, tail)`. That gives **position identity, not element identity**.
    /// Each position yields what its cell holds when the stream fetches its
    /// chunk. A pop before that fetch therefore reads absent, and the stream
    /// **skips** the position. This is the skip that a TTL hole already
    /// requires, never an error. A pop and then a push that reuses the position
    /// yields the new occupant.
    ///
    /// A window of at most `DEQUE_POINT_ITERATION_MAX` entries point-reads each
    /// absolute index in chunks of `STREAM_CHUNK`. A wider window falls back to
    /// one durable range scan over the same window. Both arms give identical
    /// items in identical order, live pages, and the same skip-absent rule.
    ///
    /// A bounded-arm read failure can surface **after** a yielded prefix.
    /// Chunked point gets yield the earlier chunks before a later chunk's read
    /// fails. The scan arm behaves the same way: it yields a prefix before it
    /// fails at a page boundary. Within a chunk the error is atomic, so a
    /// failing chunk yields none of its items.
    ///
    /// The stream takes session admission only for the init bounds read and
    /// once per chunk, at most `STREAM_CHUNK` point reads each. A chunk's
    /// admission covers its batch fetch, and the stream releases that admission
    /// before it decodes and resolves the chunk. The stream therefore holds no
    /// admission across a yield, for items and errors alike. A handler may
    /// mutate this deque between stream items without deadlock
    /// (`StreamYieldFree`; see [`SessionGate`](crate::state::session)).
    pub fn stream(
        &self,
        dir: Direction,
    ) -> impl Stream<Item = Result<ResolvedOf<T>, DequeStateError<CellCodecError<T>>>> + '_
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        // Hand-built span: `#[instrument]` cannot follow a returned `Stream`,
        // so each inner await is instrumented with a clone instead; the
        // span's recorded time is the stream's own work. Unlike the sibling
        // ops' `err`, failures are yielded per item rather than recorded on
        // the span — a failing chunk ends with an OK-status span, and the
        // yielded `Err` surfaces to the caller inside this span's scope.
        let span = info_span!(
            "deque.stream",
            collection = self.cells.name().as_str(),
            direction = ?dir,
        );
        try_stream! {
            // Init: `stream_plan` reads the bounds cell under an admission
            // that it drops as it returns, before this `?` sees the result.
            let inner = self.stream_plan(dir).instrument(span.clone()).await?.entries();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().instrument(span.clone()).await {
                // The driver yields the decoded index. The module's window
                // invariant makes that index redundant, so expose only the
                // resolved element.
                let (_, value) = item?;
                yield value;
            }
        }
    }

    /// Appends `value` at the back, extending the window to `tail + 1`.
    ///
    /// # Bounded capacity
    ///
    /// On a deque registered with a `capacity`, a push first evicts from the
    /// **front** toward the cap (see the module's capacity invariant). It
    /// evicts up to `TRIM_MAX` slots per push. Each eviction is one single-cell
    /// clear, staged beside the append and the bounds move. It runs no decode
    /// and no resolver, and it discards the evicted value.
    ///
    /// The evictions and the append stage as one transaction. A
    /// `ReadCommitted` rollback restores the evicted front slots.
    /// `ReadUncommitted` applies them eagerly.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when `value` does not encode, a
    /// `Permanent` meta error on index-space exhaustion, or an access error.
    #[instrument(name = "deque.push_back", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn push_back(
        &self,
        value: WriteOf<'_, T>,
    ) -> Result<(), DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        let next_tail = window
            .tail
            .checked_add(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let evict = i64::from(evictions(window, op.capacity()));
        let new_head = window
            .head
            .checked_add(evict)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        // Append first, the sole encode, then evict the front. `evict ≤ span`
        // (see `evictions`), so `new_head ≤ tail`. The cleared half-open range
        // therefore never holds the slot this push appended.
        op.set(DequeKind::<T>::ENTRIES, &window.tail, value)?;
        for index in window.head..new_head {
            op.clear(DequeKind::<T>::ENTRIES, &index);
        }
        write_bounds(op, Window::new(new_head, next_tail)?)
    }

    /// Prepends `value` at the front, extending the window to `head − 1`.
    ///
    /// This is the mirror of [`Self::push_back`]. On a bounded deque it evicts
    /// from the **back** toward the cap.
    ///
    /// # Errors
    ///
    /// See [`Self::push_back`].
    #[instrument(name = "deque.push_front", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn push_front(
        &self,
        value: WriteOf<'_, T>,
    ) -> Result<(), DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        let prev_head = window
            .head
            .checked_sub(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let evict = i64::from(evictions(window, op.capacity()));
        let new_tail = window
            .tail
            .checked_sub(evict)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        op.set(DequeKind::<T>::ENTRIES, &prev_head, value)?;
        for index in new_tail..window.tail {
            op.clear(DequeKind::<T>::ENTRIES, &index);
        }
        write_bounds(op, Window::new(prev_head, new_tail)?)
    }

    /// Removes and returns the front element, and moves `head` past it. Returns
    /// `None` when the deque is empty. The element resolves *before* the clear
    /// and the head move, so a resolve failure stages nothing at all.
    ///
    /// A pop is an endpoint-slot mutation. Under a TTL an expired front slot
    /// yields `None`, and the pop still consumes that slot: it clears the slot
    /// and moves `head` on. A `while let Some(v) = pop_front()` drain therefore
    /// stops at the first hole. See [`peek_front`](Self::peek_front) for the
    /// endpoint-slot contract.
    ///
    /// A cancelled or failed pop is atomic for a structural reason, not a
    /// checked one. One journal holds the whole invocation's mutations, and
    /// that journal replays only on a successful return.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the entry does not decode, a
    /// `Permanent` meta error on corruption, or an access error.
    #[instrument(name = "deque.pop_front", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn pop_front(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        if window.head >= window.tail {
            return Ok(None);
        }
        // `head < tail` bounds `head` strictly below `i64::MAX`, so the move
        // cannot overflow. The check keeps the arithmetic total.
        let next_head = window
            .head
            .checked_add(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let value = op.take(DequeKind::<T>::ENTRIES, &window.head).await?;
        write_bounds(op, Window::new(next_head, window.tail)?)?;
        Ok(value)
    }

    /// Removes and returns the back element, and moves `tail` back past it.
    /// Returns `None` when the deque is empty. This mirrors
    /// [`Self::pop_front`]: the element resolves before the mutation.
    ///
    /// # Errors
    ///
    /// See [`Self::pop_front`].
    #[instrument(name = "deque.pop_back", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn pop_back(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        if window.head >= window.tail {
            return Ok(None);
        }
        let last = window
            .tail
            .checked_sub(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let value = op.take(DequeKind::<T>::ENTRIES, &last).await?;
        write_bounds(op, Window::new(window.head, last)?)?;
        Ok(value)
    }

    /// Removes every element and the window bounds, and **resets the index
    /// space** (see the module's window invariant). Within the event the deque
    /// reads empty from this program point, and the next push writes index 0.
    /// After a commit, exactly the repopulated elements survive. After an
    /// abort, the deque is untouched.
    ///
    /// The cost is O(handler writes). One whole-layout reset covers both
    /// declared sections, so no cell takes a per-cell path.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(name = "deque.clear", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn clear(&self) -> Result<(), DequeStateError<CellCodecError<T>>> {
        op.clear_collection();
        Ok(())
    }

    /// Durably commits this deque's buffered ops mid-handler — entries and
    /// the window bounds together. At-least-once; see
    /// [`CellWrite::commit`](crate::state::session::CellWrite::commit) for the
    /// contract, including the over-budget batch split.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(name = "deque.commit", skip_all, fields(collection = self.cells.name().as_str()), err)]
    pub async fn commit(&self) -> Result<StoreOutcome, DequeStateError<CellCodecError<T>>>
    where
        S: WritableStateSession,
    {
        Ok(self.cells.commit().await?)
    }

    /// Discards this deque's buffered uncommitted ops — entries and the window
    /// bounds together — reverting reads to the last [`commit`](Self::commit),
    /// or the pre-event committed state if none. Infallible; see
    /// [`CellWrite::rollback`](crate::state::session::CellWrite::rollback) for
    /// the contract.
    #[instrument(name = "deque.rollback", skip_all, fields(collection = self.cells.name().as_str()))]
    pub async fn rollback(&self) -> StoreOutcome
    where
        S: WritableStateSession,
    {
        self.cells.rollback().await
    }
}

/// Declares a codec-backed deque collection named `name` over element cell type
/// `T` (JSON values by default). See
/// [`Descriptor::new`](super::Descriptor::new) for the `name` contract.
#[must_use]
pub fn deque_state<T>(name: &str) -> DequeDescriptor<T>
where
    T: CellType<Key = UnitKey>,
{
    DequeDescriptor::new(name)
}

impl<T> Descriptor<DequeKind<T>> {
    /// Bounds this deque to at most `capacity` window slots, enforced lazily on
    /// push: a `push_back` evicts from the front and a `push_front` from the
    /// back, at most `TRIM_MAX` slots per push and decode-free (see the
    /// module's capacity invariant). Runtime-only — never persisted, not part
    /// of identity, and freely changed (bounded ⇄ unbounded) across
    /// redeploys, so a reduction converges over the next pushes rather than
    /// atomically. `NonZeroUsize` keeps `0` unrepresentable.
    ///
    /// Available on Deque registrations only — a capacity on a Value or Map is
    /// uncompilable, since this inherent method exists only at this type.
    #[must_use]
    pub fn capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.def.capacity = Some(capacity);
        self
    }
}

/// Reads the bounds cell and lifts it to a validated [`Window`]. An absent
/// cell reads `[0, 0)`, which is a fresh or cleared deque. [`Window::new`]
/// validates `head ≤ tail`.
///
/// This function needs no `FromSession` bound. [`MetaCodec`] is a plain codec,
/// so its resolver context normalizes to `()`, and every session satisfies
/// that.
async fn bounds<C, T>(op: &mut C) -> Result<Window, DequeStateError<CellCodecError<T>>>
where
    C: CollectionRead<Layout = DequeKind<T>>,
    T: CellType<Key = UnitKey>,
{
    match op
        .get(DequeKind::<T>::BOUNDS, &())
        .await
        .map_err(meta_err)?
    {
        Some((head, tail)) => Ok(Window::new(head, tail)?),
        None => Ok(Window::EMPTY),
    }
}

/// Stages the bounds cell. The caller stages it in the same invocation as the
/// entry mutation it accompanies. The window move and its entry therefore
/// replay together (see the module docs).
fn write_bounds<C, T>(op: &mut C, window: Window) -> Result<(), DequeStateError<CellCodecError<T>>>
where
    C: CollectionWrite<Layout = DequeKind<T>>,
    T: CellType<Key = UnitKey>,
{
    op.set(DequeKind::<T>::BOUNDS, &(), (window.head, window.tail))
        .map_err(meta_err)
}

/// Slots to evict from the far end before a bounded push appends one,
/// converging the window toward `capacity`. Zero when unbounded or already
/// within capacity; capped at [`TRIM_MAX`], which is what makes the count fit a
/// `u8` and one push bounded, decode-free work. A push adds one slot, so
/// `len + 1` slots exist after the append and the trim is that count over
/// `capacity`.
///
/// In both branches the count stays at or below the window's own span. That is
/// what proves a push's eviction range cannot reach the slot it appended. The
/// measurable branch gives `min(len − (cap − 1), TRIM_MAX) ≤ len`. The
/// unmeasurable branch has a span of at least `i64::MAX as usize`, which is
/// `≥ TRIM_MAX`.
///
/// See the module's capacity invariant: enforcement is lazy and push-only, so a
/// persisted window may exceed `capacity`.
fn evictions(window: Window, capacity: Option<NonZeroUsize>) -> u8 {
    // Unbounded: never read `window.len()`, so a push on an over-wide window
    // (a span `Window::len` cannot measure — the `tail − head` `i64`
    // subtraction overflows, or on a 32-bit target the result exceeds `usize`;
    // reachable only from a corrupt or hand-seeded bounds cell) proceeds
    // untouched, exactly as it did before capacity existed.
    let Some(cap) = capacity else { return 0 };
    // Bounded but unmeasurable: `Window::len` fails — the `tail − head` `i64`
    // subtraction overflows (a 2^63-wide span), or on a 32-bit target the span
    // exceeds `usize`. `head <= tail` (Window invariant) makes that span a
    // length of at least `i64::MAX as usize`. Evict on that lower bound —
    // realistic caps still trim the max, while a cap so large the window is
    // actually within it under-evicts (down to zero) rather than erasing live
    // in-capacity cells. Only bounded deques ever pay the length read.
    let Ok(len) = window.len() else {
        return (i64::MAX as usize)
            .saturating_sub(cap.get() - 1)
            .min(TRIM_MAX) as u8;
    };
    // `len − (cap − 1)`, algebraically `(len + 1) − cap` but overflow-free
    // (`cap ≥ 1`): at `len == cap == usize::MAX` this is the correct single
    // eviction, where `(len + 1) − cap` would overflow and saturate to the max.
    len.saturating_sub(cap.get() - 1).min(TRIM_MAX) as u8
}

/// Re-homes a bounds-cell access or codec error under the deque's entry-codec
/// error parameter.
///
/// The [`MetaCodec`] pair types the bounds family. Its codec half is a corrupt
/// bounds frame of the wrong width, which this function routes to
/// [`DequeStateError::MetaFrame`]. Its access half joins the entries' [`Cell`]
/// arm. The key half cannot arise, because the bounds cell is unit-addressed,
/// but the match forwards it for exhaustiveness.
///
/// [`Cell`]: DequeStateError::Cell
fn meta_err<E>(err: CellStateError<MetaCodecError>) -> DequeStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    match err {
        CellStateError::Access(e) => CellStateError::Access(e).into(),
        CellStateError::Codec(frame) => DequeStateError::MetaFrame(frame),
        CellStateError::Key(e) => CellStateError::Key(e).into(),
    }
}

/// The deque's validated live window `[head, tail)`: a half-open index range
/// with `head ≤ tail`.
///
/// The [`MetaCodec`] validates the wire *form* (exactly 16 bytes); this type
/// validates the *meaning* (`head ≤ tail`), so a disordered window is
/// unrepresentable past the bounds boundary. It is deliberately not named
/// `Bounds`, to avoid confusion with [`std::ops::Bound`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Window {
    head: i64,
    tail: i64,
}

impl Window {
    /// The empty window that a fresh or cleared deque reads.
    const EMPTY: Self = Self { head: 0, tail: 0 };

    /// Lifts a decoded `(head, tail)` pair into a validated window, failing
    /// [`MetaDecodeError::Disordered`] when `tail < head`.
    fn new(head: i64, tail: i64) -> Result<Self, MetaDecodeError> {
        if tail < head {
            return Err(MetaDecodeError::Disordered { head, tail });
        }
        Ok(Self { head, tail })
    }

    /// The live-window length `tail − head` as a `usize`. `head ≤ tail` holds
    /// by construction, so the span is non-negative; a span past
    /// `i64`/`usize` is [`MetaDecodeError::IndexOverflow`].
    fn len(self) -> Result<usize, MetaDecodeError> {
        let span = self
            .tail
            .checked_sub(self.head)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        usize::try_from(span).map_err(|_| MetaDecodeError::IndexOverflow)
    }

    /// Maps a front-relative position to its absolute index `head + position`,
    /// failing [`MetaDecodeError::IndexOverflow`] past the index space.
    fn absolute(self, position: usize) -> Result<i64, MetaDecodeError> {
        let offset = i64::try_from(position).map_err(|_| MetaDecodeError::IndexOverflow)?;
        self.head
            .checked_add(offset)
            .ok_or(MetaDecodeError::IndexOverflow)
    }
}

/// Test-only: the single bounds cell at its frozen address, which is section 0
/// at the empty coordinate. A test reads the stored bounds frame directly
/// through it, and pins the deque's binding to the [`MetaCodec`] frame.
#[cfg(test)]
pub(crate) fn meta_cell() -> CellKey {
    CellKey {
        section: FrozenLayout::BOUNDS.section(),
        coordinate: <UnitKey as OrderedKeyCodec>::encode(&()),
    }
}

/// Test-only: the entry cell at index `coordinate`, which [`I64KeyCodec`] has
/// already encoded. A test seeds a sparse window directly through it. A live
/// deque never produces such holes, so this is how a test proves the
/// TTL'd-hole tolerance against the real store.
#[cfg(test)]
pub(crate) fn entry_cell_for(coordinate: &Coordinate) -> CellKey {
    CellKey {
        section: FrozenLayout::ENTRIES.section(),
        coordinate: coordinate.clone(),
    }
}

/// Test-only: the frozen `head ‖ tail` bounds frame as raw bytes. The frame is
/// two plain big-endian `i64`s, the [`MetaCodec`] layout that
/// `deque_meta_cell_bytes_are_frozen` pins. A test seeds the bounds cell
/// directly through it.
#[cfg(test)]
pub(crate) fn seed_frame(head: i64, tail: i64) -> Vec<u8> {
    [head.to_be_bytes(), tail.to_be_bytes()].concat()
}

/// Error from the deque's window bookkeeping. It is always `Permanent`,
/// because a retry cannot make a disordered or overflowing window valid. A
/// corrupt bounds *frame* of the wrong width is the `MetaCodec`'s own error,
/// which the handle reports as [`DequeStateError::MetaFrame`].
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum MetaDecodeError {
    /// The decoded bounds violated `head ≤ tail`.
    #[error("disordered deque bounds: head {head} > tail {tail}")]
    Disordered {
        /// The decoded head index.
        head: i64,
        /// The decoded tail index.
        tail: i64,
    },

    /// An index move or length exceeded the representable range.
    #[error("deque index space exhausted")]
    IndexOverflow,
}

impl ClassifyError for MetaDecodeError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

/// Error returned by [`DequeHandle`] operations.
#[derive(Debug, Error)]
pub enum DequeStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    /// A typed entry-cell op failed: an access error or an element-codec
    /// failure.
    #[error(transparent)]
    Cell(#[from] CellStateError<E>),

    /// The deque's bookkeeping was disordered or its index space exhausted.
    #[error(transparent)]
    Meta(#[from] MetaDecodeError),

    /// The stored head/tail bounds frame was corrupt (wrong width).
    #[error(transparent)]
    MetaFrame(#[from] MetaCodecError),
}

/// A raw access refusal reaches the handle as the access arm of a cell error.
/// That is the shape the scoped write invocation's final fence reports.
impl<E> From<StateAccessError> for DequeStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn from(error: StateAccessError) -> Self {
        Self::Cell(CellStateError::Access(error))
    }
}

impl<E> ClassifyError for DequeStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Cell(e) => e.classify_error(),
            Self::Meta(e) => e.classify_error(),
            // A corrupt bounds frame will not decode on retry.
            Self::MetaFrame(_) => ErrorCategory::Permanent,
        }
    }
}

#[cfg(test)]
mod tests;
