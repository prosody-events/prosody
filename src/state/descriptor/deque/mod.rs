//! Index-addressed double-ended queue collection.
//!
//! A Deque is a window of cells over a monotonic `i64` index space. The
//! [`DequeHandle`] composes the uniform `CellView` typed cell interface —
//! there is no Deque-specific store, session, or backend. Build a descriptor
//! with [`deque_state`], register it with the consumer, and bind the
//! [`Registered`](super::Registered) handle through
//! [`EventContext::state`](crate::consumer::event_context::EventContext::state).
//!
//! # Layout
//!
//! Two sections separate bookkeeping from data (`DequeNs`):
//!
//! * `Meta` holds one unit-addressed cell: `head ‖ tail` as two big-endian
//!   `i64`s, encoded by the [`(I64Codec, I64Codec)`](crate::codec::FixedCodec)
//!   pair codec with no framing. `head == tail` (and the absent cell, read as
//!   the empty window `[0, 0)`) is empty.
//! * `Entries` holds one cell per live element, addressed by the sign-flipped
//!   big-endian index ([`I64KeyCodec`]) so the clustering byte order is the
//!   signed index order, and typed by the element cell type `T`.
//!
//! # Invariant: monotonic window within its lifetime; dense without a TTL
//!
//! `[head, tail)` is a contiguous window with `head ≤ tail`, and indices are
//! monotonic and never reused **within a window's lifetime** — a pop advances
//! `head`/`tail` past the freed index, never back into it. So `len` is
//! `tail − head` (O(1) from the meta), `get(i)` reads the single cell at
//! `head + i`, and iteration point-reads each index in `[head, tail)`, never a
//! popped tombstone (which sits below `head` or at/above `tail`).
//! [`DequeHandle::clear`] ends the window's lifetime and
//! **resets the index space**: the erased bounds cell reads `[0, 0)`, so the
//! next push writes index 0. Reuse is safe — every pre-clear row is erased by
//! the clear, and a later write to a reused coordinate out-stamps any earlier
//! tombstone (single writer, monotonic timestamps). Co-stamping keeps the
//! window move and its entry mutation together: they buffer as one op and stage
//! under one settle marker with one write TS/TTL (see
//! [`KeyedStateSession::finalize`](crate::state::session)), recoverable
//! together whatever the batching — and a mid-handler [`DequeHandle::commit`]
//! drains them resolved and marker-free: one atomic batch within the batch
//! budget, but an over-budget commit can crash mid-split (the collection-grain
//! over-budget residual on `CellStore`,
//! shared with the Map keyset).
//!
//! **Without a TTL the window is also dense**: every index in `[head, tail)`
//! maps to a present entry cell, so `len` is exact and iteration yields exactly
//! `len` elements. **With a TTL** an entry's expiry is anchored at its push, so
//! entries can expire *inside* the window while it stays put — the window
//! develops holes. The bounds cell is rewritten by every op, so it outlives the
//! entries and `head`/`tail` are unaffected. Under holes `len` is an **upper
//! bound** on the live count, and `get`/`stream` **skip** an expired index — an
//! absent cell resolves as skipped (`get` → `None`, `stream` omits it), never
//! an error. This is acceptable time-window semantics: a TTL'd deque is a
//! sliding window of not-yet-expired elements.
//!
//! # Invariant: capacity
//!
//! A registered `capacity` is a runtime-only cap on window slots — never
//! persisted, not part of identity, freely changed across redeploys. It is
//! enforced **lazily, on push only**: reads, `len`, iteration, `pop`, and
//! `clear` never enforce it. A bounded [`DequeHandle::push_back`] evicts from
//! the **front** and [`DequeHandle::push_front`] from the **back**, at most
//! `TRIM_MAX` slots per push and decode-free / resolver-free — each a
//! single-cell clear co-stamped with the append and the bounds move. So a
//! persisted window may exceed the cap; for a **measurable** window a reduction
//! of excess `D` converges in `⌈D / (TRIM_MAX − 1)⌉` pushes. An unmeasurable
//! span (only reachable from a corrupt or hand-seeded bounds cell) under an
//! absurd (`≈ 2^63`) cap deliberately under-evicts and may not converge — the
//! safe direction, never erasing in-capacity cells (see `evictions`).

use super::{
    CellCodecError, CellScope, CellStateError, CellType, CellView, CollectionSpec, ContextOf,
    Descriptor, FromSession, Keyed, ResolvedOf, WriteOf,
};
use crate::codec::{I64Codec, I64CodecError, JsonCodec, PairCodecError};
use crate::error::{ClassifyError, ErrorCategory};
#[cfg(test)]
use crate::state::cell_key::{CellKey, Coordinate};
use crate::state::cell_key::{Direction, ScanEdge, Section};
use crate::state::order_codec::{I64KeyCodec, UnitKey};
use crate::state::session::{CellRead, CellWrite};
use crate::state::{CollectionKindId, StoreOutcome};
use async_stream::try_stream;
use educe::Educe;
use futures::stream::{Stream, StreamExt};
use std::error::Error;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use thiserror::Error;
use tracing::{Instrument, Span, field::Empty, info_span, instrument};

/// The deque's head/tail meta codec: two big-endian `i64`s composed with no
/// framing, byte-identical to the frozen 16-byte `head ‖ tail` frame.
type MetaCodec = (I64Codec, I64Codec);

/// The [`MetaCodec`] decode error — a corrupt (wrong-width) bounds frame.
type MetaCodecError = PairCodecError<I64CodecError, I64CodecError>;

/// The `Meta` section, holding the single bounds cell.
const META_SECTION: Section = Section::new(DequeNs::Meta as i8);

/// The `Entries` section, holding one cell per live element.
const ENTRY_SECTION: Section = Section::new(DequeNs::Entries as i8);

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

/// Deque's section enum, lowered to the opaque [`Section`]. Frozen: the
/// discriminants are a durable wire contract (the `section tinyint` column), so
/// the [`TryFrom`] guard rejects any other value as [`UnknownDequeSection`]
/// (`Permanent`).
#[repr(i8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DequeNs {
    /// Bookkeeping: the head/tail bounds cell.
    Meta = 0,

    /// Data: one cell per live element.
    Entries = 1,
}

impl From<DequeNs> for i8 {
    fn from(section: DequeNs) -> Self {
        section as i8
    }
}

impl TryFrom<i8> for DequeNs {
    type Error = UnknownDequeSection;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Meta),
            1 => Ok(Self::Entries),
            _ => Err(UnknownDequeSection(value)),
        }
    }
}

/// Descriptor for a codec-backed deque collection. Generic over an element
/// [`CellType`] `T` — a plain [`Codec`](crate::codec::Codec) (JSON by default)
/// or a codec paired with a resolver via [`WithResolver`](super::WithResolver).
/// There is no key-codec parameter: the index encoding is fixed by the kind.
/// Declare via [`deque_state`].
pub type DequeDescriptor<T = JsonCodec> = Descriptor<DequeKind<T>>;

/// The Deque [`CollectionSpec`]: an index window plus the head/tail bounds
/// cell. The index encoding is pinned by the kind ([`I64KeyCodec`]) — never a
/// registration choice — and rides the identity's key-codec token like any
/// other key axis.
pub struct DequeKind<T>(PhantomData<fn() -> T>);

impl<T: CellType<Key = UnitKey>> CollectionSpec for DequeKind<T> {
    type Cell = Keyed<I64KeyCodec, T>;
    type Handle<S: CellRead> = DequeHandle<S, T>;

    const KIND: CollectionKindId = CollectionKindId::Deque;

    fn handle<S: CellRead>(scope: CellScope<S>) -> DequeHandle<S, T> {
        DequeHandle {
            entries: scope.typed(ENTRY_SECTION),
            meta: scope.typed(META_SECTION),
        }
    }
}

/// Typed, owned handle over a codec-backed deque — a thin composition over two
/// typed `CellView`s: `entries` (the per-index data cells, addressed by the
/// `i64` index and typed by the element cell type `T`) and `meta` (the single
/// head/tail bounds cell, lifted to a validated `Window` over the
/// `MetaCodec` pair). Every operation guards on session termination through
/// the views. Cheap `Clone`.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct DequeHandle<S, T> {
    entries: CellView<S, Keyed<I64KeyCodec, T>>,
    meta: CellView<S, MetaCodec>,
}

impl<S, T> DequeHandle<S, T>
where
    S: CellRead,
    T: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    /// The number of live elements (`tail − head`, O(1) from the bounds cell).
    ///
    /// # Errors
    ///
    /// Returns a `Permanent` [`DequeStateError`] when the bounds cell is
    /// corrupt or the count exceeds `usize`, or an access error from the
    /// session.
    #[instrument(name = "deque.len", skip_all, fields(collection = self.entries.name().as_str()), err)]
    pub async fn len(&self) -> Result<usize, DequeStateError<CellCodecError<T>>> {
        let permit = self.entries.read_permit().await;
        Ok(self.bounds(&permit).await?.len()?)
    }

    /// Whether the deque holds no live elements (`head == tail`).
    ///
    /// # Errors
    ///
    /// Returns a `Permanent` [`DequeStateError`] when the bounds cell is
    /// corrupt, or an access error from the session.
    #[instrument(name = "deque.is_empty", skip_all, fields(collection = self.entries.name().as_str()), err)]
    pub async fn is_empty(&self) -> Result<bool, DequeStateError<CellCodecError<T>>> {
        let permit = self.entries.read_permit().await;
        let window = self.bounds(&permit).await?;
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
        fields(collection = self.entries.name().as_str(), deque.index = Empty),
        err
    )]
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
        let permit = self.entries.read_permit().await;
        let window = self.bounds(&permit).await?;
        if index >= window.len()? {
            return Ok(None);
        }
        let absolute = window.absolute(index)?;
        Ok(self.entries.get(&permit, &absolute).await?)
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
    #[instrument(name = "deque.peek_front", skip_all, fields(collection = self.entries.name().as_str()), err)]
    pub async fn peek_front(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let permit = self.entries.read_permit().await;
        let window = self.bounds(&permit).await?;
        if window.len()? == 0 {
            return Ok(None);
        }
        Ok(self.entries.get(&permit, &window.head).await?)
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
    #[instrument(name = "deque.peek_back", skip_all, fields(collection = self.entries.name().as_str()), err)]
    pub async fn peek_back(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let permit = self.entries.read_permit().await;
        let window = self.bounds(&permit).await?;
        if window.len()? == 0 {
            return Ok(None);
        }
        let last = window
            .tail
            .checked_sub(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        Ok(self.entries.get(&permit, &last).await?)
    }

    /// Streams the live elements in index order — front to back for
    /// [`Direction::Forward`], back to front for [`Direction::Backward`]. Each
    /// element is resolved as it is yielded.
    ///
    /// # Per-arm consistency (position identity, a paged read, not a snapshot)
    ///
    /// Iteration maps each position in `[head, tail)` (see the module's window
    /// invariant) to its absolute index and point-reads it through the scan
    /// shell's coordinate source (`CellView::scan_at`), in gate-scoped chunks
    /// of `STREAM_CHUNK`. The absolute indices are computed arithmetically from
    /// the snapshot window with the extreme index validated once at plan time
    /// (`absolute` is monotone, so the coordinate iterator is infallible). The
    /// **position window** `[head, tail)` is snapshotted at init — **position
    /// identity, not element identity**: each position yields whatever the cell
    /// holds when its chunk is fetched, so a pop observed before the fetch
    /// reads absent and is **skipped** (the same skip a TTL hole already
    /// requires, never an error), and a pop-then-push reusing the position
    /// yields the new occupant. Windows wider than
    /// `DEQUE_POINT_ITERATION_MAX` entries fall back to the scan shell's range
    /// source over the window — identical items in identical order, live
    /// pages, same skip-absent semantics.
    ///
    /// A bounded-arm read failure may surface **after** a yielded prefix (a
    /// deliberate change: chunked point gets yield the prior chunks before a
    /// later chunk's read fails, exactly as the scan arm has always yielded a
    /// prefix before failing at a page boundary — the two arms' error contracts
    /// converge); within a chunk the error is atomic — a failing chunk yields
    /// none of its items.
    ///
    /// The gate is held only for the init bounds read and per chunk
    /// (≤ `STREAM_CHUNK` point reads each): each chunk is fetched, decoded, and
    /// resolved under one permit that dies with the chunk future's scope before
    /// any of its items reach user code, so the gate is never held across a
    /// yield (items and errors alike) — a handler may mutate this deque between
    /// stream items without deadlock (`StreamYieldFree`; see
    /// [`SessionGate`](crate::state::session)).
    pub fn stream(
        &self,
        dir: Direction,
    ) -> impl Stream<Item = Result<ResolvedOf<T>, DequeStateError<CellCodecError<T>>>> + '_ {
        // Hand-built span: `#[instrument]` cannot follow a returned `Stream`,
        // so each inner await is instrumented with a clone instead; the
        // span's recorded time is the stream's own work. Unlike the sibling
        // ops' `err`, failures are yielded per item rather than recorded on
        // the span — a failing chunk ends with an OK-status span, and the
        // yielded `Err` surfaces to the caller inside this span's scope.
        let span = info_span!(
            "deque.stream",
            collection = self.entries.name().as_str(),
            direction = ?dir,
        );
        try_stream! {
            // Init: read the window bounds under one permit, released before any
            // resolve or yield. The fallible read runs inside an inner `async`
            // block (which `try_stream!` leaves untransformed, so its `?` is an
            // ordinary early return that drops the permit), so the outer `?`
            // fires only after the permit is dropped.
            let (window, len) = {
                let permit = self.entries.read_permit().instrument(span.clone()).await;
                let init = async {
                    let window = self.bounds(&permit).await?;
                    let len = window.len()?;
                    Ok::<_, DequeStateError<CellCodecError<T>>>((window, len))
                }
                .instrument(span.clone())
                .await;
                drop(permit);
                init?
            };
            if len > DEQUE_POINT_ITERATION_MAX {
                // Wide window: one durable range scan, anchored on the window —
                // front `head` to back `tail − 1` (`len > 0` proves `tail − 1`
                // does not underflow), mirrored backward. The scan drops the
                // gate after its own init and pages live thereafter.
                let head = window.head;
                let last = window
                    .tail
                    .checked_sub(1)
                    .ok_or(MetaDecodeError::IndexOverflow)?;
                let (start, end) = match dir {
                    Direction::Forward => (ScanEdge::Included(&head), ScanEdge::Included(&last)),
                    Direction::Backward => (ScanEdge::Included(&last), ScanEdge::Included(&head)),
                };
                let inner = self.entries.scan(start, dir, end, Some(len));
                futures::pin_mut!(inner);
                while let Some(item) = inner.next().instrument(span.clone()).await {
                    // The scan yields the decoded index; the module's window
                    // invariant makes it redundant, so only the resolved element
                    // is exposed.
                    let (_, value) = item?;
                    yield value;
                }
            } else {
                // Point-get arm through the scan shell. Positions map to
                // absolute indices arithmetically from the snapshot window:
                // `absolute` is monotone in the position, so validating the
                // extreme index ONCE proves every position in `[0, len)` is in
                // range and the coordinate iterator is infallible. An empty
                // window (`len == 0`) feeds an empty iterator, so it yields
                // nothing but still passes the scan-shell fence on exhaustion.
                let head = window.head;
                if len > 0 {
                    window.absolute(len - 1)?; // validate extreme; monotone ⇒ all valid
                }
                let coords = (0..len).map(move |i| {
                    let position = match dir {
                        Direction::Forward => i,
                        Direction::Backward => len - 1 - i,
                    };
                    // Infallible: the extreme index validated above and
                    // `absolute` is monotone, so this equals the checked path.
                    head + position as i64
                });
                let inner = self.entries.scan_at(coords);
                futures::pin_mut!(inner);
                while let Some(item) = inner.next().instrument(span.clone()).await {
                    // The scan yields the decoded index; the module's window
                    // invariant makes it redundant, so only the resolved element
                    // is exposed.
                    let (_, value) = item?;
                    yield value;
                }
            }
        }
    }

    /// Reads the bounds cell, lifting it to a validated [`Window`] (`[0, 0)`
    /// when absent — a fresh/empty deque). [`Window::new`] validates
    /// `head ≤ tail`.
    async fn bounds(
        &self,
        permit: &S::Permit<'_>,
    ) -> Result<Window, DequeStateError<CellCodecError<T>>> {
        match self.meta.get(permit, &()).await.map_err(meta_err)? {
            Some((head, tail)) => Ok(Window::new(head, tail)?),
            None => Ok(Window { head: 0, tail: 0 }),
        }
    }
}

impl<S, T> DequeHandle<S, T>
where
    S: CellWrite,
    T: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    /// Appends `value` at the back, extending the window to `tail + 1`.
    ///
    /// # Bounded capacity
    ///
    /// On a deque registered with a `capacity`,
    /// this first evicts from the **front** toward the cap (see the module's
    /// capacity invariant): up to `TRIM_MAX` slots per push, each a single-cell
    /// clear co-stamped with the append and the bounds move — no decode, no
    /// resolver, the evicted value discarded. The evictions and the append
    /// stage as one transaction (`ReadCommitted` rollback restores the
    /// evicted front slots; `ReadUncommitted` applies them eagerly).
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when `value` does not encode, a
    /// `Permanent` meta error on index-space exhaustion, or an access error.
    #[instrument(name = "deque.push_back", skip_all, fields(collection = self.entries.name().as_str()), err)]
    pub async fn push_back(
        &self,
        value: WriteOf<'_, T>,
    ) -> Result<(), DequeStateError<CellCodecError<T>>> {
        let permit = self.mutate_permit().await?;
        let window = self.bounds(&permit).await?;
        // Compute every fallible endpoint before mutating: a caught overflow or
        // encode error must leave the deque untouched, never a partial eviction
        // with stale bounds.
        let next_tail = window
            .tail
            .checked_add(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let evict = i64::try_from(evictions(window, self.entries.capacity()))
            .map_err(|_| MetaDecodeError::IndexOverflow)?;
        let new_head = window
            .head
            .checked_add(evict)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        // Append first (the sole encode); then evict the front. `evict ≤ len`
        // (`cap ≥ 1`), so `new_head ≤ tail` and the window never inverts.
        self.entries.set(&permit, &window.tail, value).await?;
        for idx in window.head..new_head {
            self.entries.clear(&permit, &idx).await?;
        }
        self.write_bounds(&permit, new_head, next_tail).await?;
        Ok(())
    }

    /// Prepends `value` at the front, extending the window to `head − 1`.
    ///
    /// The mirror of [`Self::push_back`]: on a bounded deque this evicts from
    /// the **back** toward the cap.
    ///
    /// # Errors
    ///
    /// See [`Self::push_back`].
    #[instrument(name = "deque.push_front", skip_all, fields(collection = self.entries.name().as_str()), err)]
    pub async fn push_front(
        &self,
        value: WriteOf<'_, T>,
    ) -> Result<(), DequeStateError<CellCodecError<T>>> {
        let permit = self.mutate_permit().await?;
        let window = self.bounds(&permit).await?;
        let prev_head = window
            .head
            .checked_sub(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let evict = i64::try_from(evictions(window, self.entries.capacity()))
            .map_err(|_| MetaDecodeError::IndexOverflow)?;
        let new_tail = window
            .tail
            .checked_sub(evict)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        self.entries.set(&permit, &prev_head, value).await?;
        for idx in new_tail..window.tail {
            self.entries.clear(&permit, &idx).await?;
        }
        self.write_bounds(&permit, prev_head, new_tail).await?;
        Ok(())
    }

    /// Removes and returns the front element, advancing `head` past it (`None`
    /// when empty). The element resolves *before* the clear and head move, so a
    /// resolve failure leaves the deque unmutated; the residue clear and the
    /// head move then co-stamp.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the entry does not decode, a
    /// `Permanent` meta error on corruption, or an access error.
    #[instrument(name = "deque.pop_front", skip_all, fields(collection = self.entries.name().as_str()), err)]
    pub async fn pop_front(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let permit = self.mutate_permit().await?;
        let window = self.bounds(&permit).await?;
        if window.head >= window.tail {
            return Ok(None);
        }
        let value = self.entries.get(&permit, &window.head).await?;
        self.entries.clear(&permit, &window.head).await?;
        let next_head = window
            .head
            .checked_add(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        self.write_bounds(&permit, next_head, window.tail).await?;
        Ok(value)
    }

    /// Removes and returns the back element, retracting `tail` past it (`None`
    /// when empty). Symmetric with [`Self::pop_front`]: the element resolves
    /// before the mutation.
    ///
    /// # Errors
    ///
    /// See [`Self::pop_front`].
    #[instrument(name = "deque.pop_back", skip_all, fields(collection = self.entries.name().as_str()), err)]
    pub async fn pop_back(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let permit = self.mutate_permit().await?;
        let window = self.bounds(&permit).await?;
        if window.head >= window.tail {
            return Ok(None);
        }
        let last = window
            .tail
            .checked_sub(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let value = self.entries.get(&permit, &last).await?;
        self.entries.clear(&permit, &last).await?;
        self.write_bounds(&permit, window.head, last).await?;
        Ok(value)
    }

    /// Removes every element and the window bounds, **resetting the index
    /// space** (see the module's window invariant): within the event the
    /// deque reads empty from this program point, and the next push writes
    /// index 0. Committed, exactly the repopulated elements survive; aborted,
    /// the deque is untouched. O(handler writes) — the entry section rides
    /// the durable section clear; only the window cell takes the per-cell
    /// path.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(name = "deque.clear", skip_all, fields(collection = self.entries.name().as_str()), err)]
    pub async fn clear(&self) -> Result<(), DequeStateError<CellCodecError<T>>> {
        let permit = self.mutate_permit().await?;
        self.entries.clear_all(&permit).await?;
        self.meta.clear(&permit, &()).await.map_err(meta_err)
    }

    /// Durably commits this deque's buffered ops mid-handler — entries and
    /// the window bounds together. At-least-once; see [`CellWrite::commit`]
    /// for the contract, including the over-budget batch split.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(name = "deque.commit", skip_all, fields(collection = self.entries.name().as_str()), err)]
    pub async fn commit(&self) -> Result<StoreOutcome, DequeStateError<CellCodecError<T>>> {
        let permit = self.mutate_permit().await?;
        Ok(self.entries.commit(&permit).await?)
    }

    /// Discards this deque's buffered uncommitted ops — entries and the window
    /// bounds together — reverting reads to the last [`commit`](Self::commit),
    /// or the pre-event committed state if none. Infallible; see
    /// [`CellWrite::rollback`] for the contract.
    #[instrument(name = "deque.rollback", skip_all, fields(collection = self.entries.name().as_str()))]
    pub async fn rollback(&self) -> StoreOutcome {
        self.entries.rollback().await
    }

    /// Acquires the session operation gate for a mutator, re-homing the
    /// closed-session error under the deque's error type.
    async fn mutate_permit(
        &self,
    ) -> Result<S::MutatePermit<'_>, DequeStateError<CellCodecError<T>>> {
        self.entries
            .mutate_permit()
            .await
            .map_err(|e| CellStateError::Access(e).into())
    }

    /// Buffers the bounds cell. Co-stamped with the entry mutation a single op
    /// also buffers, so the move and its entry stage together (module docs).
    async fn write_bounds(
        &self,
        permit: &S::MutatePermit<'_>,
        head: i64,
        tail: i64,
    ) -> Result<(), DequeStateError<CellCodecError<T>>> {
        self.meta
            .set(permit, &(), (head, tail))
            .await
            .map_err(meta_err)
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

/// Slots to evict from the far end before a bounded push appends one,
/// converging the window toward `capacity`. Zero when unbounded or already
/// within capacity; capped at `TRIM_MAX` so one push does bounded, decode-free
/// work. A push adds one slot, so `len + 1` slots exist after the append and
/// the trim is that count over `capacity`.
///
/// See the module's capacity invariant: enforcement is lazy and push-only, so a
/// persisted window may exceed `capacity`.
fn evictions(window: Window, capacity: Option<NonZeroUsize>) -> usize {
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
            .min(TRIM_MAX);
    };
    // `len − (cap − 1)`, algebraically `(len + 1) − cap` but overflow-free
    // (`cap ≥ 1`): at `len == cap == usize::MAX` this is the correct single
    // eviction, where `(len + 1) − cap` would overflow and saturate to the max.
    len.saturating_sub(cap.get() - 1).min(TRIM_MAX)
}

/// Re-homes a bounds-cell access or codec error under the deque's entry-codec
/// error parameter. The `meta` view is the [`MetaCodec`] pair, so its codec
/// half is a corrupt (wrong-width) bounds frame routed to
/// [`DequeStateError::MetaFrame`]; its access half joins the entries' [`Cell`]
/// arm. The key half cannot arise (the meta cell is unit-addressed) but is
/// forwarded for exhaustiveness.
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
/// unrepresentable past the meta boundary. It is deliberately not named
/// `Bounds`, to avoid confusion with [`std::ops::Bound`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Window {
    head: i64,
    tail: i64,
}

impl Window {
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

/// Error converting an `i8` that matches no [`DequeNs`] variant.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown deque section discriminant: {0}")]
struct UnknownDequeSection(i8);

/// Error deriving the deque's `Meta` bookkeeping. Always `Permanent`: a
/// disordered or overflowing window will not start being valid on retry. A
/// corrupt bounds *frame* (wrong width) is the `MetaCodec`'s own error,
/// surfaced as [`DequeStateError::MetaFrame`].
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

/// Test-only: the single bounds cell at its frozen address (`Meta` section,
/// empty coordinate), so a test can read the stored meta frame directly and
/// pin the deque's binding to the [`MetaCodec`] frame.
#[cfg(test)]
pub(crate) fn meta_cell() -> CellKey {
    CellKey {
        section: META_SECTION,
        coordinate: Coordinate::empty(),
    }
}

/// Test-only: the entry cell at index `coordinate` (already encoded through
/// [`I64KeyCodec`]), so a test can seed a sparse window directly — entries with
/// holes a live deque never produces — and prove the TTL'd-hole tolerance
/// against the real store.
#[cfg(test)]
pub(crate) fn entry_cell_for(coordinate: &Coordinate) -> CellKey {
    CellKey {
        section: ENTRY_SECTION,
        coordinate: coordinate.clone(),
    }
}

/// Test-only: the frozen `head ‖ tail` meta frame as raw bytes — two plain
/// big-endian `i64`s, the [`MetaCodec`] layout pinned by
/// `deque_meta_cell_bytes_are_frozen` — so a test can seed the bounds cell
/// directly.
#[cfg(test)]
pub(crate) fn seed_frame(head: i64, tail: i64) -> Vec<u8> {
    [head.to_be_bytes(), tail.to_be_bytes()].concat()
}

#[cfg(test)]
mod tests;
