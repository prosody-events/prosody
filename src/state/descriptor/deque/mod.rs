//! Index-addressed double-ended queue collection.
//!
//! A Deque is a window of cells over a monotonic `i64` index space. Every
//! [`DequeHandle`] method runs as one scoped operation over the bound
//! collection.
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
//! `head + i`, and a stream reads only indices in `[head, tail)`, never a
//! popped tombstone (which sits below `head` or at/above `tail`).
//! [`DequeHandle::clear`] ends the window's lifetime and
//! **resets the index space**: the erased bounds cell reads `[0, 0)`, so the
//! next push writes index 0. Reuse is safe — every pre-clear row is erased by
//! the clear, and a later write to a reused coordinate out-stamps any earlier
//! tombstone (single writer, monotonic timestamps).
//!
//! Co-stamping keeps the window move and its entry mutation together. One
//! invocation stages both into one journal. They buffer as one op, and they
//! stage under one settle marker with one write TS/TTL, applied by the
//! session's settle-time `finalize`. Recovery therefore
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
//! `values` **skip** an expired index: an absent cell resolves as skipped
//! (`get` → `None`, `values` omits it), never as an error. These are the
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
use crate::codec::{I64Codec, I64CodecError, JsonCodec, PairCodecError};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::DequeQuery;
use crate::state::cell_key::Direction;
#[cfg(test)]
use crate::state::cell_key::{CellKey, Coordinate};
use crate::state::collection::{
    Collection, CollectionLayout, CollectionRead, CollectionWrite, JOURNAL_INLINE, Plan,
    StateSession, WritableStateSession, collection_layout, collection_methods, same_token,
    spec_matches,
};
use crate::state::order_codec::{I64KeyCodec, UnitKey};
use crate::state::{CollectionKindId, StateAccessError, StoreOutcome};
use educe::Educe;
use futures::Stream;
use std::error::Error;
use std::num::NonZeroUsize;
use std::ops::Bound;

use thiserror::Error;
use tracing::{Span, field::Empty, instrument};

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
        spec_matches::<FrozenLayout>(families[1]),
        "the spec's cell type addresses and encodes the entries family"
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
/// [`CellType`] `T` — a plain [`Codec`](crate::codec::Codec) (JSON by default),
/// or a codec paired with a resolver via [`WithResolver`](super::WithResolver).
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

mod handle;
pub use handle::DequeHandle;

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
    /// Bounds this deque to at most `capacity` window slots. Enforcement is
    /// lazy and push-only; see the module's capacity invariant.
    /// `NonZeroUsize` keeps `0` unrepresentable.
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
    op.set(DequeKind::<T>::BOUNDS.at(&()), (window.head, window.tail))
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

#[cfg(test)]
pub(crate) use tests::{entry_cell_for, meta_cell, seed_frame};

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

/// An access refusal reaches the handle as the access arm of a cell error.
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
