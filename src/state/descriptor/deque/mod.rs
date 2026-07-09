//! Index-addressed double-ended queue collection.
//!
//! A Deque is a dense window of cells over a monotonic `i64` index space. The
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
//! # Invariant: dense, monotonic window
//!
//! `[head, tail)` is a dense, fully-live, contiguous window: every index in the
//! range maps to a present entry cell, `head ≤ tail`, and indices are monotonic
//! and never reused — a pop advances `head`/`tail` past the freed index, never
//! back into it. So `len` is `tail − head` (O(1) from the meta), `get(i)` is
//! the single cell at `head + i`, and a scan anchored at `head` with `limit =
//! tail − head` visits exactly the live window, never a popped tombstone (which
//! sits below `head` or at/above `tail`). Co-stamping makes the window heal as
//! a unit: a handler's entry mutation and the bounds move it buffers in one op
//! stage in one batch with one write TS/TTL (see
//! [`KeyedStateSession::finalize`](crate::state::session)) — and a mid-handler
//! [`DequeHandle::flush`] drains them in one batch the same way — so the
//! bounds cell can never outlive — or be outlived by — the entries it anchors.

use super::{
    CellCodecError, CellScope, CellStateError, CellType, CellView, CollectionSpec, ContextOf,
    Descriptor, FromSession, Keyed, ResolvedOf, WriteOf,
};
use crate::codec::{I64Codec, I64CodecError, JsonCodec, PairCodecError};
use crate::error::{ClassifyError, ErrorCategory};
#[cfg(test)]
use crate::state::cell_key::{CellKey, Coordinate};
use crate::state::cell_key::{Direction, Section};
use crate::state::order_codec::{I64KeyCodec, UnitKey};
use crate::state::session::CellSession;
use crate::state::{CollectionKindId, StoreOutcome};
use async_stream::try_stream;
use educe::Educe;
use futures::stream::{Stream, StreamExt};
use std::error::Error;
use std::marker::PhantomData;
use std::ops::Bound;
use thiserror::Error;

/// The deque's head/tail meta codec: two big-endian `i64`s composed with no
/// framing, byte-identical to the frozen 16-byte `head ‖ tail` frame.
type MetaCodec = (I64Codec, I64Codec);

/// The [`MetaCodec`] decode error — a corrupt (wrong-width) bounds frame.
type MetaCodecError = PairCodecError<I64CodecError, I64CodecError>;

/// The `Meta` section, holding the single bounds cell.
const META_SECTION: Section = Section::new(DequeNs::Meta as i8);

/// The `Entries` section, holding one cell per live element.
const ENTRY_SECTION: Section = Section::new(DequeNs::Entries as i8);

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

/// The Deque [`CollectionSpec`]: a dense index window plus the head/tail bounds
/// cell. The index encoding is pinned by the kind ([`I64KeyCodec`]) — never a
/// registration choice — and rides the identity's key-codec token like any
/// other key axis.
pub struct DequeKind<T>(PhantomData<fn() -> T>);

impl<T: CellType<Key = UnitKey>> CollectionSpec for DequeKind<T> {
    type Cell = Keyed<I64KeyCodec, T>;
    type Handle<S: CellSession> = DequeHandle<S, T>;

    const KIND: CollectionKindId = CollectionKindId::Deque;

    fn handle<S: CellSession>(scope: CellScope<S>) -> DequeHandle<S, T> {
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
    S: CellSession,
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
    pub async fn len(&self) -> Result<usize, DequeStateError<CellCodecError<T>>> {
        Ok(self.bounds().await?.len()?)
    }

    /// Whether the deque holds no live elements (`head == tail`).
    ///
    /// # Errors
    ///
    /// Returns a `Permanent` [`DequeStateError`] when the bounds cell is
    /// corrupt, or an access error from the session.
    pub async fn is_empty(&self) -> Result<bool, DequeStateError<CellCodecError<T>>> {
        let window = self.bounds().await?;
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
    pub async fn get(
        &self,
        index: usize,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let window = self.bounds().await?;
        if index >= window.len()? {
            return Ok(None);
        }
        let offset = i64::try_from(index).map_err(|_| MetaDecodeError::IndexOverflow)?;
        let absolute = window
            .head
            .checked_add(offset)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        Ok(self.entries.get(&absolute).await?)
    }

    /// Streams the live elements in index order — front to back for
    /// [`Direction::Forward`], back to front for [`Direction::Backward`]. Each
    /// element is resolved as it is yielded. See the module's dense-window
    /// invariant: the scan anchors at the window's leading edge and stops after
    /// `len` cells, so a popped tombstone is never yielded.
    pub fn stream(
        &self,
        dir: Direction,
    ) -> impl Stream<Item = Result<ResolvedOf<T>, DequeStateError<CellCodecError<T>>>> + '_ {
        try_stream! {
            let window = self.bounds().await?;
            let len = window.len()?;
            if len == 0 {
                return;
            }
            // The leading-edge index: `head` forward, the last live index
            // `tail − 1` backward (`len > 0` proves it does not underflow).
            let anchor = match dir {
                Direction::Forward => window.head,
                Direction::Backward => window
                    .tail
                    .checked_sub(1)
                    .ok_or(MetaDecodeError::IndexOverflow)?,
            };
            let inner = self
                .entries
                .scan(Bound::Included(&anchor), dir, Bound::Unbounded, Some(len));
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                // The scan yields the decoded index; the dense window makes it
                // redundant, so only the resolved element is exposed.
                let (_, value) = item?;
                yield value;
            }
        }
    }

    /// Reads the bounds cell, lifting it to a validated [`Window`] (`[0, 0)`
    /// when absent — a fresh/empty deque). [`Window::new`] validates
    /// `head ≤ tail`.
    async fn bounds(&self) -> Result<Window, DequeStateError<CellCodecError<T>>> {
        match self.meta.get(&()).await.map_err(meta_err)? {
            Some((head, tail)) => Ok(Window::new(head, tail)?),
            None => Ok(Window { head: 0, tail: 0 }),
        }
    }

    /// Appends `value` at the back, extending the window to `tail + 1`.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when `value` does not encode, a
    /// `Permanent` meta error on index-space exhaustion, or an access error.
    pub async fn push_back(
        &self,
        value: WriteOf<'_, T>,
    ) -> Result<(), DequeStateError<CellCodecError<T>>> {
        let window = self.bounds().await?;
        let next_tail = window
            .tail
            .checked_add(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        self.entries.set(&window.tail, value).await?;
        self.write_bounds(window.head, next_tail).await?;
        Ok(())
    }

    /// Prepends `value` at the front, extending the window to `head − 1`.
    ///
    /// # Errors
    ///
    /// See [`Self::push_back`].
    pub async fn push_front(
        &self,
        value: WriteOf<'_, T>,
    ) -> Result<(), DequeStateError<CellCodecError<T>>> {
        let window = self.bounds().await?;
        let prev_head = window
            .head
            .checked_sub(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        self.entries.set(&prev_head, value).await?;
        self.write_bounds(prev_head, window.tail).await?;
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
    pub async fn pop_front(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let window = self.bounds().await?;
        if window.head >= window.tail {
            return Ok(None);
        }
        let value = self.entries.get(&window.head).await?;
        self.entries.clear(&window.head).await?;
        let next_head = window
            .head
            .checked_add(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        self.write_bounds(next_head, window.tail).await?;
        Ok(value)
    }

    /// Removes and returns the back element, retracting `tail` past it (`None`
    /// when empty). Symmetric with [`Self::pop_front`]: the element resolves
    /// before the mutation.
    ///
    /// # Errors
    ///
    /// See [`Self::pop_front`].
    pub async fn pop_back(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let window = self.bounds().await?;
        if window.head >= window.tail {
            return Ok(None);
        }
        let last = window
            .tail
            .checked_sub(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let value = self.entries.get(&last).await?;
        self.entries.clear(&last).await?;
        self.write_bounds(window.head, last).await?;
        Ok(value)
    }

    /// Drains this deque's buffered ops — entries and the window bounds
    /// together, in one batch — straight to committed state. At-least-once;
    /// see [`CellSession::flush`] for the contract.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub async fn flush(&self) -> Result<StoreOutcome, DequeStateError<CellCodecError<T>>> {
        Ok(self.entries.flush().await?)
    }

    /// Buffers the bounds cell. Co-stamped with the entry mutation a single op
    /// also buffers, so the window heals as a unit.
    async fn write_bounds(
        &self,
        head: i64,
        tail: i64,
    ) -> Result<(), DequeStateError<CellCodecError<T>>> {
        self.meta.set(&(), (head, tail)).await.map_err(meta_err)
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
/// `Bounds`: [`std::ops::Bound`] is in scope.
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

#[cfg(test)]
mod tests;
