//! Index-addressed double-ended queue collection.
//!
//! A Deque is a dense window of cells over a monotonic `i64` index space. The
//! [`DequeHandle`] composes the uniform [`CellView`] cell interface — there is
//! no Deque-specific store, session, or backend. Build a descriptor with
//! [`deque_state`], register it with the consumer, and bind the
//! [`Registered`](super::Registered) handle through
//! [`EventContext::state`](crate::consumer::event_context::EventContext::state).
//!
//! # Layout
//!
//! Two sections separate bookkeeping from data (`DequeNs`):
//!
//! * `Meta` holds one `META_BOUNDS` cell: `head ‖ tail` as two big-endian
//!   `i64`s. `head == tail` (and the absent cell, read as `(0, 0)`) is empty.
//! * `Entries` holds one cell per live element, addressed by the sign-flipped
//!   big-endian index ([`order_preserving_i64`]) so the clustering byte order
//!   is the signed index order.
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
//! [`KeyedStateSession::finalize`](crate::state::session)), so the bounds cell
//! can never outlive — or be outlived by — the entries it anchors.

use super::{
    CellView, DescriptorIdentity, StateDescriptor, StructuralIdentity, decode_cell, encode_cell,
    ensure_live, intern_descriptor_str,
};
use crate::codec::{Codec, JsonCodec};
use crate::consumer::event_context::StateAccessError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use crate::state::order_codec::order_preserving_i64;
use crate::state::registry::CollectionDef;
use crate::state::session::{CellRead, CellSession};
use crate::state::{CollectionKindId, StateName, StateType};
use async_stream::try_stream;
use educe::Educe;
use futures::stream::{Stream, StreamExt};
use std::error::Error;
use std::marker::PhantomData;
use std::ops::Bound;
use thiserror::Error;

/// Width of the [`META_BOUNDS`] payload: `head: i64 ‖ tail: i64`, big-endian.
const META_LEN: usize = 16;

/// The `Meta` section, holding the single bounds cell.
const META_SECTION: Section = Section::new(DequeNs::Meta as i8);

/// The `Entries` section, holding one cell per live element.
const ENTRY_SECTION: Section = Section::new(DequeNs::Entries as i8);

/// The single bounds cell (`Meta` section, empty coordinate).
const META_BOUNDS: CellKey = CellKey {
    section: META_SECTION,
    coordinate: Coordinate::empty(),
};

/// Deque's section enum, lowered to the opaque [`Section`]. Frozen: the
/// discriminants are a durable wire contract (the `section tinyint` column), so
/// the [`TryFrom`] guard rejects any other value as [`MetaDecodeError`]
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
    type Error = MetaDecodeError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Meta),
            1 => Ok(Self::Entries),
            _ => Err(MetaDecodeError::UnexpectedSection(value)),
        }
    }
}

/// Descriptor for a codec-backed deque collection (JSON by default — annotate
/// the binding `DequeDescriptor<MyCodec>` to pick another codec). The element
/// codec types the cell; there is no resolver or key codec (the index encoding
/// is fixed by the kind). Declare via [`deque_state`].
#[derive(Educe)]
#[educe(Clone(bound = ""), Copy, Debug(bound = ""))]
pub struct DequeDescriptor<C = JsonCodec> {
    name: &'static str,
    def: CollectionDef,
    #[educe(Debug(ignore))]
    _marker: PhantomData<fn() -> C>,
}

impl<C> DequeDescriptor<C> {
    /// Declares a deque collection named `name`. `name` may be any runtime
    /// string and is interned (it stays `Copy`); an empty name fails loudly at
    /// registration, the fallible boundary.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: intern_descriptor_str(name),
            def: CollectionDef::new(None),
            _marker: PhantomData,
        }
    }
}

impl<C> DescriptorIdentity for DequeDescriptor<C>
where
    C: Codec,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Deque,
            codec_id: C::CODEC_ID,
            resolver_id: None,
            // The index codec is fixed by the kind, not a registration choice.
            key_codec_id: None,
        }
    }
}

impl<C> StateDescriptor for DequeDescriptor<C>
where
    C: Codec,
{
    type Handle<S: CellRead> = DequeHandle<S, C>;

    fn bind<S: CellRead>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError> {
        let name = session.verify_state_registration(
            self.name,
            self.state_type(),
            &self.structural_identity(),
        )?;
        Ok(DequeHandle::new(session.clone(), self.state_type(), name))
    }

    fn collection_def(&self) -> CollectionDef {
        self.def
    }

    fn with_collection_def(mut self, def: CollectionDef) -> Self {
        self.def = def;
        self
    }
}

/// Typed, owned handle over a codec-backed deque — a thin composition over a
/// [`CellView`]. Reads (`stream`/`get`/`len`/`is_empty`) need only
/// [`CellRead`]; the push/pop mutators need [`CellSession`], so a reader-minted
/// handle has the readers but **cannot name** a mutator (the read-only-handle
/// invariant). Every operation guards on session termination.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct DequeHandle<S, C> {
    view: CellView<S>,
    _marker: PhantomData<fn() -> C>,
}

impl<S, C> DequeHandle<S, C> {
    /// Wraps a verified session and the binding descriptor's `(state_type,
    /// name)`. Codec-agnostic so [`StateDescriptor::bind`] mints it without a
    /// [`Codec`] bound.
    fn new(session: S, state_type: StateType, name: StateName) -> Self {
        Self {
            view: CellView::new(session, state_type, name),
            _marker: PhantomData,
        }
    }
}

impl<S, C> DequeHandle<S, C>
where
    S: CellRead,
    C: Codec,
{
    /// The number of live elements (`tail − head`, O(1) from the bounds cell).
    ///
    /// # Errors
    ///
    /// Returns a `Permanent` [`DequeStateError`] when the bounds cell is
    /// corrupt or the count exceeds `usize`, or an access error from the
    /// session.
    pub async fn len(&self) -> Result<usize, DequeStateError<C::Error>> {
        ensure_live(self.view.session())?;
        let (head, tail) = self.bounds().await?;
        Ok(window_len(head, tail)?)
    }

    /// Whether the deque holds no live elements (`head == tail`).
    ///
    /// # Errors
    ///
    /// Returns a `Permanent` [`DequeStateError`] when the bounds cell is
    /// corrupt, or an access error from the session.
    pub async fn is_empty(&self) -> Result<bool, DequeStateError<C::Error>> {
        ensure_live(self.view.session())?;
        let (head, tail) = self.bounds().await?;
        Ok(head == tail)
    }

    /// Reads the element at front-relative position `index` (`VecDeque::get`
    /// semantics): position `0` is the front, a single cell read at `head +
    /// index`, `None` when `index >= len`.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the cell does not decode, a
    /// `Permanent` meta error when the bounds cell is corrupt, or an access
    /// error from the session.
    pub async fn get(&self, index: usize) -> Result<Option<C::Payload>, DequeStateError<C::Error>> {
        ensure_live(self.view.session())?;
        let (head, tail) = self.bounds().await?;
        if index >= window_len(head, tail)? {
            return Ok(None);
        }
        let offset = i64::try_from(index).map_err(|_| MetaDecodeError::IndexOverflow)?;
        let absolute = head
            .checked_add(offset)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        self.read_entry(absolute).await
    }

    /// Streams the live elements in index order — front to back for
    /// [`Direction::Forward`], back to front for [`Direction::Backward`].
    ///
    /// # Invariant
    ///
    /// The scan yields the dense `[head, tail)` window (see the type's
    /// dense-window invariant) in `dir` order. It anchors at that window's
    /// leading edge — `head` for `Forward`, `tail − 1` for `Backward` — and
    /// stops after `len` cells, so popped tombstones (below `head` or at/above
    /// `tail`) are never yielded. [`order_preserving_i64`] makes byte order the
    /// signed-index order, so the backward scan walks back-to-front across the
    /// sign boundary correctly.
    pub fn stream(
        &self,
        dir: Direction,
    ) -> impl Stream<Item = Result<C::Payload, DequeStateError<C::Error>>> + '_ {
        try_stream! {
            ensure_live(self.view.session())?;
            let (head, tail) = self.bounds().await?;
            let len = window_len(head, tail)?;
            if len == 0 {
                return;
            }
            // The leading-edge index: `head` forward, the last live index
            // `tail − 1` backward (`len > 0` proves it does not underflow).
            let anchor = match dir {
                Direction::Forward => head,
                Direction::Backward => tail.checked_sub(1).ok_or(MetaDecodeError::IndexOverflow)?,
            };
            let start = index_coordinate(anchor);
            let scan = Scan {
                section: ENTRY_SECTION,
                start: Bound::Included(&start),
                dir,
                end: Bound::Unbounded,
                limit: Some(len),
            };
            let inner = self.view.scan(scan);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                let (key, bytes) = item.map_err(|e| DequeStateError::Access(StateAccessError::store(&e)))?;
                entry_section_guard(key.section)?;
                yield decode_cell::<C>(bytes).map_err(DequeStateError::Codec)?;
            }
        }
    }

    /// Reads and decodes the entry cell at absolute index `index` (`None` when
    /// the cell is absent).
    async fn read_entry(
        &self,
        index: i64,
    ) -> Result<Option<C::Payload>, DequeStateError<C::Error>> {
        self.view
            .get(&entry_cell(index))
            .await?
            .map(|bytes| decode_cell::<C>(bytes).map_err(DequeStateError::Codec))
            .transpose()
    }

    /// Reads the bounds cell (`(0, 0)` when absent — a fresh/empty deque).
    async fn bounds(&self) -> Result<(i64, i64), DequeStateError<C::Error>> {
        match self.view.get(&META_BOUNDS).await? {
            Some(bytes) => Ok(decode_bounds(&bytes)?),
            None => Ok((0, 0)),
        }
    }
}

impl<S, C> DequeHandle<S, C>
where
    S: CellSession,
    C: Codec,
{
    /// Appends `value` at the back, extending the window to `tail + 1`.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when `value` does not encode, a
    /// `Permanent` meta error on index-space exhaustion, or an access error.
    pub async fn push_back(&self, value: C::Payload) -> Result<(), DequeStateError<C::Error>> {
        ensure_live(self.view.session())?;
        let (head, tail) = self.bounds().await?;
        let next_tail = tail.checked_add(1).ok_or(MetaDecodeError::IndexOverflow)?;
        self.write_entry(tail, value).await?;
        self.write_bounds(head, next_tail).await?;
        Ok(())
    }

    /// Prepends `value` at the front, extending the window to `head − 1`.
    ///
    /// # Errors
    ///
    /// See [`Self::push_back`].
    pub async fn push_front(&self, value: C::Payload) -> Result<(), DequeStateError<C::Error>> {
        ensure_live(self.view.session())?;
        let (head, tail) = self.bounds().await?;
        let prev_head = head.checked_sub(1).ok_or(MetaDecodeError::IndexOverflow)?;
        self.write_entry(prev_head, value).await?;
        self.write_bounds(prev_head, tail).await?;
        Ok(())
    }

    /// Removes and returns the front element, advancing `head` past it (`None`
    /// when empty). The residue clear and the head move co-stamp.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the entry does not decode, a
    /// `Permanent` meta error on corruption, or an access error.
    pub async fn pop_front(&self) -> Result<Option<C::Payload>, DequeStateError<C::Error>> {
        ensure_live(self.view.session())?;
        let (head, tail) = self.bounds().await?;
        if head >= tail {
            return Ok(None);
        }
        let value = self.read_entry(head).await?;
        self.view.clear(&entry_cell(head)).await?;
        let next_head = head.checked_add(1).ok_or(MetaDecodeError::IndexOverflow)?;
        self.write_bounds(next_head, tail).await?;
        Ok(value)
    }

    /// Removes and returns the back element, retracting `tail` past it (`None`
    /// when empty). Symmetric with [`Self::pop_front`].
    ///
    /// # Errors
    ///
    /// See [`Self::pop_front`].
    pub async fn pop_back(&self) -> Result<Option<C::Payload>, DequeStateError<C::Error>> {
        ensure_live(self.view.session())?;
        let (head, tail) = self.bounds().await?;
        if head >= tail {
            return Ok(None);
        }
        let last = tail.checked_sub(1).ok_or(MetaDecodeError::IndexOverflow)?;
        let value = self.read_entry(last).await?;
        self.view.clear(&entry_cell(last)).await?;
        self.write_bounds(head, last).await?;
        Ok(value)
    }

    /// Encodes and buffers an entry cell at absolute index `index`.
    async fn write_entry(
        &self,
        index: i64,
        value: C::Payload,
    ) -> Result<(), DequeStateError<C::Error>> {
        let buf = encode_cell::<C>(value).map_err(DequeStateError::Codec)?;
        self.view.set(&entry_cell(index), &buf).await?;
        Ok(())
    }

    /// Buffers the bounds cell. Co-stamped with the entry mutation a single op
    /// also buffers, so the window heals as a unit.
    async fn write_bounds(&self, head: i64, tail: i64) -> Result<(), StateAccessError> {
        self.view
            .set(&META_BOUNDS, &encode_bounds(head, tail))
            .await
    }
}

/// Declares a codec-backed deque collection named `name` (JSON by default —
/// annotate the binding `DequeDescriptor<MyCodec>` to pick another codec).
///
/// `name` may be any runtime string and is interned (it stays `Copy`); an empty
/// name fails loudly at registration, the fallible boundary.
#[must_use]
pub fn deque_state<C>(name: &str) -> DequeDescriptor<C>
where
    C: Codec,
{
    DequeDescriptor::new(name)
}

/// The entry cell at absolute index `index`.
fn entry_cell(index: i64) -> CellKey {
    CellKey {
        section: ENTRY_SECTION,
        coordinate: index_coordinate(index),
    }
}

/// The order-preserving coordinate for absolute index `index`.
fn index_coordinate(index: i64) -> Coordinate {
    Coordinate::from_bytes(order_preserving_i64(index).to_vec())
}

/// The live-window length `tail − head` as a `usize`.
///
/// # Errors
///
/// Returns [`MetaDecodeError::Disordered`] when `tail < head` and
/// [`MetaDecodeError::IndexOverflow`] when the count exceeds `usize`.
fn window_len(head: i64, tail: i64) -> Result<usize, MetaDecodeError> {
    let span = tail
        .checked_sub(head)
        .filter(|&span| span >= 0)
        .ok_or(MetaDecodeError::Disordered { head, tail })?;
    usize::try_from(span).map_err(|_| MetaDecodeError::IndexOverflow)
}

/// Encodes the bounds cell payload `head ‖ tail` (16 bytes, big-endian).
fn encode_bounds(head: i64, tail: i64) -> [u8; META_LEN] {
    let mut out = [0u8; META_LEN];
    out[..8].copy_from_slice(&head.to_be_bytes());
    out[8..].copy_from_slice(&tail.to_be_bytes());
    out
}

/// Decodes the bounds cell payload, validating `head ≤ tail`.
///
/// # Errors
///
/// Returns [`MetaDecodeError::BadLength`] for a wrong-width payload and
/// [`MetaDecodeError::Disordered`] when `tail < head`.
fn decode_bounds(bytes: &[u8]) -> Result<(i64, i64), MetaDecodeError> {
    if bytes.len() != META_LEN {
        return Err(MetaDecodeError::BadLength {
            expected: META_LEN,
            actual: bytes.len(),
        });
    }
    let mut head = [0u8; 8];
    let mut tail = [0u8; 8];
    head.copy_from_slice(&bytes[..8]);
    tail.copy_from_slice(&bytes[8..]);
    let head = i64::from_be_bytes(head);
    let tail = i64::from_be_bytes(tail);
    if tail < head {
        return Err(MetaDecodeError::Disordered { head, tail });
    }
    Ok((head, tail))
}

/// Asserts a scanned cell sits in the `Entries` section — a defensive,
/// forward-compatible guard (today's scan is already section-scoped). Drives
/// the [`DequeNs`] `TryFrom` so a stale cell carrying an unknown or `Meta`
/// section fails loudly rather than being decoded as an element.
fn entry_section_guard(section: Section) -> Result<(), MetaDecodeError> {
    match DequeNs::try_from(i8::from(section))? {
        DequeNs::Entries => Ok(()),
        DequeNs::Meta => Err(MetaDecodeError::UnexpectedSection(i8::from(section))),
    }
}

/// Error decoding or deriving the deque's `Meta` bookkeeping. Always
/// `Permanent`: a corrupt bounds cell, a disordered or overflowing window, or a
/// scanned cell in an unexpected section will not start being valid on retry.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum MetaDecodeError {
    /// The bounds cell was not the fixed `head ‖ tail` width.
    #[error("bad deque bounds length: expected {expected}, got {actual}")]
    BadLength {
        /// The width the bounds cell requires.
        expected: usize,
        /// The width the cell actually had.
        actual: usize,
    },

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

    /// A scanned cell carried a section that is not the entry section.
    #[error("unexpected deque section discriminant: {0}")]
    UnexpectedSection(i8),
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
    /// The context refused or failed the state access.
    #[error(transparent)]
    Access(#[from] StateAccessError),

    /// The codec failed to encode or decode an element.
    #[error("deque codec failed")]
    Codec(#[source] E),

    /// The deque's bookkeeping was corrupt or its index space exhausted.
    #[error(transparent)]
    Meta(#[from] MetaDecodeError),
}

impl<E> ClassifyError for DequeStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Access(e) => e.classify_error(),
            // A cell that does not round-trip, or corrupt bookkeeping, will not
            // begin to on retry.
            Self::Codec(_) | Self::Meta(_) => ErrorCategory::Permanent,
        }
    }
}

#[cfg(test)]
mod tests;
