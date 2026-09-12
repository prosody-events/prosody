//! The provisional-cell durability model.
//!
//! Each durable value is one **cell** holding both a committed value and,
//! while an event's outcome is still in flight, that event's provisional
//! write side by side:
//!
//! * [`Cell::Resolved`] — no event is in flight; the carried [`Committed`]
//!   value is authoritative.
//! * [`Cell::Provisional`] — an event staged a write: `data` is that event's
//!   outcome, `prev` the committed value before it, `event` the owner. The
//!   collection evidence selects the committed value.
//!
//! The model replaces the write-ahead log: rather than persisting a *recipe*
//! (ops) to re-derive the outcome durably later, both finished outcomes are
//! persisted at write time — the single-writer-per-key invariant guarantees
//! the committed base is known in-process, so no replay is ever needed.
//!
//! # Invariants
//!
//! * **Prev-is-committed** — a [`ProvisionalCell::prev`] (and a
//!   [`ProvisionalWrite`]'s `prev`) holds the committed value before the stage.
//!   Readers use collection evidence to select this base or the staged value.
//!   The type system enforces the committed base: [`ProvisionalWrite`] cannot
//!   be built without a [`Committed`], and `Committed<Values>` is mintable only
//!   inside `crate::state` — by the resolved read paths.
//! * **Presence carries no bytes** — [`Values`] yields bytes; [`Presence`]
//!   yields `()`. A presence cell has no bytes to write back.
//! * **Invalid shapes unrepresentable after decode** — a backend decoder
//!   collapses every physical column shape into one of these two variants or a
//!   typed corruption error; nothing downstream sees a half-built cell.
//! * **Cache lattice** — a cache never replaces a `Value` entry with an
//!   `Exists` entry.

use super::event_ref::EventRef;
use super::marker::ReaderEvidence;
use bytes::Bytes;
use std::fmt::Debug;

/// What a cell read yields for a present cell.
///
/// The trait is sealed. [`Values`] yields bytes. [`Presence`] yields `()`.
pub trait Projection: Copy + Send + Sync + 'static + sealed::Sealed {
    /// The payload of one present cell.
    type Payload: Clone + Debug + Eq + Send + Sync + 'static;

    /// The projection name for metrics and spans.
    const NAME: &'static str;

    /// Projects a stored value.
    fn from_value(bytes: Bytes) -> Self::Payload;

    /// Reads a cache entry. [`Read::Unknown`] means the entry cannot answer.
    fn from_cached<B: IntoBytes>(cached: CacheEntry<B>) -> Read<Self::Payload>;

    /// Returns the cache entry for one committed read.
    fn into_cached(committed: Committed<Self>) -> CacheEntry<Bytes>;
}

/// Converts a cache payload to owned bytes.
pub trait IntoBytes {
    /// Copies a borrowed slice or returns owned bytes unchanged.
    fn into_bytes(self) -> Bytes;
}

impl IntoBytes for &[u8] {
    fn into_bytes(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl IntoBytes for Bytes {
    fn into_bytes(self) -> Bytes {
        self
    }
}

/// Reads the committed bytes of a present cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Values;

/// Reads presence without a payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Presence;

/// What a cache knows about one cell. `Value` refines `Exists`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheEntry<B> {
    /// The cell is absent.
    Absent,
    /// The cell exists, but the cache has no payload.
    Exists,
    /// The cell exists with this payload.
    Value(B),
}

/// A cache answer, or an unknown result that requires a durable read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Read<T> {
    /// The cell is present.
    Present(T),
    /// The cell is absent.
    Absent,
    /// The cache cannot answer this read.
    Unknown,
}

impl<B> CacheEntry<B> {
    /// Returns whether this entry would discard a known payload.
    #[must_use]
    pub fn downgrades(&self, existing: &Self) -> bool {
        matches!((self, existing), (Self::Exists, Self::Value(_)))
    }
}

impl Projection for Values {
    type Payload = Bytes;

    const NAME: &'static str = "values";

    fn from_value(bytes: Bytes) -> Self::Payload {
        bytes
    }

    fn from_cached<B: IntoBytes>(cached: CacheEntry<B>) -> Read<Self::Payload> {
        match cached {
            CacheEntry::Value(bytes) => Read::Present(bytes.into_bytes()),
            CacheEntry::Absent => Read::Absent,
            CacheEntry::Exists => Read::Unknown,
        }
    }

    fn into_cached(committed: Committed<Self>) -> CacheEntry<Bytes> {
        committed
            .into_inner()
            .map_or(CacheEntry::Absent, CacheEntry::Value)
    }
}

impl Projection for Presence {
    type Payload = ();

    const NAME: &'static str = "presence";

    fn from_value(_bytes: Bytes) -> Self::Payload {}

    fn from_cached<B: IntoBytes>(cached: CacheEntry<B>) -> Read<Self::Payload> {
        match cached {
            CacheEntry::Value(_) | CacheEntry::Exists => Read::Present(()),
            CacheEntry::Absent => Read::Absent,
        }
    }

    fn into_cached(committed: Committed<Self>) -> CacheEntry<Bytes> {
        committed
            .into_inner()
            .map_or(CacheEntry::Absent, |()| CacheEntry::Exists)
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Values {}
    impl Sealed for super::Presence {}
}

/// A committed payload projection, or known absence.
///
/// `Committed<Values>` is mintable only inside `crate::state` by resolved read
/// paths. [`ProvisionalWrite::new`] requires this proof for its prior value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed<P: Projection = Values>(Option<P::Payload>);

impl<P: Projection> Committed<P> {
    /// Mints a committed value. Restricted to the state module so only the
    /// resolved read paths can vouch that `value` is committed.
    #[must_use]
    pub(in crate::state) fn new(value: Option<P::Payload>) -> Self {
        Self(value)
    }

    /// The committed projection, or `None` for known absence.
    #[must_use]
    pub fn get(&self) -> Option<&P::Payload> {
        self.0.as_ref()
    }

    /// Returns the committed projection.
    #[must_use]
    pub fn into_inner(self) -> Option<P::Payload> {
        self.0
    }
}

/// One durable cell: either resolved (committed) or provisional (an event's
/// outcome staged over the prior committed value).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell<P: Projection = Values> {
    /// No event in flight; `data` is committed.
    Resolved(Committed<P>),

    /// An event staged a write whose commit is not yet resolved.
    Provisional(ProvisionalCell<P>),
}

impl<P: Projection> Cell<P> {
    /// The pure committed-value projection: `prev` for a provisional cell,
    /// `data` for a resolved one. No oracle, no mutation — sound because of
    /// the prev-is-committed invariant.
    #[must_use]
    pub fn project_committed(&self) -> Option<&P::Payload> {
        match self {
            Self::Resolved(committed) => committed.get(),
            Self::Provisional(cell) => cell.prev(),
        }
    }
}

/// Resolves one external read from positive evidence, without a durable write.
pub(crate) fn resolve_for_reader<'a, P: Projection>(
    cell: &'a Cell<P>,
    evidence: &ReaderEvidence,
) -> Option<&'a P::Payload> {
    match cell {
        Cell::Provisional(cell) if evidence.committed(cell.event()) => cell.data(),
        // Legacy split stages and legacy residue orphaned by admit can leave cells without a Staged
        // row. Concurrent V4 chunks and delayed retries can produce the same state within
        // their arrival skew. Both cases project prev until the cells expire under their
        // own TTL. The skew changes only TTL precision: those cells were already due to
        // vanish within that interval. Cassandra resolves TTLs to one second.
        _ => cell.project_committed(),
    }
}

/// A staged-but-unresolved cell: the event's outcome (`data`), the committed
/// value it superseded (`prev`), and the owning event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionalCell<P: Projection = Values> {
    data: Option<P::Payload>,
    prev: Option<P::Payload>,
    event: EventRef,
}

impl<P: Projection> ProvisionalCell<P> {
    /// Reconstructs a provisional cell from decoded columns. Restricted to
    /// the state module: only a backend decoder mints one.
    #[must_use]
    pub(in crate::state) fn new(
        data: Option<P::Payload>,
        prev: Option<P::Payload>,
        event: EventRef,
    ) -> Self {
        Self { data, prev, event }
    }

    /// The event's staged outcome.
    #[must_use]
    pub fn data(&self) -> Option<&P::Payload> {
        self.data.as_ref()
    }

    /// The committed value the event superseded.
    #[must_use]
    pub fn prev(&self) -> Option<&P::Payload> {
        self.prev.as_ref()
    }

    /// The owning event.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.event
    }

    /// The staged outcome, consuming the cell (commit resolution).
    #[must_use]
    pub fn into_data(self) -> Option<P::Payload> {
        self.data
    }

    /// The committed base, consuming the cell (rollback / own-event base).
    #[must_use]
    pub fn into_prev(self) -> Option<P::Payload> {
        self.prev
    }
}

/// A staged write: the new outcome, the committed value it supersedes, and
/// the owning event.
///
/// Construction requires a [`Committed`] for `prev`, so a stage write whose
/// base is not provably committed is unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionalWrite {
    data: Option<Bytes>,
    prev: Committed,
    event: EventRef,
}

impl ProvisionalWrite {
    /// Builds a stage write over a committed base.
    #[must_use]
    pub fn new(data: Option<Bytes>, prev: Committed, event: EventRef) -> Self {
        Self { data, prev, event }
    }

    /// The new staged outcome.
    #[must_use]
    pub fn data(&self) -> Option<&Bytes> {
        self.data.as_ref()
    }

    /// The committed base this write supersedes.
    #[must_use]
    pub fn prev(&self) -> Option<&Bytes> {
        self.prev.get()
    }

    /// The owning event.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.event
    }
}

#[cfg(test)]
mod tests;
