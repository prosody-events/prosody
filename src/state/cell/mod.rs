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
//! `P` is the payload projection. A presence cell (`P = ()`) carries no bytes.
//! It cannot be written back as a value.
//! A [`ProvisionalWrite`] requires a committed byte value for its prior value.
//! Decoders reject invalid column shapes before they construct a cell.

use super::event_ref::EventRef;
use super::marker::ReaderEvidence;
use bytes::Bytes;

/// A committed payload projection, or known absence.
///
/// `Committed<Bytes>` is mintable only inside `crate::state` by resolved read
/// paths. [`ProvisionalWrite::new`] requires this proof for its prior value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed<P = Bytes>(Option<P>);

impl<P> Committed<P> {
    /// Mints a committed value. Restricted to the state module so only the
    /// resolved read paths can vouch that `value` is committed.
    #[must_use]
    pub(in crate::state) fn new(value: Option<P>) -> Self {
        Self(value)
    }

    /// The committed projection, or `None` for known absence.
    #[must_use]
    pub fn get(&self) -> Option<&P> {
        self.0.as_ref()
    }

    /// Returns the committed projection.
    #[must_use]
    pub fn into_inner(self) -> Option<P> {
        self.0
    }
}

/// One durable cell: either resolved (committed) or provisional (an event's
/// outcome staged over the prior committed value).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell<P = Bytes> {
    /// No event in flight; `data` is committed.
    Resolved(Committed<P>),

    /// An event staged a write whose commit is not yet resolved.
    Provisional(ProvisionalCell<P>),
}

impl<P> Cell<P> {
    /// The pure committed-value projection: `prev` for a provisional cell,
    /// `data` for a resolved one. No oracle, no mutation — sound because of
    /// the prev-is-committed invariant.
    #[must_use]
    pub fn project_committed(&self) -> Option<&P> {
        match self {
            Self::Resolved(committed) => committed.get(),
            Self::Provisional(cell) => cell.prev(),
        }
    }
}

/// Resolves one external read from positive evidence, without a durable write.
pub(crate) fn resolve_for_reader<'a, P>(
    cell: &'a Cell<P>,
    evidence: &ReaderEvidence,
) -> Option<&'a P> {
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
pub struct ProvisionalCell<P = Bytes> {
    data: Option<P>,
    prev: Option<P>,
    event: EventRef,
}

impl<P> ProvisionalCell<P> {
    /// Reconstructs a provisional cell from decoded columns. Restricted to
    /// the state module: only a backend decoder mints one.
    #[must_use]
    pub(in crate::state) fn new(data: Option<P>, prev: Option<P>, event: EventRef) -> Self {
        Self { data, prev, event }
    }

    /// The event's staged outcome.
    #[must_use]
    pub fn data(&self) -> Option<&P> {
        self.data.as_ref()
    }

    /// The committed value the event superseded.
    #[must_use]
    pub fn prev(&self) -> Option<&P> {
        self.prev.as_ref()
    }

    /// The owning event.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.event
    }

    /// The staged outcome, consuming the cell (commit resolution).
    #[must_use]
    pub fn into_data(self) -> Option<P> {
        self.data
    }

    /// The committed base, consuming the cell (rollback / own-event base).
    #[must_use]
    pub fn into_prev(self) -> Option<P> {
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
