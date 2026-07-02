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
//!   commit oracle decides which of the two becomes committed.
//!
//! The model replaces the write-ahead log: rather than persisting a *recipe*
//! (ops) to re-derive the outcome durably later, both finished outcomes are
//! persisted at write time — the single-writer-per-key invariant guarantees
//! the committed base is known in-process, so no replay is ever needed.
//!
//! # Invariants
//!
//! * **Prev-is-committed** — a [`ProvisionalCell::prev`] (and a
//!   [`ProvisionalWrite`]'s `prev`) is always the latest committed value. This
//!   is what makes [`Cell::project_committed`] a sound pure projection for
//!   external readers, and it is enforced in the type system:
//!   [`ProvisionalWrite`] cannot be built without a [`Committed`], and
//!   [`Committed`] is mintable only inside `crate::state` — by the resolved
//!   read paths.
//! * **Invalid shapes unrepresentable after decode** — a backend decoder
//!   collapses every physical column shape into one of these two variants or a
//!   typed corruption error; nothing downstream sees a half-built cell.

use super::event_ref::EventRef;
use bytes::Bytes;

/// A committed value: the authoritative bytes (or known-absence) that
/// internal and external readers observe.
///
/// The inner constructor is `pub(in crate::state)`, so a `Committed` can be
/// minted only by the resolved read paths inside the state module — never
/// fabricated from an arbitrary value. That privacy is the enforcement of
/// the prev-is-committed invariant: every [`ProvisionalWrite::new`] requires
/// one, and the only way to get one is to have established the value is
/// committed (a [`Cell::Resolved`] read or a [`super::resolve`] decision).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed(Option<Bytes>);

impl Committed {
    /// Mints a committed value. Restricted to the state module so only the
    /// resolved read paths can vouch that `value` is committed.
    #[must_use]
    pub(in crate::state) fn new(value: Option<Bytes>) -> Self {
        Self(value)
    }

    /// The committed bytes, or `None` when the value is known-absent.
    #[must_use]
    pub fn get(&self) -> Option<&Bytes> {
        self.0.as_ref()
    }

    /// Decomposes into the committed bytes.
    #[must_use]
    pub fn into_inner(self) -> Option<Bytes> {
        self.0
    }
}

/// One durable cell: either resolved (committed) or provisional (an event's
/// outcome staged over the prior committed value).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell {
    /// No event in flight; `data` is committed.
    Resolved(Committed),

    /// An event staged a write whose commit is not yet resolved.
    Provisional(ProvisionalCell),
}

impl Cell {
    /// The pure committed-value projection: `prev` for a provisional cell,
    /// `data` for a resolved one. No oracle, no mutation — sound because of
    /// the prev-is-committed invariant.
    ///
    /// This is the committed-projection primitive a future non-owner reader
    /// will observe: one point read, committed-only, possibly stale by the
    /// single in-flight event. No production caller consumes it yet.
    #[must_use]
    pub fn project_committed(&self) -> Option<&Bytes> {
        match self {
            Self::Resolved(committed) => committed.get(),
            Self::Provisional(cell) => cell.prev(),
        }
    }
}

/// A staged-but-unresolved cell: the event's outcome (`data`), the committed
/// value it superseded (`prev`), and the owning event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionalCell {
    data: Option<Bytes>,
    prev: Option<Bytes>,
    event: EventRef,
}

impl ProvisionalCell {
    /// Reconstructs a provisional cell from decoded columns. Restricted to
    /// the state module: only a backend decoder mints one.
    #[must_use]
    pub(in crate::state) fn new(data: Option<Bytes>, prev: Option<Bytes>, event: EventRef) -> Self {
        Self { data, prev, event }
    }

    /// The event's staged outcome.
    #[must_use]
    pub fn data(&self) -> Option<&Bytes> {
        self.data.as_ref()
    }

    /// The committed value the event superseded.
    #[must_use]
    pub fn prev(&self) -> Option<&Bytes> {
        self.prev.as_ref()
    }

    /// The owning event.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.event
    }

    /// The staged outcome, consuming the cell (commit resolution).
    #[must_use]
    pub fn into_data(self) -> Option<Bytes> {
        self.data
    }

    /// The committed base, consuming the cell (rollback / own-event base).
    #[must_use]
    pub fn into_prev(self) -> Option<Bytes> {
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
mod tests {
    use super::{Cell, Committed, ProvisionalCell};
    use crate::state::EventRef;
    use bytes::Bytes;
    use uuid::Uuid;

    fn event() -> EventRef {
        EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        }
    }

    /// The pure committed-value projection (the external reader's view): a
    /// resolved cell projects its committed value, a provisional cell projects
    /// its `prev` (the committed base, stale by exactly the in-flight event) —
    /// never its in-flight `data`. A cleared/rolled-back/promote-of-clear
    /// residue all project absence (the `ClearIsAbsence` corollary, inv 5).
    #[test]
    fn project_committed_is_prev_for_provisional_and_data_for_resolved() {
        let data = Bytes::from_static(b"data");
        let prev = Bytes::from_static(b"prev");

        // Resolved → its committed value (present or absent).
        assert_eq!(
            Cell::Resolved(Committed::new(Some(data.clone()))).project_committed(),
            Some(&data),
        );
        assert_eq!(
            Cell::Resolved(Committed::new(None)).project_committed(),
            None,
        );

        // Provisional → its `prev`, NOT the in-flight `data`.
        assert_eq!(
            Cell::Provisional(ProvisionalCell::new(
                Some(data.clone()),
                Some(prev.clone()),
                event(),
            ))
            .project_committed(),
            Some(&prev),
        );

        // A clear over a present base still projects the (committed) prev.
        assert_eq!(
            Cell::Provisional(ProvisionalCell::new(None, Some(prev.clone()), event()))
                .project_committed(),
            Some(&prev),
        );

        // A promote-of-clear residue (both blobs null) projects absence.
        assert_eq!(
            Cell::Provisional(ProvisionalCell::new(None, None, event())).project_committed(),
            None,
        );
    }
}
