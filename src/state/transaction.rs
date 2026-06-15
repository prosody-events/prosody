//! Transaction-side state shapes.
//!
//! [`CommitMode`] is the per-collection read guarantee; [`Read`] is the
//! three-valued overlay read.

/// Persistence mode for a collection's state changes, chosen per collection
/// at registration
/// ([`CollectionDef::with_commit_mode`](crate::state::registry::CollectionDef::with_commit_mode);
/// the default is [`Self::ReadCommitted`]).
///
/// The modes are named by the **read guarantee** they give, not the mechanism:
///
/// * **`ReadCommitted` — atomic with the event, crash-recoverable.** On handler
///   success the buffered write stages as a provisional cell (new value beside
///   the prior committed value) *before* the event's commit marker, then
///   promotes to committed after the marker is durable; crash recovery resolves
///   the cell through the commit oracle. A handler that fails or redelivers
///   never exposes its writes — internal and external readers only ever observe
///   committed values.
/// * **`ReadUncommitted` — cheaper, at-least-once.** The buffered write applies
///   straight to the committed value when the handler succeeds, visible even if
///   the event later fails. A crash between the apply and the event's commit
///   re-runs the handler against already-applied state, so writes must be
///   idempotent (last-writer-wins `set`s usually are). Choose it for state
///   where re-application is harmless and the extra promote per event matters.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CommitMode {
    /// Stage the write provisionally before the event commit marker and
    /// promote it after — readers observe committed values only.
    ReadCommitted,

    /// Apply the write to committed state immediately — cheaper, with
    /// at-least-once, read-uncommitted semantics.
    ReadUncommitted,
}

/// Three-valued read used by overlays.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum Read<T> {
    /// Value is present.
    Present(T),

    /// Value is known absent.
    Absent,

    /// This layer has not observed the value.
    Unknown,
}
