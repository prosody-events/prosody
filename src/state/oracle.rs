//! Commit oracle trait for keyed-state recovery.
//!
//! [`CommitOracle`] owns both halves of the message commit relation — the
//! one whose row presence the recovery reader interprets as "this event
//! committed". [`CommitOracle::record_message`] writes that row at the
//! durability boundary (the marker flush, strictly after the WAL seal);
//! [`CommitOracle::resolve`] reads it back during recovery, bridging the
//! existing [`crate::commit_manager::CommitManager`] bool API into an
//! [`EventRef`]-shaped [`CommitDecision`] used by the read-side recovery
//! combinator [`crate::state::recovering::RecoveringValueStore`].
//!
//! The resolve half is intentionally kind-agnostic at the callsite — the
//! [`CollectionId<ValueKind>`] supplies the application [`Key`] the timer
//! arm needs (`docs/keyed-state/design-summary.md` §"Commit Oracles"), and
//! the message arm ignores it. The write half is message-only: the timer
//! oracle's row (the trigger tag) is written by the timer's own commit
//! machinery, never through this trait.
//!
//! [`Key`]: crate::Key

use super::value::ValueKind;
use super::{CollectionId, CommitDecision, EventRef};
use crate::error::ClassifyError;
use std::error::Error;
use std::future::Future;
use uuid::Uuid;

/// Resolves a sealed WAL's [`EventRef`] into a [`CommitDecision`], and
/// records the message commit marker the resolve half later reads.
pub trait CommitOracle: Clone + Send + Sync + 'static {
    /// Error type for oracle resolution.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Records the message commit marker for `dedup_id`.
    ///
    /// This is the write half of the message commit relation: a present
    /// row certifies that the event's sealed state is durable. The
    /// durability boundary calls it as the marker-flush step, strictly
    /// after the WAL seal, so the invariant "marker present ⇒ seal
    /// durable" holds structurally. The timer arm has no analog — a
    /// trigger's tag is written by its own commit machinery.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the upstream store write fails; the
    /// boundary retries transient failures in place.
    fn record_message(
        &self,
        dedup_id: Uuid,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Resolves whether the event that sealed a WAL committed upstream.
    ///
    /// The `collection` supplies the application key the timer arm needs;
    /// the message arm ignores it. Implementations consult the upstream
    /// commit source (deduplication store for messages, timer-row tag for
    /// timers) and translate the boolean answer into [`CommitDecision`].
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the upstream store read fails.
    fn resolve<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        event: EventRef,
    ) -> impl Future<Output = Result<CommitDecision, Self::Error>> + Send + 'a;
}
