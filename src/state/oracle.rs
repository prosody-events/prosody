//! Commit oracle trait for keyed-state recovery.
//!
//! [`CommitOracle`] bridges the existing
//! [`crate::commit_manager::CommitManager`] bool API into an
//! [`EventRef`]-shaped [`CommitDecision`] used by the read-side recovery
//! combinator [`crate::state::recovering::RecoveringValueStore`].
//!
//! The trait is intentionally kind-agnostic at the resolve callsite — the
//! [`CollectionId<ValueKind>`] supplies the application [`Key`] the timer
//! arm needs (`docs/keyed-state/design-summary.md` §"Commit Oracles"), and
//! the message arm ignores it.
//!
//! [`Key`]: crate::Key

use super::value::ValueKind;
use super::{CollectionId, CommitDecision, EventRef};
use crate::error::ClassifyError;
use std::error::Error;
use std::future::Future;

/// Resolves a sealed WAL's [`EventRef`] into a [`CommitDecision`].
pub trait CommitOracle: Clone + Send + Sync + 'static {
    /// Error type for oracle resolution.
    type Error: ClassifyError + Error + Send + Sync + 'static;

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
