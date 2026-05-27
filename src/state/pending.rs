//! Write- and read-side pending-index traits shared by the keyed-state
//! backends.
//!
//! Seal inserts a pending-index row before writing the WAL columns; apply
//! and rollback delete that row after the WAL is cleared. The read-side
//! [`PendingIndexScanner`] streams the pending rows for a `(segment, key)`
//! partition so the keyed-state middleware's [`TimerType::StateRecovery`]
//! sweep can resolve crashed seals without scanning per-kind tables.
//!
//! [`TimerType::StateRecovery`]: crate::timers::TimerType::StateRecovery

use super::{CollectionId, CollectionKind, CollectionKindId, StateKey, StateName, StateType};
use crate::error::ClassifyError;
use futures::Stream;
use std::error::Error;
use std::future::Future;

/// Write-side pending-WAL index for one durable backend.
///
/// The pending index is partition-keyed by `(segment_id, key)` and lets
/// recovery find sealed WALs without scanning every per-kind table. Both
/// methods are idempotent at the storage layer: re-inserting a row writes
/// the same primary key; deleting an absent row is a no-op.
pub trait PendingIndexStore: Clone + Send + Sync + 'static {
    /// Error type for pending-index operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Records a pending sealed WAL on the `(segment, key)` partition.
    ///
    /// Called by seal *before* writing the WAL columns. A crash between
    /// the two writes leaves a stale pending row and an Idle partition —
    /// design-acceptable per Crash Robustness §WAL Mode.
    fn insert_pending<'a, K>(
        &'a self,
        id: &'a CollectionId<K>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a
    where
        K: CollectionKind;

    /// Deletes the pending-index row after a resolution lands.
    ///
    /// Called by `apply_sealed` / `rollback_sealed` *after* the WAL
    /// columns are cleared.
    fn delete_pending<'a, K>(
        &'a self,
        id: &'a CollectionId<K>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a
    where
        K: CollectionKind;
}

/// Read-side of the pending-index, used by the keyed-state middleware's
/// `StateRecovery` sweep.
///
/// `scan_pending` streams every pending row on the `(segment, key)`
/// partition. The stream is kind-agnostic: each [`PendingEntry`] carries the
/// runtime [`CollectionKindId`] so the middleware can dispatch to the
/// matching per-kind recovery handler. The stream shape honors the
/// "Collection scans are streaming" invariant in
/// `docs/keyed-state/design-summary.md` — implementations must not buffer an
/// unbounded number of rows in memory.
pub trait PendingIndexScanner: Clone + Send + Sync + 'static {
    /// Error type for streamed pending-index scans.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Streamed scan result.
    type Stream: Stream<Item = Result<PendingEntry, Self::Error>> + Send;

    /// Streams the pending-index rows for one `(segment, key)` partition.
    fn scan_pending(&self, state_key: &StateKey) -> Self::Stream;
}

/// One row from the pending index.
///
/// `kind` is the discriminator the keyed-state middleware uses to dispatch a
/// pending row to the right per-kind recovery handler; today only
/// [`CollectionKindId::Value`] is implemented, and other kinds are skipped
/// with a WARN by the middleware. `name` lets recovery look up the
/// collection's per-collection [`crate::timers::duration::CompactDuration`]
/// TTL override in the middleware's registry.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PendingEntry {
    /// Logical namespace (application, middleware, …).
    pub state_type: StateType,

    /// Collection-kind discriminator. Recovery dispatches on this value.
    pub kind: CollectionKindId,

    /// Collection name.
    pub name: StateName,
}

impl PendingEntry {
    /// Creates a typed pending-index entry.
    #[must_use]
    pub fn new(state_type: StateType, kind: CollectionKindId, name: StateName) -> Self {
        Self {
            state_type,
            kind,
            name,
        }
    }
}
