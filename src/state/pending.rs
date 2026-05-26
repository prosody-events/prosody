//! Write-side pending-index trait shared by the keyed-state backends.
//!
//! Seal inserts a pending-index row before writing the WAL columns; apply
//! and rollback delete that row after the WAL is cleared. The read side
//! (`scan_pending`, first-touch resolution) lands in Slice 7+ as a sibling
//! `PendingIndexScanner` trait.

use super::{CollectionId, CollectionKind};
use crate::error::ClassifyError;
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
