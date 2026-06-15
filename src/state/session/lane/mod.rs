//! One kind's per-event lane.
//!
//! A [`Lane`] is the per-kind body of a keyed-state session: today's Value
//! finalize / commit / rollback bodies, made addr-generic over
//! [`CollectionKind`] and regrouped to **collection grain**. It owns the kind's
//! partition-lifetime [`PartitionStateStore`], a per-event in-memory
//! [`DirtyStore`], and the staged set the lifecycle promotes or rolls back.
//!
//! The session composes one lane per kind ([`Lanes`](super::Lanes)) and fans
//! the lifecycle across them; the session-level singletons (oracle marker,
//! armed backstop, event, registry, …) live on the session, never per lane.
//!
//! # Fan-out shape
//!
//! Within a lane, `stage`/`resolve` fan out **across collections**
//! (distinct durable partitions, concurrent via
//! [`STATE_FANOUT_CONCURRENCY`](crate::state::STATE_FANOUT_CONCURRENCY) +
//! [`cooperative`]) and **batch within a collection** (a collection's touched
//! cells share a row key, so they stage/promote in one same-partition mutation
//! — `PartitionStateStore::*_batch`). Value is single-cell, so each batch is
//! size-1, identical to the original Value lifecycle.

#[cfg(test)]
mod tests;

use super::sealed::ApplyOutcome;
use crate::consumer::event_context::StateAccessError;
use crate::state::cell::ProvisionalWrite;
use crate::state::dirty::DirtyStore;
use crate::state::identity::{CollectionId, CollectionKind, CollectionRef};
use crate::state::oracle::CommitOracle;
use crate::state::partition_store::{CommittedCache, PartitionStateStore};
use crate::state::registry::CollectionDefRegistry;
use crate::state::store::CellStore;
use crate::state::{CommitMode, EventRef, Read, STATE_FANOUT_CONCURRENCY, StoreOutcome};
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use parking_lot::Mutex as SyncMutex;
use tokio::task::coop::cooperative;
use tracing::warn;

/// One collection's staged cells: each `(addr, write)`'s `data` is the value to
/// promote to, `prev` the committed base to roll back to.
type StagedCells<K> = Vec<(<K as CollectionKind>::CellAddr, ProvisionalWrite)>;

/// The per-collection staged entries `stage` records — one entry per touched
/// `ReadCommitted` collection (its ref carries the TTL).
type StagedEntries<K> = Vec<(CollectionRef<K>, StagedCells<K>)>;

/// The provisional cells `finalize` staged for the promote / rollback hooks,
/// grouped by collection. Only `ReadCommitted` collections appear;
/// `ReadUncommitted` writes resolve at stage time with nothing to resolve.
struct StagedSet<K>
where
    K: CollectionKind,
{
    entries: StagedEntries<K>,
}

/// How [`Lane::resolve`] settles a staged set once the event's outcome is
/// known. Promote and rollback are symmetric — same fan-out, differing only in
/// which side of each staged write becomes committed and which durable move
/// writes it — so they share one parameterized resolution.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Resolve {
    /// The event committed: each cell's staged `data` becomes committed, via an
    /// O(1) promote that nulls `event`/`prev`.
    Promote,

    /// The event aborted: each cell's committed base `prev` is written back as
    /// the resolved value.
    Rollback,
}

impl Resolve {
    /// The side of a staged write that becomes committed under this resolution.
    fn committed_side(self, write: &ProvisionalWrite) -> Option<Bytes> {
        match self {
            Self::Promote => write.data().cloned(),
            Self::Rollback => write.prev().cloned(),
        }
    }

    /// Writes a collection's resolved cells through the durable move this
    /// resolution uses (one batched store call per collection).
    async fn write<K, S, O, C>(
        self,
        store: &PartitionStateStore<K, S, O, C>,
        collection: &CollectionRef<K>,
        cells: &[(K::CellAddr, Option<Bytes>)],
    ) -> Result<(), S::Error>
    where
        K: CollectionKind,
        S: CellStore<K>,
        O: CommitOracle,
        C: CommittedCache<K>,
    {
        match self {
            Self::Promote => store.promote_batch(collection, cells).await,
            Self::Rollback => store.write_resolved_batch(collection, cells).await,
        }
    }

    /// Log label for a failed resolution.
    fn label(self) -> &'static str {
        match self {
            Self::Promote => "promote",
            Self::Rollback => "rollback",
        }
    }
}

/// One kind's per-event lane: its durable store, its dirty workspace, and the
/// staged set the lifecycle resolves.
pub(crate) struct Lane<K, S, O, C>
where
    K: CollectionKind,
{
    store: PartitionStateStore<K, S, O, C>,
    /// Per-event dirty workspace; cleared in place on `reset`. Owned uniquely
    /// by this lane — never shared, never a durability or recovery source.
    dirty: DirtyStore<K>,
    staged: SyncMutex<Option<StagedSet<K>>>,
}

impl<K, S, O, C> Lane<K, S, O, C>
where
    K: CollectionKind,
    S: CellStore<K>,
    O: CommitOracle,
    C: CommittedCache<K>,
{
    /// Opens a lane over its partition-lifetime store, with a fresh dirty
    /// workspace and no staged set.
    pub(crate) fn new(store: PartitionStateStore<K, S, O, C>) -> Self {
        Self {
            store,
            dirty: DirtyStore::new(),
            staged: SyncMutex::new(None),
        }
    }

    /// Reads a cell's currently visible value within this event's transaction:
    /// the buffered op overlaid on the committed base. A buffered op that fully
    /// determines the cell ([`Read::Present`]/[`Read::Absent`]) answers without
    /// a committed read (Value `Set`/`Clear`); an op that does not
    /// ([`Read::Unknown`], an additive delta) applies over the committed base.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Store`] when the committed read fails.
    pub(crate) async fn read_cell(
        &self,
        collection: &CollectionId<K>,
        addr: &K::CellAddr,
        event: EventRef,
    ) -> Result<Option<Bytes>, StateAccessError> {
        match self.dirty.pending_op(collection, addr) {
            Some(op) => match K::read_overlay(&op) {
                Read::Present(payload) => Ok(Some(payload)),
                Read::Absent => Ok(None),
                Read::Unknown => {
                    let base = self
                        .store
                        .committed_value(collection, addr, event)
                        .await
                        .map_err(|e| StateAccessError::store(&e))?;
                    Ok(K::apply(base, &op))
                }
            },
            None => self
                .store
                .committed_value(collection, addr, event)
                .await
                .map_err(|e| StateAccessError::store(&e)),
        }
    }

    /// Buffers a set of a cell's bytes into the dirty workspace.
    pub(crate) async fn set_cell(
        &self,
        collection: &CollectionId<K>,
        addr: &K::CellAddr,
        cell: &[u8],
    ) {
        self.dirty.set(collection, addr, cell).await;
    }

    /// Buffers a clear of a cell into the dirty workspace.
    pub(crate) async fn clear_cell(&self, collection: &CollectionId<K>, addr: &K::CellAddr) {
        self.dirty.clear(collection, addr).await;
    }

    /// Writes a cell's buffered op straight to committed state and clears the
    /// buffer — the mid-handler write-through escape hatch. Folds the op from
    /// the empty base (last-writer-wins for Value; at-least-once for an
    /// additive kind, which should use `ReadCommitted` instead).
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Store`] when the durable write fails.
    pub(crate) async fn flush_cell(
        &self,
        collection: &CollectionRef<K>,
        addr: &K::CellAddr,
    ) -> Result<StoreOutcome, StateAccessError> {
        let Some(op) = self.dirty.pending_op(collection.id(), addr) else {
            return Ok(StoreOutcome::NoOp);
        };
        let data = K::apply(None, &op);
        self.store
            .write_resolved(collection, addr, data.as_ref())
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        self.dirty.clear_cell(collection.id(), addr);
        Ok(StoreOutcome::Applied)
    }

    /// Stages this lane's touched collections by their commit mode, recording
    /// the staged set. Returns `true` iff at least one `ReadCommitted` cell
    /// staged a provisional cell (so the caller arms the backstop).
    ///
    /// Each collection's touched cells are read, applied, and written in
    /// **one** batch; distinct collections fan out concurrently. The stage
    /// feeds [`CollectionKind::apply`]'s base from the own-event committed
    /// read, which returns the cell's `prev` while this event's provisional
    /// cell stands — so an in-place transient retry re-applies over the
    /// same base and re-stages the identical write (idempotent).
    ///
    /// # Errors
    ///
    /// Returns a store error when a stage write or committed read fails;
    /// nothing is recorded in that case (the prior staged set is untouched).
    pub(crate) async fn stage(
        &self,
        event: EventRef,
        registry: &CollectionDefRegistry,
    ) -> Result<bool, StateAccessError> {
        let touched = self.dirty.touched_cells();
        let store = &self.store;
        let staged: StagedEntries<K> = stream::iter(touched)
            .map(|(id, cells)| {
                // `cooperative` adds a per-collection coop-budget checkpoint:
                // the in-memory store completes each future without a tokio
                // leaf await, so a key touching many collections would
                // otherwise drain the whole batch in one poll and starve the
                // worker. Concurrency is unchanged — `buffer_unordered` still
                // drives `STATE_FANOUT_CONCURRENCY` of these at once.
                cooperative(stage_collection(store, registry, event, id, cells))
            })
            .buffer_unordered(STATE_FANOUT_CONCURRENCY)
            .try_filter_map(|opt| async move { Ok(opt) })
            .try_collect()
            .await?;
        // Replace the staged set only after every write succeeded: any error
        // returned above via `?`, leaving the prior staged set untouched.
        if staged.is_empty() {
            Ok(false)
        } else {
            *self.staged.lock() = Some(StagedSet { entries: staged });
            Ok(true)
        }
    }

    /// Resolves this lane's recorded staged set after the event's outcome is
    /// known — [`Resolve::Promote`] on commit, [`Resolve::Rollback`] on abort.
    /// Best-effort: drives every per-collection resolution to completion
    /// regardless of siblings' failures, reporting [`ApplyOutcome::Incomplete`]
    /// if any failed (the backstop, always left armed by the boundary, lets the
    /// sweep retry). The two arms differ only in which side of each staged
    /// write becomes committed (`data` vs `prev`) and which durable move
    /// writes it.
    pub(crate) async fn resolve(&self, how: Resolve) -> ApplyOutcome {
        let Some(set) = self.staged.lock().take() else {
            return ApplyOutcome::NothingStaged;
        };
        let store = &self.store;
        let all_resolved = stream::iter(set.entries)
            .map(|(collection_ref, writes)| {
                cooperative(async move {
                    let cells: Vec<(K::CellAddr, Option<Bytes>)> = writes
                        .iter()
                        .map(|(addr, write)| (addr.clone(), how.committed_side(write)))
                        .collect();
                    match how.write(store, &collection_ref, &cells).await {
                        Ok(()) => true,
                        Err(error) => {
                            warn!(error = ?error, op = how.label(), "cell resolution failed; leaving provisional for the sweep");
                            false
                        }
                    }
                })
            })
            .buffer_unordered(STATE_FANOUT_CONCURRENCY)
            .fold(true, |all, ok| async move { all && ok })
            .await;
        outcome(all_resolved)
    }

    /// Discards the dirty workspace and staged set in place so the next attempt
    /// starts clean. Per-key serialization means no handler op is in flight.
    pub(crate) fn reset(&self) {
        self.dirty.clear_all();
        *self.staged.lock() = None;
    }
}

/// Stages one collection's touched cells in a single batch, returning the
/// staged writes for the lifecycle to promote / roll back (or `None` for a
/// `ReadUncommitted` collection, which resolves at stage time). Free function
/// so no `self` borrow crosses the concurrent fan-out.
async fn stage_collection<K, S, O, C>(
    store: &PartitionStateStore<K, S, O, C>,
    registry: &CollectionDefRegistry,
    event: EventRef,
    id: CollectionId<K>,
    cells: impl IntoIterator<Item = (K::CellAddr, K::Op)>,
) -> Result<Option<(CollectionRef<K>, StagedCells<K>)>, StateAccessError>
where
    K: CollectionKind,
    S: CellStore<K>,
    O: CommitOracle,
    C: CommittedCache<K>,
{
    let collection_ref = CollectionRef::new(id.clone(), registry.ttl_for(id.name()));
    match registry.commit_mode_for(id.name()) {
        CommitMode::ReadCommitted => {
            let mut writes = Vec::new();
            for (addr, op) in cells {
                // The own-event committed read returns this event's `prev` while
                // its provisional cell stands, so a retry re-applies over the
                // same base. The fold ignores the base for Value (LWW) and adds
                // to it for an additive kind.
                let prev = store
                    .committed(&id, &addr, event)
                    .await
                    .map_err(|e| StateAccessError::store(&e))?;
                let data = K::apply(prev.get().cloned(), &op);
                writes.push((addr, ProvisionalWrite::new(data, prev, event)));
            }
            store
                .write_provisional_batch(&collection_ref, &writes)
                .await
                .map_err(|e| StateAccessError::store(&e))?;
            Ok(Some((collection_ref, writes)))
        }
        CommitMode::ReadUncommitted => {
            // Folds each op from the empty base: last-writer-wins for Value.
            let resolved: Vec<(K::CellAddr, Option<Bytes>)> = cells
                .into_iter()
                .map(|(addr, op)| (addr, K::apply(None, &op)))
                .collect();
            store
                .write_resolved_batch(&collection_ref, &resolved)
                .await
                .map_err(|e| StateAccessError::store(&e))?;
            Ok(None)
        }
    }
}

/// Folds a per-collection best-effort result into an [`ApplyOutcome`].
fn outcome(all_resolved: bool) -> ApplyOutcome {
    if all_resolved {
        ApplyOutcome::Resolved
    } else {
        ApplyOutcome::Incomplete
    }
}
