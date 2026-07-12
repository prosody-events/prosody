//! The single cell-resolution decision, shared by every recovery path.
//!
//! `resolve_cell` is the one place a provisional cell becomes committed:
//! consult the commit oracle for the cell's owning event, then resolve it one
//! of three ways — *promote* present data in place
//! ([`CellStore::mark_resolved`], the commit arm for a `Set`), **delete** the
//! row for a committed clear ([`CellStore::write_resolved`]`(None)`, the
//! row-absence invariant), or write the committed base back as resolved
//! ([`CellStore::write_resolved`], the rollback arm). Eager promotion after
//! commit, the quiescence sweep, and first-touch all funnel through here, so "a
//! provisional cell is resolved only via the oracle" (the oracle-always
//! invariant) holds by construction.
//!
//! `sweep_provisional` is the recovery loop built once over the public trait:
//! it resolves the collection's standing event marker as a unit
//! (`resolve_marker`) then streams any remaining provisional cells, returning
//! whether the stage ended fully resolved. The backstop that triggered the
//! sweep is never unscheduled directly (finding F2); it clears only by firing.

use super::CommitDecision;
use super::SHARD_FANOUT_CONCURRENCY;
use super::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::CellKey;
use super::event_ref::EventRef;
use super::identity::{CollectionId, CollectionRef};
use super::marker::EventMarker;
use super::oracle::CommitOracle;
use super::registry::CollectionDefRegistry;
use super::store::CellStore;
use crate::error::{ClassifyError, ErrorCategory};
use futures::{StreamExt, TryStreamExt};
use std::error::Error;
use std::fmt;
use std::slice;
use std::sync::Arc;
use thiserror::Error;
use tracing::error;

/// The per-partition resolution context both bottom [`CellStore`]s compose: the
/// commit oracle they resolve provisional cells through, and the registry that
/// supplies each collection's TTL for resolution write-backs.
///
/// This is a plain data bundle, **not** a `CellStore` combinator — the bottom
/// stores own their own raw reads and merely delegate the resolution decision
/// (and TTL lookup) here, so the subtle logic lives once without a second store
/// type or type parameter threaded through the layering.
#[derive(Clone)]
pub(crate) struct Resolver<O> {
    oracle: O,
    registry: Arc<CollectionDefRegistry>,
}

impl<O> fmt::Debug for Resolver<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resolver").finish_non_exhaustive()
    }
}

impl<O> Resolver<O> {
    /// Composes a resolution context over the shared oracle and registry.
    #[must_use]
    pub(crate) fn new(oracle: O, registry: Arc<CollectionDefRegistry>) -> Self {
        Self { oracle, registry }
    }

    /// The shared commit oracle.
    pub(crate) fn oracle(&self) -> &O {
        &self.oracle
    }

    /// Builds the per-collection [`CollectionRef`] with its registry TTL, for a
    /// resolution write-back.
    pub(crate) fn collection_ref(&self, id: &CollectionId) -> CollectionRef {
        CollectionRef::new(
            id.clone(),
            self.registry.ttl_for(id.state_type(), id.name()),
        )
    }
}

/// Collapses the one redundant nesting a bottom store's `get`/`scan` produce.
///
/// [`resolve_read`]/[`resolve_cell`] are generic over `S: CellStore` and wrap
/// the store's mutator error in [`ResolveCellError::Store`]. When the store is
/// itself a *bottom* store (whose `CellStore::Error` is already a
/// `ResolveCellError<RawErr, OracleErr>`, since its `get` resolves), that nests
/// one extra layer. The two `OracleErr`s are identical (one oracle), so the
/// nesting is pure redundancy: unwrap the inner store error and re-tag the
/// oracle arm. (The recovery sweep keeps the nesting — its
/// [`ResolveCellError::Store`] holds the cell store's whole error — so this
/// only applies to `get`/`scan`.)
pub(crate) fn flatten_resolve<StoreErr, OracleErr>(
    error: ResolveCellError<ResolveCellError<StoreErr, OracleErr>, OracleErr>,
) -> ResolveCellError<StoreErr, OracleErr>
where
    StoreErr: Error + 'static,
    OracleErr: Error + 'static,
{
    match error {
        ResolveCellError::Store(inner) => inner,
        ResolveCellError::Oracle(oracle) => ResolveCellError::Oracle(oracle),
    }
}

/// Resolves one raw cell to its visible committed value, for a bottom store's
/// resolving `get`/`scan_cells`.
///
/// A resolved cell is already committed; a provisional cell owned by `own` is
/// the running handler's own write — provably uncommitted while the handler
/// runs — so the committed base is its `prev`, returned without an oracle
/// consult or a durable write (the own-event-base-is-prev invariant); any
/// other provisional cell is resolved through the oracle (eager write-back)
/// via [`resolve_cell`]. Post-settle contexts (the apply hooks, running after
/// `finalize` drained the event's dirty buffer) can hold an own [`EventRef`]
/// for an event that already committed — a partially-promoted stage — where
/// the short-circuit still answers `prev`: a per-cell committed projection,
/// accepted as the best-effort hook-window contract rather than an oracle
/// consult.
pub(crate) async fn resolve_read<S, O>(
    store: &S,
    oracle: &O,
    collection: &CollectionRef,
    cell: &CellKey,
    own: EventRef,
    raw: Cell,
) -> Result<Committed, ResolveCellError<S::Error, O::Error>>
where
    S: CellStore,
    O: CommitOracle,
{
    match raw {
        Cell::Resolved(committed) => Ok(committed),
        Cell::Provisional(provisional) if provisional.event() == own => {
            Ok(Committed::new(provisional.into_prev()))
        }
        Cell::Provisional(provisional) => {
            resolve_cell(store, oracle, collection, cell, provisional).await
        }
    }
}

/// Resolves one provisional cell through the commit oracle.
///
/// Returns the now-committed value: `data` when the oracle says the owning
/// event committed (the cell is promoted in place), or `prev` when it did not
/// (the committed base is written back as resolved). Fails with
/// [`ResolveCellError::Oracle`] if the oracle read fails, or
/// [`ResolveCellError::Store`] if the promote / write-back fails.
///
/// First-touch resolution stays **cell-grained and marker-free**: it uses the
/// primitive verbs and leaves any standing event marker in place. The marker's
/// resulting over-report is harmless — a later point read sees the cell already
/// resolved and drops it — and the marker is cleared by the next stage boundary
/// or the sweep's marker leg ([`resolve_marker`]).
pub(crate) async fn resolve_cell<S, O>(
    store: &S,
    oracle: &O,
    collection: &CollectionRef,
    cell: &CellKey,
    provisional: ProvisionalCell,
) -> Result<Committed, ResolveCellError<S::Error, O::Error>>
where
    S: CellStore,
    O: CommitOracle,
{
    let decision = oracle
        .resolve(collection.id().state_key(), provisional.event())
        .await
        .map_err(ResolveCellError::Oracle)?;
    match decision {
        CommitDecision::Committed => {
            // Promote-of-a-clear must delete the row, not null it: an absent-data
            // commit routes to `write_resolved(None)` (the row-absence
            // invariant), present data promotes in place.
            if provisional.data().is_some() {
                store
                    .mark_resolved(collection, slice::from_ref(cell))
                    .await
                    .map_err(ResolveCellError::Store)?;
            } else {
                store
                    .write_resolved(collection, &[(cell.clone(), None)], &[])
                    .await
                    .map_err(ResolveCellError::Store)?;
            }
            Ok(Committed::new(provisional.into_data()))
        }
        CommitDecision::NotCommitted => {
            store
                .write_resolved(
                    collection,
                    &[(cell.clone(), provisional.prev().cloned())],
                    &[],
                )
                .await
                .map_err(ResolveCellError::Store)?;
            Ok(Committed::new(provisional.into_prev()))
        }
    }
}

/// Resolves a collection's standing **event marker** as a unit, the shared leg
/// of the sweep and (in the memory backend) the stage boundary.
///
/// One oracle verdict on the marker's event decides the whole stage: rebuild
/// the still-live staged writes — for each listed coordinate,
/// [`CellStore::provisional_cell_at`], keeping only a cell still owned by the
/// marker's event (an absent / resolved / foreign-event cell is already
/// settled, an over-report-safe drop) — then
/// [`commit_provisional`](CellStore::commit_provisional) (committed) or
/// [`abort_provisional`](CellStore::abort_provisional) (not committed). Both
/// verbs delete the marker, including the exhausted case (no live writes),
/// which is why no separate marker-delete verb exists. `clears` ride through
/// verbatim to the commit arm, which applies the frozen gap erase.
///
/// # Errors
///
/// Returns [`ResolveCellError::Oracle`] on an oracle failure or
/// [`ResolveCellError::Store`] on a rebuild / settle failure.
pub(crate) async fn resolve_marker<S, O>(
    store: &S,
    oracle: &O,
    collection: &CollectionRef,
    marker: &EventMarker,
) -> Result<(), ResolveCellError<S::Error, O::Error>>
where
    S: CellStore,
    O: CommitOracle,
{
    let decision = oracle
        .resolve(collection.id().state_key(), marker.event())
        .await
        .map_err(ResolveCellError::Oracle)?;
    let mut writes: Vec<(CellKey, ProvisionalWrite)> = Vec::with_capacity(marker.staged().len());
    for cell in marker.staged() {
        let provisional = store
            .provisional_cell_at(collection.id(), cell)
            .await
            .map_err(ResolveCellError::Store)?;
        if let Some(provisional) = provisional
            && provisional.event() == marker.event()
        {
            // A resolved decision site: `Committed::new` is legal here.
            writes.push((
                cell.clone(),
                ProvisionalWrite::new(
                    provisional.data().cloned(),
                    Committed::new(provisional.prev().cloned()),
                    provisional.event(),
                ),
            ));
        }
    }
    match decision {
        CommitDecision::Committed => store
            .commit_provisional(collection, &writes, marker.clears())
            .await
            .map_err(ResolveCellError::Store),
        CommitDecision::NotCommitted => store
            .abort_provisional(collection, &writes)
            .await
            .map_err(ResolveCellError::Store),
    }
}

/// The committed-unapplied read window: a standing **foreign** event marker
/// that carries section clears means gap tombstones may not have landed yet,
/// and a pre-clear resolved row cannot be caught by per-cell oracle resolution
/// the way a provisional cell is. Resolve the whole marker through the sweep
/// path ([`resolve_marker`]) before serving the read; markers without clears
/// are left standing (first-touch stays cell-grained and marker-free).
///
/// The one read-help decision, shared by both bottom stores' `get`/`scan`
/// pairs. Returns whether it resolved, so a caller that read concurrently with
/// the marker consult re-issues its raw read. The own-event guard is
/// load-bearing: the staging event's own reads between stage and settle must
/// NOT resolve its own marker (that would settle the event mid-flight) — the
/// trigger is strictly *foreign AND clears non-empty*.
///
/// # Errors
///
/// Returns [`ResolveCellError`] as [`resolve_marker`] would.
pub(crate) async fn help_read_window<S, O>(
    store: &S,
    oracle: &O,
    collection: &CollectionRef,
    marker: Option<&EventMarker>,
    own: EventRef,
) -> Result<bool, ResolveCellError<S::Error, O::Error>>
where
    S: CellStore,
    O: CommitOracle,
{
    let Some(marker) = marker else {
        return Ok(false);
    };
    if marker.event() == own || marker.clears().is_empty() {
        return Ok(false);
    }
    resolve_marker(store, oracle, collection, marker).await?;
    Ok(true)
}

/// Resolves a collection's in-flight stage during recovery. Returns `true` iff
/// nothing was left unresolved, for the recovery sweep to act on.
///
/// Two legs, both with the retry-forever posture: the **marker leg** first
/// ([`resolve_marker`] on any standing event marker), then the per-cell
/// **mop-up** (the cold `provisional_cells` scan) that covers any cells the
/// marker leg left behind (a Permanent-skipped cell, a marker-listed
/// coordinate resolved concurrently). A `Permanent` failure in either leg is
/// logged and skipped, leaving the work for first-touch or a later sweep and
/// yielding `false`; anything else — a transient/terminal backend or oracle
/// failure, or a `provisional_cells` stream failure — propagates so the trigger
/// aborts and the sweep refires.
pub(crate) async fn sweep_provisional<S, O>(
    store: &S,
    oracle: &O,
    collection: &CollectionRef,
) -> Result<bool, ResolveCellError<S::Error, O::Error>>
where
    S: CellStore,
    O: CommitOracle,
{
    // Marker leg: resolve the standing event marker as a unit before the
    // per-cell mop-up. A quiescent collection answers `None` (RAM-native on the
    // memory backend, memo-warm on Cassandra — no durable marker read either
    // way), so this is a free no-op almost always.
    let marker_ok = match store
        .standing_marker(collection.id())
        .await
        .map_err(ResolveCellError::Store)?
    {
        Some(marker) => match resolve_marker(store, oracle, collection, &marker).await {
            Ok(()) => true,
            Err(error) if error.classify_error() == ErrorCategory::Permanent => {
                error!(
                    name = collection.id().name().as_str(),
                    "skipping permanently-failing event marker; first-touch or the next stage \
                     boundary must resolve it: {error:#}"
                );
                false
            }
            Err(error) => return Err(error),
        },
        None => true,
    };

    // Resolutions of distinct cells are independent and commutative (each cell
    // is promoted/rolled-back once), so pipeline them on the partition's shard:
    // `try_fold` ANDs the per-cell Permanent-skip flags and short-circuits on a
    // propagating error — the same outcome as the sequential loop, order-free.
    // Cassandra/oracle I/O leaves drive the coop budget, so no `cooperative`.
    let cells_ok = store
        .provisional_cells(collection.id())
        .map_err(ResolveCellError::Store)
        .map(|item| async move {
            let (cell, provisional) = item?;
            match resolve_cell(store, oracle, collection, &cell, provisional).await {
                Ok(_) => Ok(true),
                Err(error) if error.classify_error() == ErrorCategory::Permanent => {
                    error!(
                        name = collection.id().name().as_str(),
                        "skipping permanently-failing provisional cell; first-touch or the cell \
                         TTL must resolve it: {error:#}"
                    );
                    Ok(false)
                }
                Err(error) => Err(error),
            }
        })
        .buffer_unordered(SHARD_FANOUT_CONCURRENCY)
        .try_fold(true, |all, ok| async move { Ok(all && ok) })
        .await?;
    Ok(marker_ok && cells_ok)
}

/// Error raised by cell resolution: the bottom stores' resolving `get`/
/// `scan_cells` (via `resolve_read`) and the recovery sweep
/// (`sweep_provisional`).
///
/// It is the bottom `CellStore`s' associated `Error`: their currency is the
/// resolved `Committed` cell, so a read can fail either in the raw store
/// (`Store`) or in the oracle consult (`Oracle`).
#[derive(Debug, Error)]
pub enum ResolveCellError<StoreErr, OracleErr>
where
    StoreErr: Error + 'static,
    OracleErr: Error + 'static,
{
    /// The durable promote / write-back failed.
    #[error("keyed-state cell store failed")]
    Store(#[source] StoreErr),

    /// The commit oracle failed.
    #[error("keyed-state commit oracle failed")]
    Oracle(#[source] OracleErr),
}

impl<StoreErr, OracleErr> ClassifyError for ResolveCellError<StoreErr, OracleErr>
where
    StoreErr: ClassifyError + Error + 'static,
    OracleErr: ClassifyError + Error + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Store(e) => e.classify_error(),
            Self::Oracle(e) => e.classify_error(),
        }
    }
}
