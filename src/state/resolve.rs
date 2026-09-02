//! The single cell-resolution decision, shared by every recovery path.
//!
//! `resolve_cell` is the one place a provisional cell becomes committed:
//! consult the commit oracle for the cell's owning event, then resolve it one
//! of three ways — *promote* present data in place
//! (`CellStore::mark_resolved`, the commit arm for a `Set`), **delete** the
//! row for a committed clear (`CellStore::write_resolved(None)`, the
//! row-absence invariant), or write the committed base back as resolved
//! (`CellStore::write_resolved`, the rollback arm). Eager promotion after
//! commit, the quiescence sweep, and first-touch all funnel through here, so "a
//! provisional cell is resolved only via the oracle" (the oracle-always
//! invariant) holds by construction.
//!
//! `sweep_provisional` is the recovery loop built once over the public trait:
//! it resolves the collection's standing event marker as a unit
//! (`resolve_event_marker`) then streams any remaining provisional cells,
//! returning whether the stage ended fully resolved. The backstop that
//! triggered the sweep is never unscheduled directly (finding F2); it clears
//! only by firing.

use super::CommitDecision;
use super::SHARD_FANOUT_CONCURRENCY;
use super::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::CellKey;
use super::event_ref::EventRef;
use super::identity::{CollectionId, CollectionRef};
use super::marker::EventMarker;
use super::oracle::CommitOracle;
use super::registry::CollectionDefRegistry;
use super::store::{CellBuffer, CellStore, section_batches};
use crate::error::{ClassifyError, ErrorCategory};
use futures::future::join;
use futures::{StreamExt, TryStreamExt, stream};
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

/// A marker that has at least one section clear.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UnsettledClear<'a>(&'a EventMarker);

impl<'a> UnsettledClear<'a> {
    #[must_use]
    pub(crate) fn new(marker: &'a EventMarker) -> Option<Self> {
        (!marker.clears().is_empty()).then_some(Self(marker))
    }

    #[must_use]
    pub(crate) fn marker(self) -> &'a EventMarker {
        self.0
    }
}

/// A section clear from an event other than the current event.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PriorEventClear<'a>(UnsettledClear<'a>);

impl<'a> PriorEventClear<'a> {
    #[must_use]
    pub(crate) fn new(marker: &'a EventMarker, current_event: EventRef) -> Option<Self> {
        (marker.event() != current_event)
            .then(|| UnsettledClear::new(marker))
            .flatten()
            .map(Self)
    }

    #[must_use]
    pub(crate) fn marker(self) -> &'a EventMarker {
        self.0.marker()
    }
}

/// States if read preparation changed durable state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadPreparation {
    Unchanged,
    DurableStateChanged,
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
/// resolving point read (`get`/`get_for_cache`) — the arm that MAY durably
/// repair. The scan path uses the read-only sibling [`peek_read`] instead.
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

/// Resolves one raw cell to its visible committed value for a bottom store's
/// **scan** — the read-only sibling of [`resolve_read`].
///
/// A scan runs gate-free over a pager/snapshot view (the chunked stream
/// contract on [`SessionGate`](crate::state::session)), so it must NOT perform
/// the durable write-back a point read does: a repair computed from that
/// snapshot could delete or overwrite a newer same-handler `commit()` of the
/// same cell (a later monotonic driver timestamp makes the stale write win — a
/// lost durable write with no repair site). The foreign-provisional arm
/// therefore consults the oracle and returns the resolved value — `data`
/// (committed) or `prev` (not committed) — WITHOUT calling
/// [`mark_resolved`](CellStore::mark_resolved) or
/// [`write_resolved`](CellStore::write_resolved). The cell stays provisional,
/// behaviorally identical to "the scan never repaired it"; the point read
/// ([`resolve_read`]), first-touch, and the recovery sweep remain the repair
/// paths. Fails only if the oracle read fails.
pub(crate) async fn peek_read<O>(
    oracle: &O,
    collection: &CollectionRef,
    own: EventRef,
    raw: Cell,
) -> Result<Committed, O::Error>
where
    O: CommitOracle,
{
    match raw {
        Cell::Resolved(committed) => Ok(committed),
        Cell::Provisional(provisional) if provisional.event() == own => {
            Ok(Committed::new(provisional.into_prev()))
        }
        Cell::Provisional(provisional) => {
            let decision = oracle
                .resolve(collection.id().state_key(), provisional.event())
                .await?;
            Ok(match decision {
                CommitDecision::Committed => Committed::new(provisional.into_data()),
                CommitDecision::NotCommitted => Committed::new(provisional.into_prev()),
            })
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
/// or the sweep's marker leg ([`resolve_event_marker`]).
///
/// # Repair-provenance invariant
///
/// A repair's payload derives from durable state that logically **predates**
/// any standing marker, so it must never land *after* that marker resolves:
/// [`write_resolved`]'s write-help boundary
/// ([`resolve_unsettled_clear_before_write`]) resolves a
/// standing clears-bearing marker first, and a positional clear erases only the
/// frozen survivors, so a stale repair write-back landing afterward would
/// resurrect a row the committed clear erases — a silently lost committed
/// clear. Beneath a standing clears-bearing marker the repair therefore
/// **degrades to peek semantics** (value-only, the scan path's posture, no
/// durable write): the marker's own resolution supersedes it. Once the marker
/// settles — a commit lands the frozen clears and promotes the marker's own
/// staged survivors (the erasure argument on [`commit_provisional`] covers
/// every other row in a cleared section); an abort applies no clears — a later
/// first-touch or sweep repairs any still-provisional cell durably. This is
/// what keeps [`write_resolved`]'s boundary sound: every payload that reaches
/// it is handler-fresh and postdates the marker.
///
/// [`commit_provisional`]: CellStore::commit_provisional
/// [`write_resolved`]: CellStore::write_resolved
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
    // Repair-provenance guard (invariant above): beneath a standing
    // clears-bearing marker the repair degrades to peek semantics — value-only,
    // deferred to the marker's own resolution. `unsettled_marker` is RAM-native
    // on memory and memo-backed on Cassandra, so this is one RAM read on the
    // rare repair path.
    let deferred = store
        .unsettled_marker(collection.id())
        .await
        .map_err(ResolveCellError::Store)?
        .is_some_and(|marker| !marker.clears().is_empty());
    if deferred {
        return Ok(Committed::new(match decision {
            CommitDecision::Committed => provisional.into_data(),
            CommitDecision::NotCommitted => provisional.into_prev(),
        }));
    }
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
/// the still-live staged writes — the marker's already-sorted staged
/// coordinates are grouped by section into `<=CELL_BATCH` raw batches
/// ([`CellStore::provisional_many`]), each survivor's [`CellKey`] rebuilt from
/// its chunk's section (coordinates repeat across sections, so the section is
/// reattached inside the chunk stage), keeping only a cell still owned by the
/// marker's event (an absent / resolved / foreign-event cell is already
/// settled, an over-report-safe drop) — then
/// [`commit_provisional`](CellStore::commit_provisional) (committed) or
/// [`abort_provisional`](CellStore::abort_provisional) (not committed). Both
/// verbs delete the marker, including the exhausted case (no live writes),
/// which is why no separate marker-delete verb exists. `clears` ride through
/// verbatim to the commit arm, which applies the frozen gap erase.
///
/// # Oracle-preempts-cell precedence
///
/// The single oracle verdict runs **concurrently** with the read-only batch
/// reads via [`join`] — both legs are read-only until the join completes, so
/// the overlap is observably identical to reading the oracle first. The
/// results are matched **oracle-first**, so a double failure surfaces the
/// [`ResolveCellError::Oracle`] error and its retry/skip classification
/// governs; a first-error-short-circuiting combinator would instead surface
/// whichever leg failed first, making the classification schedule-dependent.
/// Mutations (commit/abort) run strictly after the join.
///
/// # Errors
///
/// Returns [`ResolveCellError::Oracle`] on an oracle failure (preempting a
/// concurrent cell-read failure) or [`ResolveCellError::Store`] on a rebuild /
/// settle failure.
pub(crate) async fn resolve_event_marker<S, O>(
    store: &S,
    oracle: &O,
    collection: &CollectionRef,
    marker: &EventMarker,
) -> Result<(), ResolveCellError<S::Error, O::Error>>
where
    S: CellStore,
    O: CommitOracle,
{
    // The read-only rebuild: one raw batch per per-section chunk of the
    // marker's already-sorted staged coordinates, each survivor's `CellKey`
    // rebuilt from the chunk's section. `buffered` makes the first-failing chunk
    // deterministic (lowest chunk in staged order).
    let reads = stream::iter(section_batches(marker.staged()))
        .map(|(section, batch)| async move {
            let survivors = store
                .provisional_many(collection.id(), section, &batch)
                .await
                .map_err(ResolveCellError::Store)?;
            Ok::<CellBuffer<(CellKey, ProvisionalCell)>, ResolveCellError<S::Error, O::Error>>(
                survivors
                    .into_iter()
                    .map(|(coordinate, provisional)| {
                        (
                            CellKey {
                                section,
                                coordinate,
                            },
                            provisional,
                        )
                    })
                    .collect(),
            )
        })
        .buffered(SHARD_FANOUT_CONCURRENCY)
        .try_collect::<Vec<_>>();

    // Overlap the oracle verdict with the batch reads; match the oracle result
    // FIRST (see the precedence invariant above). Both legs are read-only, so
    // no mutation happens until the join completes.
    let (decision, rebuilt) = join(
        oracle.resolve(collection.id().state_key(), marker.event()),
        reads,
    )
    .await;
    let decision = decision.map_err(ResolveCellError::Oracle)?;
    let rebuilt = rebuilt?;

    // Keep only cells still owned by the marker's event (over-report-safe drop).
    let mut writes: Vec<(CellKey, ProvisionalWrite)> = Vec::with_capacity(marker.staged().len());
    for (cell, provisional) in rebuilt.into_iter().flatten() {
        if provisional.event() == marker.event() {
            // A resolved decision site: `Committed::new` is legal here.
            writes.push((
                cell,
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

/// Resolves an unsettled section clear before a durable resolved write.
///
/// The caller creates [`UnsettledClear`] only when the marker has a clear.
/// Resolution makes the later write newer than this clear.
pub(crate) async fn resolve_unsettled_clear_before_write<S, O>(
    store: &S,
    oracle: &O,
    collection: &CollectionRef,
    clear: Option<UnsettledClear<'_>>,
) -> Result<(), ResolveCellError<S::Error, O::Error>>
where
    S: CellStore,
    O: CommitOracle,
{
    let Some(clear) = clear else {
        return Ok(());
    };
    resolve_event_marker(store, oracle, collection, clear.marker()).await
}

/// Resolves a prior event's section clear before a read.
///
/// The caller creates [`PriorEventClear`] only for a different event.
/// A concurrent raw read must run again when this function changes durable
/// state.
pub(crate) async fn resolve_prior_clear_before_read<S, O>(
    store: &S,
    oracle: &O,
    collection: &CollectionRef,
    clear: Option<PriorEventClear<'_>>,
) -> Result<ReadPreparation, ResolveCellError<S::Error, O::Error>>
where
    S: CellStore,
    O: CommitOracle,
{
    let Some(clear) = clear else {
        return Ok(ReadPreparation::Unchanged);
    };
    resolve_event_marker(store, oracle, collection, clear.marker()).await?;
    Ok(ReadPreparation::DurableStateChanged)
}

/// Resolves a collection's in-flight stage during recovery. Returns `true` iff
/// nothing was left unresolved, for the recovery sweep to act on.
///
/// Two legs, both with the retry-forever posture: the **marker leg** first
/// ([`resolve_event_marker`] on any standing event marker), then the per-cell
/// **mop-up** (the cold `provisional_cells` scan) that resolves any cells the
/// marker leg left behind (a Permanent-skipped cell, a marker-listed
/// coordinate resolved concurrently). A `Permanent` failure in either leg is
/// logged and skipped, leaving the work for first-touch or a later sweep and
/// yielding `false`; anything else — a transient/terminal backend or oracle
/// failure, or a `provisional_cells` stream failure — propagates so the trigger
/// aborts and the sweep refires.
///
/// Ruling: grouping the per-cell mop-up by `(collection, event)` — cells of one
/// event sharing a single oracle decision and one commit/abort write batch — is
/// deferred (low priority). The marker leg already performs one oracle lookup
/// and one collection-grain settle for the common standing marker, so the
/// mop-up is the rare residue (a Permanent-skipped or concurrently-resolved
/// cell); grouping would complicate the permanent-skip / transient-fail posture
/// for a gain that only matters if residues prove common.
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
        .unsettled_marker(collection.id())
        .await
        .map_err(ResolveCellError::Store)?
    {
        Some(marker) => match resolve_event_marker(store, oracle, collection, &marker).await {
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

/// Error raised by cell resolution: the bottom stores' resolving reads (`get`/
/// `get_for_cache` via `resolve_read`, `scan_cells` via the read-only
/// `peek_read`) and the recovery sweep (`sweep_provisional`).
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
