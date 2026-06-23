//! The single cell-resolution decision, shared by every recovery path.
//!
//! `resolve_cell` is the one place a provisional cell becomes committed:
//! consult the commit oracle for the cell's owning event, then either *promote*
//! it ([`CellStore::mark_resolved`], the commit arm) or write the committed
//! base back as resolved ([`CellStore::write_resolved`], the rollback arm).
//! Eager promotion after commit, the quiescence sweep, and first-touch all
//! funnel through here, so "a provisional cell is resolved only via the oracle"
//! (the oracle-always invariant) holds by construction.
//!
//! `sweep_provisional` is the recovery loop built once over the public trait:
//! it streams a collection's provisional cells and resolves each, returning
//! whether every cell ended resolved — the no-strand signal the recovery sweep
//! gates its `unschedule_all` on.

use super::CommitDecision;
use super::cell::{Cell, Committed, ProvisionalCell};
use super::cell_key::CellKey;
use super::event_ref::EventRef;
use super::identity::{CollectionId, CollectionRef};
use super::oracle::CommitOracle;
use super::registry::CollectionDefRegistry;
use super::store::CellStore;
use crate::error::{ClassifyError, ErrorCategory};
use futures::StreamExt;
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
/// oracle arm. (The recovery sweep keeps the nesting — `RecoveryError::Store`
/// holds the cell store's whole error — so this only applies to `get`/`scan`.)
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
/// the running handler's own (provably uncommitted) write, so the committed
/// base is its `prev` — returned without an oracle consult or a durable write
/// (the own-event-base-is-prev invariant); any other provisional cell is
/// resolved through the oracle (eager write-back) via [`resolve_cell`].
///
/// # Errors
///
/// Returns [`ResolveCellError`] when the oracle or the resolution write-back
/// fails.
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
/// (the committed base is written back as resolved).
///
/// # Errors
///
/// Returns [`ResolveCellError::Oracle`] if the oracle read fails, or
/// [`ResolveCellError::Store`] if the promote / write-back fails.
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
            store
                .mark_resolved(collection, slice::from_ref(cell))
                .await
                .map_err(ResolveCellError::Store)?;
            Ok(Committed::new(provisional.into_data()))
        }
        CommitDecision::NotCommitted => {
            store
                .write_resolved(collection, &[(cell.clone(), provisional.prev().cloned())])
                .await
                .map_err(ResolveCellError::Store)?;
            Ok(Committed::new(provisional.into_prev()))
        }
    }
}

/// Resolves every provisional cell of a collection through the oracle (the
/// quiescence sweep loop). Returns `true` iff every cell ended resolved — the
/// caller unschedules the backstop only then (the no-strand invariant).
///
/// A per-cell `Permanent` failure is logged and skipped, leaving the cell for
/// first-touch or a later sweep and yielding `false`; anything else propagates
/// so the trigger aborts and the sweep refires.
///
/// # Errors
///
/// Returns [`ResolveCellError`] on a transient/terminal backend or oracle
/// failure, or a `provisional_cells` stream failure.
pub(crate) async fn sweep_provisional<S, O>(
    store: &S,
    oracle: &O,
    collection: &CollectionRef,
) -> Result<bool, ResolveCellError<S::Error, O::Error>>
where
    S: CellStore,
    O: CommitOracle,
{
    let stream = store.provisional_cells(collection.id());
    futures::pin_mut!(stream);
    let mut all_resolved = true;
    while let Some(item) = stream.next().await {
        let (cell, provisional) = item.map_err(ResolveCellError::Store)?;
        match resolve_cell(store, oracle, collection, &cell, provisional).await {
            Ok(_) => {}
            Err(error) if error.classify_error() == ErrorCategory::Permanent => {
                error!(
                    name = collection.id().name().as_str(),
                    "skipping permanently-failing provisional cell; first-touch or the cell TTL \
                     must resolve it: {error:#}"
                );
                all_resolved = false;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(all_resolved)
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
