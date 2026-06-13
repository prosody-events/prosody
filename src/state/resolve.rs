//! The single cell-resolution decision, shared by every recovery path.
//!
//! `resolve_cell` is the one place a provisional cell becomes committed:
//! consult the commit oracle for the cell's owning event, then either
//! *promote* it ([`CellStore::mark_resolved`], the commit arm) or write the
//! committed base back as resolved ([`CellStore::write_resolved`], the
//! rollback arm). Eager promotion after commit, the quiescence sweep, and
//! first-touch all funnel through here, so "a provisional cell is resolved
//! only via the oracle" (the oracle-always invariant) holds by construction.

use super::CommitDecision;
use super::cell::{Committed, ProvisionalCell};
use super::identity::{CollectionKind, CollectionRef};
use super::oracle::CommitOracle;
use super::store::CellStore;
use crate::error::{ClassifyError, ErrorCategory};
use std::error::Error;
use thiserror::Error;

/// Resolves one provisional cell through the commit oracle.
///
/// Returns the now-committed value: `data` when the oracle says the owning
/// event committed (the cell is promoted in place), or `prev` when it did
/// not (the committed base is written back as resolved).
///
/// # Errors
///
/// Returns [`ResolveCellError::Oracle`] if the oracle read fails, or
/// [`ResolveCellError::Store`] if the promote / write-back fails.
pub(crate) async fn resolve_cell<K, S, O>(
    store: &S,
    oracle: &O,
    collection: &CollectionRef<K>,
    addr: &K::CellAddr,
    cell: ProvisionalCell,
) -> Result<Committed, ResolveCellError<S::Error, O::Error>>
where
    K: CollectionKind,
    S: CellStore<K>,
    O: CommitOracle,
{
    let decision = oracle
        .resolve(collection.id().state_key(), cell.event())
        .await
        .map_err(ResolveCellError::Oracle)?;
    match decision {
        CommitDecision::Committed => {
            store
                .mark_resolved(collection, addr)
                .await
                .map_err(ResolveCellError::Store)?;
            Ok(Committed::new(cell.into_data()))
        }
        CommitDecision::NotCommitted => {
            store
                .write_resolved(collection, addr, cell.prev())
                .await
                .map_err(ResolveCellError::Store)?;
            Ok(Committed::new(cell.into_prev()))
        }
    }
}

/// Error raised by [`resolve_cell`].
///
/// Kept distinct from the partition store's and sweep's error enums so the
/// shared helper carries no caller-specific variants; each callsite maps it
/// into its own enum.
#[derive(Debug, Error)]
pub(crate) enum ResolveCellError<StoreErr, OracleErr>
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
