//! Resolves admitted cells and durable event residue.

use super::CommitDecision;
use super::SHARD_FANOUT_CONCURRENCY;
use super::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite, resolve_for_reader};
use super::cell_key::CellKey;
use super::identity::{CollectionId, CollectionRef};
use super::marker::{EventMarker, ReaderEvidence};
use super::store::{CellBuffer, CellStore, section_batches};
use crate::error::{ClassifyError, ErrorCategory};
use futures::{StreamExt, TryStreamExt, stream};
use std::error::Error;
use thiserror::Error;
use tokio::task::coop::cooperative;

/// Returns the committed base for an admitted event.
/// Provisional cells use the same evidence as standalone readers.
/// Each provisional read reads the collection marker and each touched sibling
/// marker at most once.
pub(crate) async fn resolve_read<S: CellStore>(
    store: &S,
    collection: &CollectionId,
    raw: Cell,
) -> Result<Committed, S::Error> {
    match raw {
        Cell::Resolved(committed) => Ok(committed),
        Cell::Provisional(provisional) => {
            let state = store.marker_state(collection).await?;
            let mut staged_committed = false;
            if let Some(marker) = &state.staged {
                staged_committed = stream::iter((0..marker.touched().len()).filter(|&index| {
                    let (kind, name) = &marker.touched()[index];
                    *kind != collection.state_type() || name != collection.name()
                }))
                .map(|index| {
                    cooperative(async move {
                        let (kind, name) = &marker.touched()[index];
                        let sibling =
                            CollectionId::new(collection.state_key().clone(), *kind, name.clone());
                        Ok::<_, S::Error>(
                            store
                                .marker_state(&sibling)
                                .await?
                                .committed
                                .is_some_and(|evidence| evidence.certifies(marker)),
                        )
                    })
                })
                .buffer_unordered(marker.touched().len().max(1))
                .try_fold(false, |any, committed| async move { Ok(any || committed) })
                .await?;
            }
            let evidence = ReaderEvidence {
                state,
                staged_committed,
            };
            Ok(Committed::new(
                resolve_for_reader(&Cell::Provisional(provisional), &evidence).cloned(),
            ))
        }
    }
}

/// Applies the admission decision to all cells that still belong to this
/// marker. The marker bounds cell reads and supplies the frozen clear
/// survivors.
pub(crate) async fn resolve_event_marker<S: CellStore>(
    store: &S,
    collection: &CollectionRef,
    marker: &EventMarker,
    decision: CommitDecision,
) -> Result<(), S::Error> {
    // Read each section in bounded batches.
    let reads = stream::iter(section_batches(marker.staged()))
        .map(|(section, batch)| async move {
            let survivors = store
                .provisional_many(collection.id(), section, &batch)
                .await?;
            Ok::<CellBuffer<(CellKey, ProvisionalCell)>, S::Error>(
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

    let rebuilt = reads.await?;

    // Keep only cells that still belong to this event.
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
        CommitDecision::Committed => store.commit_provisional(collection, marker, &writes).await,
        CommitDecision::NotCommitted => store.abort_provisional(collection, &writes).await,
    }
}

/// An owner read failed in the store.
#[derive(Debug, Error)]
pub enum ResolveCellError<StoreErr>
where
    StoreErr: Error + 'static,
{
    /// The cell store operation failed.
    #[error("keyed-state cell store failed")]
    Store(#[source] StoreErr),
}

impl<StoreErr> ClassifyError for ResolveCellError<StoreErr>
where
    StoreErr: ClassifyError + Error + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Store(error) => error.classify_error(),
        }
    }
}
