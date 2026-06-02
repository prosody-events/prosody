//! Commit oracle for keyed-state recovery.
//!
//! [`CommitManager`] answers "did this event commit?" for both message and
//! timer events. It is the read-only interface consumed by the state
//! middleware during WAL-based recovery; no write operations are exposed.
//!
//! # Message side
//!
//! `is_message_committed(dedup_id)` delegates to a [`DeduplicationStore`]:
//! row present ⇔ committed.
//!
//! # Timer side
//!
//! `is_timer_committed(key, type, time, wal_tag)` compares the WAL-recorded
//! tag against the current tag in storage:
//! - row absent → committed (fired-and-removed)
//! - `Some(cur) == wal_tag` → not committed
//! - `Some(cur) != wal_tag` → committed-and-rescheduled (returns `true`)
//!
//! **Encapsulation note**: no accessor returns the inner store or manager;
//! the state middleware physically cannot reach write operations through this
//! type.

use crate::Key;
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::oracle::CommitOracle;
use crate::state::value::ValueKind;
use crate::state::{CollectionId, CommitDecision, EventRef, TimerEventRef};
use crate::timers::TimerManager;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::error::TimerManagerError;
use crate::timers::store::TriggerStore;
use std::error::Error;
use thiserror::Error;
use uuid::Uuid;

/// Read-only oracle for commit state of message and timer events.
#[derive(Clone, Debug)]
pub struct CommitManager<D, T>
where
    D: DeduplicationStore,
    T: TriggerStore,
{
    dedup: D,
    timers: TimerManager<T>,
}

impl<D, T> CommitManager<D, T>
where
    D: DeduplicationStore,
    T: TriggerStore,
    T::Error: ClassifyError,
{
    /// Creates a new commit manager.
    pub fn new(dedup: D, timers: TimerManager<T>) -> Self {
        Self { dedup, timers }
    }

    /// Returns `true` if the message identified by `dedup_id` has been
    /// committed to the deduplication store.
    ///
    /// # Errors
    ///
    /// Returns `CommitManagerError::Dedup` if the store read fails.
    pub async fn is_message_committed(
        &self,
        dedup_id: Uuid,
    ) -> Result<bool, CommitManagerError<D::Error, T::Error>> {
        self.dedup
            .exists(dedup_id)
            .await
            .map_err(CommitManagerError::Dedup)
    }

    /// Returns `true` if the timer event identified by `(key, timer_type,
    /// time, wal_tag)` has been committed.
    ///
    /// Three-state decision:
    /// - row absent → `true` (fired-and-removed → committed)
    /// - `current_tag == wal_tag` → `false` (still scheduled, not committed)
    /// - `current_tag != wal_tag` → `true` (committed-and-rescheduled)
    ///
    /// # Errors
    ///
    /// Returns `CommitManagerError::Timer` if the timer store read fails.
    pub async fn is_timer_committed(
        &self,
        key: &Key,
        timer_type: TimerType,
        time: CompactDateTime,
        wal_tag: i32,
    ) -> Result<bool, CommitManagerError<D::Error, T::Error>> {
        match self
            .timers
            .current_tag(key, time, timer_type)
            .await
            .map_err(CommitManagerError::Timer)?
        {
            None => Ok(true),
            Some(cur) => Ok(cur != wal_tag),
        }
    }
}

impl<D, T> CommitOracle for CommitManager<D, T>
where
    D: DeduplicationStore,
    T: TriggerStore,
    T::Error: ClassifyError,
{
    type Error = CommitManagerError<D::Error, T::Error>;

    async fn resolve<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        let committed = match event {
            EventRef::Message { dedup_id } => self.is_message_committed(dedup_id).await?,
            EventRef::Timer(TimerEventRef {
                timer_type,
                time,
                tag,
            }) => {
                self.is_timer_committed(&collection.state_key().key, timer_type, time, tag)
                    .await?
            }
        };
        Ok(if committed {
            CommitDecision::Committed
        } else {
            CommitDecision::NotCommitted
        })
    }
}

/// Error type for [`CommitManager`] operations.
#[derive(Debug, Error)]
pub enum CommitManagerError<DE, TE>
where
    DE: Error + Send + Sync + 'static,
    TE: ClassifyError + Error + Send + Sync + 'static,
{
    /// Deduplication store read failed.
    #[error("deduplication store error")]
    Dedup(#[source] DE),
    /// Timer store read failed.
    #[error("timer store error")]
    Timer(#[source] TimerManagerError<TE>),
}

impl<DE, TE> ClassifyError for CommitManagerError<DE, TE>
where
    DE: Error + Send + Sync + 'static,
    TE: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        // Both halves are storage reads — transient.
        ErrorCategory::Transient
    }
}

#[cfg(test)]
#[path = "commit_manager_tests.rs"]
mod tests;
