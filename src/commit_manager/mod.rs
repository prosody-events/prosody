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
use std::fmt;
use std::future::Future;
use thiserror::Error;
use uuid::Uuid;

/// Source of the current tag for a timer row — the timer half of the
/// commit oracle.
///
/// Two implementations exist: [`TimerManager`] (consults its in-memory
/// scheduler before the store; used where a live manager is in hand, e.g.
/// tests that drive the full timer lifecycle) and [`StoreTagSource`] (a
/// bare [`TriggerStore`] read; production keyed-state wiring, where the
/// partition's live manager is not reachable and the oracle is only ever
/// consulted for events that have fully completed — per-key serialization
/// guarantees their durability markers landed before recovery runs).
pub trait TimerTagSource: Clone + Send + Sync + 'static {
    /// Error type for tag reads.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Returns the current tag for `(key, time, timer_type)`, or `None`
    /// when no such timer row exists.
    fn current_timer_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Option<i32>, Self::Error>> + Send;
}

impl<T> TimerTagSource for TimerManager<T>
where
    T: TriggerStore,
    T::Error: ClassifyError,
{
    type Error = TimerManagerError<T::Error>;

    async fn current_timer_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<Option<i32>, Self::Error> {
        self.current_tag(key, time, timer_type).await
    }
}

/// [`TimerTagSource`] over a bare [`TriggerStore`].
///
/// A newtype rather than a blanket impl so it cannot overlap with the
/// [`TimerManager`] impl.
#[derive(Clone)]
pub struct StoreTagSource<T>(pub T);

impl<T> fmt::Debug for StoreTagSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreTagSource").finish_non_exhaustive()
    }
}

impl<T> TimerTagSource for StoreTagSource<T>
where
    T: TriggerStore,
{
    type Error = T::Error;

    async fn current_timer_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<Option<i32>, Self::Error> {
        self.0.current_tag(key, time, timer_type).await
    }
}

/// Read-only oracle for commit state of message and timer events.
///
/// The deduplication slot is `Option` so consumers that disable
/// deduplication can still construct the oracle for an *empty* keyed-state
/// registry (it is never consulted there — the consumer build rejects
/// registered state without deduplication). Resolving a message event with
/// the slot empty fails loudly with
/// [`CommitManagerError::DeduplicationDisabled`].
#[derive(Clone)]
pub struct CommitManager<D, TS>
where
    D: DeduplicationStore,
    TS: TimerTagSource,
{
    dedup: Option<D>,
    timers: TS,
}

impl<D, TS> fmt::Debug for CommitManager<D, TS>
where
    D: DeduplicationStore,
    TS: TimerTagSource,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitManager")
            .field("dedup_enabled", &self.dedup.is_some())
            .finish_non_exhaustive()
    }
}

impl<D, TS> CommitManager<D, TS>
where
    D: DeduplicationStore,
    TS: TimerTagSource,
{
    /// Creates a new commit manager.
    pub fn new(dedup: D, timers: TS) -> Self {
        Self {
            dedup: Some(dedup),
            timers,
        }
    }

    /// Creates a commit manager whose deduplication slot may be empty.
    ///
    /// Pass `None` only when no keyed-state collections are registered —
    /// message-event resolution fails Permanent without a deduplication
    /// store.
    pub fn with_optional_dedup(dedup: Option<D>, timers: TS) -> Self {
        Self { dedup, timers }
    }

    /// Returns `true` if the message identified by `dedup_id` has been
    /// committed to the deduplication store.
    ///
    /// # Errors
    ///
    /// Returns `CommitManagerError::Dedup` if the store read fails, or
    /// `CommitManagerError::DeduplicationDisabled` when the deduplication
    /// slot is empty.
    pub async fn is_message_committed(
        &self,
        dedup_id: Uuid,
    ) -> Result<bool, CommitManagerError<D::Error, TS::Error>> {
        let Some(dedup) = &self.dedup else {
            return Err(CommitManagerError::DeduplicationDisabled);
        };
        dedup
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
    ) -> Result<bool, CommitManagerError<D::Error, TS::Error>> {
        match self
            .timers
            .current_timer_tag(key, time, timer_type)
            .await
            .map_err(CommitManagerError::Timer)?
        {
            None => Ok(true),
            Some(cur) => Ok(cur != wal_tag),
        }
    }
}

impl<D, TS> CommitOracle for CommitManager<D, TS>
where
    D: DeduplicationStore,
    TS: TimerTagSource,
{
    type Error = CommitManagerError<D::Error, TS::Error>;

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
    /// Timer tag read failed.
    #[error("timer store error")]
    Timer(#[source] TE),
    /// A message event was resolved without a deduplication store. Only
    /// reachable when WAL-mode keyed state runs with deduplication
    /// disabled — a configuration the consumer build rejects.
    #[error("message commit state requires the deduplication middleware")]
    DeduplicationDisabled,
}

impl<DE, TE> ClassifyError for CommitManagerError<DE, TE>
where
    DE: Error + Send + Sync + 'static,
    TE: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Both halves are storage reads — transient.
            Self::Dedup(_) | Self::Timer(_) => ErrorCategory::Transient,
            // Configuration error; retrying cannot help.
            Self::DeduplicationDisabled => ErrorCategory::Permanent,
        }
    }
}

#[cfg(test)]
mod tests;
