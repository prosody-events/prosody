//! Commit oracle for keyed-state recovery.
//!
//! [`CommitManager`] answers "did this event commit?" for both message and
//! timer events, and writes the one marker it owns end-to-end: the message
//! dedup row. It implements [`CommitOracle`] — the recovery reader's
//! resolve half plus the durability boundary's [`CommitOracle::record_message`]
//! write half.
//!
//! # Message side
//!
//! `is_message_committed(dedup_id)` delegates a read to a
//! [`DeduplicationStore`]: row present ⇔ committed.
//! [`CommitOracle::record_message`] writes that row via
//! [`DeduplicationStore::insert`] — the boundary's marker-flush step,
//! strictly after the stage, so a present row always certifies a durable
//! stage.
//!
//! # Timer side
//!
//! `is_timer_committed(key, type, time, wal_tag)` compares the caller-supplied
//! tag against the current tag in storage — see its doc comment for the
//! three-state decision.
//!
//! The timer side is read-only here: a trigger's tag row is written by the
//! timer manager's own commit machinery, never through this type. See
//! [`StoreTagSource`] for the production timer-tag source.
//!
//! **Encapsulation note**: the only write exposed is the message marker via
//! the [`CommitOracle`] trait; no accessor returns the inner store or
//! manager.

use crate::Key;
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::oracle::CommitOracle;
use crate::state::{CommitDecision, EventRef, StateKey, TimerEventRef};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::TriggerStore;
use std::error::Error;
use std::fmt;
use std::future::Future;
use thiserror::Error;
use uuid::Uuid;

/// Source of the current tag for a timer row — the timer half of the
/// commit oracle.
///
/// The implementation is [`StoreTagSource`]: a bare [`TriggerStore`] read
/// over the handle [`StateBackendFactory::for_partition`] passes down (the
/// one-identity-one-value invariant lives there). No scheduler-aware
/// source is needed — the oracle is only ever consulted for events that
/// have fully completed, and per-key serialization guarantees their
/// durability markers landed before recovery runs.
///
/// [`StateBackendFactory::for_partition`]: crate::state::StateBackendFactory::for_partition
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

/// [`TimerTagSource`] over a bare [`TriggerStore`].
///
/// A newtype rather than a blanket impl over every [`TriggerStore`], so a
/// store must be wrapped explicitly to serve as the oracle's tag source.
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
/// Deduplication is mandatory — it is the commit oracle for message events, so
/// the store is always present.
#[derive(Clone)]
pub struct CommitManager<D, TS> {
    dedup: D,
    timers: TS,
}

impl<D, TS> fmt::Debug for CommitManager<D, TS> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitManager").finish_non_exhaustive()
    }
}

impl<D, TS> CommitManager<D, TS>
where
    D: DeduplicationStore,
    TS: TimerTagSource,
{
    /// Creates a new commit manager.
    pub fn new(dedup: D, timers: TS) -> Self {
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
    ) -> Result<bool, CommitManagerError<D::Error, TS::Error>> {
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

    async fn record_message(&self, dedup_id: Uuid) -> Result<(), Self::Error> {
        self.dedup
            .insert(dedup_id)
            .await
            .map_err(CommitManagerError::Dedup)
    }

    async fn resolve<'a>(
        &'a self,
        state_key: &'a StateKey,
        event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        let committed = match event {
            EventRef::Message { dedup_id } => self.is_message_committed(dedup_id).await?,
            EventRef::Timer(TimerEventRef {
                timer_type,
                time,
                tag,
            }) => {
                self.is_timer_committed(&state_key.key, timer_type, time, tag)
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
    DE: Error + 'static,
    TE: Error + 'static,
{
    /// Deduplication store read failed.
    #[error("deduplication store error")]
    Dedup(#[source] DE),
    /// Timer tag read failed.
    #[error("timer store error")]
    Timer(#[source] TE),
}

impl<DE, TE> ClassifyError for CommitManagerError<DE, TE>
where
    DE: ClassifyError + Error + Send + Sync + 'static,
    TE: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        // Delegate to the underlying store's own classification. Most oracle
        // read failures are transient storage errors and retry, but a store
        // that reports a `Permanent` error (e.g. an unparseable row) is taken
        // at its word rather than masked as transient.
        match self {
            Self::Dedup(e) => e.classify_error(),
            Self::Timer(e) => e.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests;
