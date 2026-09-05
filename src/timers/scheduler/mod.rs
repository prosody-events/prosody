//! Asynchronous in-memory scheduler for timer triggers.
//!
//! Provides [`TriggerScheduler`] backed by a single actor task that owns:
//!
//! - **Slab metadata** writes: when a command schedules a trigger for a slab
//!   the actor hasn't seen, it writes the slab clustering row to persistent
//!   storage. Past-time slabs (`slab.id <= last_persisted_watermark`) route
//!   through an atomic UNLOGGED BATCH that also lowers the watermark, so
//!   invariant I1 holds across crashes.
//! - **Slab preloading**: future slabs are loaded from the store and their
//!   triggers inserted into the in-memory queue.
//! - **Slab cleanup**: completed slabs are deleted from the store; the
//!   watermark is raised opportunistically so the next restart skips the
//!   tombstones.
//!
//! Load and cleanup process one slab per `select!` arm invocation, so
//! Cassandra round-trips yield back to command processing and trigger
//! emission between slabs.

use crate::Key;
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::heartbeat::HeartbeatRegistry;
use crate::timers::active::ActiveTriggers;
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::queue::{RemoveOutcome, TriggerQueue};
use crate::timers::store::{Segment, TriggerStore};
use crate::timers::{TimerType, Trigger};
use futures::TryFutureExt;
use std::error::Error as StdError;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::{mpsc, oneshot, watch};

mod actor;

#[cfg(test)]
mod tests;

/// Size of the internal command and trigger channels.
pub(super) const BUFFER_SIZE: usize = 64;

/// Asynchronous scheduler for timer [`Trigger`]s.
///
/// Cloning shares the same underlying actor task and active-triggers
/// registry.
#[derive(Debug)]
pub struct TriggerScheduler<E> {
    command_tx: mpsc::Sender<Command<E>>,
    active_triggers: ActiveTriggers,
}

impl<E> Clone for TriggerScheduler<E> {
    fn clone(&self) -> Self {
        Self {
            command_tx: self.command_tx.clone(),
            active_triggers: self.active_triggers.clone(),
        }
    }
}

/// Sends a store operation or a queue tag update to the actor.
#[derive(Debug)]
pub(super) enum Command<E> {
    /// Run a store operation. The reply carries the removed trigger, if any.
    Apply {
        operation: CommandOperation,
        trigger: Trigger,
        result_tx: oneshot::Sender<Result<Option<Trigger>, E>>,
    },
    /// This queue update needs no store result or reply allocation.
    Retag(Trigger),
    /// Cancel a live schedule. The reply says whether the queue kept an older
    /// source.
    Remove {
        trigger: Trigger,
        key_tag: Option<i32>,
        result_tx: oneshot::Sender<RemoveOutcome>,
    },
}

/// Operation variants for [`Command`].
#[derive(Copy, Clone, Debug)]
pub(super) enum CommandOperation {
    /// Insert a trigger: persist slab metadata + add to `TriggerQueue`/
    /// `ActiveTriggers`.
    Add,
    /// Add a trigger to the `DelayQueue` only (used when the caller has
    /// already transitioned `ActiveTriggers` to `FiringRescheduled`).
    AddToQueue,
    /// Remove a trigger from the `DelayQueue` only (cancel an earlier
    /// `AddToQueue`).
    RemoveFromQueue,
}

impl<E> TriggerScheduler<E>
where
    E: ClassifyError + StdError + Send + Sync + 'static,
{
    /// Creates a new scheduler, spawning the unified actor task.
    ///
    /// The actor owns the trigger queue, slab metadata writes, slab loads,
    /// and slab cleanup. `store` must be `Clone` so the manager can keep its
    /// own handle for trigger-row writes; the actor serves commands through
    /// `Draining` (so settling handlers can still schedule) and exits at
    /// `shutdown_rx >= Cancelling`. Returns the scheduler handle and a
    /// receiver that yields the expired-trigger channel after the first load.
    pub fn new<T>(
        store: T,
        segment: Segment,
        heartbeats: &HeartbeatRegistry,
        shutdown_rx: watch::Receiver<ShutdownPhase>,
    ) -> (oneshot::Receiver<mpsc::Receiver<Trigger>>, Self)
    where
        T: TriggerStore<Error = E>,
    {
        let (command_tx, commands_rx) = mpsc::channel(BUFFER_SIZE);
        let triggers = TriggerQueue::new();
        let active_triggers = triggers.active_triggers().clone();
        let heartbeat = heartbeats.register("timer scheduler");

        let (ready_tx, ready_rx) = oneshot::channel();
        spawn(actor::run_actor(
            store,
            segment,
            triggers,
            commands_rx,
            ready_tx,
            heartbeat,
            shutdown_rx,
        ));

        (
            ready_rx,
            Self {
                command_tx,
                active_triggers,
            },
        )
    }

    /// Returns a reference to the set of active triggers.
    pub fn active_triggers(&self) -> &ActiveTriggers {
        &self.active_triggers
    }

    /// Schedule a new [`Trigger`] for future emission.
    ///
    /// The actor persists slab metadata (if not already known) and inserts
    /// the trigger into the in-memory queue. If the slab is at or below the
    /// current watermark, the slab insert and watermark update are written
    /// in one UNLOGGED BATCH.
    pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerSchedulerError<E>> {
        self.send_command(CommandOperation::Add, trigger)
            .await
            .map(|_| ())
    }

    /// Cancel the live schedule and preserve an older queued source.
    pub(crate) async fn unschedule(
        &self,
        trigger: Trigger,
        key_tag: Option<i32>,
    ) -> Result<RemoveOutcome, TimerSchedulerError<E>> {
        let (result_tx, result_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Remove {
                trigger,
                key_tag,
                result_tx,
            })
            .await
            .map_err(|_| TimerSchedulerError::Shutdown)?;
        result_rx.await.map_err(|_| TimerSchedulerError::Shutdown)
    }

    /// Add a trigger to the `DelayQueue` without modifying `ActiveTriggers`.
    pub(crate) async fn add_to_queue(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerSchedulerError<E>> {
        self.send_command(CommandOperation::AddToQueue, trigger)
            .await
            .map(|_| ())
    }

    /// Remove a trigger from the `DelayQueue` without modifying
    /// `ActiveTriggers`.
    pub(crate) async fn remove_from_queue(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerSchedulerError<E>> {
        self.send_command(CommandOperation::RemoveFromQueue, trigger)
            .await
            .map(|_| ())
    }

    /// Set the queued tag after the store rotates it.
    pub(crate) async fn retag(&self, trigger: Trigger) -> Result<(), TimerSchedulerError<E>> {
        self.command_tx
            .send(Command::Retag(trigger))
            .await
            .map_err(|_| TimerSchedulerError::Shutdown)
    }

    /// Remove a queued trigger for a manual test dispatch.
    #[cfg(test)]
    pub(crate) async fn take_from_queue(
        &self,
        trigger: Trigger,
    ) -> Result<Option<Trigger>, TimerSchedulerError<E>> {
        self.send_command(CommandOperation::RemoveFromQueue, trigger)
            .await
    }

    /// Transitions a timer from `Scheduled` to `Firing` state.
    ///
    /// Returns `true` if the transition succeeded.
    pub(crate) async fn fire(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> bool {
        use crate::timers::active::TimerState;

        if let Some(TimerState::Scheduled) =
            self.active_triggers.get_state(key, time, timer_type).await
        {
            self.active_triggers
                .set_state(key, time, timer_type, TimerState::Firing)
                .await
        } else {
            false
        }
    }

    /// Deactivate a trigger without removing it from the persistent queue.
    pub async fn deactivate(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        self.active_triggers.remove(key, time, timer_type).await;
    }

    async fn send_command(
        &self,
        operation: CommandOperation,
        trigger: Trigger,
    ) -> Result<Option<Trigger>, TimerSchedulerError<E>> {
        let (result_tx, result_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Apply {
                operation,
                trigger,
                result_tx,
            })
            .map_err(|_| TimerSchedulerError::Shutdown)
            .await?;
        result_rx
            .await
            .map_err(|_| TimerSchedulerError::Shutdown)?
            .map_err(TimerSchedulerError::Store)
    }
}

/// Errors returned by [`TriggerScheduler`] methods.
#[derive(Debug, Error)]
pub enum TimerSchedulerError<E>
where
    E: ClassifyError + StdError + Send + Sync + 'static,
{
    /// A datetime conversion error occurred when scheduling a trigger.
    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),

    /// The scheduler has been shut down and cannot accept commands.
    #[error("Timer has been shutdown")]
    Shutdown,

    /// A store-layer write performed by the actor failed.
    #[error("Timer store error: {0:#}")]
    Store(E),
}

impl<E> ClassifyError for TimerSchedulerError<E>
where
    E: ClassifyError + StdError + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::DateTime(e) => e.classify_error(),
            // Scheduler is shutting down — partition rebalance / draining.
            Self::Shutdown => ErrorCategory::Transient,
            Self::Store(e) => e.classify_error(),
        }
    }
}
