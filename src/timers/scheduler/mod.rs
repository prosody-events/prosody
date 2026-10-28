//! Asynchronous in-memory scheduler for timer triggers.
//!
//! Provides [`TriggerScheduler`] backed by a single actor task that owns:
//!
//! - **Slab metadata** writes: when a hold names a slab the actor hasn't seen,
//!   it writes the slab clustering row before the trigger row exists. Past-time
//!   slabs (`slab.id <= last_persisted_watermark`) route through an atomic
//!   UNLOGGED BATCH that also lowers the watermark, so invariant I1 holds
//!   across crashes.
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
use crate::timers::active::{ActiveTriggers, TimerState};
use crate::timers::datetime::CompactDateTime;
use crate::timers::queue::TriggerQueue;
use crate::timers::store::{StoredSegment, TriggerStore};
use crate::timers::{TimerType, Trigger};
use std::error::Error as StdError;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::mpsc::Permit;
use tokio::sync::{mpsc, oneshot, watch};

mod actor;

#[cfg(test)]
pub(crate) mod tests;

/// Size of the internal command and trigger channels.
pub(crate) const BUFFER_SIZE: usize = 64;

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

/// Command sent from `TriggerScheduler` handles to the actor task.
///
/// Carries an `oneshot` reply so the caller can observe success, a store
/// error (only the Hold operation can produce one), or a shutdown signal via
/// the channel being closed.
#[derive(Debug)]
pub(super) struct Command<E> {
    pub(super) operation: CommandOperation,
    pub(super) trigger: Trigger,
    pub(super) result_tx: oneshot::Sender<Result<(), E>>,
}

/// Operation variants for [`Command`].
#[derive(Copy, Clone, Debug)]
pub(super) enum CommandOperation {
    /// Persist slab metadata and hold the trigger in `ActiveTriggers`.
    Hold,
    /// Release the trigger's hold; its row write never started.
    Release,
    /// Queue a trigger whose row write ended, in the given state.
    Add(TimerState),
    /// Remove a trigger from both `DelayQueue` and `ActiveTriggers`.
    Remove,
    /// Remove the coordinate only if no replacement tag stands.
    RetireCommitted,
    /// Remove the queue entry and preserve its registry state.
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
    /// `shutdown_rx >= Cancelling`. Returns the receiver end of the
    /// expired-trigger channel along with the scheduler handle.
    pub fn new<T>(
        store: T,
        segment: StoredSegment,
        heartbeats: &HeartbeatRegistry,
        shutdown_rx: watch::Receiver<ShutdownPhase>,
    ) -> (mpsc::Receiver<Trigger>, Self)
    where
        T: TriggerStore<Error = E>,
    {
        let (command_tx, commands_rx) = mpsc::channel(BUFFER_SIZE);
        let (trigger_tx, triggers_rx) = mpsc::channel(BUFFER_SIZE);
        let triggers = TriggerQueue::new();
        let active_triggers = triggers.active_triggers().clone();
        let heartbeat = heartbeats.register("timer scheduler");

        spawn(actor::run_actor(
            store,
            segment,
            triggers,
            commands_rx,
            trigger_tx,
            heartbeat,
            shutdown_rx,
        ));

        (
            triggers_rx,
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

    /// Runs `write`, which stores the row of `trigger`, and then queues the
    /// trigger in the `queued` state.
    ///
    /// Before the write, the actor registers the slab and holds the trigger.
    /// After it, the actor queues the trigger if it owns the slab. Otherwise
    /// it drops the hold, and the load loop reads the row later. Commands for
    /// a dropped or failed call are sent on drop through a reserved permit.
    pub(crate) async fn insert<X>(
        &self,
        trigger: Trigger,
        queued: TimerState,
        write: impl Future<Output = Result<(), X>>,
    ) -> Result<(), X>
    where
        X: From<TimerSchedulerError<E>>,
    {
        let mut permits = self
            .command_tx
            .reserve_many(2)
            .await
            .map_err(|_| TimerSchedulerError::Shutdown)?;
        let mut settle = Settle {
            permit: permits.next(),
            trigger: trigger.clone(),
            on_drop: CommandOperation::Release,
        };
        let hold = permits
            .next()
            .map(|permit| command(permit, CommandOperation::Hold, trigger));
        reply(hold).await?;

        // The row may exist once the write starts.
        settle.on_drop = CommandOperation::Add(TimerState::Unconfirmed);
        write.await?;
        Ok(reply(settle.send(CommandOperation::Add(queued))).await?)
    }

    /// Unschedule a previously scheduled [`Trigger`].
    pub async fn unschedule(&self, trigger: Trigger) -> Result<(), TimerSchedulerError<E>> {
        self.send_command(CommandOperation::Remove, trigger).await
    }

    /// Removes a committed attempt and preserves a replacement tag.
    pub(crate) async fn retire_committed(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerSchedulerError<E>> {
        self.send_command(CommandOperation::RetireCommitted, trigger)
            .await
    }

    /// Remove a trigger from the `DelayQueue` without modifying
    /// `ActiveTriggers`.
    pub(crate) async fn remove_from_queue(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerSchedulerError<E>> {
        self.send_command(CommandOperation::RemoveFromQueue, trigger)
            .await
    }

    /// Transitions a timer from `Scheduled` to `Firing` state.
    ///
    /// Returns `true` if the transition succeeded.
    pub(crate) async fn fire(&self, trigger: &Trigger) -> bool {
        self.active_triggers.fire(trigger).await
    }

    /// Deactivate a trigger without removing it from the persistent queue.
    pub async fn deactivate(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        self.active_triggers.remove(key, time, timer_type).await;
    }

    async fn send_command(
        &self,
        operation: CommandOperation,
        trigger: Trigger,
    ) -> Result<(), TimerSchedulerError<E>> {
        let permit = self
            .command_tx
            .reserve()
            .await
            .map_err(|_| TimerSchedulerError::Shutdown)?;
        reply(Some(command(permit, operation, trigger))).await
    }
}

/// Sends the final command of an insert. Dropping it sends `on_drop`.
struct Settle<'a, E> {
    permit: Option<Permit<'a, Command<E>>>,
    trigger: Trigger,
    on_drop: CommandOperation,
}

impl<E> Settle<'_, E> {
    fn send(&mut self, operation: CommandOperation) -> Option<oneshot::Receiver<Result<(), E>>> {
        let permit = self.permit.take()?;
        Some(command(permit, operation, self.trigger.clone()))
    }
}

impl<E> Drop for Settle<'_, E> {
    fn drop(&mut self) {
        self.send(self.on_drop);
    }
}

/// Sends a command through a reserved permit and returns its reply channel.
fn command<E>(
    permit: Permit<'_, Command<E>>,
    operation: CommandOperation,
    trigger: Trigger,
) -> oneshot::Receiver<Result<(), E>> {
    let (result_tx, result_rx) = oneshot::channel();
    permit.send(Command {
        operation,
        trigger,
        result_tx,
    });
    result_rx
}

/// Awaits a command reply. A missing permit or reply means shutdown.
async fn reply<E>(
    result_rx: Option<oneshot::Receiver<Result<(), E>>>,
) -> Result<(), TimerSchedulerError<E>>
where
    E: ClassifyError + StdError + Send + Sync + 'static,
{
    result_rx
        .ok_or(TimerSchedulerError::Shutdown)?
        .await
        .map_err(|_| TimerSchedulerError::Shutdown)?
        .map_err(TimerSchedulerError::Store)
}

/// Errors returned by [`TriggerScheduler`] methods.
#[derive(Debug, Error)]
pub enum TimerSchedulerError<E>
where
    E: ClassifyError + StdError + Send + Sync + 'static,
{
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
            // Scheduler is shutting down — partition rebalance / draining.
            Self::Shutdown => ErrorCategory::Transient,
            Self::Store(e) => e.classify_error(),
        }
    }
}
