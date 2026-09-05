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
use crate::timers::queue::{Kept, TriggerQueue};
use crate::timers::store::{Segment, TriggerStore};
use crate::timers::{TimerType, Trigger};
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

/// One request to the actor. A reply sender closes on shutdown.
#[derive(Debug)]
pub(super) enum Command<E> {
    /// Persist slab metadata, then queue and register the trigger.
    Add {
        trigger: Trigger,
        reply: oneshot::Sender<Result<(), E>>,
    },
    /// Queue a trigger whose registry entry the caller already set.
    AddToQueue {
        trigger: Trigger,
        reply: oneshot::Sender<()>,
    },
    /// Remove the queue entry only and reply with it.
    RemoveFromQueue {
        trigger: Trigger,
        reply: oneshot::Sender<Option<Trigger>>,
    },
    /// Deliver a queued trigger for a manual test dispatch.
    #[cfg(test)]
    TakeFromQueue {
        trigger: Trigger,
        reply: oneshot::Sender<Option<Trigger>>,
    },
    /// Cancel a live schedule. The reply says whether the queue kept a
    /// retained source.
    Remove {
        trigger: Trigger,
        reply: oneshot::Sender<Kept>,
    },
    /// Set the item tag, then reply.
    Retag {
        trigger: Trigger,
        reply: oneshot::Sender<()>,
    },
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
        self.ask(|reply| Command::Add { trigger, reply })
            .await?
            .map_err(TimerSchedulerError::Store)
    }

    /// Cancel the live schedule whose key tag is `trigger.tag`. A queued item
    /// with another tag is a retained source and stays.
    pub(crate) async fn unschedule(
        &self,
        trigger: Trigger,
    ) -> Result<Kept, TimerSchedulerError<E>> {
        self.ask(|reply| Command::Remove { trigger, reply }).await
    }

    /// Add a trigger to the `DelayQueue` without modifying `ActiveTriggers`.
    pub(crate) async fn add_to_queue(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerSchedulerError<E>> {
        self.ask(|reply| Command::AddToQueue { trigger, reply })
            .await
    }

    /// Remove a trigger from the `DelayQueue` without modifying
    /// `ActiveTriggers`.
    pub(crate) async fn remove_from_queue(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerSchedulerError<E>> {
        self.ask(|reply| Command::RemoveFromQueue { trigger, reply })
            .await
            .map(|_| ())
    }

    /// Returns after the actor sets the item tag at its current location.
    pub(crate) async fn retag(&self, trigger: Trigger) -> Result<(), TimerSchedulerError<E>> {
        self.ask(|reply| Command::Retag { trigger, reply }).await
    }

    /// Remove a queued trigger for a manual test dispatch.
    #[cfg(test)]
    pub(crate) async fn take_from_queue(
        &self,
        trigger: Trigger,
    ) -> Result<Option<Trigger>, TimerSchedulerError<E>> {
        self.ask(|reply| Command::TakeFromQueue { trigger, reply })
            .await
    }

    /// Starts the delivered item's attempt and returns its current tag.
    pub(crate) async fn fire(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<i32> {
        self.active_triggers.fire(key, time, timer_type).await
    }

    /// Deactivate a trigger without removing it from the persistent queue.
    pub async fn deactivate(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        self.active_triggers.remove(key, time, timer_type).await;
    }

    /// Send one command and wait for its reply. A closed channel means the
    /// actor stopped.
    async fn ask<R>(
        &self,
        command: impl FnOnce(oneshot::Sender<R>) -> Command<E>,
    ) -> Result<R, TimerSchedulerError<E>> {
        let (reply, response) = oneshot::channel();
        self.command_tx
            .send(command(reply))
            .await
            .map_err(|_| TimerSchedulerError::Shutdown)?;
        response.await.map_err(|_| TimerSchedulerError::Shutdown)
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
