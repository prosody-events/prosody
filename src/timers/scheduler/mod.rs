//! The timer scheduler: one actor task that owns all in-memory timer state.
//!
//! [`TriggerScheduler`] is the handle. Every read and change of timer memory is
//! a command to the actor, so the actor applies them in the order of sending.
//! The actor owns:
//!
//! - **Timer memory**: the [`Timers`](crate::timers::active::Timers) of the
//!   partition.
//! - **Slab metadata** writes: before an insert, the actor registers the slab
//!   of the timer. Past-time slabs (`slab.id <= last_persisted_watermark`)
//!   route through an atomic UNLOGGED BATCH that also lowers the watermark, so
//!   invariant I1 holds across crashes.
//! - **Slab preloading**: future slabs are loaded from the store and their
//!   triggers queued.
//! - **Slab cleanup**: completed slabs are deleted from the store; the
//!   watermark is raised so the next restart skips the tombstones.
//!
//! An operation that changes a row is a start command, a row change that the
//! caller runs, and an end command. The start returns a [`Ticket`], which sends
//! the end when it drops. A caller that stops early thus ends its operation at
//! once, before it or anyone else on the key can start another operation.

use crate::Key;
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::heartbeat::HeartbeatRegistry;
use crate::timers::active::{Change, Insert, Listed, Outcome, TimerSnapshot};
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::{StoredSegment, TriggerStore};
use crate::timers::{TimerType, Trigger, TriggerId};
use smallvec::SmallVec;
use std::error::Error as StdError;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::mpsc::{UnboundedSender, WeakUnboundedSender};
use tokio::sync::{mpsc, oneshot, watch};

mod actor;
#[cfg(test)]
pub(crate) mod tests;

/// Size of the channel of expired triggers.
const BUFFER_SIZE: usize = 64;

/// Asynchronous scheduler for timer [`Trigger`]s.
///
/// Cloning shares the same actor task. A command send never waits, so a
/// dropped call cannot leave a command half sent. The command channel has no
/// bound: a live call waits for the reply to each command, and a dropped call
/// sends at most one more command.
#[derive(Debug)]
pub struct TriggerScheduler<E> {
    command_tx: UnboundedSender<Command<E>>,
}

impl<E> Clone for TriggerScheduler<E> {
    fn clone(&self) -> Self {
        Self {
            command_tx: self.command_tx.clone(),
        }
    }
}

/// A command to the actor. A command that replies carries the reply sender.
#[derive(Debug)]
pub(super) enum Command<E> {
    /// Registers the slab of `trigger`, and starts its insert.
    Insert {
        trigger: Trigger,
        insert: Insert,
        reply: oneshot::Sender<Result<(Trigger, Ticket<E>), E>>,
    },
    /// Starts the removal of one timer.
    Unschedule {
        id: TriggerId,
        reply: oneshot::Sender<Option<Ticket<E>>>,
    },
    /// Starts the removal of every timer of a key and type.
    UnscheduleAll {
        key: Key,
        timer_type: TimerType,
        reply: oneshot::Sender<(Ticket<E>, SmallVec<[CompactDateTime; 1]>)>,
    },
    /// Ends a delivery.
    Complete {
        id: TriggerId,
        reply: oneshot::Sender<Option<Ticket<E>>>,
    },
    /// Aborts a delivery, or the delivered attempt with `tag`.
    Abort { id: TriggerId, tag: i32 },
    /// Ends an operation.
    End(Change, Outcome),
    /// Starts the delivery of an attempt that left the queue.
    Fire {
        trigger: Trigger,
        reply: oneshot::Sender<bool>,
    },
    /// Lists the times of a key that memory decides.
    List {
        key: Key,
        timer_type: TimerType,
        reply: oneshot::Sender<SmallVec<[(CompactDateTime, Listed); 2]>>,
    },
    /// Counts the timers in memory.
    Snapshot {
        now: CompactDateTime,
        reply: oneshot::Sender<TimerSnapshot>,
    },
}

/// An operation that the actor started.
///
/// It ends when it drops. Before [`Ticket::run`], the row is unchanged. During
/// it, the row change has an unknown result. After it succeeds, the row change
/// is done.
#[derive(Debug)]
pub(crate) struct Ticket<E> {
    change: Option<Change>,
    outcome: Outcome,
    commands: WeakUnboundedSender<Command<E>>,
}

impl<E> TriggerScheduler<E>
where
    E: ClassifyError + StdError + Send + Sync + 'static,
{
    /// Creates a new scheduler, spawning the actor task.
    ///
    /// The actor serves commands through `Draining` (so settling handlers can
    /// still schedule) and exits at `shutdown_rx >= Cancelling`. Returns the
    /// receiver end of the expired-trigger channel along with the handle.
    pub fn new<T>(
        store: T,
        segment: StoredSegment,
        heartbeats: &HeartbeatRegistry,
        shutdown_rx: watch::Receiver<ShutdownPhase>,
    ) -> (mpsc::Receiver<Trigger>, Self)
    where
        T: TriggerStore<Error = E>,
    {
        let (scheduler, commands_rx) = Self::channel();
        let (trigger_tx, triggers_rx) = mpsc::channel(BUFFER_SIZE);
        let heartbeat = heartbeats.register("timer scheduler");

        spawn(actor::run_actor(
            store,
            segment,
            scheduler.command_tx.downgrade(),
            commands_rx,
            trigger_tx,
            heartbeat,
            shutdown_rx,
        ));

        (triggers_rx, scheduler)
    }

    /// Creates a handle and the receiver of its commands.
    fn channel() -> (Self, mpsc::UnboundedReceiver<Command<E>>) {
        let (command_tx, commands_rx) = mpsc::unbounded_channel();
        (Self { command_tx }, commands_rx)
    }

    /// Starts an insert of `trigger`. Returns the attempt to write, which
    /// keeps the tag of an attempt that waits, and the ticket of the insert.
    pub(crate) async fn insert(
        &self,
        trigger: Trigger,
        insert: Insert,
    ) -> Result<(Trigger, Ticket<E>), TimerSchedulerError<E>> {
        self.request(|reply| Command::Insert {
            trigger,
            insert,
            reply,
        })
        .await?
        .map_err(TimerSchedulerError::Store)
    }

    /// Starts the removal of one timer. Returns `None` when a delivery of the
    /// timer runs: it keeps its row, and only its waiting attempt goes.
    pub(crate) async fn unschedule(
        &self,
        id: TriggerId,
    ) -> Result<Option<Ticket<E>>, TimerSchedulerError<E>> {
        self.request(|reply| Command::Unschedule { id, reply })
            .await
    }

    /// Starts the removal of every timer of a key and type. Also returns the
    /// times whose delivery runs: they keep their rows.
    pub(crate) async fn unschedule_all(
        &self,
        key: Key,
        timer_type: TimerType,
    ) -> Result<(Ticket<E>, SmallVec<[CompactDateTime; 1]>), TimerSchedulerError<E>> {
        self.request(|reply| Command::UnscheduleAll {
            key,
            timer_type,
            reply,
        })
        .await
    }

    /// Ends a delivery. Returns the ticket of the row delete, or `None` when
    /// an attempt waits and keeps the row.
    pub(crate) async fn complete(
        &self,
        id: TriggerId,
    ) -> Result<Option<Ticket<E>>, TimerSchedulerError<E>> {
        self.request(|reply| Command::Complete { id, reply }).await
    }

    /// Aborts a delivery, or the delivered attempt with `tag` that never
    /// fired. The row stays for recovery.
    pub(crate) fn abort(&self, id: TriggerId, tag: i32) {
        // A closed channel means the actor stopped, and its state is gone.
        let _ = self.command_tx.send(Command::Abort { id, tag });
    }

    /// Starts the delivery of `trigger` if it is still the attempt that
    /// fires next.
    pub(crate) async fn fire(&self, trigger: Trigger) -> Result<bool, TimerSchedulerError<E>> {
        self.request(|reply| Command::Fire { trigger, reply }).await
    }

    /// Lists the times of a key that memory decides.
    pub(crate) async fn list(
        &self,
        key: Key,
        timer_type: TimerType,
    ) -> Result<SmallVec<[(CompactDateTime, Listed); 2]>, TimerSchedulerError<E>> {
        self.request(|reply| Command::List {
            key,
            timer_type,
            reply,
        })
        .await
    }

    /// Counts the timers in memory.
    pub(crate) async fn snapshot(
        &self,
        now: CompactDateTime,
    ) -> Result<TimerSnapshot, TimerSchedulerError<E>> {
        self.request(|reply| Command::Snapshot { now, reply }).await
    }

    /// Sends a command and waits for its reply. A closed channel means
    /// shutdown.
    async fn request<R>(
        &self,
        command: impl FnOnce(oneshot::Sender<R>) -> Command<E>,
    ) -> Result<R, TimerSchedulerError<E>> {
        let (reply, response) = oneshot::channel();
        self.command_tx
            .send(command(reply))
            .map_err(|_| TimerSchedulerError::Shutdown)?;
        response.await.map_err(|_| TimerSchedulerError::Shutdown)
    }
}

impl<E> Ticket<E> {
    fn new(change: Change, commands: WeakUnboundedSender<Command<E>>) -> Self {
        Self {
            change: Some(change),
            outcome: Outcome::Unchanged,
            commands,
        }
    }

    /// Runs `change`, the row change of the operation, and then ends the
    /// operation.
    pub(crate) async fn run<R, X>(
        mut self,
        change: impl Future<Output = Result<R, X>>,
    ) -> Result<R, X> {
        self.outcome = Outcome::Unknown;
        let result = change.await?;
        self.outcome = Outcome::Done;
        Ok(result)
    }

    /// Takes the change back, so that the ticket sends no end.
    fn into_change(mut self) -> Option<Change> {
        self.change.take()
    }
}

impl<E> Drop for Ticket<E> {
    fn drop(&mut self) {
        if let Some(change) = self.change.take()
            && let Some(commands) = self.commands.upgrade()
        {
            // A closed channel means the actor stopped, and its state is gone.
            let _ = commands.send(Command::End(change, self.outcome));
        }
    }
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
