//! Asynchronous in-memory scheduler for timer triggers.
//!
//! Provides [`TriggerScheduler`] which manages scheduling and expiration of
//! [`Trigger`] events. Accepts scheduling commands, maintains active triggers,
//! and emits expired triggers on a receiver channel.
//!
//! The scheduler uses non-blocking operations with oneshot channels for
//! completion signaling. Trigger expiration and command processing run
//! on a spawned Tokio task.

use crate::Key;
use crate::error::{ClassifyError, ErrorCategory};
use crate::heartbeat::{Heartbeat, HeartbeatRegistry};
use crate::timers::active::ActiveTriggers;
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::queue::TriggerQueue;
use crate::timers::{TimerType, Trigger};
use futures::TryFutureExt;
use std::fmt::Debug;
use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::{select, spawn};

/// Size of the internal command and trigger channels.
const BUFFER_SIZE: usize = 64;

/// Asynchronous scheduler for timer [`Trigger`]s.
///
/// Accepts scheduling commands and emits expired triggers on a receiver.
/// Tracks active triggers for membership queries.
#[derive(Clone, Debug)]
pub struct TriggerScheduler {
    /// Transmits scheduling commands to the processor task.
    command_tx: mpsc::Sender<Command>,

    /// Set of currently active triggers for membership checks.
    active_triggers: ActiveTriggers,
}

/// Internal command to add or remove a trigger in the queue.
#[derive(Debug)]
struct Command {
    /// Channel to signal completion of the command.
    result_tx: oneshot::Sender<()>,

    /// The trigger involved in this operation.
    trigger: Trigger,

    /// Whether to add or remove the trigger.
    operation: CommandOperation,
}

/// Enumeration of scheduling operations.
#[derive(Copy, Clone, Debug)]
enum CommandOperation {
    /// Insert a trigger into the scheduler.
    Add,

    /// Remove a trigger from the scheduler.
    Remove,

    /// Add a trigger to the `DelayQueue` only (for rescheduling while firing).
    ///
    /// Unlike `Add`, this does not modify `ActiveTriggers` state since
    /// the caller has already transitioned to `FiringRescheduled`.
    AddToQueue,

    /// Remove a trigger from the `DelayQueue` only (for canceling a
    /// reschedule).
    ///
    /// Unlike `Remove`, this does not modify `ActiveTriggers` state since
    /// the caller handles state transitions separately.
    RemoveFromQueue,
}

impl TriggerScheduler {
    /// Creates a new scheduler and returns a receiver for expired triggers.
    ///
    /// # Arguments
    ///
    /// * `heartbeats` - Registry for registering scheduler heartbeat monitoring
    ///
    /// # Returns
    ///
    /// A tuple of:
    /// - [`mpsc::Receiver<Trigger>`]: yields triggers when their scheduled time
    ///   arrives.
    /// - [`TriggerScheduler`]: handle for scheduling or unscheduling triggers.
    pub fn new(heartbeats: &HeartbeatRegistry) -> (mpsc::Receiver<Trigger>, Self) {
        let (command_tx, commands_rx) = mpsc::channel(BUFFER_SIZE);
        let (triggers_tx, triggers_rx) = mpsc::channel(BUFFER_SIZE);
        let triggers = TriggerQueue::new();
        let active_triggers = triggers.active_triggers().clone();

        // Spawn the background task that processes scheduling commands
        // and emits expired triggers.
        spawn(process_commands(
            commands_rx,
            triggers_tx,
            triggers,
            heartbeats.register("timer scheduler"),
        ));

        (
            triggers_rx,
            Self {
                command_tx,
                active_triggers,
            },
        )
    }

    /// Schedule a new [`Trigger`] for future emission.
    ///
    /// The trigger is stored in the internal delay queue and emitted on the
    /// returned receiver when its scheduled time arrives.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The [`Trigger`] to schedule.
    ///
    /// # Errors
    ///
    /// Returns [`TimerSchedulerError::Shutdown`] if the scheduler task has
    /// shut down or the command channel is closed.
    pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerSchedulerError> {
        let (result_tx, result_rx) = oneshot::channel();
        let operation = CommandOperation::Add;

        self.command_tx
            .send(Command {
                result_tx,
                trigger,
                operation,
            })
            .map_err(|_| TimerSchedulerError::Shutdown)
            .await?;

        // Await confirmation that the command was processed.
        result_rx.await.map_err(|_| TimerSchedulerError::Shutdown)
    }

    /// Unschedule a previously scheduled [`Trigger`].
    ///
    /// If the trigger is not present or has already expired, this is a no-op.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The [`Trigger`] to remove.
    ///
    /// # Errors
    ///
    /// Returns [`TimerSchedulerError::Shutdown`] if the scheduler task has
    /// shut down or the command channel is closed.
    pub async fn unschedule(&self, trigger: Trigger) -> Result<(), TimerSchedulerError> {
        let (result_tx, result_rx) = oneshot::channel();
        let operation = CommandOperation::Remove;

        self.command_tx
            .send(Command {
                result_tx,
                trigger,
                operation,
            })
            .map_err(|_| TimerSchedulerError::Shutdown)
            .await?;

        result_rx.await.map_err(|_| TimerSchedulerError::Shutdown)
    }

    /// Returns a reference to the set of active triggers.
    pub fn active_triggers(&self) -> &ActiveTriggers {
        &self.active_triggers
    }

    /// Transitions a timer from `Scheduled` to `Firing` state.
    ///
    /// Called when a timer is delivered from the queue and about to be
    /// processed by a handler. If the timer is still in `Scheduled` state,
    /// transitions it to `Firing` and returns `true`. If the timer is not
    /// present or not in `Scheduled` state, returns `false`.
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] of the trigger.
    /// * `time` - The scheduled time of the trigger.
    /// * `timer_type` - The [`TimerType`] of the trigger.
    ///
    /// # Returns
    ///
    /// `true` if the timer was successfully transitioned to `Firing`.
    pub(crate) async fn fire(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> bool {
        use crate::timers::active::TimerState;

        // Only transition if currently in Scheduled state
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
    ///
    /// Removes the trigger from the active set. Does not cancel pending
    /// emission.
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] of the trigger.
    /// * `time` - The scheduled time of the trigger.
    /// * `timer_type` - The [`TimerType`] of the trigger.
    pub async fn deactivate(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        self.active_triggers.remove(key, time, timer_type).await;
    }

    /// Add a trigger to the `DelayQueue` without modifying `ActiveTriggers`.
    ///
    /// Used for rescheduling: the caller has already transitioned the state
    /// to `FiringRescheduled` and only needs the timer re-added to the queue.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The [`Trigger`] to add to the queue.
    ///
    /// # Errors
    ///
    /// Returns [`TimerSchedulerError::Shutdown`] if the scheduler task has
    /// shut down or the command channel is closed.
    pub async fn add_to_queue(&self, trigger: Trigger) -> Result<(), TimerSchedulerError> {
        let (result_tx, result_rx) = oneshot::channel();
        let operation = CommandOperation::AddToQueue;

        self.command_tx
            .send(Command {
                result_tx,
                trigger,
                operation,
            })
            .map_err(|_| TimerSchedulerError::Shutdown)
            .await?;

        result_rx.await.map_err(|_| TimerSchedulerError::Shutdown)
    }

    /// Remove a trigger from the `DelayQueue` without modifying
    /// `ActiveTriggers`.
    ///
    /// Used for canceling a reschedule: the caller transitions the state
    /// from `FiringRescheduled` back to `Firing` and needs the timer removed
    /// from the queue.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The [`Trigger`] to remove from the queue.
    ///
    /// # Errors
    ///
    /// Returns [`TimerSchedulerError::Shutdown`] if the scheduler task has
    /// shut down or the command channel is closed.
    pub async fn remove_from_queue(&self, trigger: Trigger) -> Result<(), TimerSchedulerError> {
        let (result_tx, result_rx) = oneshot::channel();
        let operation = CommandOperation::RemoveFromQueue;

        self.command_tx
            .send(Command {
                result_tx,
                trigger,
                operation,
            })
            .map_err(|_| TimerSchedulerError::Shutdown)
            .await?;

        result_rx.await.map_err(|_| TimerSchedulerError::Shutdown)
    }
}

/// Background task that processes scheduling commands and emits expired
/// triggers.
///
/// Concurrently receives [`Command`] messages to add or remove triggers and
/// waits for expired triggers to send them. If the receiver is full, retains
/// the trigger and retries sending. The `trigger_to_send` buffer ensures no
/// expired triggers are dropped when the outgoing channel is temporarily full.
///
/// # Arguments
///
/// * `heartbeat` - Heartbeat monitor for detecting stalled command processing.
async fn process_commands(
    mut commands: mpsc::Receiver<Command>,
    trigger_tx: mpsc::Sender<Trigger>,
    mut triggers: TriggerQueue,
    heartbeat: Heartbeat,
) {
    // Buffer for an expired trigger that could not be sent immediately.
    let mut trigger_to_send: Option<Trigger> = None;

    loop {
        // Signal that the command processor is active
        heartbeat.beat();

        if let Some(trigger) = &trigger_to_send {
            select! {
                result = commands.recv() => {
                    let Some(command) = result else {
                        break;
                    };
                    process_command(&mut triggers, command).await;
                }

                result = trigger_tx.send(trigger.clone()) => {
                    trigger_to_send = None;
                    if result.is_err() {
                        break;
                    }
                }

                () = heartbeat.next() => {},
            }
        } else {
            select! {
                result = commands.recv() => {
                    let Some(command) = result else {
                        break;
                    };
                    process_command(&mut triggers, command).await;
                }

                Some(trigger) = triggers.next() => {
                    if let Err(error) = trigger_tx.try_send(trigger) {
                        match error {
                            TrySendError::Full(trigger) => trigger_to_send = Some(trigger),
                            TrySendError::Closed(_) => break
                        }
                    }
                }

                () = heartbeat.next() => {},
            }
        }
    }
}

/// Execute a scheduling command against the [`TriggerQueue`].
///
/// Insertion and removal update both the delay queue and the active set.
/// Queue-only operations modify only the delay queue.
/// Signals the caller via `result_tx` afterward.
async fn process_command(
    triggers: &mut TriggerQueue,
    Command {
        result_tx,
        trigger,
        operation,
    }: Command,
) {
    match operation {
        CommandOperation::Add => triggers.insert(trigger.clone()).await,
        CommandOperation::Remove => triggers.remove(&trigger).await,
        CommandOperation::AddToQueue => triggers.insert_queue_only(trigger),
        CommandOperation::RemoveFromQueue => triggers.remove_queue_only(&trigger),
    }

    // Ignore send errors: caller will observe a shutdown if necessary.
    let _ = result_tx.send(());
}

/// Errors returned by [`TriggerScheduler`] methods.
#[derive(Debug, Error)]
pub enum TimerSchedulerError {
    /// A datetime conversion error occurred when scheduling a trigger.
    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),

    /// The scheduler has been shut down and cannot accept commands.
    #[error("Timer has been shutdown")]
    Shutdown,
}

impl ClassifyError for TimerSchedulerError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // DateTime conversion error. Delegate to nested error's classification.
            Self::DateTime(e) => e.classify_error(),

            // Scheduler has been shut down. System is shutting down or partition is being
            // rebalanced. Transient allows retry after rebalance completes or on different
            // partition.
            Self::Shutdown => ErrorCategory::Transient,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Key;
    use crate::timers::datetime::CompactDateTime;
    use crate::timers::duration::CompactDuration;
    use crate::timers::{TimerType, Trigger};
    use crate::tracing::init_test_logging;
    use tokio::time::{Duration, advance, pause, sleep};
    use tracing::Span;

    #[tokio::test]
    async fn test_schedule_and_unschedule() -> Result<(), String> {
        init_test_logging();
        pause();

        let (mut trigger_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());

        let key = Key::from("test-key");
        let time = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(5)))
            .map_err(|e| format!("Failed to calculate future time: {e:?}"))?;
        let trigger = Trigger::new(key.clone(), time, TimerType::Application, Span::current());

        // Schedule the trigger
        scheduler
            .schedule(trigger.clone())
            .await
            .map_err(|e| format!("Failed to schedule trigger: {e:?}"))?;

        // Verify the trigger is active
        if !scheduler
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
        {
            return Err("Trigger is not active after scheduling".to_owned());
        }

        // Unschedule the trigger
        scheduler
            .unschedule(trigger.clone())
            .await
            .map_err(|e| format!("Failed to unschedule trigger: {e:?}"))?;

        // Verify the trigger is no longer active
        if scheduler
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
        {
            return Err("Trigger is still active after unscheduling".to_owned());
        }

        // Ensure no triggers are emitted
        advance(Duration::from_secs(5)).await;
        if trigger_rx.try_recv().is_ok() {
            return Err("Unexpected trigger emitted after unscheduling".to_owned());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_trigger_emission() -> Result<(), String> {
        init_test_logging();
        pause();

        let (mut trigger_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());

        let key = Key::from("test-key");
        let time = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(2)))
            .map_err(|e| format!("Failed to calculate future time: {e:?}"))?;
        let trigger = Trigger::new(key.clone(), time, TimerType::Application, Span::current());

        // Schedule the trigger
        scheduler
            .schedule(trigger.clone())
            .await
            .map_err(|e| format!("Failed to schedule trigger: {e:?}"))?;

        // Advance time to trigger expiration
        advance(Duration::from_secs(2)).await;

        // Allow the scheduler to process the trigger
        sleep(Duration::from_millis(10)).await;

        // Verify the trigger is emitted
        let emitted_trigger = trigger_rx
            .try_recv()
            .map_err(|_| "Expected trigger to be emitted".to_owned())?;
        if emitted_trigger != trigger {
            return Err("Emitted trigger does not match the scheduled trigger".to_owned());
        }

        // Verify the trigger is still active
        if !scheduler
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
        {
            return Err(format!(
                "Trigger is not active after emission. Key: {key:?}, Time: {time:?}"
            ));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_triggers() -> Result<(), String> {
        init_test_logging();
        pause();

        let (mut trigger_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());

        let key1 = Key::from("key1");
        let time1 = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(1)))
            .map_err(|e| format!("Failed to calculate future time for key1: {e:?}"))?;
        let trigger1 = Trigger::new(key1.clone(), time1, TimerType::Application, Span::current());

        let key2 = Key::from("key2");
        let time2 = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(3)))
            .map_err(|e| format!("Failed to calculate future time for key2: {e:?}"))?;
        let trigger2 = Trigger::new(key2.clone(), time2, TimerType::Application, Span::current());

        // Schedule both triggers
        scheduler
            .schedule(trigger1.clone())
            .await
            .map_err(|e| format!("Failed to schedule trigger1: {e:?}"))?;
        scheduler
            .schedule(trigger2.clone())
            .await
            .map_err(|e| format!("Failed to schedule trigger2: {e:?}"))?;

        // Advance time to trigger the first trigger
        advance(Duration::from_secs(1)).await;

        // Allow the scheduler to process the first trigger
        sleep(Duration::from_millis(10)).await;

        // Verify the first trigger is emitted
        let emitted_trigger1 = trigger_rx
            .try_recv()
            .map_err(|_| "Expected first trigger to be emitted".to_owned())?;
        if emitted_trigger1 != trigger1 {
            return Err("First emitted trigger does not match the scheduled trigger".to_owned());
        }

        // Verify the first trigger is still active
        if !scheduler
            .active_triggers()
            .contains(&key1, time1, TimerType::Application)
            .await
        {
            return Err(format!(
                "First trigger is not active after emission. Key: {key1:?}, Time: {time1:?}"
            ));
        }

        // Advance time to trigger the second trigger
        advance(Duration::from_secs(2)).await;

        // Allow the scheduler to process the second trigger
        sleep(Duration::from_millis(10)).await;

        // Verify the second trigger is emitted
        let emitted_trigger2 = trigger_rx
            .try_recv()
            .map_err(|_| "Expected second trigger to be emitted".to_owned())?;
        if emitted_trigger2 != trigger2 {
            return Err("Second emitted trigger does not match the scheduled trigger".to_owned());
        }

        // Verify the second trigger is still active
        if !scheduler
            .active_triggers()
            .contains(&key2, time2, TimerType::Application)
            .await
        {
            return Err(format!(
                "Second trigger is not active after emission. Key: {key2:?}, Time: {time2:?}"
            ));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_deactivate_trigger() -> Result<(), String> {
        init_test_logging();
        pause();

        let (mut trigger_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let key = Key::from("test-key");
        let time = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(2)))
            .map_err(|e| format!("Failed to calculate future time: {e:?}"))?;

        let trigger = Trigger::new(key.clone(), time, TimerType::Application, Span::current());

        // Schedule the trigger
        scheduler
            .schedule(trigger)
            .await
            .map_err(|e| format!("Failed to schedule trigger: {e:?}"))?;

        // Advance time to trigger expiration
        advance(Duration::from_secs(2)).await;

        // Allow the scheduler to process the trigger
        sleep(Duration::from_millis(10)).await;

        // Verify the trigger is emitted
        trigger_rx
            .try_recv()
            .map_err(|_| "Expected trigger to be emitted".to_owned())?;

        // Deactivate a trigger
        scheduler
            .deactivate(&key, time, TimerType::Application)
            .await;

        // Verify the trigger is no longer active
        if scheduler
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
        {
            return Err("Trigger is still active after deactivation".to_owned());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_times_for_single_key() -> Result<(), String> {
        init_test_logging();
        pause();

        let (mut trigger_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let key = Key::from("shared-key");

        // Schedule multiple triggers with the same key but different times
        let time1 = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(1)))
            .map_err(|e| format!("Failed to calculate future time for time1: {e:?}"))?;
        let trigger1 = Trigger::new(key.clone(), time1, TimerType::Application, Span::current());

        let time2 = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(3)))
            .map_err(|e| format!("Failed to calculate future time for time2: {e:?}"))?;
        let trigger2 = Trigger::new(key.clone(), time2, TimerType::Application, Span::current());

        let time3 = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(5)))
            .map_err(|e| format!("Failed to calculate future time for time3: {e:?}"))?;
        let trigger3 = Trigger::new(key.clone(), time3, TimerType::Application, Span::current());

        // Schedule all triggers
        scheduler
            .schedule(trigger1.clone())
            .await
            .map_err(|e| format!("Failed to schedule trigger1: {e:?}"))?;
        scheduler
            .schedule(trigger2.clone())
            .await
            .map_err(|e| format!("Failed to schedule trigger2: {e:?}"))?;
        scheduler
            .schedule(trigger3.clone())
            .await
            .map_err(|e| format!("Failed to schedule trigger3: {e:?}"))?;

        // Advance time to trigger the first trigger
        advance(Duration::from_secs(1)).await;

        // Allow the scheduler to process the first trigger
        sleep(Duration::from_millis(10)).await;

        // Verify the first trigger is emitted
        let emitted_trigger1 = trigger_rx
            .try_recv()
            .map_err(|_| "Expected first trigger to be emitted".to_owned())?;
        assert_eq!(emitted_trigger1, trigger1);

        // Verify the first trigger is still active
        assert!(
            scheduler
                .active_triggers()
                .contains(&key, time1, TimerType::Application)
                .await,
            "First trigger is not active after emission"
        );

        // Advance time to trigger the second trigger
        advance(Duration::from_secs(2)).await;

        // Allow the scheduler to process the second trigger
        sleep(Duration::from_millis(10)).await;

        // Verify the second trigger is emitted
        let emitted_trigger2 = trigger_rx
            .try_recv()
            .map_err(|_| "Expected second trigger to be emitted".to_owned())?;
        assert_eq!(emitted_trigger2, trigger2);

        // Verify the second trigger is still active
        assert!(
            scheduler
                .active_triggers()
                .contains(&key, time2, TimerType::Application)
                .await,
            "Second trigger is not active after emission"
        );

        // Advance time to trigger the third trigger
        advance(Duration::from_secs(2)).await;

        // Allow the scheduler to process the third trigger
        sleep(Duration::from_millis(10)).await;

        // Verify the third trigger is emitted
        let emitted_trigger3 = trigger_rx
            .try_recv()
            .map_err(|_| "Expected third trigger to be emitted".to_owned())?;
        assert_eq!(emitted_trigger3, trigger3);

        // Verify the third trigger is still active
        assert!(
            scheduler
                .active_triggers()
                .contains(&key, time3, TimerType::Application)
                .await,
            "Third trigger is not active after emission"
        );

        // Deactivate all triggers
        scheduler
            .deactivate(&key, time1, TimerType::Application)
            .await;
        scheduler
            .deactivate(&key, time2, TimerType::Application)
            .await;
        scheduler
            .deactivate(&key, time3, TimerType::Application)
            .await;

        // Verify all triggers are no longer active
        assert!(
            !scheduler
                .active_triggers()
                .contains(&key, time1, TimerType::Application)
                .await,
            "First trigger is still active after deactivation"
        );
        assert!(
            !scheduler
                .active_triggers()
                .contains(&key, time2, TimerType::Application)
                .await,
            "Second trigger is still active after deactivation"
        );
        assert!(
            !scheduler
                .active_triggers()
                .contains(&key, time3, TimerType::Application)
                .await,
            "Third trigger is still active after deactivation"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_backpressure_handling() -> Result<(), String> {
        init_test_logging();
        pause();

        let (mut trigger_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());

        // Schedule triggers that will create backpressure
        let fire_time = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(1)))
            .map_err(|e| format!("Failed to calculate fire time: {e:?}"))?;

        // Schedule exactly BUFFER_SIZE + 5 triggers to test backpressure clearly
        let num_triggers = 69_usize; // 64 (buffer) + 5 (to test backpressure)
        for i in 0_usize..num_triggers {
            let trigger = Trigger::new(
                Key::from(format!("trigger-{i}")),
                fire_time,
                TimerType::Application,
                Span::current(),
            );

            scheduler
                .schedule(trigger)
                .await
                .map_err(|e| format!("Failed to schedule trigger {i}: {e:?}"))?;
        }

        // Advance time to make all triggers ready (don't read from channel yet)
        advance(Duration::from_secs(1)).await;
        sleep(Duration::from_millis(10)).await;

        // At this point:
        // - 64 triggers should be in the channel (BUFFER_SIZE)
        // - 1 trigger should be buffered in trigger_to_send
        // - 4 triggers should still be waiting in the delay queue
        // - The scheduler should be in backpressure mode

        // Test that commands are still processed during backpressure
        let later_time = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(10)))
            .map_err(|e| format!("Failed to calculate later time: {e:?}"))?;

        let command_test_trigger = Trigger::new(
            Key::from("command-test"),
            later_time,
            TimerType::Application,
            Span::current(),
        );

        // This should succeed even though trigger output is backed up
        scheduler
            .schedule(command_test_trigger.clone())
            .await
            .map_err(|_| "Command processing failed during backpressure")?;

        // Verify the command was processed by checking if trigger is active
        if !scheduler
            .active_triggers()
            .contains(
                &command_test_trigger.key,
                command_test_trigger.time,
                TimerType::Application,
            )
            .await
        {
            return Err(
                "Command test trigger not active - command processing failed during backpressure"
                    .to_owned(),
            );
        }

        // Now verify backpressure: should get exactly BUFFER_SIZE triggers initially
        let mut initial_count = 0_usize;
        while trigger_rx.try_recv().is_ok() {
            initial_count += 1_usize;
        }

        if initial_count != 64_usize {
            return Err(format!(
                "Expected exactly 64 triggers in channel, got {initial_count}"
            ));
        }

        // Test unscheduling during backpressure
        scheduler
            .unschedule(command_test_trigger.clone())
            .await
            .map_err(|_| "Unschedule failed during backpressure")?;

        if scheduler
            .active_triggers()
            .contains(
                &command_test_trigger.key,
                command_test_trigger.time,
                TimerType::Application,
            )
            .await
        {
            return Err("Trigger still active after unscheduling during backpressure".to_owned());
        }

        // Give scheduler time to send the buffered trigger now that channel has space
        sleep(Duration::from_millis(10)).await;

        // Count how many additional triggers we get
        let mut additional_count = 0_usize;
        while trigger_rx.try_recv().is_ok() {
            additional_count += 1_usize;
        }

        // We should get exactly the remaining 5 triggers (69 total - 64 initial = 5)
        if additional_count != 5_usize {
            return Err(format!(
                "Expected exactly 5 additional triggers, got {additional_count}"
            ));
        }

        // This test demonstrates key backpressure behaviors:
        // 1. Backpressure limits initial triggers to BUFFER_SIZE (64)
        // 2. Commands are still processed during backpressure (schedule/unschedule
        //    work)
        // 3. When channel space becomes available, remaining triggers are processed
        //    efficiently
        // 4. The scheduler maintains correct trigger ordering and delivery

        Ok(())
    }
}
