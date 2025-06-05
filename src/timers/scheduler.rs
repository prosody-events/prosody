use crate::Key;
use crate::timers::Trigger;
use crate::timers::active::ActiveTriggers;
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::queue::Triggers;
use futures::TryFutureExt;
use std::fmt::Debug;
use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::{select, spawn};

const BUFFER_SIZE: usize = 64;

#[derive(Clone, Debug)]
pub struct TriggerScheduler {
    command_tx: mpsc::Sender<Command>,
    active_triggers: ActiveTriggers,
}

#[derive(Debug)]
struct Command {
    result_tx: oneshot::Sender<()>,
    trigger: Trigger,
    operation: CommandOperation,
}

#[derive(Copy, Clone, Debug)]
enum CommandOperation {
    Add,
    Remove,
}

impl TriggerScheduler {
    pub fn new() -> (mpsc::Receiver<Trigger>, Self) {
        let (command_tx, commands_rx) = mpsc::channel(BUFFER_SIZE);
        let (triggers_tx, triggers_rx) = mpsc::channel(BUFFER_SIZE);
        let triggers = Triggers::new();
        let active_triggers = triggers.active_triggers().clone();

        spawn(process_commands(commands_rx, triggers_tx, triggers));

        (
            triggers_rx,
            Self {
                command_tx,
                active_triggers,
            },
        )
    }

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

        result_rx.await.map_err(|_| TimerSchedulerError::Shutdown)
    }

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

    pub fn active_triggers(&self) -> &ActiveTriggers {
        &self.active_triggers
    }

    pub async fn is_active(&self, key: &Key, time: CompactDateTime) -> bool {
        self.active_triggers.contains(key, time).await
    }

    pub async fn deactivate(&self, key: &Key, time: CompactDateTime) {
        self.active_triggers.remove(key, time).await;
    }
}

async fn process_commands(
    mut commands: mpsc::Receiver<Command>,
    trigger_tx: mpsc::Sender<Trigger>,
    mut triggers: Triggers,
) {
    let mut trigger_to_send: Option<Trigger> = None;

    loop {
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
            }
        }
    }
}

async fn process_command(
    triggers: &mut Triggers,
    Command {
        result_tx,
        trigger,
        operation,
    }: Command,
) {
    match operation {
        CommandOperation::Add => triggers.insert(trigger.clone()).await,
        CommandOperation::Remove => triggers.remove(&trigger).await,
    }

    let _ = result_tx.send(());
}

#[derive(Debug, Error)]
pub enum TimerSchedulerError {
    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),

    #[error("Timer has been shutdown")]
    Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Key;
    use crate::timers::datetime::CompactDateTime;
    use crate::timers::duration::CompactDuration;
    use tokio::time::{Duration, advance, pause, sleep};
    use tracing::Span;

    #[tokio::test]
    async fn test_schedule_and_unschedule() -> Result<(), String> {
        pause();

        let (mut trigger_rx, scheduler) = TriggerScheduler::new();

        let key = Key::from("test-key");
        let time = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(5)))
            .map_err(|e| format!("Failed to calculate future time: {e:?}"))?;
        let trigger = Trigger {
            key: key.clone(),
            time,
            span: Span::current(),
        };

        // Schedule the trigger
        scheduler
            .schedule(trigger.clone())
            .await
            .map_err(|e| format!("Failed to schedule trigger: {e:?}"))?;

        // Verify the trigger is active
        if !scheduler.is_active(&key, time).await {
            return Err("Trigger is not active after scheduling".to_owned());
        }

        // Unschedule the trigger
        scheduler
            .unschedule(trigger.clone())
            .await
            .map_err(|e| format!("Failed to unschedule trigger: {e:?}"))?;

        // Verify the trigger is no longer active
        if scheduler.is_active(&key, time).await {
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
        pause();

        let (mut trigger_rx, scheduler) = TriggerScheduler::new();

        let key = Key::from("test-key");
        let time = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(2)))
            .map_err(|e| format!("Failed to calculate future time: {e:?}"))?;
        let trigger = Trigger {
            key: key.clone(),
            time,
            span: Span::current(),
        };

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
        if !scheduler.is_active(&key, time).await {
            return Err(format!(
                "Trigger is not active after emission. Key: {key:?}, Time: {time:?}"
            ));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_triggers() -> Result<(), String> {
        pause();

        let (mut trigger_rx, scheduler) = TriggerScheduler::new();

        let key1 = Key::from("key1");
        let time1 = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(1)))
            .map_err(|e| format!("Failed to calculate future time for key1: {e:?}"))?;
        let trigger1 = Trigger {
            key: key1.clone(),
            time: time1,
            span: Span::current(),
        };

        let key2 = Key::from("key2");
        let time2 = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(3)))
            .map_err(|e| format!("Failed to calculate future time for key2: {e:?}"))?;
        let trigger2 = Trigger {
            key: key2.clone(),
            time: time2,
            span: Span::current(),
        };

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
        if !scheduler.is_active(&key1, time1).await {
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
        if !scheduler.is_active(&key2, time2).await {
            return Err(format!(
                "Second trigger is not active after emission. Key: {key2:?}, Time: {time2:?}"
            ));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_deactivate_trigger() -> Result<(), String> {
        pause();

        let (mut trigger_rx, scheduler) = TriggerScheduler::new();
        let key = Key::from("test-key");
        let time = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(2)))
            .map_err(|e| format!("Failed to calculate future time: {e:?}"))?;

        let trigger = Trigger {
            key: key.clone(),
            time,
            span: Span::current(),
        };

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
        scheduler.deactivate(&key, time).await;

        // Verify the trigger is no longer active
        if scheduler.is_active(&key, time).await {
            return Err("Trigger is still active after deactivation".to_owned());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_times_for_single_key() -> Result<(), String> {
        pause();

        let (mut trigger_rx, scheduler) = TriggerScheduler::new();
        let key = Key::from("shared-key");

        // Schedule multiple triggers with the same key but different times
        let time1 = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(1)))
            .map_err(|e| format!("Failed to calculate future time for time1: {e:?}"))?;
        let trigger1 = Trigger {
            key: key.clone(),
            time: time1,
            span: Span::current(),
        };

        let time2 = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(3)))
            .map_err(|e| format!("Failed to calculate future time for time2: {e:?}"))?;
        let trigger2 = Trigger {
            key: key.clone(),
            time: time2,
            span: Span::current(),
        };

        let time3 = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(5)))
            .map_err(|e| format!("Failed to calculate future time for time3: {e:?}"))?;
        let trigger3 = Trigger {
            key: key.clone(),
            time: time3,
            span: Span::current(),
        };

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
            scheduler.is_active(&key, time1).await,
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
            scheduler.is_active(&key, time2).await,
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
            scheduler.is_active(&key, time3).await,
            "Third trigger is not active after emission"
        );

        // Deactivate all triggers
        scheduler.deactivate(&key, time1).await;
        scheduler.deactivate(&key, time2).await;
        scheduler.deactivate(&key, time3).await;

        // Verify all triggers are no longer active
        assert!(
            !scheduler.is_active(&key, time1).await,
            "First trigger is still active after deactivation"
        );
        assert!(
            !scheduler.is_active(&key, time2).await,
            "Second trigger is still active after deactivation"
        );
        assert!(
            !scheduler.is_active(&key, time3).await,
            "Third trigger is still active after deactivation"
        );

        Ok(())
    }
}
