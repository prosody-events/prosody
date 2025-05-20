use crate::timers::Trigger;
use crate::timers::active::ActiveTriggers;
use crate::timers::datetime::CompactDateTimeError;
use crate::timers::triggers::Triggers;
use chrono::OutOfRangeError;
use futures::TryFutureExt;
use std::fmt::Debug;
use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::{select, spawn};
use tracing::debug;

const BUFFER_SIZE: usize = 64;

#[derive(Clone, Debug)]
pub struct TriggerScheduler {
    command_tx: mpsc::Sender<Command>,
    active_triggers: ActiveTriggers,
}

#[derive(Debug)]
struct Command {
    result_tx: oneshot::Sender<Result<(), TimerSchedulerError>>,
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
        let active_triggers = triggers.active().clone();

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

        result_rx.map_err(|_| TimerSchedulerError::Shutdown).await?
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

        result_rx.map_err(|_| TimerSchedulerError::Shutdown).await?
    }

    pub async fn is_active(&self, trigger: &Trigger) -> bool {
        self.active_triggers.contains(trigger).await
    }

    pub async fn deactivate(&self, trigger: &Trigger) {
        self.active_triggers.remove(trigger).await;
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
    let result = match operation {
        CommandOperation::Add => triggers.insert(trigger.clone()).await,
        CommandOperation::Remove => triggers.remove(&trigger).await,
    };

    if let Err(result) = result_tx.send(result) {
        debug!(?trigger, ?result, "Failed to send timer result");
    }
}

#[derive(Debug, Error)]
pub enum TimerSchedulerError {
    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),

    #[error("Time must be in the future: {0:#}")]
    PastTime(#[from] OutOfRangeError),

    #[error("Time not found")]
    NotFound,

    #[error("Timer has been shutdown")]
    Shutdown,
}
