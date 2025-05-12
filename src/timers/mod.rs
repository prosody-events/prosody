#![allow(dead_code, clippy::unused_async)]

use crate::Key;
use crate::timers::active::ActiveTriggers;
use crate::timers::triggers::Triggers;
use chrono::{DateTime, OutOfRangeError, Utc};
use futures::TryFutureExt;
use std::collections::BTreeSet;
use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::{select, spawn};
use tracing::{debug, error};

mod active;
mod triggers;

const BUFFER_SIZE: usize = 64;

#[derive(Clone, Debug)]
pub struct TimerManager {
    loading_rx: watch::Receiver<bool>,
    command_tx: mpsc::Sender<Command>,
    active_triggers: ActiveTriggers,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Trigger {
    key: Key,
    time: DateTime<Utc>,
}

#[derive(Debug)]
struct Command {
    result_tx: oneshot::Sender<Result<(), TimerError>>,
    trigger: Trigger,
    operation: CommandOperation,
}

#[derive(Copy, Clone, Debug)]
enum CommandOperation {
    Add,
    Remove,
}

impl TimerManager {
    pub fn new() -> (mpsc::Receiver<Trigger>, Self) {
        let (loading_tx, loading_rx) = watch::channel(true);
        let (command_tx, commands_rx) = mpsc::channel(BUFFER_SIZE);
        let (triggers_tx, triggers_rx) = mpsc::channel(BUFFER_SIZE);
        let triggers = Triggers::new();
        let active_triggers = triggers.active().clone();

        spawn(process_commands(commands_rx, triggers_tx, triggers));

        // todo: spawn timer load process

        (
            triggers_rx,
            Self {
                loading_rx,
                command_tx,
                active_triggers,
            },
        )
    }

    pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.command_tx
            .send(Command {
                result_tx,
                trigger,
                operation: CommandOperation::Remove,
            })
            .map_err(|_| TimerError::Shutdown)
            .await?;

        result_rx.map_err(|_| TimerError::Shutdown).await?
    }

    pub async fn unschedule(&self, trigger: Trigger) -> Result<(), TimerError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.ensure_loaded().await;
        self.command_tx
            .send(Command {
                result_tx,
                trigger,
                operation: CommandOperation::Remove,
            })
            .map_err(|_| TimerError::Shutdown)
            .await?;

        result_rx.map_err(|_| TimerError::Shutdown).await?
    }

    pub async fn active_times(&self, key: &Key) -> BTreeSet<DateTime<Utc>> {
        self.ensure_loaded().await;
        self.active_triggers.key_times(key).await
    }

    pub async fn is_active(&self, trigger: &Trigger) -> bool {
        self.ensure_loaded().await;
        self.active_triggers.contains(trigger).await
    }

    async fn ensure_loaded(&self) {
        if *self.loading_rx.borrow() {
            let _ = self
                .loading_rx
                .clone()
                .wait_for(|is_loading| !is_loading)
                .await;
        }
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
                    trigger_to_send.take();
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
        CommandOperation::Add => {
            let result = triggers.insert(trigger.clone()).await;
            if let Err(result) = result_tx.send(result) {
                debug!(?trigger, ?result, "Failed to send timer add result");
            }
        }
        CommandOperation::Remove => {
            let result = triggers.remove(&trigger).await;
            if let Err(result) = result_tx.send(result) {
                debug!(?trigger, ?result, "Failed to send timer remove result");
            }
        }
    }
}

#[derive(Clone, Debug, Error)]
pub enum TimerError {
    #[error("Time must be in the future")]
    PastTime(#[from] OutOfRangeError),

    #[error("Time not found")]
    NotFound,

    #[error("Timer has been shutdown")]
    Shutdown,
}
