#![allow(dead_code, clippy::unused_async)]

use crate::Key;
use ahash::{HashMap, HashMapExt, HashSet};
use chrono::{DateTime, OutOfRangeError, Utc};
use futures::TryFutureExt;
use scc::hash_map::Entry;
use std::future::poll_fn;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::{select, spawn};
use tokio_util::time::{DelayQueue, delay_queue};
use tracing::error;

const BUFFER_SIZE: usize = 64;

type ActiveTriggers = scc::HashMap<Key, HashSet<DateTime<Utc>>>;

#[derive(Clone, Debug)]
struct Timer {
    command_tx: mpsc::Sender<Command>,
    active_triggers: Arc<ActiveTriggers>,
}

impl Timer {
    pub fn new() -> (mpsc::Receiver<Trigger>, Self) {
        let (command_tx, commands_rx) = mpsc::channel(BUFFER_SIZE);
        let (triggers_tx, triggers_rx) = mpsc::channel(BUFFER_SIZE);
        let active_triggers = Arc::new(ActiveTriggers::new());

        spawn(process_commands(
            commands_rx,
            triggers_tx,
            active_triggers.clone(),
        ));

        (
            triggers_rx,
            Self {
                command_tx,
                active_triggers,
            },
        )
    }

    pub async fn schedule(&self, key: Key, time: DateTime<Utc>) -> Result<(), TimerError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.command_tx
            .send(Command {
                result_tx,
                key,
                time,
                operation: CommandOperation::Remove,
            })
            .map_err(|_| TimerError::Shutdown)
            .await?;

        result_rx.map_err(|_| TimerError::Shutdown).await?
    }

    pub async fn unschedule(&self, key: Key, time: DateTime<Utc>) -> Result<(), TimerError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.command_tx
            .send(Command {
                result_tx,
                key,
                time,
                operation: CommandOperation::Remove,
            })
            .map_err(|_| TimerError::Shutdown)
            .await?;

        result_rx.map_err(|_| TimerError::Shutdown).await?
    }

    pub async fn active_times(&self, key: &Key) -> Option<Vec<DateTime<Utc>>> {
        let times = self.active_triggers.get_async(key).await?;
        let mut sorted_times: Vec<DateTime<Utc>> = times.iter().copied().collect();
        sorted_times.sort();
        Some(sorted_times)
    }
}

#[derive(Debug)]
struct Command {
    result_tx: oneshot::Sender<Result<(), TimerError>>,
    key: Key,
    time: DateTime<Utc>,
    operation: CommandOperation,
}

#[derive(Copy, Clone, Debug)]
enum CommandOperation {
    Add,
    Remove,
}

#[derive(Clone, Debug)]
struct Trigger {
    key: Key,
    time: DateTime<Utc>,
}

async fn process_commands(
    mut commands: mpsc::Receiver<Command>,
    triggers: mpsc::Sender<Trigger>,
    active: Arc<ActiveTriggers>,
) {
    let mut queue = DelayQueue::new();
    let mut queue_keys = HashMap::new();
    let mut trigger_to_send: Option<Trigger> = None;

    loop {
        if let Some(trigger) = &trigger_to_send {
            select! {
                result = commands.recv() => {
                    let Some(command) = result else {
                        break;
                    };
                    process_command(&mut queue, &mut queue_keys, &active, command).await;
                }

                result = triggers.send(trigger.clone()) => {
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
                    process_command(&mut queue, &mut queue_keys, &active, command).await;
                }

                Some(expired) = poll_fn(|cx| queue.poll_expired(cx)) => {
                    let (key, time) = expired.into_inner();
                    if let Err(error) = triggers.try_send(Trigger {
                        key,
                        time,
                    }) {
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
    queue: &mut DelayQueue<(Key, DateTime<Utc>)>,
    queue_keys: &mut HashMap<(Key, DateTime<Utc>), delay_queue::Key>,
    active: &ActiveTriggers,
    command: Command,
) {
    match command.operation {
        CommandOperation::Add => add_time(queue, queue_keys, active, command).await,
        CommandOperation::Remove => remove_time(queue, queue_keys, active, command).await,
    }
}

async fn add_time(
    queue: &mut DelayQueue<(Key, DateTime<Utc>)>,
    queue_keys: &mut HashMap<(Key, DateTime<Utc>), delay_queue::Key>,
    active: &ActiveTriggers,
    command: Command,
) {
    let time = command.time;
    let key = command.key;

    let duration = match time.signed_duration_since(Utc::now()).to_std() {
        Ok(duration) => duration,
        Err(error) => {
            send_result(command.result_tx, &key, &time, Err(error.into()));
            return;
        }
    };

    let queue_key = queue.insert((key.clone(), time), duration);
    queue_keys.insert((key.clone(), time), queue_key);

    active
        .entry_async(key.clone())
        .await
        .or_default()
        .get_mut()
        .insert(time);

    send_result(command.result_tx, &key, &time, Ok(()));
}

async fn remove_time(
    delay_queue: &mut DelayQueue<(Key, DateTime<Utc>)>,
    timer_handles: &mut HashMap<(Key, DateTime<Utc>), delay_queue::Key>,
    active_triggers: &ActiveTriggers,
    command: Command,
) {
    let Command {
        result_tx: result,
        key,
        time,
        ..
    } = command;

    let scheduled_entry = (key, time);
    let Some(handle) = timer_handles.remove(&scheduled_entry) else {
        send_result(
            result,
            &scheduled_entry.0,
            &scheduled_entry.1,
            Err(TimerError::NotFound),
        );

        return;
    };

    let removed = delay_queue.remove(&handle);
    let (expired_key, expired_time) = removed.into_inner();

    if let Entry::Occupied(mut occ) = active_triggers.entry_async(expired_key).await {
        let times = occ.get_mut();
        times.remove(&expired_time);
        if times.is_empty() {
            let _ = occ.remove();
        }
    }

    send_result(result, &scheduled_entry.0, &scheduled_entry.1, Ok(()));
}

fn send_result(
    channel: oneshot::Sender<Result<(), TimerError>>,
    key: &Key,
    time: &DateTime<Utc>,
    result: Result<(), TimerError>,
) {
    if let Err(result) = channel.send(result) {
        error!(%key, ?time, ?result, "Failed to send timer result");
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
