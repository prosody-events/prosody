#![allow(dead_code, clippy::unused_async)]

use crate::Key;
use ahash::{HashMap, HashMapExt, HashSet};
use chrono::{DateTime, OutOfRangeError, Utc};
use std::future::poll_fn;
use std::sync::Arc;
use thiserror::Error;
use tokio::select;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio_util::time::{DelayQueue, delay_queue};
use tracing::error;

type ActiveTriggers = scc::HashMap<Key, HashSet<DateTime<Utc>>>;

struct Timer {
    commands: mpsc::Sender<Command>,
    active_triggers: Arc<ActiveTriggers>,
}

struct Command {
    result: oneshot::Sender<Result<(), TimerError>>,
    key: Key,
    time: DateTime<Utc>,
    operation: CommandOperation,
}

enum CommandOperation {
    Add,
    Remove,
}

struct Trigger {
    key: Key,
    time: DateTime<Utc>,
    operation: TriggerOperation,
}

enum TriggerOperation {
    Schedule,
    Remove,
}

async fn process_commands(
    mut commands: mpsc::Receiver<Command>,
    triggers: mpsc::Sender<Trigger>,
    active: &ActiveTriggers,
) {
    let mut queue = DelayQueue::new();
    let mut queue_keys = HashMap::new();
    let mut maybe_trigger = None;

    loop {
        if let Some(trigger) = maybe_trigger.take() {
            select! {
                result = commands.recv() => {
                    let Some(command) = result else {
                        break;
                    };
                    process_command(&mut queue, &mut queue_keys, active, command);
                }

                result = triggers.send(trigger) => {
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
                    process_command(&mut queue, &mut queue_keys, active, command);
                }

                Some(expired) = poll_fn(|cx| queue.poll_expired(cx)) => {
                    let (key, time) = expired.into_inner();
                    active.entry_async(key.clone()).await.or_default().get_mut().insert(time);
                    if let Err(error) = triggers.try_send(Trigger {
                        key,
                        time,
                        operation: TriggerOperation::Remove,
                    }) {
                        match error {
                            TrySendError::Full(trigger) => maybe_trigger = Some(trigger),
                            TrySendError::Closed(_) => break
                        }
                    }
                }
            }
        }
    }
}

fn process_command(
    queue: &mut DelayQueue<(Key, DateTime<Utc>)>,
    queue_keys: &mut HashMap<(Key, DateTime<Utc>), delay_queue::Key>,
    active: &ActiveTriggers,
    command: Command,
) {
    match command.operation {
        CommandOperation::Add => add_time(queue, queue_keys, command),
        CommandOperation::Remove => remove_time(queue, queue_keys, command),
    }
}

fn add_time(
    queue: &mut DelayQueue<(Key, DateTime<Utc>)>,
    queue_keys: &mut HashMap<(Key, DateTime<Utc>), delay_queue::Key>,
    command: Command,
) {
    let time = command.time;
    let key = command.key;

    let duration = match time.signed_duration_since(Utc::now()).to_std() {
        Ok(duration) => duration,
        Err(error) => {
            send_result(command.result, &key, &time, Err(error.into()));
            return;
        }
    };

    let queue_key = queue.insert((key.clone(), time), duration);
    queue_keys.insert((key.clone(), time), queue_key);
    send_result(command.result, &key, &time, Ok(()));
}

fn remove_time(
    queue_keys: &mut DelayQueue<(Key, DateTime<Utc>)>,
    keys: &mut HashMap<(Key, DateTime<Utc>), delay_queue::Key>,
    command: Command,
) {
    let key = command.key;
    let time = command.time;

    let queue_key = match keys.remove(&(key.clone(), time)) {
        None => {
            send_result(command.result, &key, &time, Err(TimerError::NotFound));
            return;
        }
        Some(queue_key) => queue_key,
    };

    queue_keys.remove(&queue_key);
    send_result(command.result, &key, &time, Ok(()));
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
}
