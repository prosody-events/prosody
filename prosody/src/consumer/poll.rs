use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::sleep;
use std::time::Duration;

use ahash::HashMap;
use crossbeam_utils::CachePadded;
use internment::Intern;
use parking_lot::Mutex;
use rdkafka::{Message, TopicPartitionList};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::util::Timeout;
use thiserror::Error;
use tracing::{error, Span};

use crate::{Key, Offset, Partition, Payload, Topic};
use crate::consumer::context::Context;
use crate::consumer::message::UntrackedMessage;
use crate::consumer::MessageHandler;
use crate::consumer::partition::PartitionManager;

pub fn poll<T>(
    poll_interval: Duration,
    consumer: BaseConsumer<Context<T>>,
    watermark_version: Arc<CachePadded<AtomicUsize>>,
    managers: Arc<Mutex<HashMap<(Topic, Partition), PartitionManager>>>,
) where
    T: MessageHandler + Clone + Send + Sync + 'static,
{
    let mut last_version = watermark_version.load(Ordering::Acquire);
    let mut is_paused = false;

    loop {
        let current_version = watermark_version.load(Ordering::Acquire);
        if current_version != last_version && commit_watermarks(&consumer, &managers.lock()) {
            last_version = current_version;
        }

        if let Err(error) = pause_busy_partitions(&mut is_paused, &consumer, &managers) {
            error!("error pausing busy partitions: {error:#}; retrying");
            sleep(poll_interval);
            continue;
        }

        let Some(result) = consumer.poll(Timeout::After(poll_interval)) else {
            continue;
        };

        let message = match result {
            Ok(message) => message,
            Err(error) => {
                error!("error polling for message: {error:#}");
                continue;
            }
        };

        let topic: Topic = Intern::from(message.topic());
        let partition: Partition = message.partition();
        let offset: Offset = message.offset();

        let Some(key_data) = message.key() else {
            error!("missing key on {topic}:{partition}@{offset}; discarding message");
            continue;
        };

        let Some(payload_data) = message.payload() else {
            error!("missing payload on {topic}:{partition}@{offset}; discarding message");
            continue;
        };

        let key: Key = match std::str::from_utf8(key_data) {
            Ok(key) => key.into(),
            Err(error) => {
                error!("invalid key encoding: {error:#}; discarding message");
                continue;
            }
        };

        let payload: Payload = match serde_json::from_slice(payload_data) {
            Ok(payload) => payload,
            Err(error) => {
                error!("invalid payload on {topic}:{partition}@{offset}: {error:#}; discarding message");
                continue;
            }
        };

        let mut message = UntrackedMessage {
            topic,
            partition,
            offset,
            key,
            payload,
            span: Span::current(), // todo: extract from headers
        };

        loop {
            let Err(error) = dispatch(message, managers.lock().deref_mut()) else {
                break;
            };

            match error {
                DispatchError::PartitionNotFound(_) => {
                    error!("failed to dispatch message: {error:#}; discarding");
                    break;
                }
                DispatchError::Busy(failed) => {
                    error!("failed to dispatch message because partition is busy; retrying");
                    message = failed;
                    sleep(poll_interval);
                }
            }
        }
    }
}

fn pause_busy_partitions<T>(
    is_paused: &mut bool,
    consumer: &BaseConsumer<Context<T>>,
    managers: &Mutex<HashMap<(Topic, Partition), PartitionManager>>,
) -> Result<(), KafkaError>
where
    T: MessageHandler + Clone + Send + Sync + 'static,
{
    let managers = managers.lock();

    // short circuit when all partitions have capacity to avoid allocation
    if !*is_paused && managers.values().all(PartitionManager::has_capacity) {
        return Ok(());
    }

    let mut paused = TopicPartitionList::with_capacity(managers.len());
    let mut resumed = TopicPartitionList::with_capacity(managers.len());

    for ((topic, partition), manager) in managers.iter() {
        if manager.has_capacity() {
            resumed.add_partition(topic.as_ref(), *partition);
        } else {
            paused.add_partition(topic.as_ref(), *partition);
        }
    }

    *is_paused = paused.count() > 0;

    if *is_paused {
        consumer.pause(&paused)?;
    }

    if resumed.count() > 0 {
        consumer.resume(&resumed)?;
    }

    Ok(())
}

fn dispatch(
    message: UntrackedMessage,
    managers: &mut HashMap<(Topic, Partition), PartitionManager>,
) -> Result<(), DispatchError> {
    let Some(manager) = managers.get(&(message.topic, message.partition)) else {
        return Err(DispatchError::PartitionNotFound(message));
    };

    let Err(message) = manager.try_send(message) else {
        return Ok(());
    };

    Err(DispatchError::Busy(message))
}

fn commit_watermarks<T>(
    consumer: &BaseConsumer<Context<T>>,
    managers: &HashMap<(Topic, Partition), PartitionManager>,
) -> bool
where
    T: MessageHandler + Clone + Send + Sync + 'static,
{
    let mut success = true;
    for ((topic, partition), manager) in managers.iter() {
        let Some(watermark) = manager.watermark() else {
            continue;
        };

        if let Err(error) = consumer.store_offset(topic, *partition, watermark) {
            error!(%topic, %partition, %watermark, "failed to commit offset: #{error:#}");
            success = false;
        }
    }

    success
}

#[derive(Debug, Error)]
enum DispatchError {
    #[error("message sent to unassigned partition")]
    PartitionNotFound(UntrackedMessage),

    #[error("partition is busy")]
    Busy(UntrackedMessage),
}
