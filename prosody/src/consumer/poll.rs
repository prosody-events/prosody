//! Consumer module for polling and processing Kafka messages with distributed
//! tracing support.

use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

use internment::Intern;
use opentelemetry::propagation::TextMapPropagator;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::util::Timeout;
use rdkafka::{Message, Offset, TopicPartitionList};
use thiserror::Error;
use tracing::field::Empty;
use tracing::{error, info_span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::consumer::context::Context;
use crate::consumer::extractor::MessageExtractor;
use crate::consumer::message::UntrackedMessage;
use crate::consumer::partition::PartitionManager;
use crate::consumer::{Managers, MessageHandler, WatermarkVersion};
use crate::propagator::new_propagator;
use crate::Key;

/// Polls messages from Kafka, processes them, and handles partition management.
///
/// # Arguments
/// - `poll_interval`: Duration to wait between polling attempts.
/// - `commit_interval`: Duration to wait between offset commits.
/// - `consumer`: Kafka consumer instance.
/// - `watermark_version`: Current watermark version for offset tracking.
/// - `managers`: Manager collection for handling partitions.
/// - `shutdown`: Atomic boolean to signal shutdown.
pub fn poll<T>(
    poll_interval: Duration,
    commit_interval: Duration,
    consumer: &BaseConsumer<Context<T>>,
    watermark_version: &WatermarkVersion,
    managers: &Managers,
    shutdown: &AtomicBool,
) where
    T: MessageHandler + Clone + Send + Sync + 'static,
{
    let propagator = new_propagator();
    let mut last_version = watermark_version.load(Ordering::Acquire);
    let mut last_commit = Instant::now();
    let mut is_paused = false;

    while !shutdown.load(Ordering::Relaxed) {
        // Commit offsets periodically
        commit_watermarks(
            &commit_interval,
            consumer,
            watermark_version,
            managers,
            &mut last_version,
            &mut last_commit,
        );

        // Manage busy partitions by pausing/resuming them
        if let Err(error) = pause_busy_partitions(&mut is_paused, consumer, managers) {
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

        let topic = Intern::from(message.topic());
        let partition = message.partition();
        let offset = message.offset();

        // Extract context for tracing
        let context = propagator.extract(&MessageExtractor::new(&message));
        let span = info_span!(
            "receive-message",
            %topic, %partition, %offset, key = Empty, payload_size = Empty
        );

        span.set_parent(context);
        let _enter = span.enter();

        let Some(key_data) = message.key() else {
            error!("missing key; discarding message");
            continue;
        };

        let Some(payload_data) = message.payload() else {
            error!("missing payload; discarding message");
            continue;
        };
        span.record("payload_size", payload_data.len());

        let key: Key = match str::from_utf8(key_data) {
            Ok(key) => {
                span.record("key", key);
                key.into()
            }
            Err(error) => {
                error!("invalid key encoding: {error:#}; discarding message");
                continue;
            }
        };

        let payload = match serde_json::from_slice(payload_data) {
            Ok(payload) => payload,
            Err(error) => {
                error!("invalid payload: {error:#}; discarding message");
                continue;
            }
        };

        let mut message = UntrackedMessage {
            topic,
            partition,
            offset,
            key,
            payload,
            span: span.clone(),
        };

        // Dispatch message to appropriate handler
        loop {
            match dispatch_message(message, managers) {
                Ok(()) => break,
                Err(error @ DispatchError::PartitionNotFound(_)) => {
                    warn!("failed to dispatch message: {error:#}; discarding");
                    break;
                }
                Err(DispatchError::Busy(failed)) => {
                    error!("failed to dispatch message because partition is busy; retrying");
                    message = failed;
                    sleep(poll_interval);
                }
            }
        }
    }
}

/// Commits the current offsets for all managed partitions if necessary.
///
/// # Arguments
/// - `commit_interval`: Interval between offset commits.
/// - `consumer`: Kafka consumer instance.
/// - `watermark_version`: Current watermark version for offset tracking.
/// - `managers`: Manager collection for handling partitions.
/// - `last_version`: Last committed watermark version.
/// - `last_commit`: Time of the last commit attempt.
fn commit_watermarks<T>(
    commit_interval: &Duration,
    consumer: &BaseConsumer<Context<T>>,
    watermark_version: &WatermarkVersion,
    managers: &Managers,
    last_version: &mut usize,
    last_commit: &mut Instant,
) where
    T: MessageHandler + Clone + Send + Sync + 'static,
{
    let current_version = watermark_version.load(Ordering::Acquire);
    if current_version == *last_version {
        return;
    }

    let now = Instant::now();
    if now.duration_since(*last_commit) < *commit_interval {
        return;
    }

    let mut success = true;

    let managers = managers.lock();
    let mut list = TopicPartitionList::with_capacity(managers.len());
    for ((topic, partition), manager) in managers.iter() {
        let Some(watermark) = manager.watermark() else {
            continue;
        };

        let next_offset = Offset::Offset(watermark + 1);
        if let Err(error) = list.add_partition_offset(topic, *partition, next_offset) {
            error!(%topic, %partition, %watermark, "failed to add offset to commit list: {error:#}");
            success = false;
        }
    }

    if let Err(error) = consumer.commit(&list, CommitMode::Async) {
        error!("failed to add commit offsets: {error:#}");
        success = false;
    }

    if success {
        *last_version = current_version;
        *last_commit = now;
    }
}

/// Pauses or resumes partitions based on their capacity status.
///
/// # Arguments
/// - `is_paused`: Indicates if any partitions are currently paused.
/// - `consumer`: Kafka consumer instance.
/// - `managers`: Manager collection for handling partitions.
///
/// # Errors
/// Returns `KafkaError` if there is an error pausing or resuming partitions.
fn pause_busy_partitions<T>(
    is_paused: &mut bool,
    consumer: &BaseConsumer<Context<T>>,
    managers: &Managers,
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

/// Dispatches a message to the appropriate partition manager.
///
/// # Arguments
/// - `message`: The message to dispatch.
/// - `managers`: Manager collection for handling partitions.
///
/// # Returns
/// Returns a `DispatchError` if dispatch fails.
fn dispatch_message(message: UntrackedMessage, managers: &Managers) -> Result<(), DispatchError> {
    let managers = managers.lock();
    let Some(manager) = managers.get(&(message.topic, message.partition)) else {
        return Err(DispatchError::PartitionNotFound(message));
    };

    let Err(message) = manager.try_send(message) else {
        return Ok(());
    };

    Err(DispatchError::Busy(message))
}

/// Errors that can occur during message dispatch.
#[derive(Debug, Error)]
enum DispatchError {
    #[error("message sent to unassigned partition")]
    PartitionNotFound(UntrackedMessage),

    #[error("partition is busy")]
    Busy(UntrackedMessage),
}
