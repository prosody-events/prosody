//! # Kafka Polling and Message Dispatch Module
//!
//! This module implements the main polling loop that consumes messages from
//! Kafka. It provides distributed tracing support, manages offset commits,
//! controls Kafka partition pausing/resuming based on capacity, and dispatches
//! messages to the correct partition manager. It encapsulates the configuration
//! for polling, message extraction, validation, retrying dispatches if
//! partitions are busy, and committing watermarks.
//!
//! All functionality is scaffolded through the [`PollConfig`] type and a set of
//! functions that operate on Kafka messages.

use aho_corasick::{AhoCorasick, Anchored, Input};
use chrono::{MappedLocalTime, TimeZone, Utc};
use internment::Intern;
use opentelemetry::propagation::TextMapPropagator;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::util::Timeout;
use rdkafka::{Message, Offset, Timestamp, TopicPartitionList};
use serde_json::Value;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::field::Empty;
use tracing::{debug, error, info_span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::consumer::context::Context;
use crate::consumer::extractor::MessageExtractor;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::partition::PartitionManager;
use crate::consumer::{HandlerProvider, Managers, WatermarkVersion};
use crate::propagator::new_propagator;
use crate::{SOURCE_SYSTEM_HEADER, Topic};

#[cfg(not(target_arch = "arm"))]
use simd_json::Buffers;
#[cfg(not(target_arch = "arm"))]
use simd_json::serde::from_reader_with_buffers;

/// Bundles all parameters necessary for polling Kafka messages.
///
/// # Type Parameters
///
/// * `T`: The type that implements [`HandlerProvider`] to supply
///   partition-specific message handlers.
///
/// # Fields
///
/// * `poll_interval`: Duration between consecutive poll attempts.
/// * `commit_interval`: Duration between offset commit attempts.
/// * `group_id`: Identifier for the Kafka consumer group.
/// * `allowed_events`: Optional filter that permits only allowed event types.
/// * `consumer`: The Kafka consumer instance with the appropriate context.
/// * `watermark_version`: Atomic watermark version for tracking offset updates.
/// * `managers`: Shared partition managers handling messages per partition.
/// * `shutdown`: Atomic flag for graceful shutdown termination.
pub struct PollConfig<'a, T>
where
    T: HandlerProvider,
{
    pub poll_interval: Duration,
    pub commit_interval: Duration,
    pub group_id: String,
    pub allowed_events: Option<AhoCorasick>,
    pub consumer: BaseConsumer<Context<T>>,
    pub watermark_version: &'a WatermarkVersion,
    pub managers: &'a Managers,
    pub shutdown: &'a AtomicBool,
}

/// Starts the main polling loop which:
/// - Commits offsets if new watermarks are available.
/// - Pauses/resumes partitions by evaluating their capacity.
/// - Polls Kafka for messages using the configured poll interval.
/// - Processes each valid message by extraction and validation.
/// - Dispatches messages with retry logic if the partition is busy.
///
/// The loop continues until the [`shutdown`] flag becomes true.
///
/// # Arguments
///
/// * `config`: A [`PollConfig`] instance with all the necessary configuration.
///
/// # Panics
///
/// This function does not explicitly panic unless an underlying dependency
/// (like sleep or consumer functions) panics.
pub fn poll<T>(config: PollConfig<T>)
where
    T: HandlerProvider,
{
    // Create buffers for SIMD JSON parsing if supported.
    #[cfg(not(target_arch = "arm"))]
    let mut buffers = Buffers::default();

    // Destructure configuration for readability.
    let PollConfig {
        poll_interval,
        commit_interval,
        group_id,
        allowed_events,
        consumer,
        watermark_version,
        managers,
        shutdown,
    } = config;

    // Create a propagator for distributed tracing.
    let propagator = new_propagator();
    let mut last_version = watermark_version.load(Ordering::Acquire);
    let mut last_commit = Instant::now();
    let mut is_paused = false;

    // Enter main polling loop until shutdown signal is received.
    while !shutdown.load(Ordering::Relaxed) {
        // Commit offsets if new watermarks are available and commit interval has
        // passed.
        commit_watermarks(
            &commit_interval,
            &consumer,
            watermark_version,
            managers,
            &mut last_version,
            &mut last_commit,
        );

        // Pause/resume partitions if their capacity changes.
        if let Err(error) = pause_busy_partitions(&mut is_paused, &consumer, managers) {
            error!("error pausing busy partitions: {error:#}; retrying");
            sleep(poll_interval);
            continue;
        }

        // Poll for a new Kafka message.
        let Some(result) = consumer.poll(Timeout::After(poll_interval)) else {
            continue;
        };
        let message = match result {
            Ok(msg) => msg,
            Err(error) => {
                error!("error polling for message: {error:#}");
                continue;
            }
        };

        // Extract and validate the Kafka message.
        let maybe_msg = process_message(
            &message,
            &group_id,
            allowed_events.as_ref(),
            &propagator,
            #[cfg(not(target_arch = "arm"))]
            &mut buffers,
        );
        if let Some(consumer_message) = maybe_msg {
            // Dispatch the message to its partition manager with retry logic.
            dispatch_with_retry(consumer_message, poll_interval, managers);
        }
    }
}

/// Extracts metadata from a Kafka message, validates its key and payload,
/// applies filtering on allowed events, and returns an instrumented
/// [`ConsumerMessage`] ready for processing.
///
/// This function logs errors (via tracing) and returns `None` if critical
/// metadata is missing or if the message does not meet event type filtering
/// criteria.
///
/// # Arguments
///
/// * `message`: A borrowed Kafka message to process.
/// * `group_id`: Identifier of the consuming group used to skip messages
///   originating from ourselves.
/// * `allowed_events`: Optional pattern filter that restricts processing to
///   allowed event types.
/// * `propagator`: A distributed tracing propagator to extract context from
///   message headers.
/// * `buffers`: A mutable reference to JSON parsing buffers (only when not on
///   ARM).
///
/// # Returns
///
/// An `Option<ConsumerMessage>` upon successful extraction and validation, or
/// `None` if the message should be discarded.
fn process_message(
    message: &BorrowedMessage,
    group_id: &str,
    allowed_events: Option<&AhoCorasick>,
    propagator: &impl TextMapPropagator,
    #[cfg(not(target_arch = "arm"))] buffers: &mut Buffers,
) -> Option<ConsumerMessage> {
    // Derive the topic and partition from the message.
    let topic: Topic = Intern::from(message.topic());
    let partition = message.partition();
    let offset = message.offset();

    // Create a tracing span and attach extracted context for distributed tracing.
    let context = propagator.extract(&MessageExtractor::new(message));
    let span = info_span!(
        "receive",
        partition,
        offset,
        topic = topic.as_ref(),
        key = Empty,
        payload_size = Empty,
        skipped = Empty,
        event_type = Empty,
    );
    span.set_parent(context);
    let _enter = span.enter();

    // Skip messages that originate from our own source as indicated in headers.
    if message
        .headers()
        .into_iter()
        .flat_map(|headers| headers.iter())
        .any(|header| {
            header.key == SOURCE_SYSTEM_HEADER && header.value == Some(group_id.as_bytes())
        })
    {
        span.record("skipped", true);
        debug!("skipping message because source system header matches the group identifier");
        return None;
    }

    // Ensure the message has a payload.
    let Some(payload_data) = message.payload() else {
        error!("missing payload; discarding message");
        return None;
    };
    span.record("payload_size", payload_data.len());

    // Parse the payload from JSON. Use simd_json on non-ARM platforms.
    #[cfg(target_arch = "arm")]
    let payload = serde_json::from_slice(payload_data);
    #[cfg(not(target_arch = "arm"))]
    let payload = from_reader_with_buffers(payload_data, buffers);
    let payload: crate::Payload = match payload {
        Ok(p) => p,
        Err(error) => {
            error!("invalid payload: {error:#}; discarding message");
            return None;
        }
    };

    // Apply filtering based on allowed event types if provided.
    if let Some(event_type) = payload.get("type").and_then(Value::as_str) {
        span.record("event_type", event_type);
        if allowed_events.as_ref().is_some_and(|pattern| {
            let input = Input::new(event_type).anchored(Anchored::Yes);
            pattern.find(input).is_none()
        }) {
            span.record("skipped", true);
            debug!("skipping message because {event_type} is not an allowed event type");
            return None;
        }
    }

    // Ensure the message has a key.
    let Some(key_data) = message.key() else {
        error!("missing key; discarding message");
        return None;
    };

    // Parse the key as a UTF-8 string.
    let key = match str::from_utf8(key_data) {
        Ok(key_str) => {
            span.record("key", key_str);
            key_str.into()
        }
        Err(error) => {
            error!("invalid key encoding: {error:#}; discarding message");
            return None;
        }
    };

    // Determine the message timestamp using available metadata.
    let timestamp = match message.timestamp() {
        Timestamp::NotAvailable => Utc::now(),
        Timestamp::CreateTime(millis) | Timestamp::LogAppendTime(millis) => {
            match Utc.timestamp_millis_opt(millis) {
                MappedLocalTime::Single(ts) => ts,
                MappedLocalTime::Ambiguous(earliest, ..) => earliest,
                MappedLocalTime::None => Utc::now(),
            }
        }
    };

    // Return a new ConsumerMessage with tracing instrumentation.
    Some(ConsumerMessage::new(
        topic,
        partition,
        offset,
        key,
        timestamp,
        payload,
        span.clone(),
    ))
}

/// Attempts to dispatch a [`ConsumerMessage`] to its assigned partition
/// manager. Retries dispatching if the corresponding partition is currently
/// busy, sleeping for the configured poll interval between attempts.
///
/// # Arguments
///
/// * `message`: The consumer message to dispatch.
/// * `poll_interval`: Duration to wait between retry attempts.
/// * `managers`: Shared reference to the collection of partition managers.
fn dispatch_with_retry(message: ConsumerMessage, poll_interval: Duration, managers: &Managers) {
    let mut current_message = message;
    loop {
        match dispatch_message(current_message, managers) {
            Ok(()) => break,
            Err(DispatchError::PartitionNotFound(_)) => {
                warn!("failed to dispatch message: partition not found; discarding");
                break;
            }
            Err(DispatchError::Busy(failed)) => {
                error!("failed to dispatch message because partition is busy; retrying");
                current_message = failed;
                sleep(poll_interval);
            }
        }
    }
}

/// Commits offsets for all managed partitions if updated watermarks are
/// available. It inspects each partition manager for a committed watermark,
/// builds a new commit list, and issues an asynchronous commit to the Kafka
/// broker if the commit interval has passed.
///
/// # Arguments
///
/// * `commit_interval`: The minimum duration between offset commit attempts.
/// * `consumer`: The Kafka consumer instance to perform the commit.
/// * `watermark_version`: Atomic counter tracking changes in committed offsets.
/// * `managers`: Shared reference to all partition managers.
/// * `last_version`: Mutable reference to store the last committed watermark
///   version.
/// * `last_commit`: Mutable reference to store the last commit timestamp.
fn commit_watermarks<T>(
    commit_interval: &Duration,
    consumer: &BaseConsumer<Context<T>>,
    watermark_version: &WatermarkVersion,
    managers: &Managers,
    last_version: &mut usize,
    last_commit: &mut Instant,
) where
    T: HandlerProvider,
{
    // Check if watermarks have been updated.
    let current_version = watermark_version.load(Ordering::Acquire);
    if current_version == *last_version {
        return;
    }

    let now = Instant::now();
    if now.duration_since(*last_commit) < *commit_interval {
        return;
    }

    let mut success = true;
    let managers = managers.read();
    let mut list = TopicPartitionList::with_capacity(managers.len());

    // Build a commit list from each partition manager's watermark.
    for ((topic, partition), manager) in managers.iter() {
        let Some(watermark) = manager.watermark() else {
            continue;
        };

        let next_offset = Offset::Offset(watermark + 1);
        if let Err(error) = list.add_partition_offset(topic, *partition, next_offset) {
            error!(
                topic = Topic::as_ref(*topic),
                partition, watermark, "failed to add offset to commit list: {error:#}"
            );

            success = false;
        }
    }

    // Issue an asynchronous commit to Kafka.
    if let Err(error) = consumer.commit(&list, CommitMode::Async) {
        error!("failed to commit offsets: {error:#}");
        success = false;
    }

    if success {
        *last_version = current_version;
        *last_commit = now;
    }
}

/// Pauses or resumes Kafka partitions based on their processing capacity.
/// If a partition's manager is full, it pauses that partition; otherwise, it
/// resumes it.
///
/// # Arguments
///
/// * `is_paused`: Mutable flag indicating if partitions are currently paused.
/// * `consumer`: The Kafka consumer instance used to pause or resume
///   partitions.
/// * `managers`: Shared reference to partition managers.
///
/// # Returns
///
/// * `Ok(())` if the pause/resume actions succeed.
/// * `Err(KafkaError)` if any underlying Kafka operation fails.
fn pause_busy_partitions<T>(
    is_paused: &mut bool,
    consumer: &BaseConsumer<Context<T>>,
    managers: &Managers,
) -> Result<(), KafkaError>
where
    T: HandlerProvider,
{
    let managers = managers.read();

    // Short circuit when all partitions have capacity to avoid allocation
    if !*is_paused && managers.values().all(PartitionManager::has_capacity) {
        return Ok(());
    }

    let mut paused = TopicPartitionList::with_capacity(managers.len());
    let mut resumed = TopicPartitionList::with_capacity(managers.len());

    // Categorize partitions as paused or resumed based on capacity
    for ((topic, partition), manager) in managers.iter() {
        if manager.has_capacity() {
            resumed.add_partition(topic.as_ref(), *partition);
        } else {
            paused.add_partition(topic.as_ref(), *partition);
        }
    }

    *is_paused = paused.count() > 0;

    // Apply pause and resume operations
    if *is_paused {
        consumer.pause(&paused)?;
    }

    if resumed.count() > 0 {
        consumer.resume(&resumed)?;
    }

    Ok(())
}

/// Dispatches a [`ConsumerMessage`] to its designated partition manager.
///
/// This function searches the managers for the partition matching the message's
/// topic and partition and attempts to send the message. It returns an error if
/// the partition is not found or if it is busy (i.e. its message queue is
/// full).
///
/// # Arguments
///
/// * `message`: The consumer message to dispatch.
/// * `managers`: Shared reference to the collection of partition managers.
///
/// # Returns
///
/// * `Ok(())` if the message is successfully dispatched.
/// * `Err(DispatchError)` specifying why dispatch failed.
fn dispatch_message(message: ConsumerMessage, managers: &Managers) -> Result<(), DispatchError> {
    let managers = managers.read();
    let Some(manager) = managers.get(&(message.topic(), message.partition())) else {
        return Err(DispatchError::PartitionNotFound(message));
    };

    let Err(message) = manager.try_send(message) else {
        return Ok(());
    };

    Err(DispatchError::Busy(message))
}

/// Enumerates possible errors that can occur during message dispatch.
///
/// - `PartitionNotFound`: Occurs when a message targets a partition that is not
///   assigned.
/// - `Busy`: Occurs when the target partition's manager is busy.
#[derive(Debug, Error)]
enum DispatchError {
    /// The message was sent to an unassigned partition
    #[error("message sent to unassigned partition")]
    PartitionNotFound(ConsumerMessage),

    /// The partition is currently busy and cannot accept the message
    #[error("partition is busy")]
    Busy(ConsumerMessage),
}
