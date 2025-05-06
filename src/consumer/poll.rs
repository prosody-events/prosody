//! Kafka message polling and processing pipeline.
//!
//! This module implements the main message consumption loop that:
//! - Polls messages from Kafka brokers
//! - Extracts and validates message data
//! - Applies event type filtering
//! - Maintains distributed tracing contexts
//! - Manages offset tracking and committing
//! - Controls partition pausing/resuming based on capacity
//! - Dispatches messages to the appropriate partition managers
//!
//! The main entry point is the [`poll`] function, which orchestrates all these
//! operations within a continuous loop until shutdown is signaled.

use chrono::{MappedLocalTime, TimeZone, Utc};
use internment::Intern;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::util::Timeout;
use rdkafka::{Message, Offset, Timestamp, TopicPartitionList};
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;
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
use crate::{SOURCE_SYSTEM_HEADER, SourceSystem, Topic};

use crate::consumer::heartbeat::Heartbeat;
#[cfg(not(target_arch = "arm"))]
use simd_json::Buffers;
#[cfg(not(target_arch = "arm"))]
use simd_json::serde::from_reader_with_buffers;

/// Configuration for the Kafka message polling process.
///
/// Bundles all parameters required to run the message polling loop, including
/// the Kafka consumer, topic filtering options, shared state, and lifecycle
/// management.
///
/// # Type Parameters
///
/// * `T` - A type implementing [`HandlerProvider`] that creates message
///   handlers for assigned partitions.
pub struct PollConfig<'a, T>
where
    T: HandlerProvider,
{
    /// Time between consecutive poll operations
    pub poll_interval: Duration,

    /// The configured Kafka consumer with context
    pub consumer: BaseConsumer<Context<T>>,

    /// Reference to counter tracking watermark version changes
    pub watermark_version: &'a WatermarkVersion,

    /// Reference to the collection of partition managers
    pub managers: &'a Managers,

    /// Reference to heartbeat for tracking liveness
    pub heartbeat: &'a Heartbeat,

    /// Flag for signaling polling loop shutdown
    pub shutdown: &'a AtomicBool,
}

/// Runs the main Kafka message polling and processing loop.
///
/// This function implements the core consumption loop that:
/// 1. Monitors and updates heartbeats to detect stalls
/// 2. Stores committed offsets when watermarks advance
/// 3. Pauses partitions that have reached their capacity limit
/// 4. Polls for new messages from Kafka
/// 5. Processes valid messages through validation and filtering
/// 6. Dispatches messages to their respective partition managers
///
/// The loop continues until the shutdown flag is set to true.
///
/// # Arguments
///
/// * `config` - The configuration for the polling process
pub fn poll<T>(config: PollConfig<T>)
where
    T: HandlerProvider,
{
    // Create buffers for SIMD JSON parsing if on supported platforms
    #[cfg(not(target_arch = "arm"))]
    let mut buffers = Buffers::default();

    // Destructure configuration for cleaner access
    let PollConfig {
        poll_interval,
        consumer,
        watermark_version,
        managers,
        heartbeat,
        shutdown,
    } = config;

    // Initialize distributed tracing propagator for context extraction
    let propagator = new_propagator();
    let mut last_version = watermark_version.load(Ordering::Acquire);
    let mut is_paused = false;

    // Main polling loop
    while !shutdown.load(Ordering::Relaxed) {
        // Signal that the polling loop is active
        heartbeat.beat();

        // Periodically commit watermark offsets to Kafka
        store_watermarks(&consumer, watermark_version, managers, &mut last_version);

        // Pause/resume partitions based on their buffer capacity
        if let Err(error) = pause_busy_partitions(&mut is_paused, &consumer, managers) {
            error!("error pausing busy partitions: {error:#}; retrying");
            sleep(poll_interval);
            continue;
        }

        // Poll for next message with timeout
        let Some(result) = consumer.poll(Timeout::After(poll_interval)) else {
            continue;
        };

        // Handle poll errors
        let message = match result {
            Ok(msg) => msg,
            Err(error) => {
                error!("error polling for message: {error:#}");
                continue;
            }
        };

        let topic = message.topic();
        let partition = message.partition();
        let offset = message.offset();
        debug!(topic, partition, offset, "received message");

        // Process message through extraction, validation, and filtering
        let maybe_msg = process_message(
            &message,
            &propagator,
            #[cfg(not(target_arch = "arm"))]
            &mut buffers,
        );

        // Dispatch valid messages to their partition manager
        if let Some(consumer_message) = maybe_msg {
            dispatch_with_retry(consumer_message, poll_interval, managers);
        }

        debug!(topic, partition, offset, "poll complete");
    }

    debug!("polling stopped");
}

/// Extracts and validates data from a Kafka message.
///
/// This function performs several critical operations:
/// 1. Creates a tracing span with message metadata for observability
/// 2. Extracts distributed tracing context from message headers
/// 3. Parses the payload and validates it as JSON
/// 4. Extracts the message key and timestamp
///
/// # Arguments
///
/// * `message` - The Kafka message to process
/// * `propagator` - Distributed tracing context propagator
/// * `buffers` - (Non-ARM only) Buffers for SIMD JSON parsing
///
/// # Returns
///
/// * `Some(ConsumerMessage)` - A validated, parsed message with tracing context
/// * `None` - If the message is invalid or should be filtered out
fn process_message(
    message: &BorrowedMessage,
    propagator: &TextMapCompositePropagator,
    #[cfg(not(target_arch = "arm"))] buffers: &mut Buffers,
) -> Option<ConsumerMessage> {
    // Extract basic message coordinates
    let topic: Topic = Intern::from(message.topic());
    let partition = message.partition();
    let offset = message.offset();

    debug!(
        topic = message.topic(),
        partition, offset, "processing message"
    );

    // Create and configure tracing span with distributed context
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

    // Extract source system header if present
    let source_system = match message
        .headers()
        .into_iter()
        .flat_map(|headers| headers.iter())
        .find(|header| header.key == SOURCE_SYSTEM_HEADER)
        .and_then(|header| header.value)
        .map(str::from_utf8)
        .transpose()
    {
        Ok(source_system) => source_system.map(SourceSystem::from),
        Err(error) => {
            error!("invalid source system encoding: {error:#}; ignoring");
            None
        }
    };

    // Validate message has payload
    let Some(payload_data) = message.payload() else {
        error!("missing payload; discarding message");
        return None;
    };
    span.record("payload_size", payload_data.len());

    // Parse payload as JSON, using platform-optimized implementations
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

    // Validate message has key
    let Some(key_data) = message.key() else {
        error!("missing key; discarding message");
        return None;
    };

    // Parse key as UTF-8 string
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

    // Determine message timestamp based on available metadata
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

    // Create and return complete consumer message
    Some(ConsumerMessage::new(
        source_system,
        topic,
        partition,
        offset,
        key,
        timestamp,
        payload,
        span.clone(),
    ))
}

/// Attempts to dispatch a message to its partition manager with retries.
///
/// When a partition manager is temporarily at capacity, this function retries
/// the dispatch operation after waiting for the poll interval. If the partition
/// is not found (which happens during rebalancing), the message is discarded.
///
/// # Arguments
///
/// * `message` - The message to dispatch
/// * `poll_interval` - Duration to wait between retry attempts
/// * `managers` - Reference to the collection of partition managers
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

/// Updates offset watermarks in the Kafka consumer.
///
/// This function updates the stored offsets in the Kafka consumer when the
/// watermark version changes, which indicates that new offsets have been
/// committed by the partition managers. These stored offsets will be committed
/// to Kafka on the next auto-commit interval.
///
/// # Arguments
///
/// * `consumer` - The Kafka consumer to update offsets in
/// * `watermark_version` - Counter tracking changes to committed offsets
/// * `managers` - Collection of partition managers that track committed offsets
/// * `last_version` - The last processed watermark version
fn store_watermarks<T>(
    consumer: &BaseConsumer<Context<T>>,
    watermark_version: &WatermarkVersion,
    managers: &Managers,
    last_version: &mut usize,
) where
    T: HandlerProvider,
{
    // Skip if no watermark updates have occurred
    let current_version = watermark_version.load(Ordering::Acquire);
    if current_version == *last_version {
        return;
    }

    // Try to acquire read lock without blocking
    let Some(managers) = managers.try_read() else {
        return;
    };

    let mut success = true;
    let mut list = TopicPartitionList::with_capacity(managers.len());

    // Build list of offsets to commit from each partition manager
    for ((topic, partition), manager) in managers.iter() {
        let Some(watermark) = manager.watermark() else {
            continue;
        };

        // Store next offset after the watermark (Kafka commits the next expected
        // offset)
        let next_offset = Offset::Offset(watermark + 1);
        if let Err(error) = list.add_partition_offset(topic, *partition, next_offset) {
            error!(
                topic = Topic::as_ref(*topic),
                partition, watermark, "failed to add offset to commit list: {error:#}"
            );

            success = false;
        }
    }

    // Skip if no offsets to commit
    if list.count() == 0 {
        debug!("nothing to commit");
        return;
    }

    // Store offsets in librdkafka for auto-commit
    debug!("storing watermarks for commit: {list:?}");
    if let Err(error) = consumer.store_offsets(&list) {
        error!("failed to store offsets: {error:#}");
        success = false;
    }

    // Update version only if all operations succeeded
    if success {
        *last_version = current_version;
        debug!("watermarks stored successfully");
    }
}

/// Pauses and resumes Kafka partitions based on their buffer capacity.
///
/// This function manages backpressure by pausing partitions that are at
/// capacity and resuming partitions that have available capacity. This prevents
/// the consumer from losing its partitions due to inactivity.
///
/// # Arguments
///
/// * `is_paused` - Flag tracking whether any partitions are currently paused
/// * `consumer` - Kafka consumer to apply pause/resume operations
/// * `managers` - Collection of partition managers to check capacity
///
/// # Returns
///
/// * `Ok(())` if pause/resume operations succeeded
/// * `Err(KafkaError)` if any Kafka operation failed
///
/// # Errors
///
/// Returns any error from the underlying Kafka pause/resume operations.
fn pause_busy_partitions<T>(
    is_paused: &mut bool,
    consumer: &BaseConsumer<Context<T>>,
    managers: &Managers,
) -> Result<(), KafkaError>
where
    T: HandlerProvider,
{
    let managers = managers.read();

    // Skip if no partitions are paused and all have capacity
    if !*is_paused && managers.values().all(PartitionManager::has_capacity) {
        return Ok(());
    }

    // Prepare lists for partitions to pause and resume
    let mut paused = TopicPartitionList::with_capacity(managers.len());
    let mut resumed = TopicPartitionList::with_capacity(managers.len());

    // Categorize partitions based on their capacity
    for ((topic, partition), manager) in managers.iter() {
        if manager.has_capacity() {
            resumed.add_partition(topic.as_ref(), *partition);
        } else {
            paused.add_partition(topic.as_ref(), *partition);
        }
    }

    // Update pause state
    *is_paused = paused.count() > 0;

    // Apply pause and resume operations to the consumer
    if *is_paused {
        debug!("pausing: {paused:?}");
        consumer.pause(&paused)?;
    }

    if resumed.count() > 0 {
        debug!("resuming: {resumed:?}");
        consumer.resume(&resumed)?;
    }

    Ok(())
}

/// Dispatches a message to its assigned partition manager.
///
/// Looks up the appropriate partition manager for a message and attempts
/// to send the message to it. If the partition isn't found or the manager
/// is at capacity, appropriate errors are returned.
///
/// # Arguments
///
/// * `message` - The message to dispatch
/// * `managers` - Collection of partition managers
///
/// # Returns
///
/// * `Ok(())` if the message was successfully dispatched
/// * `Err(DispatchError)` describing the reason for failure
///
/// # Errors
///
/// - `DispatchError::PartitionNotFound` - The target partition is not assigned
/// - `DispatchError::Busy` - The partition's message queue is full
fn dispatch_message(message: ConsumerMessage, managers: &Managers) -> Result<(), DispatchError> {
    debug!(
        topic = message.topic().as_ref(),
        partition = message.partition(),
        offset = message.offset(),
        "dispatching message"
    );

    // Look up partition manager
    let managers = managers.read();
    let Some(manager) = managers.get(&(message.topic(), message.partition())) else {
        return Err(DispatchError::PartitionNotFound(message));
    };

    // Try to send message to the manager
    let Err(message) = manager.try_send(message) else {
        return Ok(());
    };

    // Return busy error if send failed
    Err(DispatchError::Busy(message))
}

/// Errors that can occur during message dispatch.
#[derive(Debug, Error)]
enum DispatchError {
    /// The target partition is not assigned to this consumer
    #[error("message sent to unassigned partition")]
    PartitionNotFound(ConsumerMessage),

    /// The partition manager's buffer is full
    #[error("partition is busy")]
    Busy(ConsumerMessage),
}
