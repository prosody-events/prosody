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

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::util::Timeout;
use rdkafka::{Message, Offset, TopicPartitionList};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, warn};

use crate::Codec;
use crate::EventType;
use crate::Topic;
use crate::consumer::decode::decode_message;
use crate::consumer::kafka_context::Context;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::partition::PartitionManager;
use crate::consumer::{EventHandler, HandlerProvider, Managers, WatermarkVersion};
use crate::heartbeat::Heartbeat;
use crate::otel::SpanRelation;
use crate::propagator::new_propagator;
use crate::related_span;

use crate::timers::store::TriggerStoreProvider;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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
/// * `P` - A type implementing [`TriggerStoreProvider`] for timer storage.
/// * `C` - A type implementing [`Codec`] for deserializing message payloads.
pub struct PollConfig<'a, T, P, C>
where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = C::Payload>,
    P: TriggerStoreProvider,
    C: Codec,
    C::Payload: Clone + EventType,
{
    /// Time between consecutive poll operations
    pub poll_interval: Duration,

    /// Maximum number of messages across all partitions
    pub max_message_count: usize,

    /// The configured Kafka consumer with context
    pub consumer: BaseConsumer<Context<T, P, C::Payload>>,

    /// Codec for deserializing message payloads
    pub codec: C,

    /// Reference to counter tracking watermark version changes
    pub watermark_version: &'a WatermarkVersion,

    /// Reference to the collection of partition managers
    pub managers: &'a Managers<C::Payload>,

    /// Reference to heartbeat for tracking liveness
    pub heartbeat: &'a Heartbeat,

    /// Flag for signaling polling loop shutdown
    pub shutdown: &'a AtomicBool,

    /// Span relation for message execution spans
    pub message_spans: SpanRelation,
}

/// Runs the main Kafka message polling and processing loop.
///
/// This function implements the core consumption loop that:
/// 1. Monitors and updates heartbeats to detect stalls
/// 2. Stores committed offsets when watermarks advance
/// 3. Manages global message buffering through semaphore permits
/// 4. Pauses partitions that have reached their capacity limit
/// 5. Polls for new messages from Kafka
/// 6. Processes valid messages through validation and filtering
/// 7. Dispatches messages to their respective partition managers
///
/// The loop continues until the shutdown flag is set to true.
///
/// # Arguments
///
/// * `config` - The configuration for the polling process
pub fn poll<T, P, C>(config: PollConfig<T, P, C>)
where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = C::Payload>,
    P: TriggerStoreProvider,
    C: Codec,
    C::Payload: Clone + EventType,
{
    // Destructure configuration for cleaner access
    let PollConfig {
        poll_interval,
        max_message_count,
        consumer,
        mut codec,
        watermark_version,
        managers,
        heartbeat,
        shutdown,
        message_spans,
    } = config;

    // Initialize distributed tracing propagator for context extraction
    let propagator = new_propagator();
    let mut last_version = watermark_version.load(Ordering::Acquire);
    let mut is_paused = false;
    let semaphore = Arc::new(Semaphore::new(max_message_count));

    // Main polling loop
    while !shutdown.load(Ordering::Relaxed) {
        // Signal that the polling loop is active
        heartbeat.beat();

        // Periodically commit watermark offsets to Kafka
        store_watermarks(&consumer, watermark_version, managers, &mut last_version);

        // Attempt to acquire semaphore to buffer a new message
        let maybe_permit = semaphore.clone().try_acquire_owned().ok();

        // Pause/resume partitions based on their buffer capacity
        if let Err(error) =
            pause_busy_partitions(&mut is_paused, maybe_permit.as_ref(), &consumer, managers)
        {
            error!("error pausing busy partitions: {error:#}; retrying");
            sleep(poll_interval);
            continue;
        }

        // Poll for next message with timeout
        let Some(result) = consumer.poll(Timeout::After(poll_interval)) else {
            continue;
        };

        // Handle poll errors
        let mut message = match result {
            Ok(msg) => msg,
            Err(error) => {
                error!("error polling for message: {error:#}");
                continue;
            }
        };

        let Some(permit) = maybe_permit else {
            // This state means that pausing failed
            error!("failed to acquire semaphore; discarding message");
            continue;
        };

        let topic = message.topic().to_owned();
        let partition = message.partition();
        let offset = message.offset();
        debug!(topic, partition, offset, "received message");

        // Decode message through extraction, validation, and filtering
        let maybe_decoded = decode_message(&mut message, &propagator, &mut codec);

        // Create consumer message with processing state and dispatch
        if let Some(decoded) = maybe_decoded {
            // Create receive span connected to parent trace context
            let receive_span = related_span!(
                message_spans,
                decoded.parent_context.clone(),
                "receive",
                partition = decoded.value.partition,
                offset = decoded.value.offset,
                topic = %decoded.value.topic,
                key = %decoded.value.key,
            );

            let consumer_message =
                ConsumerMessage::from_decoded(decoded.value, receive_span, permit);
            dispatch_with_retry(consumer_message, poll_interval, managers);
        }

        debug!(topic, partition, offset, "poll complete");
    }

    debug!("polling stopped");
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
fn dispatch_with_retry<P: Send + Sync + 'static>(
    message: ConsumerMessage<P>,
    poll_interval: Duration,
    managers: &Managers<P>,
) {
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
fn store_watermarks<T, P, PL>(
    consumer: &BaseConsumer<Context<T, P, PL>>,
    watermark_version: &WatermarkVersion,
    managers: &Managers<PL>,
    last_version: &mut usize,
) where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = PL>,
    P: TriggerStoreProvider,
    PL: Clone + Send + Sync + 'static + EventType,
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
fn pause_busy_partitions<T, P, PL>(
    is_paused: &mut bool,
    maybe_permit: Option<&OwnedSemaphorePermit>,
    consumer: &BaseConsumer<Context<T, P, PL>>,
    managers: &Managers<PL>,
) -> Result<(), KafkaError>
where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = PL>,
    P: TriggerStoreProvider,
    PL: Clone + Send + Sync + 'static + EventType,
{
    let managers = managers.read();
    let has_global_capacity = maybe_permit.is_some();
    let has_partition_capacity = managers.values().all(PartitionManager::has_capacity);

    // Skip if no partitions are paused and all have capacity
    if !*is_paused && has_global_capacity && has_partition_capacity {
        return Ok(());
    }

    // Prepare lists for partitions to pause and resume
    let mut paused = TopicPartitionList::with_capacity(managers.len());
    let mut resumed = TopicPartitionList::with_capacity(managers.len());

    // Categorize partitions based on their capacity
    for ((topic, partition), manager) in managers.iter() {
        if has_global_capacity && manager.has_capacity() {
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
fn dispatch_message<P: Send + Sync + 'static>(
    message: ConsumerMessage<P>,
    managers: &Managers<P>,
) -> Result<(), DispatchError<P>> {
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
enum DispatchError<P: Send + Sync + 'static> {
    /// The target partition is not assigned to this consumer
    #[error("message sent to unassigned partition")]
    PartitionNotFound(ConsumerMessage<P>),

    /// The partition manager's buffer is full
    #[error("partition is busy")]
    Busy(ConsumerMessage<P>),
}
