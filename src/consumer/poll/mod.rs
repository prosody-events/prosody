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
//! operations within a continuous loop.

use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::util::Timeout;
use rdkafka::{Message, Offset, TopicPartitionList};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;
use thiserror::Error;
use tracing::{Span, debug, error, warn};

use crate::Codec;
use crate::EventType;
use crate::Topic;
use crate::consumer::decode::{RecordMeta, ResultRequestReader, decode_record};
use crate::consumer::message::ConsumerRecord;
use crate::consumer::partition::PartitionManager;
use crate::consumer::{Managers, WatermarkVersion};
use crate::heartbeat::Heartbeat;
use crate::otel::SpanRelation;
use crate::propagator::new_propagator;
use crate::related_span;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

#[cfg(test)]
mod tests;

/// Configuration for the Kafka message polling process.
///
/// Bundles all parameters required to run the message polling loop, including
/// the Kafka consumer, topic filtering options, shared state, and lifecycle
/// management.
///
/// # Type Parameters
///
/// * `Ctx` - The Kafka [`ConsumerContext`] the [`BaseConsumer`] runs under. The
///   poller only calls `Consumer`-trait methods on it, so it stays agnostic to
///   the provider generics baked into the context.
/// * `C` - A type implementing [`Codec`] for deserializing message payloads.
pub struct PollConfig<'a, Ctx, C, R>
where
    Ctx: ConsumerContext,
    C: Codec,
    C::Payload: Clone + EventType,
    R: ResultRequestReader,
{
    /// Time between consecutive poll operations
    pub poll_interval: Duration,

    /// Maximum number of messages across all partitions
    pub max_message_count: usize,

    /// The configured Kafka consumer with context
    pub consumer: BaseConsumer<Ctx>,

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

    /// Result-request reader selected by the response policy.
    pub requests: R,
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
/// The loop continues until the shutdown flag is set, or until the message
/// buffer's semaphore reports closed.
pub fn poll<Ctx, C, R>(config: PollConfig<Ctx, C, R>)
where
    Ctx: ConsumerContext,
    C: Codec,
    C::Payload: Clone + EventType,
    R: ResultRequestReader,
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
        requests,
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

        // Take one of the message buffer's permits, or find out why not.
        //
        // `Closed` cannot happen: this loop creates the semaphore, and nothing
        // in the crate closes it. A closed semaphore takes no record again.
        // The loop then stops, rather than beat its heartbeat over a buffer
        // that can take nothing.
        let maybe_permit = match Arc::clone(&semaphore).try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(TryAcquireError::NoPermits) => None,
            Err(TryAcquireError::Closed) => {
                error!("the message buffer was closed; stopping the poll loop");
                break;
            }
        };

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

        let topic = Topic::from(message.topic());
        let partition = message.partition();
        let offset = message.offset();
        debug!(
            topic = topic.as_ref(),
            partition, offset, "received message"
        );

        if let Some(decoded) = decode_record(&mut message, &propagator, &mut codec, &requests) {
            let record =
                decoded.into_record(permit, |meta| create_receive_span(meta, message_spans));
            dispatch_with_retry(record, poll_interval, managers);
        }

        debug!(topic = topic.as_ref(), partition, offset, "poll complete");
    }

    debug!("polling stopped");
}

/// Creates a tracing span for a received message connected to its upstream
/// context.
///
/// Creates a span named "receive" with message metadata attributes and
/// connects it to the upstream trace via the configured [`SpanRelation`].
fn create_receive_span(meta: RecordMeta<'_>, relation: SpanRelation) -> Span {
    related_span!(
        relation,
        meta.parent_context.clone(),
        "receive",
        messaging.system = "kafka",
        partition = meta.partition,
        offset = meta.offset,
        topic = %meta.topic,
        key = %meta.key,
    )
}

/// Attempts to dispatch a message to its partition manager with retries.
///
/// When a partition manager is temporarily at capacity, this function retries
/// the dispatch operation after waiting for the poll interval. If the partition
/// is not found (which happens during rebalancing), the message is discarded.
fn dispatch_with_retry<P: Send + Sync + 'static>(
    message: ConsumerRecord<P>,
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
fn store_watermarks<Ctx, PL>(
    consumer: &BaseConsumer<Ctx>,
    watermark_version: &WatermarkVersion,
    managers: &Managers<PL>,
    last_version: &mut usize,
) where
    Ctx: ConsumerContext,
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
fn pause_busy_partitions<Ctx, PL>(
    is_paused: &mut bool,
    maybe_permit: Option<&OwnedSemaphorePermit>,
    consumer: &BaseConsumer<Ctx>,
    managers: &Managers<PL>,
) -> Result<(), KafkaError>
where
    Ctx: ConsumerContext,
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
/// Looks up the appropriate partition manager for a message and attempts to
/// send the message to it: `DispatchError::PartitionNotFound` if the target
/// partition is not assigned, or `DispatchError::Busy` if the partition's
/// message queue is full.
fn dispatch_message<P: Send + Sync + 'static>(
    message: ConsumerRecord<P>,
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
    let Err(message) = manager.try_send_record(message) else {
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
    PartitionNotFound(ConsumerRecord<P>),

    /// The partition manager's buffer is full
    #[error("partition is busy")]
    Busy(ConsumerRecord<P>),
}
