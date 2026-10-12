//! Kafka message polling and processing pipeline.
//!
//! This module implements the main message consumption loop that:
//! - Polls messages from Kafka brokers
//! - Extracts and validates message data
//! - Applies event type filtering
//! - Maintains distributed tracing contexts
//! - Manages offset tracking and committing
//! - Dispatches messages to the appropriate partition managers, holding back
//!   the ones they cannot accept yet (see [`Backpressure`])
//!
//! The main entry point is the [`poll`] function, which orchestrates all these
//! operations within a continuous loop.

use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::util::Timeout;
use rdkafka::{Message, Offset, TopicPartitionList};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::{Span, debug, error, warn};

use crate::Codec;
use crate::EventType;
use crate::Topic;
use crate::consumer::decode::{RecordMeta, ResultRequestReader, decode_record};
use crate::consumer::{Managers, WatermarkVersion};
use crate::heartbeat::Heartbeat;
use crate::otel::SpanRelation;
use crate::propagator::new_propagator;
use crate::related_span;
use backpressure::Backpressure;

mod backpressure;

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
/// 3. Dispatches the messages that wait for room, in arrival order, and waits
///    for room while they wait
/// 4. Polls for new messages from Kafka
/// 5. Processes valid messages through validation and filtering
/// 6. Dispatches messages to their respective partition managers, or holds them
///    back until there is room
///
/// The loop continues until the shutdown flag is set.
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
    let mut backpressure = Backpressure::new(max_message_count, message_spans);

    // Main polling loop
    while !shutdown.load(Ordering::Relaxed) {
        // Signal that the polling loop is active
        heartbeat.beat();

        // Periodically commit watermark offsets to Kafka
        store_watermarks(&consumer, watermark_version, managers, &mut last_version);

        // Dispatch the messages that wait for room before new ones arrive
        if let Err(error) = backpressure.drain(&consumer, poll_interval) {
            error!("error resuming partition: {error:#}");
        }

        // Poll for next message with timeout. While messages wait, the drain
        // above has already waited for room.
        let timeout = if backpressure.is_blocked() {
            Duration::ZERO
        } else {
            poll_interval
        };
        let Some(result) = consumer.poll(Timeout::After(timeout)) else {
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

        let topic = Topic::from(message.topic());
        let partition = message.partition();
        let offset = message.offset();
        debug!(
            topic = topic.as_ref(),
            partition, offset, "received message"
        );

        let Some(decoded) = decode_record(&mut message, &propagator, &mut codec, &requests) else {
            continue;
        };

        // A partition is missing while it is revoked. Its next owner reads
        // the message again.
        let assigned = managers.read();
        let Some(manager) = assigned.get(&(topic, partition)) else {
            warn!("failed to dispatch message: partition not found; discarding");
            continue;
        };

        if let Err(error) = backpressure.submit(&consumer, decoded, manager.sender()) {
            error!("error pausing partition: {error:#}");
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
