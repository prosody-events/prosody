//! Manages message processing and offset tracking for individual Kafka
//! partitions.
//!
//! This module orchestrates concurrent message processing while maintaining
//! ordering guarantees within key groups:
//!
//! - Processes messages with different keys concurrently for high throughput
//! - Preserves strict ordering for messages with the same key
//! - Tracks and commits message offsets for exactly-once processing
//! - Manages graceful shutdown of partition processing
//! - Implements backpressure through the bounded message channel
//!
//! The core component is `PartitionManager`, which coordinates all aspects
//! of partition-level message processing.

use crate::consumer::event_context::{EventContext, TimerContext};
use crate::consumer::message::{ConsumerMessage, UncommittedEvent, UncommittedMessage};
use crate::consumer::partition::keyed::KeyManager;
use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::{DemandType, EventHandler, Keyed, Uncommitted};
use crate::heartbeat::HeartbeatRegistry;
use crate::otel::SpanRelation;
use crate::telemetry::sender::TelemetrySender;
use crate::timers::duration::CompactDuration;
use crate::timers::store::{Segment, SegmentVersion, TriggerStore, TriggerStoreProvider};
use crate::timers::{PendingTimer, TimerManager, TimerManagerConfig, TimerSemaphores};
use crate::{EventType, Offset, Partition, ProcessScope, Topic};
use aho_corasick::{AhoCorasick, Anchored, Input};
use async_stream::stream;
use crossbeam_utils::CachePadded;
use educe::Educe;
use futures::stream::select;
use futures::{Stream, StreamExt, pin_mut};
use std::future::{Ready, ready};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::task::coop::cooperative;
use tokio::time::{Instant, sleep, sleep_until};
use tracing::{debug, debug_span, error, info_span, instrument};
use uuid::Uuid;

mod keyed;
pub mod offsets;
mod util;

#[cfg(test)]
mod test;

/// Grace period numerator: handlers run uninterrupted for this fraction of
/// `shutdown_timeout` before the abort signal fires.
const GRACE_PERIOD_NUMERATOR: u32 = 4;
/// Grace period denominator.
const GRACE_PERIOD_DENOMINATOR: u32 = 5;

/// Lifecycle phase of a partition, used to coordinate shutdown across all
/// partition subsystems.
///
/// Phases advance monotonically in declaration order, which also defines their
/// [`PartialOrd`] / [`Ord`] ordering. Consumers react at different thresholds:
///
/// - `>= Draining` — stop accepting new work
/// - `>= Cancelling` — abort in-flight handlers
/// - `>= Terminating` — hard stop, drop everything
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum ShutdownPhase {
    /// Normal operation.
    #[default]
    Running,
    /// Dispatch halted; in-flight handlers continue uninterrupted.
    Draining,
    /// Abort signal sent to handlers; operations short-circuit with a
    /// `Shutdown` error.
    Cancelling,
    /// Hard stop — drain loop exits, remaining queued work is dropped.
    Terminating,
}

/// Information about the Kafka partition being processed.
struct PartitionInfo {
    /// The Kafka topic name.
    topic: Topic,
    /// The partition number within the topic.
    partition: Partition,
}

/// Runtime context for partition message processing.
///
/// Groups the channels and trackers needed for partition processing,
/// separating runtime state from static configuration.
///
/// `P` is the payload type carried by incoming [`ConsumerMessage`]s.
struct PartitionContext<P> {
    /// Tracks offset commits and processing progress.
    offsets: OffsetTracker,
    /// Channel receiving messages to process.
    message_rx: Receiver<ConsumerMessage<P>>,
    /// Registry for monitoring processing and timer heartbeats.
    heartbeats: HeartbeatRegistry,
    /// Channel receiving shutdown phase transitions.
    shutdown_rx: watch::Receiver<ShutdownPhase>,
}

/// Configuration settings for a partition manager.
///
/// Contains all the parameters needed to configure message processing
/// for a Kafka partition, including buffer sizes, timer concurrency,
/// and filtering options.
///
/// `S` is a [`TriggerStoreProvider`] that creates per-partition stores.
/// `P` is the payload type carried by consumed messages.
#[derive(Clone, Debug)]
pub struct PartitionConfiguration<S, P> {
    /// Consumer group identifier
    pub group_id: Arc<str>,

    /// Maximum size of message buffers
    pub buffer_size: usize,

    /// Maximum number of uncommitted messages allowed
    pub max_uncommitted: usize,

    /// Optional automaton for filtering messages by event type
    pub allowed_events: Option<AhoCorasick>,

    /// Timeout duration for shutdown operations
    pub shutdown_timeout: Duration,

    /// Duration of inactivity allowed before considering a partition stalled
    pub stall_threshold: Duration,

    /// Shared counter tracking watermark updates
    pub watermark_version: Arc<CachePadded<AtomicUsize>>,

    /// Trigger store provider — creates per-partition stores with independent
    /// caches.
    pub trigger_provider: S,

    /// Timer slab size
    pub timer_slab_size: CompactDuration,

    /// Per-type semaphores bounding in-flight timer events across all
    /// partitions
    pub timer_semaphores: Arc<TimerSemaphores>,

    /// Telemetry sender for creating partition-scoped telemetry senders
    pub telemetry_sender: TelemetrySender,

    /// How timer dispatch spans relate to the propagated `OTel` context.
    pub timer_spans: SpanRelation,

    /// Phantom marker for the payload type, used to keep `P` consistent
    /// between [`PartitionConfiguration`] and [`PartitionManager`].
    pub _payload: PhantomData<fn() -> P>,
}

/// Manages message processing and offset tracking for a single Kafka partition.
///
/// Coordinates concurrent message processing by:
/// - Queuing messages by key to maintain ordering for each key
/// - Tracking and committing message offsets to ensure at-least-once processing
/// - Managing graceful partition shutdown during rebalancing
/// - Enforcing backpressure through the bounded message channel
/// - Monitoring for processing stalls
///
/// `P` is the payload type carried by consumed messages.
#[derive(Educe)]
#[educe(Debug)]
pub struct PartitionManager<P> {
    /// The Kafka topic this partition belongs to
    topic: Topic,
    /// The partition number this manager handles
    partition: Partition,

    /// Tracks offset commits and processing progress
    #[educe(Debug(ignore))]
    offsets: OffsetTracker,

    /// Channel for sending messages to be processed
    #[educe(Debug(ignore))]
    message_tx: Sender<ConsumerMessage<P>>,

    /// Heartbeat registry
    #[educe(Debug(ignore))]
    heartbeats: HeartbeatRegistry,

    /// Drives partition shutdown phase transitions
    #[educe(Debug(ignore))]
    shutdown_tx: watch::Sender<ShutdownPhase>,

    /// Total time budget for shutdown phase transitions
    shutdown_timeout: Duration,

    /// Handle for the message processing task
    #[educe(Debug(ignore))]
    handle: JoinHandle<()>,
}

impl<P: Send + 'static> PartitionManager<P> {
    /// Creates a new partition manager.
    ///
    /// # Arguments
    ///
    /// * `config` - The partition configuration
    /// * `handler` - The message handler that will process messages
    /// * `topic` - The Kafka topic this partition belongs to
    /// * `partition` - The partition number
    ///
    /// # Returns
    ///
    /// A new `PartitionManager<P>` instance
    pub fn new<T, S>(
        config: PartitionConfiguration<S, P>,
        handler: T,
        topic: Topic,
        partition: Partition,
    ) -> Self
    where
        T: EventHandler<Payload = P> + Send + Sync + 'static,
        S: TriggerStoreProvider,
        P: Sync + EventType,
    {
        // Initialize offset tracker to manage offset state
        let offsets = OffsetTracker::new(
            topic,
            partition,
            config.max_uncommitted,
            config.stall_threshold,
            config.watermark_version.clone(),
        );

        // Initialize heartbeats, channels, and shutdown signals
        let heartbeats =
            HeartbeatRegistry::new(format!("{topic}:{partition}"), config.stall_threshold);
        let (message_tx, message_rx) = channel(config.buffer_size);
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let shutdown_timeout = config.shutdown_timeout;

        // Spawn the background task for message handling
        let partition_info = PartitionInfo { topic, partition };
        let context = PartitionContext {
            offsets: offsets.clone(),
            message_rx,
            heartbeats: heartbeats.clone(),
            shutdown_rx,
        };
        let handle = spawn(handle_messages(config, partition_info, handler, context));

        Self {
            topic,
            partition,
            offsets,
            message_tx,
            heartbeats,
            shutdown_tx,
            shutdown_timeout,
            handle,
        }
    }

    /// Checks if the partition can accept more messages.
    ///
    /// This method indicates whether the internal message queue has capacity
    /// for more messages, which is used to implement backpressure.
    ///
    /// # Returns
    ///
    /// `true` if there is space in the message queue, `false` if the queue is
    /// at capacity
    pub fn has_capacity(&self) -> bool {
        self.message_tx.capacity() > 0
    }

    /// Attempts to enqueue a message for processing.
    ///
    /// This non-blocking method tries to send a message to the internal
    /// processing queue without waiting. If the queue is full or closed,
    /// the original message is returned.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to enqueue
    ///
    /// # Returns
    ///
    /// `Ok(())` if the message was enqueued, or `Err(ConsumerMessage)`
    /// containing the original message if the queue is full or closed
    pub fn try_send(&self, message: ConsumerMessage<P>) -> Result<(), ConsumerMessage<P>> {
        self.message_tx
            .try_send(message)
            .map_err(|error| match error {
                TrySendError::Closed(message) | TrySendError::Full(message) => message,
            })
    }

    /// Gets the current committed offset watermark.
    ///
    /// The watermark represents the highest contiguous offset that has been
    /// successfully processed and committed. This is used for offset management
    /// and reporting consumer progress.
    ///
    /// # Returns
    ///
    /// The highest contiguous committed offset if any messages have been
    /// committed, or `None` if no messages have been committed
    pub fn watermark(&self) -> Option<Offset> {
        self.offsets.watermark()
    }

    /// Checks if message processing has stalled.
    ///
    /// A partition is considered stalled if any of:
    /// - The offset tracker detects uncommitted offsets beyond the stall
    ///   threshold
    /// - The message processing heartbeat hasn't been updated within the stall
    ///   threshold
    /// - The timer system heartbeat (if present) hasn't been updated within the
    ///   stall threshold
    ///
    /// This method is used by health monitoring systems to detect processing
    /// issues.
    ///
    /// # Returns
    ///
    /// `true` if messages are not being processed within the configured stall
    /// threshold, `false` otherwise
    pub fn is_stalled(&self) -> bool {
        self.offsets.is_stalled() || self.heartbeats.any_stalled()
    }

    /// Initiates an orderly partition shutdown.
    ///
    /// This method performs a graceful shutdown sequence:
    /// 1. Closes the message channel to prevent new messages
    /// 2. Signals handlers to shut down gracefully
    /// 3. Waits for in-flight messages to complete processing
    /// 4. Performs final offset commits
    ///
    /// Used during consumer rebalancing or application shutdown.
    ///
    /// # Returns
    ///
    /// The final committed offset watermark if shutdown completes successfully,
    /// or `None` if an error occurs during shutdown
    #[instrument(level = "debug")]
    pub async fn shutdown(self) -> Option<Offset> {
        // Close the message channel to stop accepting new messages
        drop(self.message_tx);

        // Advance to Draining immediately, then schedule Cancelling and
        // Terminating at 80% and 100% of shutdown_timeout respectively.
        // If send returns Err, all receivers have dropped (partition already
        // exited) — no point spawning the phase task.
        if self.shutdown_tx.send(ShutdownPhase::Draining).is_ok() {
            debug!(topic = %self.topic, partition = self.partition, phase = "draining", "shutdown phase transition");

            let now = Instant::now();
            let grace = self.shutdown_timeout * GRACE_PERIOD_NUMERATOR / GRACE_PERIOD_DENOMINATOR;
            let cancelling_at = now + grace;
            let terminating_at = now + self.shutdown_timeout;
            let topic = self.topic;
            let partition = self.partition;
            let shutdown_tx = self.shutdown_tx;

            spawn(async move {
                sleep_until(cancelling_at).await;
                if shutdown_tx.send(ShutdownPhase::Cancelling).is_err() {
                    return;
                }
                debug!(topic = %topic, partition, phase = "cancelling", "shutdown phase transition");

                sleep_until(terminating_at).await;
                let _ = shutdown_tx.send(ShutdownPhase::Terminating);
                debug!(topic = %topic, partition, phase = "terminating", "shutdown phase transition");
            });
        }

        // Wait for message processing to complete
        if let Err(error) = self.handle.await {
            error!(
                topic = %self.topic,
                partition = self.partition,
                "error occurred while shutting down partition: {error:#}"
            );
            return None;
        }

        // Perform final offset commit and return the watermark
        self.offsets.shutdown().await
    }
}

/// Initializes a timer manager for the partition, retrying on failure until
/// the shutdown signal is received.
///
/// Returns `None` if shutdown is signaled before initialization succeeds.
/// Arguments for [`init_timer_manager`] that don't depend on the store type.
struct TimerInitContext<'a> {
    name: &'a str,
    telemetry_sender: &'a TelemetrySender,
    group_id: &'a Arc<str>,
    timer_semaphores: &'a Arc<TimerSemaphores>,
    partition_info: &'a PartitionInfo,
    heartbeats: &'a HeartbeatRegistry,
    shutdown_rx: &'a watch::Receiver<ShutdownPhase>,
}

async fn init_timer_manager<S>(
    trigger_store: S,
    ctx: TimerInitContext<'_>,
) -> Option<(impl Stream<Item = PendingTimer<S>>, TimerManager<S>)>
where
    S: TriggerStore,
{
    loop {
        if *ctx.shutdown_rx.borrow() >= ShutdownPhase::Draining {
            return None;
        }

        let timer_config = TimerManagerConfig {
            name: ctx.name.to_owned(),
            store: trigger_store.clone(),
            telemetry: ctx
                .telemetry_sender
                .for_partition(ctx.partition_info.topic, ctx.partition_info.partition),
            source: ctx.group_id.clone(),
        };

        match TimerManager::new(
            timer_config,
            ctx.heartbeats.clone(),
            ctx.shutdown_rx.clone(),
            ctx.timer_semaphores.clone(),
        )
        .await
        {
            Ok(result) => return Some(result),
            Err(error) => {
                error!("failed to initialize timer manager: {error:#}; retrying");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Store-agnostic fields extracted from [`PartitionConfiguration`] for
/// [`run_partition`].
struct PartitionParams {
    group_id: Arc<str>,
    allowed_events: Option<AhoCorasick>,
    timer_semaphores: Arc<TimerSemaphores>,
    telemetry_sender: TelemetrySender,
    name: String,
    timer_spans: SpanRelation,
}

/// Extracts the store from the provider `S` then delegates to
/// [`run_partition`], which is generic only over `S::Store`.  This keeps the
/// provider type `S` out of the long-lived coroutine state machine, preventing
/// future-size explosion with the deeply nested middleware handler type `T`.
async fn handle_messages<T, S, P>(
    config: PartitionConfiguration<S, P>,
    partition_info: PartitionInfo,
    handler: T,
    context: PartitionContext<P>,
) where
    T: EventHandler<Payload = P> + Send + Sync + 'static,
    S: TriggerStoreProvider,
    P: Send + Sync + 'static + EventType,
{
    let PartitionConfiguration {
        group_id,
        allowed_events,
        trigger_provider,
        timer_slab_size,
        timer_semaphores,
        telemetry_sender,
        timer_spans,
        ..
    } = config;

    let name = format!(
        "{}:{}/{}",
        group_id, partition_info.topic, partition_info.partition
    );
    let trigger_store = trigger_provider.create_store(Segment {
        id: Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes()),
        name: name.clone(),
        slab_size: timer_slab_size,
        version: SegmentVersion::V3,
    });

    let params = PartitionParams {
        group_id,
        allowed_events,
        timer_semaphores,
        telemetry_sender,
        name,
        timer_spans,
    };

    run_partition(trigger_store, partition_info, handler, context, params).await;
}

/// Core partition loop, generic over `S: TriggerStore` and `P` (payload type).
async fn run_partition<T, S, P>(
    trigger_store: S,
    partition_info: PartitionInfo,
    handler: T,
    context: PartitionContext<P>,
    params: PartitionParams,
) where
    T: EventHandler<Payload = P> + Send + Sync + 'static,
    S: TriggerStore,
    P: Send + Sync + 'static + EventType,
{
    let PartitionParams {
        group_id,
        allowed_events,
        timer_semaphores,
        telemetry_sender,
        name,
        timer_spans,
    } = params;
    let PartitionContext {
        offsets,
        message_rx,
        heartbeats,
        shutdown_rx,
    } = context;

    let mut highest_offset_seen = -1;

    let message_events = build_message_stream::<S, P>(
        &offsets,
        message_rx,
        &group_id,
        &mut highest_offset_seen,
        allowed_events.as_ref(),
    );

    let timer_ctx = TimerInitContext {
        name: &name,
        telemetry_sender: &telemetry_sender,
        group_id: &group_id,
        timer_semaphores: &timer_semaphores,
        partition_info: &partition_info,
        heartbeats: &heartbeats,
        shutdown_rx: &shutdown_rx,
    };
    let Some((timer_stream, timer_manager)) = init_timer_manager(trigger_store, timer_ctx).await
    else {
        return;
    };

    let timer_events = stream! {
        pin_mut!(timer_stream);
        while let Some(timer) = cooperative(timer_stream.next()).await {
            yield UncommittedEvent::<S, P>::Timer(timer);
        }
    };

    let process = |event: UncommittedEvent<S, P>| async {
        debug!(?event, "calling handler");
        process_event(event, &handler, &shutdown_rx, &timer_manager, timer_spans).await;
    };

    KeyManager::<UncommittedEvent<S, P>, _, _>::new(process)
        .process_messages(
            select(message_events, timer_events),
            heartbeats.register("event processor"),
            shutdown_rx.clone(),
        )
        .await;

    handler.shutdown().await;
}

/// Processes a single event (message or timer) through the handler.
async fn process_event<T, S, P>(
    event: UncommittedEvent<S, P>,
    handler: &T,
    shutdown_rx: &watch::Receiver<ShutdownPhase>,
    timer_manager: &TimerManager<S>,
    timer_spans: SpanRelation,
) where
    T: EventHandler<Payload = P>,
    S: TriggerStore,
    P: Send + 'static,
{
    match event {
        UncommittedEvent::Message(message) => {
            let context = TimerContext::new(
                message.key().clone(),
                shutdown_rx.clone(),
                timer_manager.clone(),
            );
            let cloned_context = context.clone();
            let _guard = message.process_scope();
            handler
                .on_message(context, message, DemandType::Normal)
                .await;
            cloned_context.invalidate();
        }
        UncommittedEvent::Timer(timer) => {
            if let Some(firing) = timer.fire().await {
                firing.set_dispatch_span(timer_spans);
                let context = TimerContext::new(
                    firing.key().clone(),
                    shutdown_rx.clone(),
                    timer_manager.clone(),
                );
                let cloned_context = context.clone();
                let _guard = firing.process_scope();
                handler.on_timer(context, firing, DemandType::Normal).await;
                cloned_context.invalidate();
            }
        }
    }
}

/// Builds a message processing stream with filtering.
///
/// Creates a stream that:
/// - Filters out duplicate messages based on offsets
/// - Reserves offsets for processing
/// - Prevents consumer group loops by filtering messages from the same group
/// - Filters messages based on their event type (if filtering is configured)
///
/// # Arguments
///
/// * `offsets` - Offset tracker for managing message offsets
/// * `message_rx` - Receiver channel for incoming messages
/// * `group_id` - Consumer group identifier
/// * `highest_offset_seen` - Tracks the highest offset processed
/// * `allowed_events` - Optional filter for permitted event types
///
/// # Returns
///
/// A stream of [`UncommittedEvent`] items (each wrapping an
/// [`UncommittedMessage`] or a timer) ready for processing
fn build_message_stream<T, P>(
    offsets: &OffsetTracker,
    mut message_rx: Receiver<ConsumerMessage<P>>,
    group_id: &str,
    highest_offset_seen: &mut i64,
    allowed_events: Option<&AhoCorasick>,
) -> impl Stream<Item = UncommittedEvent<T, P>>
where
    T: TriggerStore,
    P: Send + Sync + 'static + EventType,
{
    stream! {
        while let Some(message) = message_rx.recv().await {
            // Apply filter_rewind - skip messages with offsets we've already processed
            if !filter_rewind(highest_offset_seen, &message).await {
                continue;
            }

            // Apply reserve_offset - reserve offset and convert to UncommittedMessage
            let Some(uncommitted) = reserve_offset(offsets, message).await else {
                continue;
            };

            // Apply filter_loops - filter out messages from same consumer group
            let Some(uncommitted) = filter_loops(group_id, uncommitted).await else {
                continue;
            };

            // Apply filter_event_type - filter based on allowed event types
            let Some(uncommitted) = filter_event_type(allowed_events, uncommitted).await else {
                continue;
            };

            yield UncommittedEvent::Message(uncommitted);
        }
    }
}

/// Filters out messages with offsets we've already processed.
///
/// This prevents processing duplicate messages that might be delivered by
/// Kafka, especially after consumer rebalances.
///
/// # Arguments
///
/// * `highest_offset_seen` - Reference to the highest offset already processed
/// * `message` - The message to check
///
/// # Returns
///
/// `true` if the message should be processed, `false` if it should be filtered
/// out
fn filter_rewind<P>(highest_offset_seen: &mut i64, message: &ConsumerMessage<P>) -> Ready<bool> {
    let partition = message.partition();
    let offset = message.offset();

    // Skip messages with offsets we've already seen
    if offset <= *highest_offset_seen {
        debug_span!(
            parent: message.span(),
            "message.filtered",
            %partition, %offset, reason = "stale"
        )
        .in_scope(|| {
            debug!("filtering stale partition {partition} offset {offset}");
        });

        return ready(false);
    }

    // Update the highest offset seen
    *highest_offset_seen = offset;
    ready(true)
}

/// Reserves an offset for a message and converts it to an uncommitted message.
///
/// # Arguments
///
/// * `offsets` - The offset tracker to reserve offsets from
/// * `received` - The consumer message to process
///
/// # Returns
///
/// `Some(UncommittedMessage)` if the offset was successfully reserved,
/// `None` if the reservation failed
async fn reserve_offset<P: Send + 'static>(
    offsets: &OffsetTracker,
    received: ConsumerMessage<P>,
) -> Option<UncommittedMessage<P>> {
    // Attempt to reserve the offset
    received
        .span()
        .in_scope(|| async {
            match offsets.take(received.offset()).await {
                Ok(uncommitted_offset) => Some(received.into_uncommitted(uncommitted_offset)),
                Err(error) => {
                    error!(
                        topic = %received.topic(),
                        partition = received.partition(),
                        offset = received.offset(),
                        "unable to take uncommitted offset: {error:#}; discarding message"
                    );
                    None
                }
            }
        })
        .await
}

/// Filters out messages produced by the same consumer group to prevent loops.
///
/// # Arguments
///
/// * `group_id` - The consumer group ID
/// * `message` - The message to check
///
/// # Returns
///
/// `Some(message)` if the message should be processed,
/// `None` if it should be filtered out
async fn filter_loops<P: Send + Sync + 'static>(
    group_id: &str,
    message: UncommittedMessage<P>,
) -> Option<UncommittedMessage<P>> {
    // Check if the message comes from the same source system as our own consumer
    // group
    if message
        .source_system()
        .is_some_and(|source_system| source_system.as_str() == group_id)
    {
        info_span!(
            parent: message.span(),
            "message.filtered",
            reason = "source-system-loop"
        )
        .in_scope(|| {
            debug!("skipping message because source system header matches the group identifier");
        });

        // Commit the message and filter it out
        message.commit().await;
        return None;
    }

    Some(message)
}

/// Filters messages based on their event type if filtering is enabled.
///
/// Only messages with event types matching the allowed patterns will be
/// processed.
///
/// # Arguments
///
/// * `allowed_events` - Optional automaton defining allowed event type patterns
/// * `message` - The message to check
///
/// # Returns
///
/// `Some(message)` if the message should be processed,
/// `None` if it should be filtered out
async fn filter_event_type<P: Send + Sync + 'static + EventType>(
    allowed_events: Option<&AhoCorasick>,
    message: UncommittedMessage<P>,
) -> Option<UncommittedMessage<P>> {
    // Extract event type from message payload if present
    let Some(event_type) = message.payload().event_type() else {
        return Some(message);
    };

    // Check if the event type is allowed
    if allowed_events.as_ref().is_some_and(|automaton| {
        let input = Input::new(event_type).anchored(Anchored::Yes);
        automaton.find(input).is_none()
    }) {
        info_span!(
            parent: message.span(),
            "message.filtered",
            reason = "event-type"
        )
        .in_scope(|| {
            debug!("skipping message because {event_type} is not an allowed event type");
        });

        // Commit the message and filter it out
        message.commit().await;
        return None;
    }

    Some(message)
}
