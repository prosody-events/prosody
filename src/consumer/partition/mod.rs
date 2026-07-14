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

use crate::consumer::event_context::{EventContext, PartitionEventContext};
use crate::consumer::message::{ConsumerMessage, UncommittedEvent, UncommittedMessage};
use crate::consumer::middleware::deduplication::{DedupIdentity, dedup_uuid_for_message};
use crate::consumer::partition::keyed::KeyManager;
use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::{DemandType, EventHandler, Keyed, Uncommitted};
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MessageLoader;
use crate::otel::SpanRelation;
use crate::state::manager::{
    EventStateScope, PartitionStateManager, PartitionStateProvider, SweepResolution,
};
use crate::state::session::{CellSession, TerminationWatch};
use crate::state::{EventRef, TimerEventRef};
use crate::telemetry::sender::TelemetrySender;
use crate::timers::duration::CompactDuration;
use crate::timers::store::{Segment, TriggerStore, TriggerStoreProvider};
use crate::timers::{
    PendingTimer, TimerManager, TimerManagerConfig, TimerSemaphores, TimerType, UncommittedTimer,
};
use crate::{EventIdentity, EventType, Offset, Partition, ProcessScope, Topic};
use aho_corasick::{AhoCorasick, Anchored, Input};
use async_stream::stream;
use crossbeam_utils::CachePadded;
use educe::Educe;
use futures::stream::select;
use futures::{FutureExt, Stream, StreamExt, pin_mut};
use std::future::{Future, Ready, ready};
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, resume_unwind};
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
use tracing::{Instrument, debug, debug_span, error, info_span, instrument};

mod keyed;
mod metrics;
pub mod offsets;
mod util;

#[cfg(test)]
mod tests;

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
/// `SP` is a [`PartitionStateProvider`] that acquires per-partition
/// keyed-state managers. `P` is the payload type carried by consumed
/// messages.
#[derive(Clone, Debug)]
pub struct PartitionConfiguration<S, SP, P> {
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

    /// Deduplication hash version. Threaded into the per-message
    /// [`EventRef::Message`] dedup id so recovery resolves a message's
    /// committed state by the exact id the settle boundary records.
    pub version: Arc<str>,

    /// Trigger store provider — creates per-partition stores with independent
    /// caches.
    pub trigger_provider: S,

    /// Keyed-state provider — acquires per-partition state managers.
    pub state_provider: SP,

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
    pub(crate) _payload: PhantomData<fn() -> P>,
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
    pub fn new<T, S, SP>(
        config: PartitionConfiguration<S, SP, P>,
        handler: T,
        topic: Topic,
        partition: Partition,
    ) -> Self
    where
        T: EventHandler<Payload = P> + Send + Sync + 'static,
        S: TriggerStoreProvider,
        SP: PartitionStateProvider<S::Store>,
        <SP::Manager as PartitionStateManager>::Session:
            CellSession<Loader: MessageLoader<Payload = P>>,
        P: Sync + EventType + EventIdentity,
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
    pub fn has_capacity(&self) -> bool {
        self.message_tx.capacity() > 0
    }

    /// Attempts to enqueue a message for processing.
    ///
    /// This non-blocking method tries to send a message to the internal
    /// processing queue without waiting. If the queue is full or closed, the
    /// original message is returned to the caller in the `Err` variant.
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
    /// and reporting consumer progress. Returns `None` if no messages have
    /// been committed yet.
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
    /// Used during consumer rebalancing or application shutdown. Returns the
    /// final committed offset watermark, or `None` if an error occurs during
    /// shutdown.
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
            store: trigger_store.clone(),
            telemetry: ctx
                .telemetry_sender
                .partition_sender(ctx.partition_info.topic, ctx.partition_info.partition),
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

/// Acquires the partition's keyed-state manager, retrying on failure until
/// the shutdown signal is received — the same pattern as
/// [`init_timer_manager`]. Acquisition is eager: descriptor identities are
/// validated against the segment's durable rows before any event
/// dispatches. `trigger_store` is the partition's own store handle, cloned
/// into each attempt so the commit oracle reads timer tags through the
/// exact instance the timer manager writes through.
///
/// Returns `None` if shutdown is signaled before acquisition succeeds.
async fn init_state_manager<SP, S>(
    state_provider: &SP,
    trigger_store: &S,
    topic: Topic,
    partition: Partition,
    shutdown_rx: &watch::Receiver<ShutdownPhase>,
) -> Option<SP::Manager>
where
    SP: PartitionStateProvider<S>,
    S: Clone,
{
    loop {
        if *shutdown_rx.borrow() >= ShutdownPhase::Draining {
            return None;
        }

        match state_provider
            .acquire(topic, partition, trigger_store.clone())
            .await
        {
            Ok(manager) => return Some(manager),
            Err(error) => {
                error!("failed to acquire keyed-state manager: {error:#}; retrying");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Store-agnostic fields extracted from [`PartitionConfiguration`] for
/// [`run_partition`].
struct PartitionParams {
    group_id: Arc<str>,
    version: Arc<str>,
    allowed_events: Option<AhoCorasick>,
    timer_semaphores: Arc<TimerSemaphores>,
    telemetry_sender: TelemetrySender,
    timer_spans: SpanRelation,
}

/// Extracts the store from the provider `S` and the state manager from the
/// provider `SP`, then delegates to [`run_partition`], which is generic
/// only over `S::Store` and `SP::Manager`. This keeps the provider types
/// out of the long-lived coroutine state machine, preventing future-size
/// explosion with the deeply nested middleware handler type `T`.
async fn handle_messages<T, S, SP, P>(
    config: PartitionConfiguration<S, SP, P>,
    partition_info: PartitionInfo,
    handler: T,
    context: PartitionContext<P>,
) where
    T: EventHandler<Payload = P> + Send + Sync + 'static,
    S: TriggerStoreProvider,
    SP: PartitionStateProvider<S::Store>,
    <SP::Manager as PartitionStateManager>::Session:
        CellSession<Loader: MessageLoader<Payload = P>>,
    P: Send + Sync + 'static + EventType + EventIdentity,
{
    let PartitionConfiguration {
        group_id,
        version,
        allowed_events,
        trigger_provider,
        state_provider,
        timer_slab_size,
        timer_semaphores,
        telemetry_sender,
        timer_spans,
        ..
    } = config;

    let segment = Segment::for_partition(
        &group_id,
        partition_info.topic,
        partition_info.partition,
        timer_slab_size,
    );
    let trigger_store = trigger_provider.create_store(segment);

    let Some(state_manager) = init_state_manager(
        &state_provider,
        &trigger_store,
        partition_info.topic,
        partition_info.partition,
        &context.shutdown_rx,
    )
    .await
    else {
        return;
    };

    let params = PartitionParams {
        group_id,
        version,
        allowed_events,
        timer_semaphores,
        telemetry_sender,
        timer_spans,
    };

    run_partition(
        trigger_store,
        state_manager,
        partition_info,
        handler,
        context,
        params,
    )
    .await;
}

/// Core partition loop, generic over `S: TriggerStore`, the keyed-state
/// manager `M`, and `P` (payload type).
async fn run_partition<T, S, M, P>(
    trigger_store: S,
    state_manager: M,
    partition_info: PartitionInfo,
    handler: T,
    context: PartitionContext<P>,
    params: PartitionParams,
) where
    T: EventHandler<Payload = P> + Send + Sync + 'static,
    S: TriggerStore,
    M: PartitionStateManager<Session: CellSession<Loader: MessageLoader<Payload = P>>>,
    P: Send + Sync + 'static + EventType + EventIdentity,
{
    let PartitionParams {
        group_id,
        version,
        allowed_events,
        timer_semaphores,
        telemetry_sender,
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

    spawn(metrics::run(
        timer_manager.clone(),
        group_id.clone(),
        partition_info.topic,
        partition_info.partition,
        shutdown_rx.clone(),
    ));

    let timer_events = stream! {
        pin_mut!(timer_stream);
        while let Some(timer) = cooperative(timer_stream.next()).await {
            yield UncommittedEvent::<S, P>::Timer(timer);
        }
    };

    let process = |event: UncommittedEvent<S, P>| async {
        debug!(?event, "calling handler");
        process_event(
            event,
            &handler,
            &shutdown_rx,
            &timer_manager,
            &state_manager,
            DedupIdentity {
                version: version.as_ref(),
                group_id: group_id.as_ref(),
                topic: partition_info.topic.as_ref(),
                partition: partition_info.partition,
            },
            timer_spans,
        )
        .await;
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
///
/// Each event gets a fresh per-event keyed-state session from the state
/// manager and a fresh message-cancellation channel; the session's
/// termination watch shares the channel's receiver with the context, so
/// descriptor handles observe the same cancellation a timeout middleware
/// signals through the context.
async fn process_event<T, S, M, P>(
    event: UncommittedEvent<S, P>,
    handler: &T,
    shutdown_rx: &watch::Receiver<ShutdownPhase>,
    timer_manager: &TimerManager<S>,
    state_manager: &M,
    dedup_identity: DedupIdentity<'_>,
    timer_spans: SpanRelation,
) where
    T: EventHandler<Payload = P>,
    S: TriggerStore,
    M: PartitionStateManager<Session: CellSession<Loader: MessageLoader<Payload = P>>>,
    P: Send + Sync + 'static + EventIdentity,
{
    match event {
        UncommittedEvent::Message(message) => {
            let (cancel_tx, cancel_rx) = watch::channel(false);
            // Derive the dedup id for every message — even when no
            // descriptors are registered — because the EventRef must exist
            // before we know whether the handler touches state. The
            // derivation matches the marker the settle boundary records, so
            // recovery resolves a message by the exact recorded id.
            let msg = message.message();
            let dedup_id = dedup_uuid_for_message(dedup_identity, msg);
            // The scope owns the event's state lifetime; its `Drop` clears the
            // dirty buffer. Keep it bound (never `let _`) through dispatch and
            // `invalidate` so it drops last — per-key serialization keeps the
            // key busy until this future completes, so no next same-key event
            // sees stale dirty in the drop window.
            let scope = state_manager.session(
                msg.key().clone(),
                EventRef::Message { dedup_id },
                TerminationWatch::new(shutdown_rx.clone(), cancel_rx.clone()),
            );
            let context = PartitionEventContext::new(
                message.key().clone(),
                shutdown_rx.clone(),
                (cancel_tx, cancel_rx),
                timer_manager.clone(),
                scope.handle(),
            );
            let cloned_context = context.clone();
            let _guard = message.process_scope();
            // Instrument with the receive span so handler-created spans (and
            // `Span::current()` captures like `EventContext::schedule`) nest
            // under it ambiently.
            let receive_span = message.span();
            guarded_dispatch(
                &scope,
                cloned_context,
                handler
                    .on_message(context, message, DemandType::Normal)
                    .instrument(receive_span),
            )
            .await;
        }
        UncommittedEvent::Timer(timer) => {
            if let Some(firing) = timer.fire().await {
                firing.set_dispatch_span(timer_spans);

                // `StateRecovery` is framework-internal: the sweep runs
                // here, owned by the state manager, and user handlers
                // structurally never see the trigger. State is always
                // wired, so the sweep is always intercepted (it is inert
                // when no collections are registered). See
                // `PartitionStateManager::recover` for the
                // never-abort-except-shutdown posture behind the
                // `SweepResolution` mapping below.
                if firing.timer_type() == TimerType::StateRecovery {
                    let _guard = firing.process_scope();
                    let (trigger, commit_guard) = firing.into_inner();
                    match state_manager
                        .recover(trigger.key.clone(), timer_manager, shutdown_rx)
                        .await
                    {
                        SweepResolution::Commit => commit_guard.commit().await,
                        SweepResolution::Abort => commit_guard.abort().await,
                    }
                    return;
                }

                let (cancel_tx, cancel_rx) = watch::channel(false);
                let trigger = firing.trigger();
                let event = EventRef::Timer(TimerEventRef::new(
                    trigger.timer_type,
                    trigger.time,
                    trigger.tag,
                ));
                // Kept bound through dispatch + `invalidate` so its `Drop`
                // clears the dirty buffer last (see the message arm above).
                let scope = state_manager.session(
                    firing.key().clone(),
                    event,
                    TerminationWatch::new(shutdown_rx.clone(), cancel_rx.clone()),
                );
                let context = PartitionEventContext::new(
                    firing.key().clone(),
                    shutdown_rx.clone(),
                    (cancel_tx, cancel_rx),
                    timer_manager.clone(),
                    scope.handle(),
                );
                let cloned_context = context.clone();
                let _guard = firing.process_scope();
                // Instrument with the dispatch span so handler-created spans
                // nest under it ambiently (mirrors the message arm).
                let dispatch_span = firing.trigger().span();
                guarded_dispatch(
                    &scope,
                    cloned_context,
                    handler
                        .on_timer(context, firing, DemandType::Normal)
                        .instrument(dispatch_span),
                )
                .await;
            }
        }
    }
}

/// Runs one event's dispatch under a panic-unwind guard — the single catch
/// site above every [`EventHandler`](crate::consumer::EventHandler) impl (the
/// blanket durability-boundary impl *and* [`RetryHandler`], since retry is
/// outermost and its own `on_message`/`on_timer` is `dispatch` here).
///
/// On the normal path it tears the event down (`invalidate`), exactly as the
/// two arms did before. On an unwind it runs the gate-held terminal
/// transition on the scope's own session — acquire the closed-gate permit
/// (which FIFO-serializes *after* any already-admitted mutator, so a detached
/// op in flight lands fully before the discard), discard the uncommitted
/// overlay, flip the session terminated, release — then resumes the unwind so
/// a panic still kills the partition task. It writes **no** epoch: a leaked
/// current-pin handle is fenced for reads by termination and for mutations
/// (including `commit()`) by the closed gate, and a genuinely-stale clone stays
/// fenced by its old pin.
///
/// `process_event` legitimately owns the scope, so reaching the sealed
/// lifecycle through [`EventStateScope::handle`] here is not the tunnel the
/// settlement/marker split restricts — no `context` accessor is used.
async fn guarded_dispatch<S, C, F>(scope: &EventStateScope<S>, cloned: C, dispatch: F)
where
    S: CellSession,
    C: EventContext,
    F: Future<Output = ()>,
{
    match AssertUnwindSafe(dispatch).catch_unwind().await {
        Ok(()) => cloned.invalidate(),
        Err(panic) => {
            let session = scope.handle();
            let permit = session.close_gate().await;
            session.discard_dirty();
            session.terminate();
            drop(permit);
            resume_unwind(panic);
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
/// Yields [`UncommittedEvent`] items (each wrapping an [`UncommittedMessage`]
/// or a timer) ready for processing.
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
/// Returns `None` if the reservation failed.
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
/// Returns `None` if the message should be filtered out.
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
/// processed. Returns `None` if the message should be filtered out.
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
            reason = "event-type",
            event_type
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
