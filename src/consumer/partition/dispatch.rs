use std::future::{Future, Ready, ready};
use std::panic::{AssertUnwindSafe, resume_unwind};

use aho_corasick::{AhoCorasick, Anchored, Input};
use async_stream::stream;
use futures::{FutureExt, Stream};
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;
use tracing::{Instrument, debug, debug_span, error, info_span};

use super::ShutdownPhase;
use crate::consumer::event_context::PartitionEventContext;
use crate::consumer::message::{
    ConsumerMessage, ConsumerRecord, UncommittedEvent, UncommittedMessage,
};
use crate::consumer::middleware::deduplication::{DedupIdentity, dedup_uuid_for_message};
use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::{DemandType, EventHandler, Keyed, ReceiptedSource, Uncommitted};
use crate::loader::MessageLoader;
use crate::otel::SpanRelation;
use crate::state::manager::{EventStateScope, PartitionStateManager, SweepResolution};
use crate::state::session::{EventSession, TerminationWatch};
use crate::state::{EventRef, TimerEventRef};
use crate::timers::store::TriggerStore;
use crate::timers::uncommitted::Fired;
use crate::timers::{PendingTimer, TimerManager, TimerType, UncommittedTimer};
use crate::{EventIdentity, EventType, ProcessScope};

/// Processes one event in a fresh keyed-state session.
pub(super) async fn process_event<T, S, M, P>(
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
    M: PartitionStateManager<Session: EventSession<Loader: MessageLoader<Payload = P>>>,
    P: Send + Sync + 'static + EventIdentity,
{
    match event {
        UncommittedEvent::Message(message) => {
            process_record(
                message,
                |context, message| handler.on_message(context, message, DemandType::Normal),
                shutdown_rx,
                timer_manager,
                state_manager,
                dedup_identity,
            )
            .await;
        }
        UncommittedEvent::Excise(message) => {
            process_record(
                message,
                |context, message| handler.on_excise(context, message, DemandType::Normal),
                shutdown_rx,
                timer_manager,
                state_manager,
                dedup_identity,
            )
            .await;
        }
        UncommittedEvent::Timer(timer) => {
            process_timer(
                timer,
                handler,
                shutdown_rx,
                timer_manager,
                state_manager,
                timer_spans,
            )
            .await;
        }
    }
}

async fn process_record<S, M, Q, F, Fut>(
    message: UncommittedMessage<Q>,
    dispatch: F,
    shutdown_rx: &watch::Receiver<ShutdownPhase>,
    timer_manager: &TimerManager<S>,
    state_manager: &M,
    dedup_identity: DedupIdentity<'_>,
) where
    S: TriggerStore,
    M: PartitionStateManager<Session: EventSession<Loader: MessageLoader>>,
    Q: Send + Sync + 'static + EventIdentity,
    F: FnOnce(PartitionEventContext<S, M::Session>, UncommittedMessage<Q>) -> Fut,
    Fut: Future<Output = ()>,
{
    let (cancel_tx, cancel_rx) = watch::channel(false);
    // Derive the dedup id for every message — even when no descriptors are
    // registered — because the EventRef must exist before state access. This
    // derivation matches the marker that the settle boundary records.
    // Recovery resolves the message with this exact recorded ID.
    let msg = message.message();
    let dedup_id = dedup_uuid_for_message(dedup_identity, msg);
    // The scope owns the event's state lifetime. Its `Drop` clears the dirty
    // buffer. Keep it bound, and never use `let _`. It must drop after dispatch
    // and `invalidate`, while per-key serialization still holds the key. Thus,
    // the next event for this key cannot observe stale dirty state.
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
    // Use the receive span so handler spans and `Span::current()` captures nest
    // below it.
    let receive_span = message.span();
    guarded_dispatch(&scope, dispatch(context, message).instrument(receive_span)).await;
    cloned_context.invalidate();
}

async fn process_timer<T, S, M, P>(
    timer: PendingTimer<S>,
    handler: &T,
    shutdown_rx: &watch::Receiver<ShutdownPhase>,
    timer_manager: &TimerManager<S>,
    state_manager: &M,
    timer_spans: SpanRelation,
) where
    T: EventHandler<Payload = P>,
    S: TriggerStore,
    M: PartitionStateManager<Session: EventSession<Loader: MessageLoader<Payload = P>>>,
    P: Send + Sync + 'static,
{
    let Some(fired) = timer.fire(shutdown_rx).await else {
        return;
    };
    let firing = match fired {
        Fired::Live(firing) => firing,
        Fired::Committed(source) => {
            match state_manager
                .resolve_redelivered(source.key().clone(), timer_manager, shutdown_rx)
                .await
            {
                SweepResolution::Commit => source.retire().await,
                SweepResolution::Abort => source.keep().await,
            }
            return;
        }
    };
    firing.set_dispatch_span(timer_spans);

    // State recovery is internal. User handlers never receive its trigger.
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
    let dispatch_span = firing.trigger().span();
    guarded_dispatch(
        &scope,
        handler
            .on_timer(context, firing, DemandType::Normal)
            .instrument(dispatch_span),
    )
    .await;
    cloned_context.invalidate();
}

/// Runs one event's dispatch under a panic-unwind guard — the single catch
/// site above every [`EventHandler`] impl (the
/// blanket durability-boundary impl *and*
/// [`RetryHandler`](crate::consumer::middleware::retry::RetryHandler), since
/// retry is outermost and its own `on_message`/`on_timer` is `dispatch` here).
///
/// On the normal path this returns without action: the event's teardown
/// (`invalidate`) is hoisted to the two `process_event` call sites, run after
/// this returns. On an unwind it runs the gate-held terminal
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
pub(super) async fn guarded_dispatch<S, F>(scope: &EventStateScope<S>, dispatch: F)
where
    S: EventSession,
    F: Future<Output = ()>,
{
    if let Err(panic) = AssertUnwindSafe(dispatch).catch_unwind().await {
        let session = scope.handle();
        let permit = session.close_gate().await;
        session.discard_dirty();
        session.terminate();
        drop(permit);
        resume_unwind(panic);
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
pub(super) fn build_message_stream<T, P>(
    offsets: &OffsetTracker,
    mut message_rx: Receiver<ConsumerRecord<P>>,
    group_id: &str,
    highest_offset_seen: &mut i64,
    allowed_events: Option<&AhoCorasick>,
) -> impl Stream<Item = UncommittedEvent<T, P>>
where
    T: TriggerStore,
    P: Send + Sync + 'static + EventType,
{
    stream! {
        while let Some(record) = message_rx.recv().await {
            // Apply filter_rewind - skip messages with offsets we've already processed
            if !filter_rewind(highest_offset_seen, &record).await {
                continue;
            }

            match record {
                ConsumerRecord::Message(message) => {
                    let Some(message) = reserve_offset(offsets, message).await else { continue };
                    let Some(message) = filter_loops(group_id, message).await else { continue };
                    let Some(message) = filter_event_type(allowed_events, message).await else { continue };
                    yield UncommittedEvent::Message(message);
                }
                ConsumerRecord::Excise(message) => {
                    let Some(message) = reserve_offset(offsets, message).await else { continue };
                    let Some(message) = filter_loops(group_id, message).await else { continue };
                    yield UncommittedEvent::Excise(message);
                }
            }
        }
    }
}

/// Filters out messages with offsets we've already processed.
///
/// This prevents processing duplicate messages that might be delivered by
/// Kafka, especially after consumer rebalances.
fn filter_rewind<P>(highest_offset_seen: &mut i64, message: &ConsumerRecord<P>) -> Ready<bool> {
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
/// Only messages with matching event types pass the filter. Excise records
/// always pass because they have no event type.
async fn filter_event_type<P: Send + Sync + 'static + EventType>(
    allowed_events: Option<&AhoCorasick>,
    message: UncommittedMessage<P>,
) -> Option<UncommittedMessage<P>> {
    let event_type = message.payload().event_type();
    let Some(event_type) = event_type else {
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
