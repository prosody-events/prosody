use std::sync::Arc;
use std::time::Duration;

use aho_corasick::AhoCorasick;
use async_stream::stream;
use futures::stream::select;
use futures::{Stream, StreamExt, pin_mut};
use tokio::spawn;
use tokio::sync::watch;
use tokio::task::coop::cooperative;
use tokio::time::sleep;
use tracing::{debug, error};

use super::dispatch::{build_message_stream, process_event};
use super::{PartitionConfiguration, PartitionContext, PartitionInfo, ShutdownPhase, metrics};
use crate::consumer::EventHandler;
use crate::consumer::message::UncommittedEvent;
use crate::consumer::middleware::deduplication::DedupIdentity;
use crate::consumer::partition::keyed::KeyManager;
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MessageLoader;
use crate::otel::SpanRelation;
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::session::EventSession;
use crate::telemetry::sender::TelemetrySender;
use crate::timers::store::{Segment, TriggerStore, TriggerStoreProvider};
use crate::timers::{PendingTimer, TimerManager, TimerManagerConfig, TimerSemaphores};
use crate::{EventIdentity, EventType, Partition, Topic};

/// Arguments for [`init_timer_manager`] that don't depend on the store type.
struct TimerInitContext<'a> {
    telemetry_sender: &'a TelemetrySender,
    group_id: &'a Arc<str>,
    timer_semaphores: &'a Arc<TimerSemaphores>,
    partition_info: &'a PartitionInfo,
    heartbeats: &'a HeartbeatRegistry,
    shutdown_rx: &'a watch::Receiver<ShutdownPhase>,
}

/// Initializes a timer manager until it succeeds or shutdown starts.
async fn init_timer_manager<S>(
    trigger_store: S,
    ctx: TimerInitContext<'_>,
) -> Option<(
    impl Future<Output = Option<impl Stream<Item = PendingTimer<S>>>>,
    TimerManager<S>,
)>
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
/// [`init_timer_manager`]. Acquisition publishes routing for the owner. It
/// then validates descriptor identities before event dispatch. `trigger_store`
/// is the partition's own store handle. Each attempt clones this handle.
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
pub(super) async fn handle_messages<T, S, SP, P>(
    config: PartitionConfiguration<S, SP, P>,
    partition_info: PartitionInfo,
    handler: T,
    context: PartitionContext<P>,
) where
    T: EventHandler<Payload = P> + Send + Sync + 'static,
    S: TriggerStoreProvider,
    SP: PartitionStateProvider<S::Store>,
    <SP::Manager as PartitionStateManager>::Session:
        EventSession<Loader: MessageLoader<Payload = P>>,
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
    M: PartitionStateManager<Session: EventSession<Loader: MessageLoader<Payload = P>>>,
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

    let mut load_shutdown = shutdown_rx.clone();
    let timer_stream = tokio::select! {
        stream = timer_stream => stream,
        () = async { let _ = load_shutdown.wait_for(|phase| *phase >= ShutdownPhase::Draining).await; } => None,
    };
    let Some(timer_stream) = timer_stream else {
        handler.shutdown().await;
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
