use std::fmt::Display;
use std::future::Future;
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
use tracing::debug;

use super::dispatch::{build_message_stream, process_event};
use super::{PartitionConfiguration, PartitionContext, PartitionInfo, ShutdownPhase, metrics};
use crate::consumer::EventHandler;
use crate::consumer::message::{ConsumerRecord, UncommittedEvent};
use crate::consumer::middleware::deduplication::DedupIdentity;
use crate::consumer::partition::keyed::KeyManager;
use crate::error::{ClassifyError, RetryAlert, log_at};
use crate::loader::MessageLoader;
use crate::otel::SpanRelation;
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::session::EventSession;
use crate::telemetry::sender::TelemetrySender;
use crate::timers::store::{Segment, TriggerStore, TriggerStoreProvider};
use crate::timers::{TimerManager, TimerManagerConfig, TimerSemaphores};
use crate::{EventIdentity, EventType};

/// Runs `init` until it succeeds or the partition drains, and returns `None`
/// at a drain. The partition processes no events until `init` succeeds, so a
/// failure that does not heal alerts once.
async fn retry_init<R, E, Fut>(
    shutdown_rx: &watch::Receiver<ShutdownPhase>,
    partition: &PartitionInfo,
    what: &str,
    mut init: impl FnMut() -> Fut,
) -> Option<R>
where
    Fut: Future<Output = Result<R, E>>,
    E: ClassifyError + Display,
{
    let alert = RetryAlert::default();
    let draining = || *shutdown_rx.borrow() >= ShutdownPhase::Draining;
    while !draining() {
        let error = match init().await {
            Ok(value) => return Some(value),
            Err(error) => error,
        };
        // A failure from a revoke is expected, so it does not alert.
        if draining() {
            break;
        }
        log_at!(
            alert.level(error.classify_error()),
            topic = %partition.topic,
            partition = partition.partition,
            "Prosody cannot {what}: {error:#}. The partition processes no events while \
             Prosody retries every second."
        );
        sleep(Duration::from_secs(1)).await;
    }
    None
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
///
/// Shuts the handler down when the partition ends, also when a revoke ends
/// it during startup.
pub(super) async fn handle_messages<T, S, SP, P, Q>(
    config: PartitionConfiguration<S, SP, P>,
    partition_info: PartitionInfo,
    handler: T,
    context: PartitionContext<Q>,
) where
    Q: Stream<Item = ConsumerRecord<P>>,
    T: EventHandler<Payload = P>,
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

    // Acquisition publishes routing for the owner. It then validates
    // descriptor identities before event dispatch.
    let state_manager = retry_init(
        &context.shutdown_rx,
        &partition_info,
        "get the keyed-state manager",
        || {
            state_provider.acquire(
                partition_info.topic,
                partition_info.partition,
                trigger_store.clone(),
            )
        },
    )
    .await;

    if let Some(state_manager) = state_manager {
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
            &handler,
            context,
            params,
        )
        .await;
    }

    handler.shutdown().await;
}

/// Core partition loop, generic over `S: TriggerStore`, the keyed-state
/// manager `M`, and `P` (payload type).
async fn run_partition<T, S, M, P, Q>(
    trigger_store: S,
    state_manager: M,
    partition_info: PartitionInfo,
    handler: &T,
    context: PartitionContext<Q>,
    params: PartitionParams,
) where
    Q: Stream<Item = ConsumerRecord<P>>,
    T: EventHandler<Payload = P>,
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
        records,
        heartbeats,
        shutdown_rx,
    } = context;

    let mut highest_offset_seen = -1;

    let message_events = build_message_stream::<S, P, _>(
        &offsets,
        records,
        &group_id,
        &mut highest_offset_seen,
        allowed_events.as_ref(),
    );

    let Some((timer_stream, timer_manager)) = retry_init(
        &shutdown_rx,
        &partition_info,
        "start the timer manager",
        || {
            let config = TimerManagerConfig {
                store: trigger_store.clone(),
                telemetry: telemetry_sender
                    .partition_sender(partition_info.topic, partition_info.partition),
                source: group_id.clone(),
            };
            TimerManager::new(
                config,
                heartbeats.clone(),
                shutdown_rx.clone(),
                timer_semaphores.clone(),
            )
        },
    )
    .await
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
            handler,
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
}
