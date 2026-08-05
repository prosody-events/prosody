//! Runtime startup and assembly: builds the Kafka client, subscribes, starts
//! the poll loop, and hands back a running consumer.

use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::ConsumerError;
use crate::consumer::handler::{EventHandler, HandlerProvider};
use crate::consumer::kafka_context::{ContextHandles, PartitionProviders, new_context};
use crate::consumer::observer::KafkaObserver;
use crate::consumer::poll::{PollConfig, poll};
use crate::consumer::probes::ProbeServer;
use crate::consumer::wiring::peer::PeerAttachment;
use crate::consumer::{Managers, ProsodyConsumer, RuntimeState, WatermarkVersion};
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MessageLoader;
use crate::router::directory::GroupMembership;
use crate::router::label_fits;
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::session::CellWrite;
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use crate::timers::store::TriggerStoreProvider;
use crate::{Codec, EventIdentity, EventType, MOCK_CLUSTER_BOOTSTRAP};
use parking_lot::Mutex;
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::watch;
use tokio::task::spawn_blocking;
use tracing::{error, warn};
use validator::Validate;
use whoami::hostname;

/// Everything startup needs beyond the consumer configuration and the two
/// per-partition providers.
///
/// Deliberately not `Clone`: one value can serve only one consumer, so a mode
/// cannot hand two consumers two different observers without a second,
/// grep-visible [`KafkaObserver::new`] call.
pub(in crate::consumer) struct StartupServices<'a, P> {
    /// Idempotence version stamped into the partition configuration.
    pub(in crate::consumer) version: Arc<str>,
    /// Telemetry the partitions and middleware publish through.
    pub(in crate::consumer) telemetry: &'a Telemetry,
    /// Stall-detection registry, shared with the probe server.
    pub(in crate::consumer) heartbeats: HeartbeatRegistry,
    /// The consumer's one Kafka observation handle. The same instance its
    /// primary consumer's context holds, which updates it from the statistics
    /// callback.
    pub(in crate::consumer) observer: KafkaObserver,
    /// The partition managers shared by startup, health, and shutdown.
    pub(in crate::consumer) managers: Arc<Managers<P>>,
    /// The subsystem this consumer answers peer requests for, or `None` when
    /// it answers none. Taken from
    /// [`KeyedStateInputs::subsystem`](super::state::KeyedStateInputs::subsystem).
    pub(in crate::consumer) responder: Option<SubsystemName>,
}

/// Initializes a Prosody consumer with a trigger store provider, wiring the
/// partition machinery to a Kafka consumer and starting its background poll
/// loop. The provider creates per-partition stores with independent caches.
///
/// The primary consumer is the sole source of Kafka observations: it is the
/// client configured to report statistics, and its first observation is seeded
/// by [`KafkaObserver::install_startup_metadata`], which owns that contract.
///
/// `peer` decides whether this consumer joins the peer fleet. It is activated
/// after the client subscribes, which is the last step that can fail, and every
/// earlier failure arm abandons it.
///
/// Fails if the configuration is invalid, the probe server can't be started
/// (if enabled), the consumer context can't be created, the hostname can't be
/// retrieved for the client ID, the Kafka consumer can't be created with the
/// provided configuration, topic subscription fails, the startup metadata
/// fetch fails, or the peer node cannot be published.
pub(in crate::consumer) async fn initialize_consumer<T, P, SP, C, A>(
    consumer_config: &ConsumerConfiguration,
    handler_provider: T,
    providers: PartitionProviders<P, SP>,
    services: StartupServices<'_, C::Payload>,
    peer: A,
) -> Result<ProsodyConsumer<C>, ConsumerError>
where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = C::Payload>,
    P: TriggerStoreProvider,
    SP: PartitionStateProvider<P::Store>,
    <SP::Manager as PartitionStateManager>::Session:
        CellWrite<Loader: MessageLoader<Payload = C::Payload>>,
    C: Codec,
    C::Payload: EventType + Clone + EventIdentity,
    A: PeerAttachment + 'static,
{
    if let Err(error) = consumer_config.validate() {
        peer.abandon().await;
        return Err(error.into());
    }
    let StartupServices {
        version,
        telemetry,
        heartbeats,
        observer,
        managers,
        responder,
    } = services;

    let watermark_version: Arc<WatermarkVersion> = Arc::default();
    let shutdown: Arc<AtomicBool> = Arc::default();
    let (assignment_tx, assignment) = watch::channel(0u32);

    // Every fallible step runs before the poll loop starts. A blocking task
    // cannot be aborted, so dropping its handle on an error path would detach
    // an unreachable thread that holds the Kafka client forever. The probe
    // server binds first: a misconfigured port fails in microseconds, ahead of
    // the client's network round trips, and no consumer exists yet to release.
    let probe_server = match consumer_config
        .probe_port
        .filter(|_| !consumer_config.mock)
        .map(|port| ProbeServer::new(port, managers.clone(), heartbeats.clone()))
        .transpose()
    {
        Ok(probe_server) => probe_server,
        Err(error) => {
            peer.abandon().await;
            return Err(error.into());
        }
    };

    let started = start_client::<T, P, SP, C, A>(
        consumer_config,
        handler_provider,
        providers,
        watermark_version.clone(),
        ContextHandles {
            managers: managers.clone(),
            assignment_tx,
            telemetry: telemetry.sender(),
            observer: observer.clone(),
        },
        version,
        observer.clone(),
    )
    .await;

    // The failure arm for every step after the probe bound: the observation is
    // discarded, the prepared peer released, and the probe port freed. Clearing
    // after the task, rather than inside it, also covers a fetch that panicked
    // — see `KafkaObserver::clear`.
    let (consumer, cluster) = match started {
        Ok(started) => started,
        Err(error) => {
            observer.clear();
            peer.abandon().await;
            return Err(release_probe(probe_server, error).await);
        }
    };

    // Activation is the last step that can fail, so nothing after it can strand
    // a published node. Its own arm surrenders the client first: that client's
    // context holds a response send handle, and a release that ran while it
    // lives would wait for a sender this task still owns.
    let group = checked_membership(cluster.as_deref(), &consumer_config.group_id);
    let peer = match peer.activate(group).await {
        Ok(peer) => peer,
        Err((peer, error)) => {
            drop_client(consumer).await;
            observer.clear();
            peer.abandon().await;
            return Err(release_probe(probe_server, error).await);
        }
    };

    let poll_interval = consumer_config.poll_interval;
    let heartbeat = heartbeats.register("Kafka poll loop");
    let cloned_managers = managers.clone();
    let cloned_shutdown = shutdown.clone();
    let max_message_count = consumer_config.max_uncommitted;
    let message_spans = consumer_config.message_spans;

    let poll_handle = spawn_blocking(move || {
        poll(PollConfig {
            poll_interval,
            max_message_count,
            consumer,
            codec: C::default(),
            watermark_version: &watermark_version,
            managers: &cloned_managers,
            heartbeat: &heartbeat,
            shutdown: &cloned_shutdown,
            message_spans,
            responder,
        });
    });

    let runtime_state = Arc::new(Mutex::new(Some(RuntimeState {
        poll_handle,
        probe_server,
        observer,
        peer,
    })));

    Ok(ProsodyConsumer {
        shutdown,
        managers,
        assignment,
        runtime_state,
        heartbeats,
    })
}

/// Builds the client, subscribes it, seeds the observer, and reads the cluster
/// id.
///
/// All four run inside one blocking task. Subscribing and fetching block, and
/// dropping a `BaseConsumer` poll-loops until its queue closes, so the client
/// lives and dies inside that task. The returned context type captures no
/// borrow, which is what lets a failure arm surrender the client to a blocking
/// drop.
///
/// # Errors
///
/// Returns [`ConsumerError`] when the context, the client, the subscription or
/// the metadata fetch fails, or when the blocking task does not join.
async fn start_client<T, P, SP, C, A>(
    consumer_config: &ConsumerConfiguration,
    handler_provider: T,
    providers: PartitionProviders<P, SP>,
    watermark_version: Arc<WatermarkVersion>,
    handles: ContextHandles<C::Payload>,
    version: Arc<str>,
    observer: KafkaObserver,
) -> Result<
    (
        BaseConsumer<impl ConsumerContext + use<T, P, SP, C, A>>,
        Option<String>,
    ),
    ConsumerError,
>
where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = C::Payload>,
    P: TriggerStoreProvider,
    SP: PartitionStateProvider<P::Store>,
    <SP::Manager as PartitionStateManager>::Session:
        CellWrite<Loader: MessageLoader<Payload = C::Payload>>,
    C: Codec,
    C::Payload: EventType + Clone + EventIdentity,
    A: PeerAttachment + 'static,
{
    let context = new_context(
        consumer_config,
        handler_provider,
        providers,
        watermark_version,
        handles,
        version,
    )?;
    let consumer: BaseConsumer<_> = client_config(consumer_config)?.create_with_context(context)?;
    let topics = consumer_config.subscribed_topics.clone();
    spawn_blocking(move || {
        let topics: Vec<&str> = topics.iter().map(String::as_str).collect();
        consumer.subscribe(&topics)?;
        observer.install_startup_metadata(&consumer)?;
        let cluster = A::cluster_id(&consumer, &observer);
        Ok::<_, ConsumerError>((consumer, cluster))
    })
    .await
    .map_err(ConsumerError::StartupTask)?
}

/// Surrenders a built client to the blocking pool and waits for it to close.
///
/// Dropping a `BaseConsumer` polls until the consumer group closes, so it must
/// not run on a runtime thread. The wait is what lets the next failure step
/// drain a response path the client's own context still holds a handle to.
async fn drop_client<Ctx: ConsumerContext + 'static>(consumer: BaseConsumer<Ctx>) {
    if let Err(error) = spawn_blocking(move || drop(consumer)).await {
        error!(%error, "Kafka client teardown task did not finish");
    }
}

/// The group membership this node publishes, when both labels fit.
///
/// A node that cannot name its cluster names no cluster-scoped group, and
/// `None` says exactly that. The column routes nothing, so an oversized label
/// warns rather than refusing to consume.
fn checked_membership(cluster: Option<&str>, group_id: &str) -> Option<GroupMembership> {
    cluster.and_then(|cluster| {
        let membership = GroupMembership::checked(cluster, group_id);
        if membership.is_none() {
            let part = if label_fits(cluster) {
                "consumer group id"
            } else {
                "Kafka cluster id"
            };
            warn!(
                part,
                "peer group membership is omitted: the label is empty or too long"
            );
        }
        membership
    })
}

/// Releases the probe port, then returns `error` for construction to fail with.
///
/// Dropping a [`ProbeServer`] only signals its graceful shutdown; nothing waits
/// for the listener to close. A caller that retries construction on the same
/// port would race the old listener, so the failure path waits here instead.
async fn release_probe(probe_server: Option<ProbeServer>, error: ConsumerError) -> ConsumerError {
    if let Some(server) = probe_server {
        server.shutdown().await;
    }
    error
}

/// The primary consumer's librdkafka configuration: offsets are committed
/// automatically but stored by prosody, and the client reports statistics on
/// the configured interval.
///
/// # Errors
///
/// [`ConsumerError::Hostname`] when the client id cannot be derived.
fn client_config(consumer_config: &ConsumerConfiguration) -> Result<ClientConfig, ConsumerError> {
    let bootstrap = if consumer_config.mock {
        MOCK_CLUSTER_BOOTSTRAP.clone()
    } else {
        consumer_config.bootstrap_servers.join(",")
    };

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap)
        .set("client.id", hostname()?)
        .set("group.id", &consumer_config.group_id)
        .set("enable.auto.commit", "true")
        .set(
            "auto.commit.interval.ms",
            consumer_config.commit_interval.as_millis().to_string(),
        )
        .set(
            "statistics.interval.ms",
            consumer_config.statistics_interval.as_millis().to_string(),
        )
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set_log_level(RDKafkaLogLevel::Error);
    Ok(config)
}
