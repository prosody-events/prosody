//! Runtime startup and assembly: builds the Kafka client, subscribes, starts
//! the poll loop, and hands back a running consumer.

use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::ConsumerError;
use crate::consumer::handler::{EventHandler, HandlerProvider};
use crate::consumer::kafka_context::{ContextHandles, PartitionProviders, new_context};
use crate::consumer::observer::KafkaObserver;
use crate::consumer::poll::{PollConfig, poll};
use crate::consumer::probes::ProbeServer;
use crate::consumer::{Managers, ProsodyConsumer, RuntimeState, WatermarkVersion};
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MessageLoader;
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::session::EventSession;
use crate::telemetry::Telemetry;
use crate::timers::store::TriggerStoreProvider;
use crate::{Codec, EventIdentity, EventType, MOCK_CLUSTER_BOOTSTRAP};
use parking_lot::Mutex;
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::watch;
use tokio::task::spawn_blocking;
use validator::Validate;
use whoami::hostname;

/// Everything startup needs beyond the consumer configuration and the two
/// per-partition providers.
///
/// Deliberately not `Clone`: one value can serve only one consumer, so a mode
/// cannot hand two consumers two different observers without a second,
/// grep-visible [`KafkaObserver::new`] call.
pub(in crate::consumer) struct StartupServices<'a> {
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
}

/// Initializes a Prosody consumer with a trigger store provider, wiring the
/// partition machinery to a Kafka consumer and starting its background poll
/// loop. The provider creates per-partition stores with independent caches.
///
/// The primary consumer is the sole source of Kafka observations: it is the
/// client configured to report statistics, and its first observation is seeded
/// by [`KafkaObserver::install_startup_metadata`], which owns that contract.
///
/// Fails if the configuration is invalid, the probe server can't be started
/// (if enabled), the consumer context can't be created, the hostname can't be
/// retrieved for the client ID, the Kafka consumer can't be created with the
/// provided configuration, topic subscription fails, or the startup metadata
/// fetch fails.
pub(in crate::consumer) async fn initialize_consumer<T, P, SP, C>(
    consumer_config: &ConsumerConfiguration,
    handler_provider: T,
    trigger_provider: P,
    state_provider: SP,
    services: StartupServices<'_>,
) -> Result<ProsodyConsumer<C>, ConsumerError>
where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = C::Payload>,
    P: TriggerStoreProvider,
    SP: PartitionStateProvider<P::Store>,
    <SP::Manager as PartitionStateManager>::Session:
        EventSession<Loader: MessageLoader<Payload = C::Payload>>,
    C: Codec,
    C::Payload: EventType + Clone + EventIdentity,
{
    consumer_config.validate()?;

    let StartupServices {
        version,
        telemetry,
        heartbeats,
        observer,
    } = services;

    let watermark_version: Arc<WatermarkVersion> = Arc::default();
    let managers: Arc<Managers<C::Payload>> = Arc::default();
    let shutdown: Arc<AtomicBool> = Arc::default();
    let (assignment_tx, assignment) = watch::channel(0u32);

    // Every fallible step runs before the poll loop starts. A blocking task
    // cannot be aborted, so dropping its handle on an error path would detach
    // an unreachable thread that holds the Kafka client forever. The probe
    // server binds first: a misconfigured port fails in microseconds, ahead of
    // the client's network round trips, and no consumer exists yet to release.
    let probe_server = consumer_config
        .probe_port
        .filter(|_| !consumer_config.mock)
        .map(|port| ProbeServer::new(port, managers.clone(), heartbeats.clone()))
        .transpose()?;

    // Build the client, subscribe, and seed the observer, so a running consumer
    // always has an observation before it can dispatch a handler. Subscribing
    // and fetching both block, and dropping a `BaseConsumer` poll-loops until
    // its queue closes, so the client lives and dies inside the blocking task.
    let started: Result<BaseConsumer<_>, ConsumerError> = async {
        let context = new_context(
            consumer_config,
            handler_provider,
            PartitionProviders {
                triggers: trigger_provider,
                state: state_provider,
            },
            watermark_version.clone(),
            ContextHandles {
                managers: managers.clone(),
                assignment_tx,
                telemetry: telemetry.sender(),
                observer: observer.clone(),
            },
            version,
        )?;
        let consumer: BaseConsumer<_> =
            client_config(consumer_config)?.create_with_context(context)?;
        let topics = consumer_config.subscribed_topics.clone();
        let fetch_observer = observer.clone();
        spawn_blocking(move || {
            let topics: Vec<&str> = topics.iter().map(String::as_str).collect();
            consumer.subscribe(&topics)?;
            fetch_observer.install_startup_metadata(&consumer)?;
            Ok::<_, ConsumerError>(consumer)
        })
        .await
        .map_err(ConsumerError::StartupTask)?
    }
    .await;

    // One failure arm for every step after the probe bound: the observation is
    // discarded and the probe port released. Clearing after the task, rather
    // than inside it, also covers a fetch that panicked — see
    // `KafkaObserver::clear`.
    let consumer = match started {
        Ok(consumer) => consumer,
        Err(error) => {
            observer.clear();
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
        });
    });

    let runtime_state = Arc::new(Mutex::new(Some(RuntimeState {
        poll_handle,
        probe_server,
        observer,
    })));

    Ok(ProsodyConsumer {
        shutdown,
        managers,
        assignment,
        runtime_state,
        heartbeats,
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
