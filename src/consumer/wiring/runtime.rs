//! Runtime startup and assembly: builds the Kafka client, subscribes, starts
//! the poll loop, and hands back a running consumer.

use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::ConsumerError;
use crate::consumer::handler::{EventHandler, HandlerProvider};
use crate::consumer::kafka_context::{ContextHandles, PartitionProviders, new_context};
use crate::consumer::observer::{KafkaObserver, STATISTICS_INTERVAL};
use crate::consumer::poll::{PollConfig, poll};
use crate::consumer::probes::ProbeServer;
use crate::consumer::{Managers, ProsodyConsumer, RuntimeState, WatermarkVersion};
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MessageLoader;
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::session::CellWrite;
use crate::telemetry::Telemetry;
use crate::timers::store::TriggerStoreProvider;
use crate::{Codec, EventIdentity, EventType, MOCK_CLUSTER_BOOTSTRAP};
use parking_lot::Mutex;
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::watch;
use tokio::task::spawn_blocking;
use validator::Validate;
use whoami::hostname;

/// Everything startup needs beyond the consumer configuration and the two
/// per-partition providers.
///
/// Deliberately not `Clone`: one value can serve only one storage arm, so a
/// mode cannot hand two consumers two different observers without a second,
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
/// The primary consumer is the sole source of Kafka observations: it carries
/// the statistics interval, and its startup metadata is fetched here, after
/// subscribe and before the poll loop starts. That fetch is a synchronous
/// librdkafka round trip, so it runs on a blocking thread, bounded by the
/// observer's startup timeout. Construction fails if it fails.
///
/// Fails if the configuration is invalid, the consumer context can't be
/// created, the hostname can't be retrieved for the client ID, the Kafka
/// consumer can't be created with the provided configuration, topic
/// subscription fails, the probe server can't be started (if enabled), or the
/// startup metadata fetch fails.
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
        CellWrite<Loader: MessageLoader<Payload = C::Payload>>,
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

    // Create the consumer context with the message handler and shared state
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

    let consumer: BaseConsumer<_> = client_config(consumer_config)?.create_with_context(context)?;

    let topics: Vec<&str> = consumer_config
        .subscribed_topics
        .iter()
        .map(String::as_str)
        .collect();

    consumer.subscribe(&topics)?;

    // Every fallible step runs before the poll loop starts. A blocking task
    // cannot be aborted, so dropping its handle on an error path would detach
    // an unreachable thread that holds the Kafka client forever. The probe
    // server binds first: a misconfigured port fails in microseconds, ahead of
    // the metadata fetch's network round trip.
    let probe_server = consumer_config
        .probe_port
        .filter(|_| !consumer_config.mock)
        .map(|port| ProbeServer::new(port, managers.clone(), heartbeats.clone()))
        .transpose()?;

    // Seed the observer through the primary consumer before the poll loop
    // starts, so a running consumer always has an observation before it can
    // dispatch a handler. The fetch blocks, so it runs on a blocking thread and
    // hands the consumer back.
    let fetch_observer = observer.clone();
    let consumer = spawn_blocking(move || {
        fetch_observer.install_startup_metadata(&consumer)?;
        Ok::<_, KafkaError>(consumer)
    })
    .await??;

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

/// The primary consumer's librdkafka configuration: offsets are committed
/// automatically but stored by prosody, and the client emits statistics on the
/// observer's interval.
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
            STATISTICS_INTERVAL.as_millis().to_string(),
        )
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set_log_level(RDKafkaLogLevel::Error);
    Ok(config)
}
