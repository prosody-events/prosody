//! Runtime startup and assembly: builds the Kafka client, subscribes, starts
//! the poll loop, and hands back a running consumer.

use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::ConsumerError;
use crate::consumer::handler::{EventHandler, HandlerProvider};
use crate::consumer::kafka_context::{ManagerRegistry, PartitionProviders, new_context};
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
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::watch;
use tokio::task::spawn_blocking;
use validator::Validate;
use whoami::hostname;

/// Initializes a Prosody consumer with a trigger store provider, wiring the
/// partition machinery to a Kafka consumer and starting its background poll
/// loop. The provider creates per-partition stores with independent caches.
/// Fails if the hostname can't be retrieved for the client ID, the Kafka
/// consumer can't be created with the provided configuration, topic
/// subscription fails, or the probe server can't be started (if enabled).
pub(in crate::consumer) fn initialize_consumer<T, P, SP, C>(
    consumer_config: &ConsumerConfiguration,
    version: Arc<str>,
    handler_provider: T,
    trigger_provider: P,
    state_provider: SP,
    telemetry: &Telemetry,
    heartbeats: HeartbeatRegistry,
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
        ManagerRegistry {
            managers: managers.clone(),
            assignment_tx,
        },
        telemetry.sender(),
        version,
    )?;

    let bootstrap = if consumer_config.mock {
        MOCK_CLUSTER_BOOTSTRAP.clone()
    } else {
        consumer_config.bootstrap_servers.join(",")
    };

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", bootstrap)
        .set("client.id", hostname()?)
        .set("group.id", &consumer_config.group_id)
        .set("enable.auto.commit", "true")
        .set(
            "auto.commit.interval.ms",
            consumer_config.commit_interval.as_millis().to_string(),
        )
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set_log_level(RDKafkaLogLevel::Error);

    let consumer: BaseConsumer<_> = client_config.create_with_context(context)?;

    let topics: Vec<&str> = consumer_config
        .subscribed_topics
        .iter()
        .map(String::as_str)
        .collect();

    consumer.subscribe(&topics)?;

    let poll_interval = consumer_config.poll_interval;
    let heartbeat = heartbeats.register("Kafka poll loop");
    let cloned_managers = managers.clone();
    let cloned_heartbeat = heartbeat.clone();
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
            heartbeat: &cloned_heartbeat,
            shutdown: &cloned_shutdown,
            message_spans,
        });
    });

    let probe_server = consumer_config
        .probe_port
        .filter(|_| !consumer_config.mock)
        .map(|port| ProbeServer::new(port, managers.clone(), heartbeats.clone()))
        .transpose()?;

    let runtime_state = Arc::new(Mutex::new(Some(RuntimeState {
        poll_handle,
        probe_server,
    })));

    Ok(ProsodyConsumer {
        shutdown,
        managers,
        assignment,
        runtime_state,
        heartbeats,
    })
}
