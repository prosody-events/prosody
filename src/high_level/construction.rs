//! Concrete high-level client construction.

use super::{
    CassandraClientBackend, CassandraHighLevelClient, ConsumerBuilders, ConsumerState,
    HighLevelClient, HighLevelClientError, MemoryClientBackend, MemoryHighLevelClient, Mode, Wire,
    WireError,
};
use crate::cassandra::config::CassandraConfiguration;
use crate::high_level::config::ModeConfigurationBuildParams;
use crate::high_level::topics::missing_topics;
use crate::peer::Router;
use crate::producer::{ProducerConfigurationBuilder, ProsodyProducer};
use crate::propagator::new_propagator;
use crate::state_reader::StateReaderClient;
use crate::telemetry::{Telemetry, spawn_telemetry_emitter};
use tokio::sync::Mutex;

async fn new_with_backend<T, B>(
    backend: B,
    mock: bool,
    mode: Mode,
    producer_builder: &mut ProducerConfigurationBuilder,
    consumer_builders: &ConsumerBuilders,
) -> Result<HighLevelClient<T, B>, HighLevelClientError<WireError<T>>>
where
    T: super::ClientHandler,
    T::Payload: crate::EventIdentity,
    B: super::ClientBackend<Wire<T>>,
{
    producer_builder.mock(mock);
    let mut consumer_builders = consumer_builders.clone();
    consumer_builders.consumer.mock(mock);

    if let (None, Some(group_id)) = (
        producer_builder.configured_source_system(),
        consumer_builders.consumer.configured_consumer_group(),
    ) {
        producer_builder.source_system(group_id);
    }

    let producer_config = producer_builder.build()?;
    let telemetry = Telemetry::new();
    let producer: ProsodyProducer<Wire<T>> = match mode {
        Mode::Pipeline => {
            ProsodyProducer::pipeline_producer(producer_config.clone(), telemetry.sender())
        }
        Mode::LowLatency => {
            ProsodyProducer::low_latency_producer(producer_config.clone(), telemetry.sender())
        }
        Mode::BestEffort => {
            ProsodyProducer::best_effort_producer(producer_config.clone(), telemetry.sender())
        }
    }?;

    spawn_telemetry_emitter(
        &consumer_builders.emitter,
        &producer_config.bootstrap_servers,
        &telemetry,
        producer_config.mock,
    )?;

    let mode_config = super::config::ModeConfiguration::build(&ModeConfigurationBuildParams {
        mode,
        consumer_builders: &consumer_builders,
    })
    .map_err(HighLevelClientError::ConsumerConfiguration)?;
    let reader_config = super::deps::ReaderConfiguration::from_mode(&mode_config);
    let consumer_state = ConsumerState::Configured {
        config: mode_config,
    };
    if !producer_config.mock
        && let ConsumerState::Configured { config, .. } = &consumer_state
    {
        let missing = missing_topics(&producer, config.configured_topics())?;
        if !missing.is_empty() {
            return Err(HighLevelClientError::TopicsNotFound(missing));
        }
    }

    let peer = &consumer_builders.peer;
    let reader = backend
        .build_reader(&reader_config)
        .await
        .map_err(HighLevelClientError::StateReader)?;
    let router = backend.build_router(peer, &reader).await?;
    let (producer_peer, consumer_peer, router_owner) = router.split();
    let requester = producer_peer.requester(producer.clone());

    Ok(HighLevelClient {
        producer,
        producer_config,
        consumer: Mutex::new(consumer_state),
        reader: StateReaderClient::new(reader),
        requester,
        consumer_peer,
        subsystem: consumer_builders.keyed_state.subsystem.clone(),
        router_owner,
        propagator: new_propagator(),
        telemetry,
    })
}

impl<T> MemoryHighLevelClient<T>
where
    T: super::ClientHandler,
    T::Payload: crate::EventIdentity + Clone,
{
    /// Creates a fully in-memory client.
    ///
    /// The producer and consumer use mock Kafka. No Cassandra configuration is
    /// required.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration or producer initialization fails.
    pub async fn new(
        mode: Mode,
        producer: &mut ProducerConfigurationBuilder,
        consumers: &ConsumerBuilders,
    ) -> Result<Self, HighLevelClientError<WireError<T>>> {
        new_with_backend(MemoryClientBackend::new(), true, mode, producer, consumers).await
    }
}

impl<T> CassandraHighLevelClient<T>
where
    T: super::ClientHandler,
    T::Payload: crate::EventIdentity + Clone,
{
    /// Creates a client backed by Cassandra and Kafka.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration or producer initialization fails.
    pub async fn new(
        cassandra: CassandraConfiguration,
        mode: Mode,
        producer: &mut ProducerConfigurationBuilder,
        consumers: &ConsumerBuilders,
    ) -> Result<Self, HighLevelClientError<WireError<T>>> {
        new_with_backend(
            CassandraClientBackend::new(cassandra),
            false,
            mode,
            producer,
            consumers,
        )
        .await
    }
}
