//! Concrete high-level client construction.

use super::{
    CassandraClientBackend, CassandraHighLevelClient, ConsumerBuilders, ConsumerState,
    HighLevelClient, HighLevelClientError, MemoryClientBackend, MemoryHighLevelClient, Mode,
};
use crate::Codec;
use crate::cassandra::config::CassandraConfiguration;
use crate::high_level::config::ModeConfigurationBuildParams;
use crate::high_level::topics::missing_topics;
use crate::producer::{ProducerConfigurationBuilder, ProsodyProducer};
use crate::propagator::new_propagator;
use crate::telemetry::{Telemetry, spawn_telemetry_emitter};
use tokio::sync::{Mutex, OnceCell};

fn new_with_backend<T, C, B>(
    backend: B,
    mock: bool,
    mode: Mode,
    producer_builder: &mut ProducerConfigurationBuilder,
    consumer_builders: &ConsumerBuilders,
) -> Result<HighLevelClient<T, C, B>, HighLevelClientError<C::Error>>
where
    C: Codec,
    C::Payload: crate::EventIdentity,
    B: super::ClientBackend<C>,
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
    let producer: ProsodyProducer<C> = match mode {
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

    let consumer_state = ConsumerState::build(&ModeConfigurationBuildParams {
        mode,
        consumer_builders: &consumer_builders,
    });
    let reader_config = match &consumer_state {
        ConsumerState::Configured { config } => {
            Some(super::deps::ReaderConfiguration::from_mode(config))
        }
        ConsumerState::Unconfigured
        | ConsumerState::ConfigurationFailed(_)
        | ConsumerState::Running { .. } => None,
    };
    if !producer_config.mock
        && let ConsumerState::Configured { config, .. } = &consumer_state
    {
        let missing = missing_topics(&producer, config.configured_topics())?;
        if !missing.is_empty() {
            return Err(HighLevelClientError::TopicsNotFound(missing));
        }
    }

    Ok(HighLevelClient {
        producer,
        producer_config,
        consumer: Mutex::new(consumer_state),
        reader: OnceCell::new(),
        reader_config,
        backend,
        propagator: new_propagator(),
        telemetry,
    })
}

impl<T, C> MemoryHighLevelClient<T, C>
where
    C: Codec,
    C::Payload: crate::EventIdentity + Clone,
{
    /// Creates a fully in-memory client.
    ///
    /// The producer and consumer use mock Kafka. No Cassandra configuration is
    /// required.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration or producer initialization fails.
    pub fn new(
        mode: Mode,
        producer: &mut ProducerConfigurationBuilder,
        consumers: &ConsumerBuilders,
    ) -> Result<Self, HighLevelClientError<C::Error>> {
        new_with_backend(MemoryClientBackend::new(), true, mode, producer, consumers)
    }
}

impl<T, C> CassandraHighLevelClient<T, C>
where
    C: Codec,
    C::Payload: crate::EventIdentity + Clone,
{
    /// Creates a client backed by Cassandra and Kafka.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration or producer initialization fails.
    pub fn new(
        cassandra: CassandraConfiguration,
        mode: Mode,
        producer: &mut ProducerConfigurationBuilder,
        consumers: &ConsumerBuilders,
    ) -> Result<Self, HighLevelClientError<C::Error>> {
        new_with_backend(
            CassandraClientBackend::new(cassandra),
            false,
            mode,
            producer,
            consumers,
        )
    }
}
