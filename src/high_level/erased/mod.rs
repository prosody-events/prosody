//! Optional type erasure for foreign-language client wrappers.

use crate::cassandra::config::CassandraConfiguration;
use crate::consumer::MockConfigurationError;
use crate::consumer::middleware::FallibleHandler;
use crate::high_level::config::ModeConfiguration;
use crate::high_level::state::ConsumerState;
use crate::high_level::{
    CassandraHighLevelClient, ClientBackend, ConsumerBuilders, HighLevelClient,
    HighLevelClientError, MemoryHighLevelClient, Mode,
};
use crate::producer::{ProducerConfiguration, ProducerConfigurationBuilder};
use crate::state_reader::ConsumerReaderBackend;
use crate::{Codec, EventIdentity, EventType, Topic};
use async_trait::async_trait;
use opentelemetry::propagation::TextMapCompositePropagator;
use std::error::Error as StdError;
use std::sync::Arc;
use thiserror::Error;

mod readers;

pub use readers::{
    ErasedDequeReader, ErasedDirection, ErasedMapReader, ErasedReadCache, ErasedReaderBuildError,
    ErasedValueReader, SharedDequeReader, SharedMapReader, SharedValueReader,
};

/// Consumer lifecycle state materialized across an FFI boundary.
#[derive(Clone, Debug)]
pub enum ErasedConsumerState<T> {
    /// No valid consumer configuration exists.
    Unconfigured,
    /// Consumer configuration failed.
    ConfigurationFailed(String),
    /// The consumer is ready to subscribe.
    Configured(ErasedConsumerConfiguration),
    /// The consumer is running.
    Running {
        /// Configuration retained across the running transition.
        config: ErasedConsumerConfiguration,
        /// Handler retained by the running client.
        handler: T,
    },
}

/// Owned consumer details needed by native wrapper diagnostics.
#[derive(Clone, Debug)]
pub struct ErasedConsumerConfiguration {
    /// Operational mode.
    pub mode: Mode,
    /// Subscribed topic names or patterns.
    pub topics: Vec<String>,
    /// Kafka consumer group.
    pub group_id: String,
}

/// Lifecycle operations used by foreign-language client wrappers.
///
/// Rust callers use [`HighLevelClient`] directly and pay no type-erasure cost.
#[async_trait]
pub trait ErasedHighLevelClient<T, C>: Send + Sync
where
    C: Codec,
{
    /// Sends one event.
    async fn send(
        &self,
        topic: Topic,
        key: String,
        payload: C::Payload,
    ) -> Result<(), HighLevelClientError<C::Error>>;
    /// Starts consuming.
    async fn subscribe(&self, handler: T) -> Result<(), HighLevelClientError<C::Error>>;
    /// Stops consuming.
    async fn unsubscribe(&self) -> Result<(), HighLevelClientError<C::Error>>;
    /// Returns the current lifecycle state.
    async fn consumer_state(&self) -> ErasedConsumerState<T>;
    /// Builds a read-only view of one published value collection.
    async fn value_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedValueReader<C>, ErasedReaderBuildError<C::Error>>;
    /// Builds a read-only view of one published string-keyed map collection.
    async fn map_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedMapReader<C>, ErasedReaderBuildError<C::Error>>;
    /// Builds a read-only view of one published deque collection.
    async fn deque_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedDequeReader<C>, ErasedReaderBuildError<C::Error>>;
    /// Returns the assigned partition count.
    async fn assigned_partition_count(&self) -> u32;
    /// Reports whether any consumer heartbeat is stalled.
    async fn is_stalled(&self) -> bool;
    /// Producer configuration.
    fn producer_config(&self) -> &ProducerConfiguration;
    /// Trace-context propagator.
    fn propagator(&self) -> &TextMapCompositePropagator;
    /// Configured source system.
    fn source_system(&self) -> &str;
}

/// Shared erased client representation stored by native FFI wrappers.
pub type SharedHighLevelClient<T, C> = Arc<dyn ErasedHighLevelClient<T, C>>;

/// Constructs and erases the backend selected by an FFI configuration.
///
/// # Errors
///
/// Returns [`ErasedClientBuildError::MissingCassandra`] when a live client has
/// no Cassandra configuration. Other construction failures retain the
/// structured high-level error as their source.
pub fn new_erased<T, C>(
    mode: Mode,
    producer: &mut ProducerConfigurationBuilder,
    consumers: &ConsumerBuilders,
    cassandra: Option<CassandraConfiguration>,
) -> Result<SharedHighLevelClient<T, C>, ErasedClientBuildError<C::Error>>
where
    T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
    C: Codec + Send + Sync,
    C::Payload: EventIdentity + EventType + Clone,
{
    let mock = consumers.consumer.configured_mock()?;

    if mock {
        Ok(Arc::new(ErasedClient(MemoryHighLevelClient::new(
            mode, producer, consumers,
        )?)))
    } else {
        let cassandra = cassandra.ok_or(ErasedClientBuildError::MissingCassandra)?;
        Ok(Arc::new(ErasedClient(CassandraHighLevelClient::new(
            cassandra, mode, producer, consumers,
        )?)))
    }
}

struct ErasedClient<T, C, B>(HighLevelClient<T, C, B>)
where
    C: Codec,
    C::Payload: EventIdentity,
    B: ClientBackend<C>;

#[async_trait]
impl<T, C, B> ErasedHighLevelClient<T, C> for ErasedClient<T, C, B>
where
    T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
    C: Codec + Send + Sync,
    C::Payload: EventIdentity + EventType + Clone,
    B: ClientBackend<C>,
    B::Reader: ConsumerReaderBackend<C>,
{
    async fn send(
        &self,
        topic: Topic,
        key: String,
        payload: C::Payload,
    ) -> Result<(), HighLevelClientError<C::Error>> {
        self.0.send(topic, &key, payload).await
    }

    async fn subscribe(&self, handler: T) -> Result<(), HighLevelClientError<C::Error>> {
        self.0.subscribe_inner(handler).await
    }

    async fn unsubscribe(&self) -> Result<(), HighLevelClientError<C::Error>> {
        self.0.unsubscribe().await
    }

    async fn consumer_state(&self) -> ErasedConsumerState<T> {
        match &*self.0.consumer_state().await {
            ConsumerState::Unconfigured => ErasedConsumerState::Unconfigured,
            ConsumerState::ConfigurationFailed(error) => {
                ErasedConsumerState::ConfigurationFailed(error.to_string())
            }
            ConsumerState::Configured { config, .. } => {
                ErasedConsumerState::Configured(erased_config(config))
            }
            ConsumerState::Running {
                config, handler, ..
            } => ErasedConsumerState::Running {
                config: erased_config(config),
                handler: handler.clone(),
            },
        }
    }

    async fn value_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedValueReader<C>, ErasedReaderBuildError<C::Error>> {
        readers::value(&self.0, subsystem, name, cache).await
    }

    async fn map_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedMapReader<C>, ErasedReaderBuildError<C::Error>> {
        readers::map(&self.0, subsystem, name, cache).await
    }

    async fn deque_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedDequeReader<C>, ErasedReaderBuildError<C::Error>> {
        readers::deque(&self.0, subsystem, name, cache).await
    }

    async fn assigned_partition_count(&self) -> u32 {
        self.0.assigned_partition_count().await
    }

    async fn is_stalled(&self) -> bool {
        self.0.is_stalled().await
    }

    fn producer_config(&self) -> &ProducerConfiguration {
        self.0.producer_config()
    }

    fn propagator(&self) -> &TextMapCompositePropagator {
        self.0.propagator()
    }

    fn source_system(&self) -> &str {
        self.0.source_system()
    }
}

fn erased_config(config: &ModeConfiguration) -> ErasedConsumerConfiguration {
    let consumer = config.consumer_config();
    ErasedConsumerConfiguration {
        mode: config.mode(),
        topics: consumer.subscribed_topics.clone(),
        group_id: consumer.group_id.clone(),
    }
}

/// Failure to construct an erased FFI client.
#[derive(Debug, Error)]
pub enum ErasedClientBuildError<E>
where
    E: StdError + Send + Sync + 'static,
{
    /// The existing mock-mode environment override could not be parsed.
    #[error(transparent)]
    MockConfiguration(#[from] MockConfigurationError),
    /// A live client requires Cassandra storage configuration.
    #[error("Cassandra configuration is required when mock mode is disabled")]
    MissingCassandra,
    /// Concrete client construction failed.
    #[error(transparent)]
    Client(#[from] HighLevelClientError<E>),
}
