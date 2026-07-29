//! Optional type erasure for foreign-language client wrappers.

use crate::cassandra::config::CassandraConfiguration;
use crate::consumer::middleware::FallibleHandler;
use crate::high_level::config::ModeConfiguration;
use crate::high_level::state::ConsumerState;
use crate::high_level::{
    CassandraClientBackend, ConsumerBuilders, HighLevelClient, HighLevelClientError,
    MemoryClientBackend, Mode,
};
use crate::producer::{ProducerConfiguration, ProducerConfigurationBuilder};
use crate::{Codec, EventIdentity, EventType, Topic};
use async_trait::async_trait;
use opentelemetry::propagation::TextMapCompositePropagator;
use std::error::Error as StdError;
use std::sync::Arc;
use thiserror::Error;

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
    async fn erased_send(
        &self,
        topic: Topic,
        key: String,
        payload: C::Payload,
    ) -> Result<(), HighLevelClientError<C::Error>>;
    /// Starts consuming.
    async fn erased_subscribe(&self, handler: T) -> Result<(), HighLevelClientError<C::Error>>;
    /// Stops consuming.
    async fn erased_unsubscribe(&self) -> Result<(), HighLevelClientError<C::Error>>;
    /// Returns the current lifecycle state.
    async fn erased_consumer_state(&self) -> ErasedConsumerState<T>;
    /// Returns the assigned partition count.
    async fn erased_assigned_partition_count(&self) -> u32;
    /// Reports whether any consumer heartbeat is stalled.
    async fn erased_is_stalled(&self) -> bool;
    /// Producer configuration.
    fn erased_producer_config(&self) -> &ProducerConfiguration;
    /// Trace-context propagator.
    fn erased_propagator(&self) -> &TextMapCompositePropagator;
    /// Configured source system.
    fn erased_source_system(&self) -> &str;
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
    mock: bool,
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
    if mock {
        Ok(Arc::new(HighLevelClient::new(
            MemoryClientBackend::new(),
            mode,
            producer,
            consumers,
        )?))
    } else {
        let cassandra = cassandra.ok_or(ErasedClientBuildError::MissingCassandra)?;
        Ok(Arc::new(HighLevelClient::new(
            CassandraClientBackend::new(cassandra),
            mode,
            producer,
            consumers,
        )?))
    }
}

macro_rules! impl_erased_client {
    ($backend:ty) => {
        #[async_trait]
        impl<T, C> ErasedHighLevelClient<T, C> for HighLevelClient<T, C, $backend>
        where
            T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
            C: Codec + Send + Sync,
            C::Payload: EventIdentity + EventType + Clone,
        {
            async fn erased_send(
                &self,
                topic: Topic,
                key: String,
                payload: C::Payload,
            ) -> Result<(), HighLevelClientError<C::Error>> {
                HighLevelClient::send(self, topic, &key, payload).await
            }

            async fn erased_subscribe(
                &self,
                handler: T,
            ) -> Result<(), HighLevelClientError<C::Error>> {
                self.subscribe(handler).await
            }

            async fn erased_unsubscribe(&self) -> Result<(), HighLevelClientError<C::Error>> {
                HighLevelClient::unsubscribe(self).await
            }

            async fn erased_consumer_state(&self) -> ErasedConsumerState<T> {
                match &*HighLevelClient::consumer_state(self).await {
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

            async fn erased_assigned_partition_count(&self) -> u32 {
                HighLevelClient::assigned_partition_count(self).await
            }

            async fn erased_is_stalled(&self) -> bool {
                HighLevelClient::is_stalled(self).await
            }

            fn erased_producer_config(&self) -> &ProducerConfiguration {
                HighLevelClient::producer_config(self)
            }

            fn erased_propagator(&self) -> &TextMapCompositePropagator {
                HighLevelClient::propagator(self)
            }

            fn erased_source_system(&self) -> &str {
                HighLevelClient::source_system(self)
            }
        }
    };
}

impl_erased_client!(MemoryClientBackend<C>);
impl_erased_client!(CassandraClientBackend<C>);

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
    /// A live client requires Cassandra storage configuration.
    #[error("Cassandra configuration is required when mock mode is disabled")]
    MissingCassandra,
    /// Concrete client construction failed.
    #[error(transparent)]
    Client(#[from] HighLevelClientError<E>),
}
