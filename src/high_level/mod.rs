//! High-level client module for managing both producer and consumer operations.
//!
//! This module provides a unified interface for message production and
//! consumption in various operational modes through the `HighLevelClient`
//! struct.

use crate::consumer::failure::FallibleHandler;
use crate::consumer::failure::retry::RetryConfigurationBuilder;
use crate::consumer::failure::topic::FailureTopicConfigurationBuilder;
use crate::consumer::{ConsumerConfigurationBuilder, ConsumerError, ProsodyConsumer};
use crate::high_level::config::ModeConfiguration;
use crate::high_level::mode::Mode;
use crate::high_level::state::{ConsumerState, ConsumerStateView};
use crate::producer::{
    ProducerConfiguration, ProducerConfigurationBuilder, ProducerConfigurationBuilderError,
    ProducerError, ProsodyProducer,
};
use crate::propagator::new_propagator;
use crate::timers::store::cassandra::CassandraConfigurationBuilder;
use crate::{Payload, Topic};
use internment::Intern;
use opentelemetry::propagation::TextMapCompositePropagator;
use std::mem::take;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::info;

pub mod config;
pub mod mode;
pub mod state;

/// A combined client that manages both producer and consumer operations.
#[derive(Debug)]
pub struct HighLevelClient<T> {
    producer: ProsodyProducer,
    producer_config: ProducerConfiguration,
    consumer: Mutex<ConsumerState<T>>,
    propagator: TextMapCompositePropagator,
}

impl<T> HighLevelClient<T> {
    /// Returns a reference to the internal `ProsodyProducer`.
    pub fn producer(&self) -> &ProsodyProducer {
        &self.producer
    }

    /// Returns a reference to the producer configuration.
    pub fn producer_config(&self) -> &ProducerConfiguration {
        &self.producer_config
    }

    /// Returns a view of the current consumer state.
    pub async fn consumer_state(&self) -> ConsumerStateView<'_, T> {
        ConsumerStateView(self.consumer.lock().await)
    }

    /// Returns a reference to the OpenTelemetry propagator.
    pub fn propagator(&self) -> &TextMapCompositePropagator {
        &self.propagator
    }

    /// Creates a new `HighLevelClient` with the specified configurations.
    ///
    /// # Arguments
    ///
    /// * `mode` - The operational mode for the client.
    /// * `producer_builder` - Builder for the producer configuration.
    /// * `consumer_builder` - Builder for the consumer configuration.
    /// * `retry_builder` - Builder for the retry configuration.
    /// * `failure_topic_builder` - Builder for the failure topic configuration.
    /// * `cassandra_builder` - Builder for the Cassandra configuration.
    ///
    /// # Errors
    ///
    /// Returns a `HighLevelClientError` if:
    /// - Any of the configuration builds fail.
    /// - Producer initialization fails.
    /// - Required topics are not found.
    pub fn new(
        mode: Mode,
        producer_builder: &mut ProducerConfigurationBuilder,
        consumer_builder: &ConsumerConfigurationBuilder,
        retry_builder: &RetryConfigurationBuilder,
        failure_topic_builder: &FailureTopicConfigurationBuilder,
        cassandra_builder: &CassandraConfigurationBuilder,
    ) -> Result<Self, HighLevelClientError> {
        // Set the producer source system to the consumer group if unspecified
        if let (None, Some(group_id)) = (
            producer_builder.configured_source_system(),
            consumer_builder.configured_consumer_group(),
        ) {
            producer_builder.source_system(group_id);
        }

        let producer_config = producer_builder.build()?;
        let cloned_config = producer_config.clone();
        let producer = match mode {
            Mode::Pipeline => ProsodyProducer::pipeline_producer(cloned_config),
            Mode::LowLatency => ProsodyProducer::low_latency_producer(cloned_config),
            Mode::BestEffort => ProsodyProducer::best_effort_producer(cloned_config),
        }?;

        let consumer_state = ConsumerState::build(
            mode,
            consumer_builder,
            retry_builder,
            failure_topic_builder,
            cassandra_builder,
        );

        // Check for topic existence only if not in mock mode
        if !producer_config.mock {
            check_topic_existence(&producer, &consumer_state)?;
        }

        let consumer = Mutex::new(consumer_state);

        Ok(Self {
            producer,
            producer_config,
            consumer,
            propagator: new_propagator(),
        })
    }

    /// Sends a message to the specified topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to send the message to.
    /// * `key` - The key associated with the message.
    /// * `payload` - The payload of the message.
    ///
    /// # Errors
    ///
    /// Returns a `HighLevelClientError` if the send operation fails.
    pub async fn send(
        &self,
        topic: Topic,
        key: &str,
        payload: &Payload,
    ) -> Result<(), HighLevelClientError> {
        self.producer.send([], topic, key, payload).await?;
        Ok(())
    }

    /// Subscribes the consumer with the provided handler.
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler to process consumed messages.
    ///
    /// # Errors
    ///
    /// Returns a `HighLevelClientError` if:
    /// - The consumer is unconfigured.
    /// - The consumer is already subscribed.
    /// - Consumer initialization fails.
    pub async fn subscribe(&self, handler: T) -> Result<(), HighLevelClientError>
    where
        T: FallibleHandler,
    {
        let mut guard = self.consumer.lock().await;
        let consumer_ref = &mut *guard;

        let config = match take(consumer_ref) {
            ConsumerState::Unconfigured => return Err(HighLevelClientError::UnconfiguredConsumer),
            ConsumerState::Configured(config) => config,
            running @ ConsumerState::Running { .. } => {
                *consumer_ref = running;
                return Err(HighLevelClientError::AlreadySubscribed);
            }
        };

        // Initialize the consumer based on the mode configuration
        let consumer = match &config {
            ModeConfiguration::Pipeline {
                consumer,
                retry,
                trigger_store,
                ..
            } => {
                ProsodyConsumer::pipeline_consumer(
                    consumer,
                    trigger_store,
                    retry.clone(),
                    handler.clone(),
                )
                .await?
            }
            ModeConfiguration::LowLatency {
                consumer,
                retry,
                failure_topic,
                trigger_store,
                ..
            } => {
                ProsodyConsumer::low_latency_consumer(
                    consumer,
                    trigger_store,
                    retry.clone(),
                    failure_topic.clone(),
                    self.producer.clone(),
                    handler.clone(),
                )
                .await?
            }
            ModeConfiguration::BestEffort {
                consumer,
                trigger_store,
                ..
            } => {
                ProsodyConsumer::best_effort_consumer(consumer, trigger_store, handler.clone())
                    .await?
            }
        };

        *consumer_ref = ConsumerState::Running {
            consumer,
            config,
            handler,
        };

        Ok(())
    }

    /// Unsubscribes the consumer.
    ///
    /// # Errors
    ///
    /// Returns a `HighLevelClientError` if the consumer is not currently
    /// subscribed.
    pub async fn unsubscribe(&self) -> Result<(), HighLevelClientError> {
        let consumer = {
            let mut guard = self.consumer.lock().await;
            let consumer_ref = &mut *guard;

            match take(consumer_ref) {
                state @ (ConsumerState::Unconfigured | ConsumerState::Configured(_)) => {
                    *consumer_ref = state;
                    return Err(HighLevelClientError::NotSubscribed);
                }
                ConsumerState::Running {
                    consumer, config, ..
                } => {
                    *consumer_ref = ConsumerState::Configured(config);
                    consumer
                }
            }
        };

        info!("shutting down consumer");
        consumer.shutdown().await;
        Ok(())
    }

    /// Returns the number of partitions assigned to the consumer.
    ///
    /// Returns 0 if the consumer is not in the Running state.
    pub async fn assigned_partition_count(&self) -> u32 {
        let ConsumerState::Running { ref consumer, .. } = *self.consumer_state().await else {
            return 0;
        };

        consumer.assigned_partition_count()
    }

    /// Checks if the consumer is stalled.
    ///
    /// Returns `false` if the consumer is not in the Running state.
    pub async fn is_stalled(&self) -> bool {
        let ConsumerState::Running { ref consumer, .. } = *self.consumer_state().await else {
            return false;
        };

        consumer.is_stalled()
    }
}

/// Checks if all required topics exist for the given consumer state.
///
/// # Arguments
///
/// * `producer` - The producer used to fetch metadata.
/// * `consumer_state` - The current state of the consumer.
///
/// # Errors
///
/// Returns a `HighLevelClientError` if any required topics are missing.
fn check_topic_existence<S>(
    producer: &ProsodyProducer,
    consumer_state: &ConsumerState<S>,
) -> Result<(), HighLevelClientError> {
    let ConsumerState::Configured(mode_config) = &consumer_state else {
        return Ok(());
    };

    let missing_topics = missing_topics(producer, mode_config.configured_topics())?;
    if missing_topics.is_empty() {
        Ok(())
    } else {
        Err(HighLevelClientError::TopicsNotFound(missing_topics))
    }
}

/// Identifies which topics from the given list are missing in the Kafka
/// cluster.
///
/// # Arguments
///
/// * `producer` - The producer used to fetch metadata.
/// * `topics` - A list of topics to check for existence.
///
/// # Errors
///
/// Returns a `ProducerError` if metadata fetching fails.
fn missing_topics(
    producer: &ProsodyProducer,
    mut topics: Vec<Topic>,
) -> Result<Vec<Topic>, ProducerError> {
    const TIMEOUT: Duration = Duration::from_secs(60);
    let metadata = producer.kafka_client().fetch_metadata(None, TIMEOUT)?;

    topics.sort_unstable();
    topics.dedup();

    for metadata_topic in metadata.topics() {
        let topic_name = metadata_topic.name();
        let Some(position) = topics
            .iter()
            .position(|&topic| Intern::as_ref(topic) == topic_name)
        else {
            continue;
        };

        topics.swap_remove(position);
        if topics.is_empty() {
            return Ok(topics);
        }
    }

    Ok(topics)
}

/// Errors that can occur in the `HighLevelClient` operations.
#[derive(Debug, Error)]
pub enum HighLevelClientError {
    /// Error when the producer configuration is invalid.
    #[error("invalid producer configuration: {0:#}")]
    ProducerConfiguration(#[from] ProducerConfigurationBuilderError),

    /// Error when initializing the producer fails.
    #[error("failed to initialize producer: {0:#}")]
    Producer(#[from] ProducerError),

    /// Error when initializing the consumer fails.
    #[error("failed to initialize consumer: {0:#}")]
    Consumer(#[from] ConsumerError),

    /// Error when attempting to use an unconfigured consumer.
    #[error("unconfigured consumer; client does not have a valid consumer configuration")]
    UnconfiguredConsumer,

    /// Error when attempting to subscribe an already subscribed consumer.
    #[error("consumer is already subscribed")]
    AlreadySubscribed,

    /// Error when attempting to unsubscribe a not subscribed consumer.
    #[error("consumer is not subscribed")]
    NotSubscribed,

    /// Error when required topics are not found in the Kafka cluster.
    #[error("topics not found: {}", .0.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(", "))]
    TopicsNotFound(Vec<Topic>),
}
