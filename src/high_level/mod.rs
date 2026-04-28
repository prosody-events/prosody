//! High-level client module for managing both producer and consumer operations.
//!
//! This module provides a unified interface for message production and
//! consumption in various operational modes through the `HighLevelClient`
//! struct.

use crate::cassandra::config::CassandraConfigurationBuilder;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::deduplication::DeduplicationConfigurationBuilder;
use crate::consumer::middleware::defer::DeferConfigurationBuilder;
use crate::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
use crate::consumer::middleware::retry::RetryConfigurationBuilder;
use crate::consumer::middleware::scheduler::{SchedulerConfigurationBuilder, SchedulerInitError};
use crate::consumer::middleware::timeout::TimeoutConfigurationBuilder;
use crate::consumer::middleware::topic::FailureTopicConfigurationBuilder;
use crate::consumer::{
    ConsumerConfigurationBuilder, ConsumerError, LowLatencyMiddlewareConfiguration,
    PipelineMiddlewareConfiguration, ProsodyConsumer,
};
use crate::high_level::config::{
    ModeConfiguration, ModeConfigurationBuildParams, ModeConfigurationError,
};
pub use crate::high_level::mode::Mode;
use crate::high_level::state::{ConsumerState, ConsumerStateView};
use crate::producer::{
    ProducerConfiguration, ProducerConfigurationBuilder, ProducerConfigurationBuilderError,
    ProducerError, ProsodyProducer,
};
use crate::propagator::new_propagator;
use crate::telemetry::emitter::TelemetryEmitterConfiguration;
use crate::telemetry::{EmitterError, Telemetry, spawn_telemetry_emitter};
use crate::{Codec, JsonCodec, Topic};
use opentelemetry::propagation::TextMapCompositePropagator;
use std::mem::take;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::info;

pub mod config;
pub mod mode;
pub mod state;

#[cfg(test)]
mod tests;

/// Builder configuration for consumer and middleware components.
///
/// Bundles all consumer-related configuration builders to reduce parameter
/// count in `HighLevelClient::new`.
#[derive(Default)]
pub struct ConsumerBuilders {
    /// Consumer configuration builder.
    pub consumer: ConsumerConfigurationBuilder,
    /// Retry middleware configuration builder.
    pub retry: RetryConfigurationBuilder,
    /// Failure topic middleware configuration builder.
    pub failure_topic: FailureTopicConfigurationBuilder,
    /// Scheduler middleware configuration builder.
    pub scheduler: SchedulerConfigurationBuilder,
    /// Monopolization middleware configuration builder.
    pub monopolization: MonopolizationConfigurationBuilder,
    /// Defer middleware configuration builder.
    pub defer: DeferConfigurationBuilder,
    /// Deduplication middleware configuration builder.
    pub dedup: DeduplicationConfigurationBuilder,
    /// Timeout middleware configuration builder.
    pub timeout: TimeoutConfigurationBuilder,
    /// Telemetry emitter configuration.
    pub emitter: TelemetryEmitterConfiguration,
}

/// A combined client that manages both producer and consumer operations.
#[derive(Debug)]
pub struct HighLevelClient<T, C: Codec = JsonCodec>
where
    C::Payload: crate::EventIdentity,
{
    producer: ProsodyProducer<C>,
    producer_config: ProducerConfiguration,
    consumer: Mutex<ConsumerState<T, C>>,
    propagator: TextMapCompositePropagator,
    telemetry: Telemetry,
}

impl<T, C: Codec> HighLevelClient<T, C>
where
    C::Payload: crate::EventIdentity,
{
    /// Returns a reference to the internal `ProsodyProducer`.
    pub fn producer(&self) -> &ProsodyProducer<C> {
        &self.producer
    }

    /// Returns a reference to the producer configuration.
    pub fn producer_config(&self) -> &ProducerConfiguration {
        &self.producer_config
    }

    /// Returns a view of the current consumer state.
    pub async fn consumer_state(&self) -> ConsumerStateView<'_, T, C> {
        ConsumerStateView(self.consumer.lock().await)
    }

    /// Returns a reference to the OpenTelemetry propagator.
    pub fn propagator(&self) -> &TextMapCompositePropagator {
        &self.propagator
    }

    /// Returns the configured source system identifier.
    ///
    /// The source system is used to identify the originating service or
    /// component in produced messages, enabling message tracing and loop
    /// detection.
    ///
    /// # Returns
    ///
    /// A string slice containing the source system identifier.
    #[must_use]
    pub fn source_system(&self) -> &str {
        &self.producer_config.source_system
    }

    /// Returns a reference to the shared telemetry instance.
    pub fn telemetry(&self) -> &Telemetry {
        &self.telemetry
    }

    /// Creates a new `HighLevelClient` with the specified configurations.
    ///
    /// # Arguments
    ///
    /// * `mode` - The operational mode for the client.
    /// * `producer_builder` - Builder for the producer configuration.
    /// * `consumer_builders` - Bundled consumer and middleware configuration
    ///   builders.
    /// * `cassandra_builder` - Builder for the Cassandra configuration.
    ///
    /// # Errors
    ///
    /// Returns a `HighLevelClientError` if:
    /// - Any of the configuration builds fail.
    /// - Producer initialization fails.
    /// - Required topics are not found.
    /// - The telemetry emitter cannot be started.
    pub fn new(
        mode: Mode,
        producer_builder: &mut ProducerConfigurationBuilder,
        consumer_builders: &ConsumerBuilders,
        cassandra_builder: &CassandraConfigurationBuilder,
    ) -> Result<Self, HighLevelClientError<C::Error>> {
        // Set the producer source system to the consumer group if unspecified
        if let (None, Some(group_id)) = (
            producer_builder.configured_source_system(),
            consumer_builders.consumer.configured_consumer_group(),
        ) {
            producer_builder.source_system(group_id);
        }

        let producer_config = producer_builder.build()?;
        let cloned_config = producer_config.clone();
        let telemetry = Telemetry::new();
        let producer: ProsodyProducer<C> = match mode {
            Mode::Pipeline => ProsodyProducer::pipeline_producer(cloned_config, telemetry.sender()),
            Mode::LowLatency => {
                ProsodyProducer::low_latency_producer(cloned_config, telemetry.sender())
            }
            Mode::BestEffort => {
                ProsodyProducer::best_effort_producer(cloned_config, telemetry.sender())
            }
        }?;

        spawn_telemetry_emitter(
            &consumer_builders.emitter,
            &producer_config.bootstrap_servers,
            &telemetry,
        )?;

        let consumer_state = ConsumerState::build(&ModeConfigurationBuildParams {
            mode,
            consumer_builder: &consumer_builders.consumer,
            retry_builder: &consumer_builders.retry,
            failure_topic_builder: &consumer_builders.failure_topic,
            scheduler_builder: &consumer_builders.scheduler,
            monopolization_builder: &consumer_builders.monopolization,
            defer_builder: &consumer_builders.defer,
            dedup_builder: &consumer_builders.dedup,
            timeout_builder: &consumer_builders.timeout,
            cassandra_builder,
        });

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
            telemetry,
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
        payload: &C::Payload,
    ) -> Result<(), HighLevelClientError<C::Error>> {
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
    pub async fn subscribe(&self, handler: T) -> Result<(), HighLevelClientError<C::Error>>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone,
        C::Payload: crate::EventType + Clone,
    {
        let mut guard = self.consumer.lock().await;
        let consumer_ref = &mut *guard;

        let config = match take(consumer_ref) {
            ConsumerState::Unconfigured => return Err(HighLevelClientError::UnconfiguredConsumer),
            ConsumerState::ConfigurationFailed(error) => {
                return Err(HighLevelClientError::ConsumerConfiguration(error));
            }
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
                monopolization,
                defer,
                dedup,
                common,
                trigger_store,
            } => {
                ProsodyConsumer::<C>::pipeline_consumer(
                    consumer,
                    trigger_store,
                    PipelineMiddlewareConfiguration {
                        retry: retry.clone(),
                        monopolization: monopolization.clone(),
                        defer: defer.clone(),
                        dedup: dedup.clone(),
                    },
                    common,
                    self.telemetry.clone(),
                    handler.clone(),
                )
                .await?
            }
            ModeConfiguration::LowLatency {
                consumer,
                retry,
                failure_topic,
                common,
                trigger_store,
                ..
            } => {
                ProsodyConsumer::low_latency_consumer(
                    consumer,
                    trigger_store,
                    LowLatencyMiddlewareConfiguration {
                        retry: retry.clone(),
                        failure_topic: failure_topic.clone(),
                    },
                    common,
                    self.producer.clone(),
                    self.telemetry.clone(),
                    handler.clone(),
                )
                .await?
            }
            ModeConfiguration::BestEffort {
                consumer,
                common,
                trigger_store,
                ..
            } => {
                ProsodyConsumer::<C>::best_effort_consumer(
                    consumer,
                    trigger_store,
                    common,
                    self.telemetry.clone(),
                    handler.clone(),
                )
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
    pub async fn unsubscribe(&self) -> Result<(), HighLevelClientError<C::Error>> {
        let consumer = {
            let mut guard = self.consumer.lock().await;
            let consumer_ref = &mut *guard;

            match take(consumer_ref) {
                state @ (ConsumerState::Unconfigured
                | ConsumerState::ConfigurationFailed(_)
                | ConsumerState::Configured(_)) => {
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
fn check_topic_existence<S, C: Codec, D: Codec>(
    producer: &ProsodyProducer<C>,
    consumer_state: &ConsumerState<S, D>,
) -> Result<(), HighLevelClientError<C::Error>>
where
    C::Payload: crate::EventIdentity,
{
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
fn missing_topics<C: Codec>(
    producer: &ProsodyProducer<C>,
    mut topics: Vec<Topic>,
) -> Result<Vec<Topic>, ProducerError<C::Error>>
where
    C::Payload: crate::EventIdentity,
{
    const TIMEOUT: Duration = Duration::from_mins(1);
    let metadata = producer.kafka_client().fetch_metadata(None, TIMEOUT)?;

    topics.sort_unstable();
    topics.dedup();

    // Filter out topics that start with '^' as they are pattern-based subscriptions
    topics.retain(|topic| !topic.starts_with('^'));

    for metadata_topic in metadata.topics() {
        let topic_name = metadata_topic.name();
        let Some(position) = topics
            .iter()
            .position(|&topic| topic.as_ref() == topic_name)
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
pub enum HighLevelClientError<E> {
    /// Error when the producer configuration is invalid.
    #[error("invalid producer configuration: {0:#}")]
    ProducerConfiguration(#[from] ProducerConfigurationBuilderError),

    /// Error when initializing the producer fails.
    #[error("failed to initialize producer: {0:#}")]
    Producer(#[from] ProducerError<E>),

    /// Error when initializing the consumer fails.
    #[error("failed to initialize consumer: {0:#}")]
    Consumer(#[from] ConsumerError),

    /// Error when the scheduler configuration is invalid.
    #[error("invalid scheduler configuration: {0:#}")]
    SchedulerConfiguration(#[from] SchedulerInitError),

    /// Error when attempting to use an unconfigured consumer.
    #[error("unconfigured consumer; client does not have a valid consumer configuration")]
    UnconfiguredConsumer,

    /// Error when the consumer configuration failed during build.
    #[error("consumer configuration failed: {0:#}")]
    ConsumerConfiguration(ModeConfigurationError),

    /// Error when attempting to subscribe an already subscribed consumer.
    #[error("consumer is already subscribed")]
    AlreadySubscribed,

    /// Error when attempting to unsubscribe a not subscribed consumer.
    #[error("consumer is not subscribed")]
    NotSubscribed,

    /// Error when required topics are not found in the Kafka cluster.
    #[error("topics not found: {}", .0.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(", "))]
    TopicsNotFound(Vec<Topic>),

    /// Error when the telemetry emitter cannot be started.
    #[error("failed to start telemetry emitter: {0:#}")]
    TelemetryEmitter(#[from] EmitterError),
}
