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
    ConsumerConfigurationBuilder, ConsumerError, KeyedStateConfiguration,
    LowLatencyMiddlewareConfiguration, PipelineMiddlewareConfiguration, ProsodyConsumer,
};
use crate::high_level::config::{
    ModeConfiguration, ModeConfigurationBuildParams, ModeConfigurationError,
    TriggerStoreConfiguration,
};
pub use crate::high_level::mode::Mode;
use crate::high_level::state::{ConsumerState, ConsumerStateView};
use crate::loader::MemoryLoader;
use crate::producer::{
    ProducerConfiguration, ProducerConfigurationBuilder, ProducerConfigurationBuilderError,
    ProducerError, ProsodyProducer,
};
use crate::propagator::new_propagator;
use crate::state::descriptor::{Registered, StateDescriptor};
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state_reader::{
    DEFAULT_READER_CACHE_SIZE_BYTES, SharedDeps, StateReader, StateReaderError,
};
use crate::subsystem::SubsystemName;
use crate::telemetry::emitter::TelemetryEmitterConfiguration;
use crate::telemetry::{EmitterError, Telemetry, spawn_telemetry_emitter};
use crate::{Codec, JsonCodec, Topic};
use educe::Educe;
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
    /// Keyed-state configuration (always-on; carries collection
    /// registrations). Mode-independent — every mode threads it through.
    pub keyed_state: KeyedStateConfiguration,
    /// Telemetry emitter configuration.
    pub emitter: TelemetryEmitterConfiguration,
}

/// A combined client that manages both producer and consumer operations.
#[derive(Educe)]
#[educe(Debug)]
pub struct HighLevelClient<T, C: Codec = JsonCodec>
where
    C::Payload: crate::EventIdentity,
{
    producer: ProsodyProducer<C>,
    producer_config: ProducerConfiguration,
    consumer: Mutex<ConsumerState<T, C>>,
    propagator: TextMapCompositePropagator,
    telemetry: Telemetry,
    /// The one shared infrastructure bundle this client owns for its whole
    /// lifecycle. Built lazily at the first point of need ([`Self::state`]
    /// while `Configured`, or [`Self::subscribe`]), retained across
    /// `Configured→Running`, and handed to the consumer it starts, so no state
    /// transition constructs a second session/loader/memory store.
    ///
    /// Lock order: acquire `consumer` before `deps`, always.
    #[educe(Debug(ignore))]
    deps: Mutex<Option<SharedDeps<C>>>,
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

        // Mock mode is offline: the emitter opens no real broker connection
        // (just as `check_topic_existence` below is skipped). The returned
        // spawn flag is unused here.
        spawn_telemetry_emitter(
            &consumer_builders.emitter,
            &producer_config.bootstrap_servers,
            &telemetry,
            producer_config.mock,
        )?;

        let consumer_state = ConsumerState::build(&ModeConfigurationBuildParams {
            mode,
            consumer_builders,
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
            deps: Mutex::new(None),
        })
    }

    /// Sends a message to the specified topic.
    ///
    /// # Errors
    ///
    /// Returns a `HighLevelClientError` if the send operation fails.
    pub async fn send(
        &self,
        topic: Topic,
        key: &str,
        payload: C::Payload,
    ) -> Result<(), HighLevelClientError<C::Error>> {
        self.producer.send([], topic, key, payload).await?;
        Ok(())
    }

    /// Registers a keyed-state collection, returning the [`Registered`]
    /// capability handle a handler binds via `ctx.state(...)`.
    ///
    /// Call this while the consumer is `Configured`, before
    /// [`subscribe`](Self::subscribe) freezes the registrations into the
    /// running consumer. Tokens survive the `unsubscribe`/re-subscribe cycle,
    /// so a re-subscribe needs no re-registration.
    ///
    /// # Errors
    ///
    /// Returns [`HighLevelClientError::AlreadySubscribed`] when the consumer
    /// is already running (registrations are frozen), or
    /// [`HighLevelClientError::UnconfiguredConsumer`] when there is no valid
    /// consumer configuration to register against.
    pub async fn register<D>(
        &self,
        descriptor: D,
    ) -> Result<Registered<D>, HighLevelClientError<C::Error>>
    where
        D: StateDescriptor,
    {
        let mut guard = self.consumer.lock().await;
        match &mut *guard {
            ConsumerState::Configured(config) => Ok(config.register(descriptor)),
            ConsumerState::Running { .. } => Err(HighLevelClientError::AlreadySubscribed),
            ConsumerState::Unconfigured | ConsumerState::ConfigurationFailed(_) => {
                Err(HighLevelClientError::UnconfiguredConsumer)
            }
        }
    }

    /// The client's retained shared bundle, if one has been built. The test
    /// hook the composition suite uses to seed committed state into the exact
    /// stores the running consumer and the client's readers share.
    #[cfg(test)]
    pub(crate) async fn retained_deps(&self) -> Option<SharedDeps<C>> {
        self.deps.lock().await.clone()
    }

    /// Returns the client's one shared infrastructure bundle, building it from
    /// `mode` on first call and retaining it across the Configured -> Running
    /// transition. `unsubscribe` drops it (see there); the next `subscribe`
    /// rebuilds a fresh bundle.
    ///
    /// The caller must already hold the `consumer` lock (this locks `deps`
    /// under it — the invariant lock order). A second call clones the retained
    /// handle; it never constructs a second session/loader/memory store.
    async fn shared_deps(
        &self,
        mode: &ModeConfiguration,
    ) -> Result<SharedDeps<C>, HighLevelClientError<C::Error>>
    where
        C::Payload: Clone,
    {
        let mut guard = self.deps.lock().await;
        if let Some(deps) = guard.as_ref() {
            return Ok(deps.clone());
        }
        let (consumer, keyed_state, trigger_store) = match mode {
            ModeConfiguration::Pipeline {
                consumer,
                common,
                trigger_store,
                ..
            }
            | ModeConfiguration::LowLatency {
                consumer,
                common,
                trigger_store,
                ..
            }
            | ModeConfiguration::BestEffort {
                consumer,
                common,
                trigger_store,
            } => (consumer, &common.keyed_state, trigger_store),
        };
        // One knob: the reader cache follows `read_cache_size_bytes`, then
        // `cache_size_bytes`, then the built-in default.
        let budget = keyed_state
            .read_cache_size_bytes
            .or(keyed_state.cache_size_bytes)
            .unwrap_or(DEFAULT_READER_CACHE_SIZE_BYTES);
        let deps = match trigger_store {
            TriggerStoreConfiguration::InMemory => SharedDeps::memory(
                consumer.group_id.clone(),
                consumer.stall_threshold,
                MemoryCells::new(),
                MemoryPublicationStore::new(),
                MemoryDescriptorIdentityStore::new(),
                MemoryLoader::new(),
                budget.get(),
            ),
            TriggerStoreConfiguration::Cassandra(cassandra) => {
                SharedDeps::connect(consumer, cassandra, budget).await?
            }
        };
        *guard = Some(deps.clone());
        Ok(deps)
    }

    /// Composes a standalone [`StateReader`] over this client's one shared
    /// bundle, for `descriptor` routed under `subsystem`.
    ///
    /// Valid once the consumer is `Configured` or `Running` — both draw from
    /// the same retained bundle, so a reader minted before `subscribe` and one
    /// minted after share one session/loader/memory store.
    ///
    /// # Errors
    ///
    /// [`HighLevelClientError::UnconfiguredConsumer`] if the client has no
    /// consumer configuration; [`HighLevelClientError::StateReader`] if the
    /// bundle cannot connect or the descriptor is rejected.
    pub async fn state<D>(
        &self,
        subsystem: SubsystemName,
        descriptor: D,
    ) -> Result<StateReader<D, C>, HighLevelClientError<C::Error>>
    where
        D: StateDescriptor,
        C::Payload: Clone,
    {
        let guard = self.consumer.lock().await;
        let mode = match &*guard {
            ConsumerState::Configured(config) | ConsumerState::Running { config, .. } => config,
            ConsumerState::Unconfigured | ConsumerState::ConfigurationFailed(_) => {
                return Err(HighLevelClientError::UnconfiguredConsumer);
            }
        };
        let deps = self.shared_deps(mode).await?;
        StateReader::new(&deps, subsystem, descriptor).map_err(HighLevelClientError::StateReader)
    }

    /// Subscribes the consumer with the provided handler.
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

        // Build (or reuse) the one shared bundle while the config is still
        // available — before `take` — so the running consumer and any reader
        // share it. Only `Configured` has a config to build from; other states
        // fall through to their existing errors below.
        let deps = match &*guard {
            ConsumerState::Configured(config) => Some(self.shared_deps(config).await?),
            _ => None,
        };

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
                    },
                    common,
                    self.telemetry.clone(),
                    handler.clone(),
                    deps,
                )
                .await?
            }
            ModeConfiguration::LowLatency {
                consumer,
                retry,
                failure_topic,
                common,
                trigger_store,
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
                    deps,
                )
                .await?
            }
            ModeConfiguration::BestEffort {
                consumer,
                common,
                trigger_store,
            } => {
                ProsodyConsumer::<C>::best_effort_consumer(
                    consumer,
                    trigger_store,
                    common,
                    self.telemetry.clone(),
                    handler.clone(),
                    deps,
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

            let consumer = match take(consumer_ref) {
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
            };

            // Drop the retained bundle. Its heartbeat registry holds this
            // consumer's poll-loop heartbeat, which stops beating at shutdown;
            // reusing the same registry on a later `subscribe` would fold that
            // permanently-dead heartbeat into `is_stalled` forever and grow the
            // registry without a removal path. The next `subscribe` rebuilds a
            // fresh bundle. Cleared while holding the consumer lock to preserve
            // the consumer -> deps lock order.
            *self.deps.lock().await = None;

            consumer
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

    /// Error building or using a standalone state reader from the shared bundle
    /// (a connect failure, or a descriptor the reader rejects).
    #[error("state reader failed: {0:#}")]
    StateReader(#[from] StateReaderError),
}
