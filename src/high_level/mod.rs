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
    CommonConfiguration, ConsumerConfiguration, ConsumerConfigurationBuilder, ConsumerError,
    ConsumerSetup, KeyedStateConfiguration, LowLatencyMiddlewareConfiguration,
    PipelineMiddlewareConfiguration, ProsodyConsumer,
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
    /// The consumer state. On its `Configured` and `Running` variants it owns
    /// the one shared infrastructure bundle ([`SharedDeps`]). The bundle is
    /// built lazily on first use, by [`Self::state`] or [`Self::subscribe`],
    /// and retained across the `Configured → Running` transition. No state
    /// transition builds a second session, loader, or memory store.
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
            ConsumerState::Configured { config, .. } => Ok(config.register(descriptor)),
            ConsumerState::Running { .. } => Err(HighLevelClientError::AlreadySubscribed),
            ConsumerState::Unconfigured | ConsumerState::ConfigurationFailed(_) => {
                Err(HighLevelClientError::UnconfiguredConsumer)
            }
        }
    }

    /// The client's retained shared bundle, if one has been built. This test
    /// hook lets the composition suite seed committed state into the exact
    /// stores that the running consumer and the client's readers share.
    #[cfg(test)]
    pub(crate) async fn retained_deps(&self) -> Option<SharedDeps<C>> {
        match &*self.consumer.lock().await {
            ConsumerState::Configured { deps, .. } => deps.clone(),
            ConsumerState::Running { deps, .. } => Some(deps.clone()),
            ConsumerState::Unconfigured | ConsumerState::ConfigurationFailed(_) => None,
        }
    }

    /// Composes a standalone [`StateReader`] over this client's one shared
    /// bundle, for `descriptor` routed under `subsystem`.
    ///
    /// Valid once the consumer is `Configured` or `Running`. Both draw from
    /// the same retained bundle. A reader built before `subscribe` and one
    /// built after therefore share one session, loader, and memory store.
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
        let mut guard = self.consumer.lock().await;
        let deps = shared_deps(&mut guard).await?;
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
        let consumer_ref = &mut *guard;

        // Take the state out. Only `Configured` proceeds; the others restore
        // themselves (or leave `Unconfigured`) and return their errors.
        let (config, existing_deps) = match take(consumer_ref) {
            ConsumerState::Unconfigured => return Err(HighLevelClientError::UnconfiguredConsumer),
            ConsumerState::ConfigurationFailed(error) => {
                return Err(HighLevelClientError::ConsumerConfiguration(error));
            }
            ConsumerState::Configured { config, deps } => (config, deps),
            running @ ConsumerState::Running { .. } => {
                *consumer_ref = running;
                return Err(HighLevelClientError::AlreadySubscribed);
            }
        };

        // Build (or reuse the memoized) bundle now that we own the config, so
        // the running consumer and any reader share it. A build failure here
        // must stay retryable, so restore `Configured` (no bundle) and return.
        let deps = match existing_deps {
            Some(deps) => deps,
            None => match build_shared_deps(&config).await {
                Ok(deps) => deps,
                Err(error) => {
                    *consumer_ref = ConsumerState::Configured { config, deps: None };
                    return Err(error);
                }
            },
        };

        // Build the consumer. `take` moved the config out, so any failure must
        // undo both: the match below restores `Configured` and drops the
        // bundle. See there for why each step is needed.
        let built: Result<_, HighLevelClientError<C::Error>> = match &config {
            ModeConfiguration::Pipeline {
                consumer,
                retry,
                monopolization,
                defer,
                common,
                trigger_store,
            } => ProsodyConsumer::<C>::pipeline_consumer(
                setup(consumer, trigger_store, common, &deps),
                PipelineMiddlewareConfiguration {
                    retry: retry.clone(),
                    monopolization: monopolization.clone(),
                    defer: defer.clone(),
                },
                self.telemetry.clone(),
                handler.clone(),
            )
            .await
            .map_err(Into::into),
            ModeConfiguration::LowLatency {
                consumer,
                retry,
                failure_topic,
                common,
                trigger_store,
            } => ProsodyConsumer::low_latency_consumer(
                setup(consumer, trigger_store, common, &deps),
                LowLatencyMiddlewareConfiguration {
                    retry: retry.clone(),
                    failure_topic: failure_topic.clone(),
                },
                self.producer.clone(),
                self.telemetry.clone(),
                handler.clone(),
            )
            .await
            .map_err(Into::into),
            ModeConfiguration::BestEffort {
                consumer,
                common,
                trigger_store,
            } => ProsodyConsumer::<C>::best_effort_consumer(
                setup(consumer, trigger_store, common, &deps),
                self.telemetry.clone(),
                handler.clone(),
            )
            .await
            .map_err(Into::into),
        };

        let consumer = match built {
            Ok(consumer) => consumer,
            Err(error) => {
                // Restore the configured state so a transient build failure
                // stays retryable. Drop the bundle: its open scylla session,
                // live rdkafka poll thread, and registered heartbeat would
                // otherwise be stranded. The next `subscribe` rebuilds a
                // fresh one.
                *consumer_ref = ConsumerState::Configured { config, deps: None };
                return Err(error);
            }
        };

        *consumer_ref = ConsumerState::Running {
            consumer,
            config,
            handler,
            deps,
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

            // Restore `Configured` without a bundle: the taken `Running.deps`
            // is dropped here. Its heartbeat registry holds this consumer's
            // poll-loop heartbeat, which stops beating at shutdown. Reusing the
            // same registry on a later `subscribe` would count that dead
            // heartbeat in `is_stalled` forever and grow the registry with no
            // removal path. The next `subscribe` rebuilds a fresh bundle.
            match take(consumer_ref) {
                state @ (ConsumerState::Unconfigured
                | ConsumerState::ConfigurationFailed(_)
                | ConsumerState::Configured { .. }) => {
                    *consumer_ref = state;
                    return Err(HighLevelClientError::NotSubscribed);
                }
                ConsumerState::Running {
                    consumer, config, ..
                } => {
                    *consumer_ref = ConsumerState::Configured { config, deps: None };
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
fn check_topic_existence<S, C: Codec, D: Codec>(
    producer: &ProsodyProducer<C>,
    consumer_state: &ConsumerState<S, D>,
) -> Result<(), HighLevelClientError<C::Error>>
where
    C::Payload: crate::EventIdentity,
{
    let ConsumerState::Configured { config, .. } = &consumer_state else {
        return Ok(());
    };

    let missing_topics = missing_topics(producer, config.configured_topics())?;
    if missing_topics.is_empty() {
        Ok(())
    } else {
        Err(HighLevelClientError::TopicsNotFound(missing_topics))
    }
}

/// Returns the shared bundle for the given consumer state, building and
/// memoizing it into a `Configured` state on first need. The caller holds the
/// `consumer` lock, so this exclusive `&mut` access is race-free. `Running`
/// already carries its bundle; `Unconfigured`/`ConfigurationFailed` have no
/// config to build from and error.
async fn shared_deps<T, C: Codec>(
    state: &mut ConsumerState<T, C>,
) -> Result<SharedDeps<C>, HighLevelClientError<C::Error>>
where
    C::Payload: Clone,
{
    match state {
        ConsumerState::Running { deps, .. } => Ok(deps.clone()),
        ConsumerState::Configured { config, deps } => {
            if let Some(existing) = deps.as_ref() {
                return Ok(existing.clone());
            }
            let built = build_shared_deps(config).await?;
            *deps = Some(built.clone());
            Ok(built)
        }
        ConsumerState::Unconfigured | ConsumerState::ConfigurationFailed(_) => {
            Err(HighLevelClientError::UnconfiguredConsumer)
        }
    }
}

/// Builds the one shared infrastructure bundle from a mode configuration.
/// Pairs a mode's configuration sections with the client's shared
/// infrastructure. Every mode passes the same bundle, so a consumer the client
/// builds always reuses the one Cassandra session and loader the client already
/// opened.
fn setup<'a, C: Codec>(
    consumer: &'a ConsumerConfiguration,
    trigger_store: &'a TriggerStoreConfiguration,
    common: &'a CommonConfiguration,
    deps: &SharedDeps<C>,
) -> ConsumerSetup<'a, C> {
    ConsumerSetup {
        consumer,
        trigger_store,
        common,
        deps: Some(deps.clone()),
    }
}

/// The bundle depends only on the trigger-store backend, group id, and cache
/// budget, all of which are mode-independent, so the same build serves any
/// mode. This mirrors the Cassandra-session reuse in `StorePair::new`.
/// `InMemory` is exactly mock mode. Its bundle carries the shared in-memory
/// stores, so a reader built from it gets read-your-writes against the
/// running consumer.
async fn build_shared_deps<C: Codec>(
    mode: &ModeConfiguration,
) -> Result<SharedDeps<C>, HighLevelClientError<C::Error>>
where
    C::Payload: Clone,
{
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
    Ok(deps.with_default_read_cache_ttl(keyed_state.read_cache_ttl))
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
