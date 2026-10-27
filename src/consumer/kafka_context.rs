//! Manages Kafka partition assignments, revocations, and consumer group
//! rebalancing.
//!
//! This module implements the Kafka consumer rebalancing protocol that handles:
//!
//! - Dynamic partition assignment and revocation as consumers join/leave a
//!   group
//! - Creation and lifecycle management of `PartitionManager` instances
//! - Concurrent shutdown of revoked partitions with proper cleanup
//! - Coordination of partition handlers during consumer group rebalances
//! - librdkafka's periodic statistics callback
//!
//! The core component is the `Context` struct which implements Kafka's
//! rebalance callbacks to manage partition lifecycle events, and owns the
//! gauges the statistics callback records. On assignment, the context splits
//! the librdkafka queue of each partition and gives it to the partition's
//! manager.

use aho_corasick::{AhoCorasick, BuildError, StartKind};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use opentelemetry::global::meter;
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::{ClientConfig, ClientContext, Statistics};
use std::array::from_fn;
use std::collections::hash_map::Entry;
use std::future::ready;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock, Weak};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::{Semaphore, watch};
use tracing::{debug, error, info, warn};

use crate::consumer::decode::ResultRequestReader;
use crate::consumer::partition::{PartitionConfiguration, PartitionManager};
use crate::consumer::source::Reader;
use crate::consumer::statistics::KafkaMetrics;
use crate::consumer::{
    ConsumerConfiguration, ConsumerError, EventHandler, HandlerProvider, Managers,
    WatermarkVersion, get_assigned_partition_count,
};
use crate::loader::MessageLoader;
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::session::EventSession;
use crate::telemetry::sender::TelemetrySender;
use crate::timers::TimerSemaphores;
use crate::timers::duration::{CompactDuration, CompactDurationError};
use crate::timers::store::TriggerStoreProvider;
use crate::{Codec, EventIdentity, EventType, Partition, Topic};

/// The shared handles a [`Context`] keeps beside its partition-manager factory.
pub(super) struct ContextHandles {
    /// Thread-safe storage for partition managers.
    pub(super) managers: Arc<Managers>,
    /// Publishes the assigned-partition count after each rebalance, so
    /// assignment readiness can be awaited rather than polled.
    pub(super) assignment_tx: watch::Sender<u32>,
    /// Telemetry the context and the partitions it creates publish through.
    pub(super) telemetry: TelemetrySender,
}

/// The per-partition factories the context threads into each
/// [`PartitionConfiguration`]: trigger stores for the timer system and
/// keyed-state managers for the state system.
pub(super) struct PartitionProviders<P, SP> {
    /// Factory for per-partition trigger stores.
    pub(super) triggers: P,

    /// Factory for per-partition keyed-state managers.
    pub(super) state: SP,
}

/// Builds the partition manager of each newly assigned partition.
pub(super) trait MakeManager: Send + Sync + 'static {
    /// Builds the manager of `topic`:`partition`, which reads `queue`.
    ///
    /// The method is generic over the client context because the context
    /// type contains the factory, so the factory cannot name it.
    fn make<Ctx: ConsumerContext + 'static>(
        &self,
        topic: Topic,
        partition: Partition,
        queue: PartitionQueue<Ctx>,
    ) -> PartitionManager;
}

/// The [`MakeManager`] of a running consumer. It holds the provider types, so
/// they never surface on the [`Context`].
struct ManagerFactory<T, S, SP, C: Codec, R> {
    /// Builds the handler of each partition.
    handler_provider: T,

    /// The configuration every partition manager starts from.
    config: PartitionConfiguration<S, SP, C::Payload>,

    /// Reads each partition queue into the record stream of its manager.
    reader: Reader<C, R>,
}

impl<T, S, SP, C, R> MakeManager for ManagerFactory<T, S, SP, C, R>
where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = C::Payload>,
    S: TriggerStoreProvider,
    SP: PartitionStateProvider<S::Store>,
    <SP::Manager as PartitionStateManager>::Session:
        EventSession<Loader: MessageLoader<Payload = C::Payload>>,
    C: Codec,
    C::Payload: Clone + EventType + EventIdentity,
    R: ResultRequestReader + 'static,
{
    fn make<Ctx: ConsumerContext + 'static>(
        &self,
        topic: Topic,
        partition: Partition,
        queue: PartitionQueue<Ctx>,
    ) -> PartitionManager {
        let handler = self
            .handler_provider
            .handler_for_partition(topic, partition);
        let records = self.reader.read(queue);
        PartitionManager::new(self.config.clone(), handler, topic, partition, records)
    }
}

/// Manages Kafka partition assignments and message processing for a consumer.
///
/// Implements the rebalance protocol to handle dynamic partition assignments
/// and revocations as consumers join or leave a consumer group. During
/// assignments, it creates `PartitionManager` instances for each assigned
/// partition via its factory. During revocations, it ensures proper cleanup
/// and graceful shutdown of partition processing.
///
/// # Invariant
///
/// Every record of an assigned partition goes to that partition's queue, and
/// only its manager reads it. The context splits the queue before librdkafka
/// assigns the partition. A split queue stays split for every later assignment
/// of the partition, so librdkafka never forwards its records to the consumer
/// queue.
///
/// # Ownership
///
/// The client owns this context, and the context holds the partition managers.
/// Each partition task holds the client through its queue. This cycle keeps the
/// client alive until the poll loop sweeps the managers in its close. Do not
/// drop the client before that sweep. A design that defers rebalances to the
/// poll loop has no cycle. It was rejected because it adds a deferred-assign
/// protocol, and its close still needs the same order.
pub(super) struct Context<F: MakeManager> {
    /// Builds a fresh partition manager for a newly assigned partition.
    factory: F,

    /// The client that runs this context, for the queue split. It is weak
    /// because the client owns this context. Only [`Context::into_client`] sets
    /// it. It is a `OnceLock` because `create_with_context` can fail and
    /// `Arc::new_cyclic` cannot.
    consumer: OnceLock<Weak<BaseConsumer<Self>>>,

    /// Thread-safe storage for partition managers
    managers: Arc<Managers>,

    /// Publishes the assigned-partition count after each rebalance so readiness
    /// can be awaited rather than polled.
    assignment_tx: watch::Sender<u32>,

    /// Publishes the rebalance events this context observes.
    telemetry: TelemetrySender,

    /// The gauges librdkafka's statistics feed. They retire when the client
    /// drops this context.
    metrics: KafkaMetrics,
}

/// Creates a new consumer context with the given configuration.
///
/// Converts the consumer-level configuration into partition-level
/// configuration and captures it, the handler provider, and `reader` in the
/// [`MakeManager`] of the context.
///
/// # Errors
///
/// [`ContextError`] when the configured `allowed_events` prefixes cannot be
/// compiled into a filter automaton, or `slab_size` is outside
/// [`CompactDuration`]'s range.
pub(super) fn new_context<T, P, SP, C, R>(
    config: &ConsumerConfiguration,
    handler_provider: T,
    providers: PartitionProviders<P, SP>,
    reader: Reader<C, R>,
    watermark_version: Arc<WatermarkVersion>,
    shared: ContextHandles,
    version: Arc<str>,
) -> Result<Context<impl MakeManager + use<T, P, SP, C, R>>, ContextError>
where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = C::Payload>,
    P: TriggerStoreProvider,
    SP: PartitionStateProvider<P::Store>,
    <SP::Manager as PartitionStateManager>::Session:
        EventSession<Loader: MessageLoader<Payload = C::Payload>>,
    C: Codec,
    C::Payload: Clone + EventType + EventIdentity,
    R: ResultRequestReader + 'static,
{
    // Compile the event-type filter automaton from the configured
    // prefixes; payloads supply their event type via [`EventType`].
    let allowed_events = config
        .allowed_events
        .as_ref()
        .map(|prefixes| {
            AhoCorasick::builder()
                .start_kind(StartKind::Anchored)
                .build(prefixes)
        })
        .transpose()?;

    // A slab size the operator set but that cannot be represented is rejected
    // here. Substituting a default would silently run the timer system at a
    // granularity nobody asked for.
    let timer_slab_size = CompactDuration::try_from(config.slab_size)?;

    let timer_semaphores: Arc<TimerSemaphores> = Arc::new(from_fn(|_| {
        Arc::new(Semaphore::new(config.max_uncommitted))
    }));

    let partition_config = PartitionConfiguration {
        group_id: Arc::from(config.group_id.as_str()),
        max_uncommitted: config.max_uncommitted,
        allowed_events,
        shutdown_timeout: config.shutdown_timeout,
        stall_threshold: config.stall_threshold,
        watermark_version,
        version,
        trigger_provider: providers.triggers,
        state_provider: providers.state,
        timer_slab_size,
        timer_semaphores,
        telemetry_sender: shared.telemetry.clone(),
        timer_spans: config.timer_spans,
        _payload: PhantomData,
    };

    Ok(Context {
        factory: ManagerFactory {
            handler_provider,
            config: partition_config,
            reader,
        },
        consumer: OnceLock::new(),
        managers: shared.managers,
        assignment_tx: shared.assignment_tx,
        telemetry: shared.telemetry,
        metrics: KafkaMetrics::new(&meter("prosody"), &config.group_id),
    })
}

impl<F: MakeManager> Context<F> {
    /// Creates the client that runs this context. The context keeps a weak
    /// reference to the client, which it needs to split partition queues.
    pub(super) fn into_client(self, config: &ClientConfig) -> KafkaResult<Arc<BaseConsumer<Self>>> {
        let consumer: Arc<BaseConsumer<Self>> = Arc::new(config.create_with_context(self)?);
        consumer
            .context()
            .consumer
            .get_or_init(|| Arc::downgrade(&consumer));
        Ok(consumer)
    }
}

impl<F: MakeManager> ClientContext for Context<F> {
    /// Receives librdkafka's periodic statistics on the poll thread, so the
    /// body must never block, spawn, or call a Kafka API.
    ///
    /// Do not delete this override: rdkafka's default `stats` logs the entire
    /// statistics tree at `info` and the gauges stop.
    fn stats(&self, statistics: Statistics) {
        self.metrics.observe(statistics);
    }
}

impl<F: MakeManager> ConsumerContext for Context<F> {
    /// Handles partition assignments and revocations during consumer group
    /// rebalancing.
    ///
    /// This method is called by librdkafka before a rebalance operation takes
    /// place. It manages the creation and shutdown of partition managers
    /// based on the rebalance type:
    ///
    /// - For assignments: Splits the queue of each newly assigned partition and
    ///   creates its `PartitionManager`
    /// - For revocations: Shuts down `PartitionManager` instances for revoked
    ///   partitions
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        debug!("rebalance is starting");

        match rebalance {
            Rebalance::Assign(partitions) => {
                // Skip empty assignments
                if partitions.count() == 0 {
                    return;
                }

                // The upgrade fails only while the client drops. Its close
                // revokes the assignment at once.
                let Some(consumer) = self.consumer.get().and_then(Weak::upgrade) else {
                    warn!("ignoring an assignment while the consumer closes");
                    return;
                };

                for element in partitions.elements() {
                    let topic = Topic::from(element.topic());
                    let partition = element.partition();

                    info!("assigning {topic}:{partition}");
                    self.telemetry.partition_assigned(topic, partition);

                    let mut managers = self.managers.write();

                    // Verify partition isn't already assigned
                    let Entry::Vacant(vacant) = managers.entry((topic, partition)) else {
                        warn!("{topic}:{partition} was already assigned");
                        continue;
                    };

                    // Split only a vacant partition. Two handles to one queue
                    // share its callback, and a drop of either handle disables
                    // it.
                    let Some(queue) = consumer.split_partition_queue(&topic, partition) else {
                        error!("cannot split the queue of {topic}:{partition}");
                        continue;
                    };

                    vacant.insert(self.factory.make(topic, partition, queue));
                    debug!("{topic}:{partition} assigned");
                }
            }
            Rebalance::Revoke(partitions) => {
                let count = partitions.count();
                if count == 0 {
                    return;
                }

                // Prepare for concurrent partition shutdown
                let shutdown_futures = FuturesUnordered::new();

                for element in partitions.elements() {
                    let topic = Topic::from(element.topic());
                    let partition = element.partition();
                    info!("revoking {topic}:{partition}");

                    // The final revoke of a close finds no manager: the poll
                    // loop stops every partition before the close.
                    let Some(manager) = self.managers.write().remove(&(topic, partition)) else {
                        debug!("{topic}:{partition} has no partition manager to revoke");
                        continue;
                    };

                    // Queue shutdown task
                    shutdown_futures.push(manager.shutdown());

                    self.telemetry.partition_revoked(topic, partition);
                }

                // Wait for all shutdowns to complete concurrently
                Handle::current().block_on(shutdown_futures.for_each(|_| ready(())));
            }
            Rebalance::Error(error) => {
                error!("unexpected rebalance error: {error:#}");
            }
        }

        // Publish the new assignment count so readiness can be awaited rather
        // than polled. Assign, revoke, and rebalance-error all fall through
        // here; empty rebalances returned early and left the count unchanged.
        self.assignment_tx
            .send_replace(get_assigned_partition_count(&self.managers));

        debug!("pre-rebalance complete");
    }
}

/// Errors raised while building a [`Context`] from consumer configuration.
///
/// Both arms are supplied-but-invalid configuration, so neither has a sensible
/// default to fall back to.
#[derive(Debug, Error)]
pub(super) enum ContextError {
    /// The `allowed_events` prefixes could not be compiled into a filter
    /// automaton.
    #[error("invalid allowed_events filter: {0}")]
    AllowedEvents(#[from] BuildError),

    /// `slab_size` is outside [`CompactDuration`]'s representable range.
    #[error("invalid timer slab size: {0}")]
    SlabSize(#[from] CompactDurationError),
}

/// Surfaces a context-build failure through the consumer's public error type.
/// Both arms already have a [`ConsumerError`] variant, so the public surface is
/// unchanged.
impl From<ContextError> for ConsumerError {
    fn from(error: ContextError) -> Self {
        match error {
            ContextError::AllowedEvents(error) => Self::AllowedEventsPattern(error),
            ContextError::SlabSize(error) => Self::InvalidSlabSize(error),
        }
    }
}
