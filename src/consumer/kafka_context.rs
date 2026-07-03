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
//!
//! The core component is the `Context` struct which implements Kafka's
//! rebalance callbacks to manage partition lifecycle events.

use aho_corasick::{AhoCorasick, BuildError, StartKind};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use rdkafka::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use std::array::from_fn;
use std::collections::hash_map::Entry;
use std::future::ready;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{Semaphore, watch};
use tracing::{debug, error, info, warn};

use crate::consumer::partition::{PartitionConfiguration, PartitionManager};
use crate::consumer::{
    ConsumerConfiguration, EventHandler, HandlerProvider, Managers, WatermarkVersion,
};
use crate::loader::MessageLoader;
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::session::CellSession;
use crate::telemetry::sender::TelemetrySender;
use crate::timers::TimerSemaphores;
use crate::timers::duration::CompactDuration;
use crate::timers::store::TriggerStoreProvider;
use crate::{EventIdentity, EventType, Partition, Topic};

/// The shared partition-manager map paired with the channel that republishes
/// its size after each rebalance, so assignment readiness can be awaited rather
/// than polled.
pub struct ManagerRegistry<PL> {
    /// Thread-safe storage for partition managers.
    pub managers: Arc<Managers<PL>>,
    /// Publishes the assigned-partition count after each rebalance.
    pub assignment_tx: watch::Sender<u32>,
}

/// The per-partition factories the context threads into each
/// [`PartitionConfiguration`]: trigger stores for the timer system and
/// keyed-state managers for the state system.
pub struct PartitionProviders<P, SP> {
    /// Factory for per-partition trigger stores.
    pub triggers: P,

    /// Factory for per-partition keyed-state managers.
    pub state: SP,
}

/// The partition-manager factory a [`Context`] holds: builds a fresh
/// [`PartitionManager`] for each newly assigned partition. Implemented
/// automatically for any matching closure, so the context can name its factory
/// bound once instead of spelling the full `Fn` signature at every use.
pub trait MakeManager<PL>:
    Fn(Topic, Partition) -> PartitionManager<PL> + Send + Sync + 'static
{
}

impl<PL, F> MakeManager<PL> for F where
    F: Fn(Topic, Partition) -> PartitionManager<PL> + Send + Sync + 'static
{
}

/// Manages Kafka partition assignments and message processing for a consumer.
///
/// Implements the rebalance protocol to handle dynamic partition assignments
/// and revocations as consumers join or leave a consumer group. During
/// assignments, it creates `PartitionManager` instances for each assigned
/// partition via `make_manager`. During revocations, it ensures proper cleanup
/// and graceful shutdown of partition processing.
///
/// The context is generic only over what it actually uses at rebalance time:
/// the payload type and a partition-manager factory. The provider types
/// (`HandlerProvider`/`TriggerStoreProvider`/`PartitionStateProvider`) are
/// discharged once inside [`new_context`] and captured into the `make_manager`
/// closure, so they never surface on the context itself.
///
/// # Type Parameters
///
/// * `F` - The partition-manager factory closure (`Fn(Topic, Partition) ->
///   PartitionManager<PL>`)
/// * `PL` - The payload type carried by consumed messages
pub struct Context<F, PL> {
    /// Builds a fresh partition manager for a newly assigned partition.
    make_manager: F,

    /// Thread-safe storage for partition managers
    managers: Arc<Managers<PL>>,

    /// Publishes the assigned-partition count after each rebalance so readiness
    /// can be awaited rather than polled.
    assignment_tx: watch::Sender<u32>,

    telemetry: TelemetrySender,
}

/// Creates a new consumer context with the given configuration.
///
/// Converts the consumer-level configuration into partition-level
/// configuration and captures the handler provider and per-partition factories
/// into a partition-manager factory closure. This discharges the provider
/// trait bounds in one place; the returned [`Context`] carries only the
/// payload-erased factory.
///
/// # Arguments
///
/// * `config` - Consumer configuration including buffer sizes and timeouts
/// * `handler_provider` - Creates message handlers for partitions
/// * `providers` - Per-partition trigger-store and keyed-state factories
/// * `watermark_version` - Shared counter tracking watermark updates
/// * `registry` - Partition-manager map plus its assignment-count publisher
///
/// # Errors
///
/// Returns a [`BuildError`] if the configured `allowed_events` prefixes
/// cannot be compiled into a filter automaton.
pub fn new_context<T, P, SP, PL>(
    config: &ConsumerConfiguration,
    handler_provider: T,
    providers: PartitionProviders<P, SP>,
    watermark_version: Arc<WatermarkVersion>,
    registry: ManagerRegistry<PL>,
    telemetry: TelemetrySender,
    version: Arc<str>,
) -> Result<Context<impl MakeManager<PL>, PL>, BuildError>
where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = PL>,
    P: TriggerStoreProvider,
    SP: PartitionStateProvider,
    <SP::Manager as PartitionStateManager>::Session:
        CellSession<Loader: MessageLoader<Payload = PL>>,
    PL: Clone + Send + Sync + 'static + EventType + EventIdentity,
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

    let timer_slab_size = config.slab_size.try_into().unwrap_or_else(|error| {
        error!("invalid timer slab size: {error:#}; using default");
        CompactDuration::new(10 * 60)
    });

    let timer_semaphores: Arc<TimerSemaphores> = Arc::new(from_fn(|_| {
        Arc::new(Semaphore::new(config.max_uncommitted))
    }));

    let partition_config = PartitionConfiguration {
        group_id: Arc::from(config.group_id.as_str()),
        buffer_size: config.max_uncommitted,
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
        telemetry_sender: telemetry.clone(),
        timer_spans: config.timer_spans,
        _payload: PhantomData,
    };

    let make_manager = move |topic: Topic, partition: Partition| {
        let handler = handler_provider.handler_for_partition(topic, partition);
        PartitionManager::new(partition_config.clone(), handler, topic, partition)
    };

    Ok(Context {
        make_manager,
        managers: registry.managers,
        assignment_tx: registry.assignment_tx,
        telemetry,
    })
}

impl<F, PL> ClientContext for Context<F, PL>
where
    F: MakeManager<PL>,
    PL: Send + Sync + 'static,
{
}

impl<F, PL> ConsumerContext for Context<F, PL>
where
    F: MakeManager<PL>,
    PL: Send + Sync + 'static,
{
    /// Handles partition assignments and revocations during consumer group
    /// rebalancing.
    ///
    /// This method is called by librdkafka before a rebalance operation takes
    /// place. It manages the creation and shutdown of partition managers
    /// based on the rebalance type:
    ///
    /// - For assignments: Creates new `PartitionManager` instances for newly
    ///   assigned partitions
    /// - For revocations: Shuts down `PartitionManager` instances for revoked
    ///   partitions
    ///
    /// # Arguments
    ///
    /// * `consumer` - The Kafka consumer instance
    /// * `rebalance` - The rebalance event details containing partition
    ///   assignments or revocations
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        debug!("rebalance is starting");

        match rebalance {
            Rebalance::Assign(partitions) => {
                // Skip empty assignments
                if partitions.count() == 0 {
                    return;
                }

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

                    // Initialize a new partition manager via the captured
                    // factory, which builds its handler and threads in the
                    // partition configuration.
                    let manager = (self.make_manager)(topic, partition);

                    vacant.insert(manager);
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

                    // Remove partition manager
                    let Some(manager) = self.managers.write().remove(&(topic, partition)) else {
                        error!("cannot revoke {topic}:{partition}; not assigned");
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
            .send_replace(self.managers.read().len() as u32);

        debug!("pre-rebalance complete");
    }

    /// Handles post-rebalance processing.
    ///
    /// This method is called by librdkafka after a rebalance operation has
    /// completed. For assignment events, it resumes consumption on the newly
    /// assigned partitions. For all events, it logs that the rebalance has
    /// completed.
    ///
    /// # Arguments
    ///
    /// * `consumer` - The Kafka consumer instance
    /// * `rebalance` - The completed rebalance event details
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        if let Rebalance::Assign(partitions) = rebalance {
            debug!("resuming assigned partitions: {partitions:#?}");

            if let Err(error) = consumer.resume(partitions) {
                error!("error while resuming assigned partitions: {error:#}");
            }
        }

        debug!("rebalance completed");
    }
}
