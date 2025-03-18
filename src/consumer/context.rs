//! Manages Kafka partition assignments, revocations, and consumer rebalancing.
//!
//! This module:
//! - Handles dynamic partition assignments as consumers join or leave consumer
//!   groups
//! - Creates and manages `PartitionManager` instances for message processing
//! - Ensures proper cleanup and offset commits during partition revocation
//! - Coordinates graceful consumer shutdown

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use std::collections::hash_map::Entry;
use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use crate::Topic;
use crate::consumer::partition::PartitionManager;
use crate::consumer::{
    ConsumerConfiguration, HandlerProvider, Managers, RebalanceGuard, WatermarkVersion,
};

/// Manages Kafka partition assignments and message processing for a consumer.
///
/// Implements Kafka's rebalance callbacks to manage partition
/// assignments/revocations and coordinates message processing through
/// `PartitionManager` instances.
///
/// # Type Parameters
///
/// * `T` - Type implementing `HandlerProvider` to create message handlers for
///   partitions
pub struct Context<T>
where
    T: HandlerProvider,
{
    /// Consumer group identifier
    group_id: Arc<str>,

    /// Maximum size of message buffers
    buffer_size: usize,

    /// Maximum number of uncommitted messages allowed
    max_uncommitted: usize,

    /// Maximum number of queued messages per key
    max_enqueued_per_key: usize,

    /// Size of idempotence cache
    idempotence_cache_size: usize,

    /// Duration of inactivity allowed before considering a partition stalled
    stall_threshold: Duration,

    /// Timeout duration for shutdown operations
    shutdown_timeout: Duration,

    /// Creates message handlers for partitions
    handler_provider: T,

    /// Shared counter tracking watermark updates
    watermark_version: Arc<WatermarkVersion>,

    /// Shared flag indicating if currently rebalancing; used to pause commits
    rebalance_guard: Arc<RebalanceGuard>,

    /// Global concurrency limit
    global_limit: Arc<Semaphore>,

    /// Thread-safe storage for partition managers
    managers: Arc<Managers>,
}

impl<T> Context<T>
where
    T: HandlerProvider,
{
    /// Creates a new consumer context with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Consumer configuration including buffer sizes and timeouts
    /// * `handler_provider` - Creates message handlers for partitions
    /// * `watermark_version` - Shared counter tracking watermark updates
    /// * `rebalance_guard` - Shared flag tracking if rebalance is in progress
    /// * `managers` - Thread-safe storage for partition managers
    pub fn new(
        config: &ConsumerConfiguration,
        handler_provider: T,
        watermark_version: Arc<WatermarkVersion>,
        rebalance_guard: Arc<RebalanceGuard>,
        managers: Arc<Managers>,
    ) -> Self {
        Self {
            group_id: Arc::from(config.group_id.as_str()),
            buffer_size: config.max_uncommitted,
            max_uncommitted: config.max_uncommitted,
            max_enqueued_per_key: config.max_enqueued_per_key,
            idempotence_cache_size: config.idempotence_cache_size,
            stall_threshold: config.stall_threshold,
            shutdown_timeout: config.shutdown_timeout,
            handler_provider,
            watermark_version,
            rebalance_guard,
            global_limit: Arc::new(Semaphore::new(config.max_concurrency)),
            managers,
        }
    }
}

impl<T> ClientContext for Context<T> where T: HandlerProvider {}

impl<T> ConsumerContext for Context<T>
where
    T: HandlerProvider,
{
    /// Handles partition assignments and revocations during consumer group
    /// rebalancing.
    ///
    /// For assignments:
    /// - Creates new `PartitionManager` instances for assigned partitions
    /// - Initializes message handlers for each partition
    ///
    /// For revocations:
    /// - Shuts down `PartitionManager` instances for revoked partitions
    /// - Commits final offsets before releasing partitions
    ///
    /// # Arguments
    ///
    /// * `consumer` - The Kafka consumer instance
    /// * `rebalance` - The rebalance event details
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        // Notify that rebalance is in progress
        self.rebalance_guard.store(true, Ordering::Release);
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

                    let mut managers = self.managers.write();

                    // Verify partition isn't already assigned
                    let Entry::Vacant(vacant) = managers.entry((topic, partition)) else {
                        warn!("{topic}:{partition} was already assigned");
                        continue;
                    };

                    let handler = self
                        .handler_provider
                        .handler_for_partition(topic, partition);

                    // Initialize new partition manager
                    let manager = PartitionManager::new(
                        self.group_id.clone(),
                        topic,
                        element.partition(),
                        handler,
                        self.buffer_size,
                        self.max_uncommitted,
                        self.max_enqueued_per_key,
                        self.idempotence_cache_size,
                        self.shutdown_timeout,
                        self.stall_threshold,
                        self.watermark_version.clone(),
                        self.global_limit.clone(),
                    );

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
                }

                // Wait for all shutdowns to complete
                Handle::current().block_on(shutdown_futures.for_each(|_| ready(())));
            }
            Rebalance::Error(error) => {
                error!("unexpected rebalance error: {error:#}");
            }
        }

        debug!("pre-rebalance complete");
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, _rebalance: &Rebalance) {
        // Notify that rebalance is complete
        self.rebalance_guard.store(false, Ordering::Release);
        debug!("rebalance completed");
    }
}
