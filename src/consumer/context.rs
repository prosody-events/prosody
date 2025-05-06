//! Manages Kafka partition assignments, revocations, and consumer rebalancing.
//!
//! This module:
//! - Handles dynamic partition assignments as consumers join or leave consumer
//!   groups
//! - Creates and manages `PartitionManager` instances for message processing
//! - Ensures proper cleanup and offset commits during partition revocation
//! - Coordinates graceful consumer shutdown

use aho_corasick::AhoCorasick;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use rdkafka::ClientContext;
use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance};
use std::collections::hash_map::Entry;
use std::future::ready;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use crate::Topic;
use crate::consumer::partition::{PartitionConfiguration, PartitionManager};
use crate::consumer::{ConsumerConfiguration, HandlerProvider, Managers, WatermarkVersion};

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
    config: PartitionConfiguration,

    /// Creates message handlers for partitions
    handler_provider: T,

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
    /// * `managers` - Thread-safe storage for partition managers
    /// * `allowed_events` - Optional filter for permitted event types
    pub fn new(
        config: &ConsumerConfiguration,
        handler_provider: T,
        watermark_version: Arc<WatermarkVersion>,
        managers: Arc<Managers>,
        allowed_events: Option<AhoCorasick>,
    ) -> Self {
        let config = PartitionConfiguration {
            group_id: Arc::from(config.group_id.as_str()),
            buffer_size: config.max_uncommitted,
            max_uncommitted: config.max_uncommitted,
            max_enqueued_per_key: config.max_enqueued_per_key,
            idempotence_cache_size: config.idempotence_cache_size,
            allowed_events,
            shutdown_timeout: config.shutdown_timeout,
            stall_threshold: config.stall_threshold,
            watermark_version,
            global_limit: Arc::new(Semaphore::new(config.max_concurrency)),
        };

        Self {
            config,
            handler_provider,
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
    /// - Does not commit final offsets before releasing partitions. See: <https://github.com/confluentinc/librdkafka/issues/4059>.
    ///
    /// # Arguments
    ///
    /// * `consumer` - The Kafka consumer instance
    /// * `rebalance` - The rebalance event details
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        // Notify that rebalance is in progress
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
                    let manager =
                        PartitionManager::new(self.config.clone(), handler, topic, partition);

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
        debug!("rebalance completed");
    }
}
