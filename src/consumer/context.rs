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
/// Implements the rebalance protocol to handle dynamic partition assignments
/// and revocations as consumers join or leave a consumer group. During
/// assignments, it creates `PartitionManager` instances for each assigned
/// partition. During revocations, it ensures proper cleanup and graceful
/// shutdown of partition processing.
///
/// # Type Parameters
///
/// * `T` - Type implementing `HandlerProvider` to create message handlers for
///   partitions
pub struct Context<T>
where
    T: HandlerProvider,
{
    /// Partition-level configuration settings
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
    /// Converts the consumer-level configuration into partition-level
    /// configuration and initializes the context with the handler provider
    /// and shared state.
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

                    let mut managers = self.managers.write();

                    // Verify partition isn't already assigned
                    let Entry::Vacant(vacant) = managers.entry((topic, partition)) else {
                        warn!("{topic}:{partition} was already assigned");
                        continue;
                    };

                    // Create a handler for this specific partition
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

                // Wait for all shutdowns to complete concurrently
                Handle::current().block_on(shutdown_futures.for_each(|_| ready(())));
            }
            Rebalance::Error(error) => {
                error!("unexpected rebalance error: {error:#}");
            }
        }

        debug!("pre-rebalance complete");
    }

    /// Handles post-rebalance processing.
    ///
    /// This method is called by librdkafka after a rebalance operation has
    /// completed. Currently, it simply logs that the rebalance has
    /// completed.
    ///
    /// # Arguments
    ///
    /// * `consumer` - The Kafka consumer instance
    /// * `rebalance` - The completed rebalance event details
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, _rebalance: &Rebalance) {
        debug!("rebalance completed");
    }
}
