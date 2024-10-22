//! Manages Kafka consumer partition assignments and revocations.
//!
//! This module integrates with Kafka's consumer group rebalancing protocol to:
//! - Handle dynamic partition assignments as consumers join or leave the group
//! - Create and manage `PartitionManager` instances for processing messages
//! - Ensure proper cleanup and offset commitment during partition revocation
//! - Coordinate graceful shutdown of partition processing

use std::collections::hash_map::Entry;
use std::future::ready;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use parking_lot::Mutex;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use tokio::runtime::Handle;
use tracing::{debug, error, info, warn};

use crate::consumer::partition::PartitionManager;
use crate::consumer::{ConsumerConfiguration, HandlerProvider, Managers, WatermarkVersion};
use crate::Topic;

/// Manages Kafka partition assignments and message processing for a consumer.
///
/// `Context` implements Kafka's rebalance callbacks and maintains
/// `PartitionManager` instances for each assigned partition. It coordinates
/// partition assignment, revocation, and graceful shutdown of message
/// processing.
///
/// # Type Parameters
///
/// * `T` - A type implementing `HandlerProvider` that creates message handlers
///   for newly assigned partitions
pub struct Context<T>
where
    T: HandlerProvider,
{
    buffer_size: usize,
    max_uncommitted: usize,
    max_enqueued_per_key: usize,
    shutdown_timeout: Duration,
    handler_provider: T,
    watermark_version: Arc<WatermarkVersion>,
    managers: Arc<Managers>,
}

impl<T> Context<T>
where
    T: HandlerProvider,
{
    /// Creates a new consumer context with the specified configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Consumer configuration including buffer sizes and timeouts
    /// * `handler_provider` - Provider that creates message handlers for
    ///   partitions
    /// * `watermark_version` - Shared state for tracking message watermarks
    /// * `managers` - Shared storage for partition manager instances
    ///
    /// # Returns
    ///
    /// A new `Context` instance initialized with the given parameters
    pub fn new(
        config: &ConsumerConfiguration,
        handler_provider: T,
        watermark_version: Arc<WatermarkVersion>,
        managers: Arc<Managers>,
    ) -> Self {
        Self {
            buffer_size: config.max_uncommitted,
            max_uncommitted: config.max_uncommitted,
            max_enqueued_per_key: config.max_enqueued_per_key,
            shutdown_timeout: config.partition_shutdown_timeout,
            handler_provider,
            watermark_version,
            managers,
        }
    }
}

impl<T> ClientContext for Context<T> where T: HandlerProvider {}

impl<T> ConsumerContext for Context<T>
where
    T: HandlerProvider,
{
    /// Handles Kafka partition assignments and revocations during rebalancing.
    ///
    /// This callback creates new `PartitionManager` instances for assigned
    /// partitions, shuts down and removes managers for revoked partitions,
    /// and commits final offsets before partitions are revoked.
    ///
    /// # Arguments
    ///
    /// * `consumer` - The Kafka consumer instance
    /// * `rebalance` - Details about the rebalance event
    ///   (assignments/revocations)
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(partitions) => {
                // Skip processing for empty assignments
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
                        topic,
                        element.partition(),
                        handler,
                        self.buffer_size,
                        self.max_uncommitted,
                        self.max_enqueued_per_key,
                        self.shutdown_timeout,
                        self.watermark_version.clone(),
                    );

                    vacant.insert(manager);
                }
            }
            Rebalance::Revoke(partitions) => {
                let count = partitions.count();
                if count == 0 {
                    return;
                }

                // Prepare for concurrent partition shutdown
                let shutdown_futures = FuturesUnordered::new();
                let list = Arc::new(Mutex::new(TopicPartitionList::with_capacity(count)));

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
                    let list = list.clone();
                    shutdown_futures.push(async move {
                        let Some(offset) = manager.shutdown().await else {
                            return;
                        };

                        let next_offset = Offset::Offset(offset + 1);
                        let mut list = list.lock();

                        // Record final offset
                        if let Err(error) =
                            list.add_partition_offset(&topic, partition, next_offset)
                        {
                            error!("failed to add offset to commit list: {error:#}");
                        }
                    });
                }

                // Wait for all shutdowns to complete
                Handle::current().block_on(shutdown_futures.for_each(|()| ready(())));

                let list = list.lock();
                if list.count() == 0 {
                    return;
                }

                // Commit final offsets
                debug!("committing {list:?}");
                if let Err(error) = consumer.commit(&list, CommitMode::Async) {
                    error!("failed to commit offsets before rebalance: {error:#}");
                }
                debug!("final offsets committed");
            }
            Rebalance::Error(_) => {}
        }
    }
}
