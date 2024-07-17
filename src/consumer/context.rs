//! Manages Kafka partition assignments and revocations for a consumer
//! application.
//!
//! This module integrates Kafka's rebalance callbacks with internal partition
//! management logic, ensuring dynamic partition management in response to
//! consumer group changes. It encapsulates configuration and state management
//! for partitions, connecting processing logic with the consumer's behavior
//! during rebalance events.

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

/// Holds operational settings and components for managing Kafka partitions.
///
/// This structure provides methods to handle partition assignments and
/// revocations through Kafka's rebalance callbacks.
pub struct Context<T>
where
    T: HandlerProvider,
{
    buffer_size: usize,
    max_uncommitted: usize,
    max_enqueued_per_key: usize,
    shutdown_timeout: Option<Duration>,
    handler_provider: T,
    watermark_version: Arc<WatermarkVersion>,
    managers: Arc<Managers>,
}

impl<T> Context<T>
where
    T: HandlerProvider,
{
    /// Creates a new `Context` with provided consumer configuration and
    /// dependencies.
    ///
    /// This method is critical for initializing the management of partition
    /// assignments and revocations.
    ///
    /// # Arguments
    ///
    /// * `config` - Consumer configuration settings such as buffer sizes and
    ///   timeouts.
    /// * `message_handler` - Handler responsible for message processing.
    /// * `watermark_version` - Shared state for tracking watermark versions.
    /// * `managers` - Central storage for managing `PartitionManager`
    ///   instances.
    ///
    /// # Returns
    ///
    /// A new `Context` instance initialized with the given parameters.
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
    /// Responds to Kafka's partition assignment events.
    ///
    /// This method initializes `PartitionManager` instances for newly assigned
    /// partitions and handles partition revocations.
    ///
    /// # Arguments
    ///
    /// * `consumer` - The base consumer instance.
    /// * `rebalance` - Details about the rebalance event from Kafka.
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(partitions) => {
                // Handle empty partition assignments
                if partitions.count() == 0 {
                    return;
                }

                for element in partitions.elements() {
                    let topic = Topic::from(element.topic());
                    let partition = element.partition();
                    info!("topic {topic} partition {partition} assigned");

                    let mut managers = self.managers.lock();

                    // Check if the partition is already assigned
                    let Entry::Vacant(vacant) = managers.entry((topic, partition)) else {
                        warn!("topic {topic} partition {partition} was already assigned");
                        continue;
                    };

                    let handler = self
                        .handler_provider
                        .handler_for_partition(topic, partition);

                    // Create and insert a new PartitionManager
                    let manager = PartitionManager::new(
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
                // Handle empty partition revocations
                if count == 0 {
                    return;
                }

                // Prepare for concurrent partition shutdown
                let shutdown_futures = FuturesUnordered::new();
                let list = Arc::new(Mutex::new(TopicPartitionList::with_capacity(count)));

                for element in partitions.elements() {
                    let topic = Topic::from(element.topic());
                    let partition = element.partition();
                    info!("topic {topic} partition {partition} revoked");

                    // Remove the PartitionManager for the revoked partition
                    let Some(manager) = self.managers.lock().remove(&(topic, partition)) else {
                        error!("cannot revoke topic {topic} partition {partition}; not assigned");
                        continue;
                    };

                    // Prepare shutdown future for the partition
                    let list = list.clone();
                    shutdown_futures.push(async move {
                        let Some(offset) = manager.shutdown().await else {
                            return;
                        };

                        let next_offset = Offset::Offset(offset + 1);
                        let mut list = list.lock();

                        // Add the next offset to the commit list
                        if let Err(error) =
                            list.add_partition_offset(&topic, partition, next_offset)
                        {
                            error!("failed to add offset to commit list: {error:#}");
                        }
                    });
                }

                // Wait for all shutdown tasks to complete
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
