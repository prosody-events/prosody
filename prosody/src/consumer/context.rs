//! Manages Kafka partition assignments and revocations for a consumer
//! application. This module integrates Kafka's rebalance callbacks with
//! internal partition management logic, ensuring that partitions are
//! dynamically managed according to consumer group changes. It encapsulates
//! configuration and state management for partitions, tying together the
//! processing logic and the consumer's behavior in response to rebalance
//! events.

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
use crate::consumer::{ConsumerConfiguration, Managers, MessageHandler, WatermarkVersion};
use crate::Topic;

/// Context holds the operational settings and components required for managing
/// Kafka partitions. This structure provides methods to handle partition
/// assignments and revocations through Kafka's rebalance callbacks.
pub struct Context<T> {
    buffer_size: usize,
    max_uncommitted: usize,
    max_enqueued_per_key: usize,
    shutdown_timeout: Option<Duration>,
    message_handler: T,
    watermark_version: Arc<WatermarkVersion>,
    managers: Arc<Managers>,
}

impl<T> Context<T> {
    /// Creates a new `Context` with provided consumer configuration and
    /// dependencies. This is critical for initializing the management of
    /// partition assignments and revocations.
    ///
    /// # Arguments
    /// * `config` - Consumer configuration settings such as buffer sizes and
    ///   timeouts.
    /// * `message_handler` - Handler responsible for message processing.
    /// * `watermark_version` - Shared state for tracking watermark versions.
    /// * `managers` - Central storage for managing `PartitionManager`
    ///   instances.
    ///
    /// # Returns
    /// A new `Context` instance initialized with the given parameters.
    pub fn new(
        config: &ConsumerConfiguration,
        message_handler: T,
        watermark_version: Arc<WatermarkVersion>,
        managers: Arc<Managers>,
    ) -> Self {
        Self {
            buffer_size: config.max_uncommitted,
            max_uncommitted: config.max_uncommitted,
            max_enqueued_per_key: config.max_enqueued_per_key,
            shutdown_timeout: config.partition_shutdown_timeout,
            message_handler,
            watermark_version,
            managers,
        }
    }
}

impl<T> ClientContext for Context<T> where T: Send + Sync {}

impl<T> ConsumerContext for Context<T>
where
    T: MessageHandler + Clone + Send + Sync + 'static,
{
    /// Responds to Kafka's partition assignment events, initializing
    /// `PartitionManager` for newly assigned partitions. This method
    /// ensures that each new partition is properly managed from the moment of
    /// assignment.
    ///
    /// # Arguments
    /// * `rebalance` - Details about the rebalance event from Kafka.
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(partitions) => {
                // see: https://github.com/fede1024/rust-rdkafka/issues/681
                if partitions.count() == 0 {
                    return;
                }

                for element in partitions.elements() {
                    let topic = Topic::from(element.topic());
                    let partition = element.partition();
                    info!("topic {topic} partition {partition} assigned");

                    let mut managers = self.managers.lock();

                    let Entry::Vacant(vacant) = managers.entry((topic, partition)) else {
                        warn!("topic {topic} partition {partition} was already assigned");
                        continue;
                    };

                    let manager = PartitionManager::new(
                        element.partition(),
                        self.message_handler.clone(),
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
                // see: https://github.com/fede1024/rust-rdkafka/issues/681
                if count == 0 {
                    return;
                }

                // Concurrently shutdown all partitions. Delaying shutdown will delay
                // re-balancing and potentially lead to unacceptable processing latencies.
                let shutdown_futures = FuturesUnordered::new();
                let list = Arc::new(Mutex::new(TopicPartitionList::with_capacity(count)));

                for element in partitions.elements() {
                    let topic = Topic::from(element.topic());
                    let partition = element.partition();
                    info!("topic {topic} partition {partition} revoked");

                    let Some(manager) = self.managers.lock().remove(&(topic, partition)) else {
                        error!("cannot revoke topic {topic} partition {partition}; not assigned");
                        continue;
                    };

                    let list = list.clone();
                    shutdown_futures.push(async move {
                        let Some(offset) = manager.shutdown().await else {
                            return;
                        };

                        let next_offset = Offset::Offset(offset + 1);
                        let mut list = list.lock();

                        if let Err(error) =
                            list.add_partition_offset(&topic, partition, next_offset)
                        {
                            error!("failed to add offset to commit list: {error:#}");
                        }
                    });
                }

                // Block the current thread until all shutdown tasks complete, ensuring all
                // processing has stopped before proceeding.
                Handle::current().block_on(shutdown_futures.for_each(|()| ready(())));

                let list = list.lock();
                if list.count() == 0 {
                    return;
                }

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
