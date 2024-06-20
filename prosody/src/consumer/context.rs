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
use rdkafka::ClientContext;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use tokio::runtime::Handle;
use tracing::{error, warn};

use crate::consumer::{ConsumerConfiguration, Managers, MessageHandler, WatermarkVersion};
use crate::consumer::partition::PartitionManager;
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
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        let Rebalance::Assign(partitions) = rebalance else {
            return;
        };

        // see: https://github.com/fede1024/rust-rdkafka/issues/681
        if partitions.count() == 0 {
            return;
        }

        for element in partitions.elements() {
            let topic = Topic::from(element.topic());
            let partition = element.partition();
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

    /// Manages the orderly shutdown of partition managers when partitions are
    /// revoked. This method ensures that resources are cleaned up properly,
    /// preventing data loss or inconsistency.
    ///
    /// # Arguments
    /// * `rebalance` - Details about the partition revocation event from Kafka.
    fn post_rebalance(&self, rebalance: &Rebalance) {
        let Rebalance::Revoke(partitions) = rebalance else {
            return;
        };

        // see: https://github.com/fede1024/rust-rdkafka/issues/681
        if partitions.count() == 0 {
            return;
        }

        // Concurrently shutdown all partitions. Delaying shutdown will delay
        // re-balancing and potentially lead to unacceptable processing latencies.
        let shutdown_futures = FuturesUnordered::new();
        for element in partitions.elements() {
            let topic = Topic::from(element.topic());
            let partition = element.partition();
            let Some(manager) = self.managers.lock().remove(&(topic, partition)) else {
                error!("cannot revoke topic {topic} partition {partition}; not assigned");
                continue;
            };
            shutdown_futures.push(manager.shutdown());
        }

        // Block the current thread until all shutdown tasks complete, ensuring all
        // processing has stopped before proceeding.
        Handle::current().block_on(shutdown_futures.for_each(|_| ready(())));
    }
}
